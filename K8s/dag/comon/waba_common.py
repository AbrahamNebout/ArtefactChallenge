"""
waba_common.py

Module partagé importé par tous les DAGs WABA — constantes et fabrique
de tâches Spark communes.

MIGRATION K8S : on ne lance plus les jobs via DockerOperator (conteneur
Docker local) mais via le Spark Operator Kubernetes (CRD SparkApplication).
Le spark-operator, tournant dans le cluster, crée alors un pod driver (qui
lui-même demande les pods executors), et les supprime automatiquement une
fois le job terminé (voir `timeToLiveSeconds` ci-dessous).

RÈGLE IMPORTANTE (inchangée) : aucun appel réseau/I/O au niveau module ici,
ce fichier est chargé à chaque scan du dag-processor.

IMPORTANT — modèle async : contrairement à DockerOperator qui bloquait
jusqu'à la fin du conteneur, SparkKubernetesOperator se contente de
SOUMETTRE la ressource SparkApplication et rend la main immédiatement.
Chaque fabrique retourne donc un tuple (submit_task, sensor_task) déjà
chaînés en interne (submit >> sensor). Dans le DAG, il faut brancher tes
dépendances amont sur `submit_task`, pas sur le tuple entier.

Pré-requis :
  - provider `apache-airflow-providers-cncf-kubernetes` installé
  - connexion Airflow `kubernetes_default` pointant vers le cluster
  - spark-operator déployé, CRD `sparkapplications.sparkoperator.k8s.io`
  - ServiceAccount `spark` avec les droits nécessaires (voir RBAC du
    spark-operator : create/get/list/watch/delete sur pods, services,
    configmaps dans le namespace `waba`)
  - PVC `spark-ivy-cache-pvc` existant dans le namespace `waba` (remplace
    le volume Docker `artefact_project_spark-ivy-cache`)
"""
from datetime import timedelta
import logging

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
import boto3
import yaml

from kubernetes.client import CustomObjectsApi
from kubernetes.client.exceptions import ApiException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.standard.operators.python import PythonOperator


from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)


SPARK_IMAGE = "abraneb97/spark_job_k8s:v2"
SPARK_VERSION = "3.5.0"
NAMESPACE = "default"
SERVICE_ACCOUNT = "spark"
IVY_CACHE_PVC = "spark-ivy-cache-pvc"
KUBERNETES_CONN_ID = "kubernetes_default"
TTL_SECONDS_AFTER_FINISHED = 120  # délai avant suppression auto des pods

COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]

DEFAULT_TASK_KWARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def get_common_env() -> list[dict]:
    """
    Même principe qu'avant : références Jinja vers les Airflow Variables,
    résolues à l'EXÉCUTION de la tâche (jamais au parsing). Format adapté
    au schéma K8s `env` (liste de {name, value}) au lieu d'un dict simple.
    """
    values = {
        "MINIO_ENDPOINT": "{{ var.value.waba_minio_endpoint }}",
        "MINIO_ACCESS_KEY": "{{ var.value.waba_minio_access_key }}",
        "MINIO_SECRET_KEY": "{{ var.value.waba_minio_secret_key }}",
        "ICEBERG_CATALOG_URI": "{{ var.value.waba_iceberg_catalog_uri }}",
        "AWS_REGION": "{{ var.value.waba_aws_region }}",
        "AWS_ACCESS_KEY_ID": "{{ var.value.waba_minio_access_key }}",
        "AWS_SECRET_ACCESS_KEY": "{{ var.value.waba_minio_secret_key }}",
    }
    return [{"name": k, "value": v} for k, v in values.items()]


def alert_on_failure(context):
    """Callback d'alerte après épuisement des retries (inchangé)."""
    ti = context["task_instance"]
    logical_date = context.get("logical_date", context.get("execution_date"))
    logging.error(
        "🚨 ALERTE — Échec définitif de '%s' dans '%s' (exécution: %s).",
        ti.task_id, ti.dag_id, logical_date,
    )


def cleanup_spark_application(app_name: str, **_context) -> None:
    """
    Supprime la ressource SparkApplication une fois qu'Airflow a confirmé
    le succès du job (appelé seulement après que le sensor _wait ait réussi,
    voir trigger_rule par défaut = all_success dans _submit_and_wait).
    Remplace le TTL natif du spark-operator, qui supprimait la ressource
    trop tôt et cassait le sensor (voir historique du debug K8s).
    """
    hook = KubernetesHook(conn_id=KUBERNETES_CONN_ID)
    custom_api = CustomObjectsApi(hook.get_conn())
    try:
        custom_api.delete_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="sparkapplications",
            name=app_name,
        )
        logging.info("🧹 SparkApplication '%s' supprimée après confirmation du succès.", app_name)
    except ApiException as e:
        if e.status == 404:
            logging.info("SparkApplication '%s' déjà absente, rien à faire.", app_name)
        else:
            raise



def _ivy_volume_block() -> tuple[list[dict], list[dict]]:
    """Volume + volumeMount partagés driver/executor pour le cache Ivy."""
    volume_mounts = [{"name": "ivy-cache", "mountPath": "/root/.ivy2"}]
    volumes = [{
        "name": "ivy-cache",
        "persistentVolumeClaim": {"claimName": IVY_CACHE_PVC},
    }]
    return volume_mounts, volumes

def _build_spark_application(app_name: str, main_file: str, arguments: list[str],
                              executor_instances: int = 2) -> dict:
    """Construit le spec SparkApplication commun à toutes les fabriques."""
    # Nom unique par jour d'exécution : évite les collisions entre deux
    # runs qui se chevauchent (ex. catchup sur plusieurs jours), tout en
    # gardant un nom déterministe (pas de suffixe aléatoire) pour que le
    # sensor puisse le retrouver directement sans passer par XCom.
    app_name = f"{app_name}-{{{{ ds_nodash }}}}".lower()
    volume_mounts, volumes = _ivy_volume_block()
    env = get_common_env()

    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": app_name, "namespace": NAMESPACE},
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": SPARK_IMAGE,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": f"local://{main_file}",
            "arguments": arguments,
            "sparkVersion": SPARK_VERSION,
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": TTL_SECONDS_AFTER_FINISHED,
            "volumes": volumes,
            "driver": {
                "cores": 1,
                "memory": "1g",
                "serviceAccount": SERVICE_ACCOUNT,
                "env": env,
                "volumeMounts": volume_mounts,
            },
            "executor": {
                "cores": 1,
                "instances": executor_instances,
                "memory": "1g",
                "env": env,
                "volumeMounts": volume_mounts,
            },
        },
    }

def _submit_and_wait(task_id: str, spec: dict) -> SparkKubernetesOperator:
    """
    Soumet et ATTEND la fin du job Spark en une seule tâche.
    Dans cette version du provider (10.17.1+), SparkKubernetesOperator
    surveille déjà nativement le pod driver et lève une exception en cas
    d'échec -- plus besoin d'un sensor séparé, qui arrive systématiquement
    trop tard : le spark-operator supprime la SparkApplication et son pod
    driver quasi immédiatement après la fin du job (COMPLETED ou FAILED),
    indépendamment de timeToLiveSeconds. Le sensor perdait donc toujours
    la course contre cette suppression.
    """
    app_name = spec["metadata"]["name"]

    submit = SparkKubernetesOperator(
        task_id=task_id,
        namespace=NAMESPACE,
        application_file=yaml.dump(spec),
        kubernetes_conn_id=KUBERNETES_CONN_ID,
        random_name_suffix=False,
        on_failure_callback=alert_on_failure,
        **DEFAULT_TASK_KWARGS,
    )
    return submit


def make_spark_task(task_id: str, data_type: str, country: str | None = None,
                     use_logical_date: bool = False, **_ignored):
    """
    Équivalent K8s de l'ancien make_spark_task (Docker).
    `outlets` n'est plus passé ici : ajoute-le côté DAG sur le sensor
    (submit_task, sensor_task = make_spark_task(...); sensor_task.outlets = [...])
    si tu veux garder le déclenchement par Asset — SparkKubernetesOperator
    ne supporte pas nativement `outlets` de la même façon que DockerOperator.
    """
    arguments = ["--data_type", data_type]
    if country:
        arguments += ["--country", country]
    if use_logical_date:
        arguments += ["--date", "{{ ds_nodash }}"]

    spec = _build_spark_application(
        app_name=task_id.replace("_", "-"),
        main_file="/app/ingest_raw.py",
        arguments=arguments,
    )
    return _submit_and_wait(task_id, spec)


def make_silver_task(task_id: str, data_type: str, use_logical_date: bool = False, **_ignored):
    arguments = ["--data_type", data_type]
    if use_logical_date:
        arguments += ["--date", "{{ ds_nodash }}"]

    spec = _build_spark_application(
        app_name=task_id.replace("_", "-"),
        main_file="/app/transform_silver.py",
        arguments=arguments,
    )
    return _submit_and_wait(task_id, spec)


def make_ensure_table_task(data_type: str):
    """Tâche UNIQUE et séquentielle qui crée la table Bronze si besoin."""
    task_id = f"ensure_table_{data_type}"
    spec = _build_spark_application(
        app_name=task_id.replace("_", "-"),
        main_file="/app/ingest_raw.py",
        arguments=["--data_type", data_type, "--ensure-table-only"],
        executor_instances=1,
    )
    return _submit_and_wait(task_id, spec)


def make_gold_task(task_id: str, kpi_name: str, **_ignored):
    spec = _build_spark_application(
        app_name=task_id.replace("_", "-"),
        main_file="/app/transform_gold.py",
        arguments=["--kpi", kpi_name],
    )
    return _submit_and_wait(task_id, spec)


def make_regulatory_report_task(task_id: str, use_logical_date: bool = True):
    arguments = []
    if use_logical_date:
        arguments += ["--date", "{{ ds_nodash }}"]

    spec = _build_spark_application(
        app_name=task_id.replace("_", "-"),
        main_file="/app/regulatory_report.py",
        arguments=arguments,
        executor_instances=1,
    )
    return _submit_and_wait(task_id, spec)


def check_file_exists(country: str, data_type: str, prefix: str, **context) -> None:
    """Inchangé — logique légère via API S3, indépendante du moteur d'exécution Spark."""
    date_str = context["ds_nodash"]

    s3 = boto3.client(
        "s3",
        endpoint_url=Variable.get("waba_minio_endpoint"),
        aws_access_key_id=Variable.get("waba_minio_access_key"),
        aws_secret_access_key=Variable.get("waba_minio_secret_key"),
    )
    key_prefix = f"{country}/{data_type}/{date_str}/{prefix}_{country}_{date_str}_"
    response = s3.list_objects_v2(Bucket="raw-landing", Prefix=key_prefix, MaxKeys=1)

    if response.get("KeyCount", 0) == 0:
        raise AirflowSkipException(
            f"Aucun fichier trouvé pour {country}/{data_type} le {date_str} "
            f"(prefix cherché: {key_prefix}) — pays ignoré pour ce jour (pas une erreur)."
        )


FILE_PREFIXES = {
    "bank_transactions": "bank_txn",
    "insurance_operations": "insurance_ops",
    "mobile_money": "mobile_money",
    "loan_repayments": "loan_repayments",
}