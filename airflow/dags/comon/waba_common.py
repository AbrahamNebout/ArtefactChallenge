"""
waba_common.py

Module partagé importé par tous les DAGs WABA — constantes et fabrique
de tâches Spark communes. RÈGLE IMPORTANTE : aucun appel réseau/I/O au
niveau module ici, car ce fichier est chargé à chaque scan du dag-processor
(même contrainte que pour un DAG classique — voir le bug DagBag timeout
qu'on a corrigé précédemment).
"""
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
import boto3
from datetime import timedelta
import logging

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

SPARK_IMAGE = "sparkjob:latest"
IVY_CACHE_VOLUME = "artefact_project_spark-ivy-cache"

COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]

DEFAULT_TASK_KWARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def get_common_env() -> dict:
    """
    Références Jinja vers les Airflow Variables — résolues à l'EXÉCUTION
    de la tâche, jamais au parsing (sinon: DagBag import timeout, déjà vu).
    """
    return {
        "MINIO_ENDPOINT": "{{ var.value.waba_minio_endpoint }}",
        "MINIO_ACCESS_KEY": "{{ var.value.waba_minio_access_key }}",
        "MINIO_SECRET_KEY": "{{ var.value.waba_minio_secret_key }}",
        "ICEBERG_CATALOG_URI": "{{ var.value.waba_iceberg_catalog_uri }}",
        "AWS_REGION": "{{ var.value.waba_aws_region }}",
        "AWS_ACCESS_KEY_ID": "{{ var.value.waba_minio_access_key }}",
        "AWS_SECRET_ACCESS_KEY": "{{ var.value.waba_minio_secret_key }}",
    }


def alert_on_failure(context):
    """Callback d'alerte après épuisement des retries (point d'extension Slack/email)."""
    ti = context["task_instance"]
    logical_date = context.get("logical_date", context.get("execution_date"))
    logging.error(
        "🚨 ALERTE — Échec définitif de '%s' dans '%s' (exécution: %s).",
        ti.task_id, ti.dag_id, logical_date,
    )



def make_spark_task(task_id: str, data_type: str, country: str | None = None,
                     outlets: list | None = None, use_logical_date: bool = False) -> DockerOperator:
    """
    Construit une tâche DockerOperator qui lance spark-submit ingest_raw.py.
    use_logical_date=True ajoute --date {{ ds_nodash }} : Airflow résout ce
    template à l'exécution avec la date de l'intervalle traité (J-1 pour un
    DAG planifié quotidiennement), garantissant qu'on traite bien "hier"
    sans calcul manuel de date.
    """
    command = ["spark-submit", "/app/ingest_raw.py", "--data_type", data_type]
    if country:
        command += ["--country", country]
    if use_logical_date:
        command += ["--date", "{{ ds_nodash }}"]

    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=command,
        docker_url="unix://var/run/docker.sock",
        network_mode="waba-network",
        auto_remove="success",
        environment=get_common_env(),
        mounts=[Mount(source=IVY_CACHE_VOLUME, target="/root/.ivy2", type="volume")],
        mount_tmp_dir=False,
        outlets=outlets or [],
        on_failure_callback=alert_on_failure,
        **DEFAULT_TASK_KWARGS,
    )

def check_file_exists(country: str, data_type: str, prefix: str, **context) -> None:
    """
    Vérifie via l'API S3 (léger, pas besoin de Spark) qu'au moins un fichier
    existe pour ce pays/jour avant de lancer l'ingestion. Si absent, la tâche
    est marquée `skipped` (pas `failed`) : un pays sans données un jour donné
    est une situation normale et tolérable, pas une erreur. La tâche
    d'ingestion en aval devient automatiquement `skipped` en cascade (trigger
    rule par défaut `all_success`), et surtout : un DagRun dont les seules
    tâches non-`success` sont `skipped` reste globalement `success` -- ce qui
    permet à dag_bronze_to_silver (ExternalTaskSensor sur DagRunState.SUCCESS)
    de continuer même si certains pays n'avaient rien à ingérer ce jour-là.
    """
    date_str = context["ds_nodash"]  # résolu à l'exécution, pas au parsing

    s3 = boto3.client(
        "s3",
        endpoint_url=Variable.get("waba_minio_endpoint"),
        aws_access_key_id=Variable.get("waba_minio_access_key"),
        aws_secret_access_key=Variable.get("waba_minio_secret_key"),
    )
    # Sous-dossier jour (date_str/) inséré ici -- app.py range désormais les
    # fichiers en pays/type/JOUR/fichier.csv (introduit pour le routage NiFi
    # au Level 3, mais qui s'applique dès la génération -> même correction
    # que dans ingest_raw.py).
    key_prefix = f"{country}/{data_type}/{date_str}/{prefix}_{country}_{date_str}_"
    response = s3.list_objects_v2(Bucket="raw-landing", Prefix=key_prefix, MaxKeys=1)

    if response.get("KeyCount", 0) == 0:
        raise AirflowSkipException(
            f"Aucun fichier trouvé pour {country}/{data_type} le {date_str} "
            f"(prefix cherché: {key_prefix}) — pays ignoré pour ce jour (pas une erreur)."
        )
    


def make_silver_task(task_id: str, data_type: str, outlets: list | None = None,
                      use_logical_date: bool = False) -> DockerOperator:
    command = ["spark-submit", "/app/transform_silver.py", "--data_type", data_type]
    if use_logical_date:
        command += ["--date", "{{ ds_nodash }}"]

    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=command,
        docker_url="unix://var/run/docker.sock",
        network_mode="waba-network",
        auto_remove="success",
        environment=get_common_env(),
        mounts=[Mount(source=IVY_CACHE_VOLUME, target="/root/.ivy2", type="volume")],
        mount_tmp_dir=False,
        outlets=outlets or [],
        on_failure_callback=alert_on_failure,
        **DEFAULT_TASK_KWARGS,
    )


def make_ensure_table_task(data_type: str) -> DockerOperator:
    """Tâche UNIQUE et séquentielle qui crée la table Bronze si besoin,
    avant que les tâches pays parallèles ne fassent leurs MERGE."""
    return DockerOperator(
        task_id=f"ensure_table_{data_type}",
        image=SPARK_IMAGE,
        command=["spark-submit", "/app/ingest_raw.py",
                 "--data_type", data_type, "--ensure-table-only"],
        docker_url="unix://var/run/docker.sock",
        network_mode="waba-network",
        auto_remove="success",
        environment=get_common_env(),
        mounts=[Mount(source=IVY_CACHE_VOLUME, target="/root/.ivy2", type="volume")],
        mount_tmp_dir=False,
        on_failure_callback=alert_on_failure,
        **DEFAULT_TASK_KWARGS,
    )


def make_gold_task(task_id: str, kpi_name: str, outlets: list | None = None) -> DockerOperator:
    """Construit une tâche DockerOperator qui lance spark-submit transform_gold.py
    pour un KPI donné (un job par table Gold, indépendants entre eux)."""
    command = ["spark-submit", "/app/transform_gold.py", "--kpi", kpi_name]

    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=command,
        docker_url="unix://var/run/docker.sock",
        network_mode="waba-network",
        auto_remove="success",
        environment=get_common_env(),
        mounts=[Mount(source=IVY_CACHE_VOLUME, target="/root/.ivy2", type="volume")],
        mount_tmp_dir=False,
        outlets=outlets or [],
        on_failure_callback=alert_on_failure,
        **DEFAULT_TASK_KWARGS,
    )


def make_regulatory_report_task(task_id: str, use_logical_date: bool = True) -> DockerOperator:
    """
    Tâche du rapport réglementaire quotidien (dag_regulatory_report).
    Contrairement à make_gold_task, celle-ci est cron+catchup (pas Asset)
    -> {{ ds_nodash }} correspond réellement au jour à traiter, l'usage de
    use_logical_date est donc légitime ici.
    """
    command = ["spark-submit", "/app/regulatory_report.py"]
    if use_logical_date:
        command += ["--date", "{{ ds_nodash }}"]

    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=command,
        docker_url="unix://var/run/docker.sock",
        network_mode="waba-network",
        auto_remove="success",
        environment=get_common_env(),
        mounts=[Mount(source=IVY_CACHE_VOLUME, target="/root/.ivy2", type="volume")],
        mount_tmp_dir=False,
        on_failure_callback=alert_on_failure,
        **DEFAULT_TASK_KWARGS,
    )


FILE_PREFIXES = {
    "bank_transactions": "bank_txn",
    "insurance_operations": "insurance_ops",
    "mobile_money": "mobile_money",
    "loan_repayments": "loan_repayments",
}