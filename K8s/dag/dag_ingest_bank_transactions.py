"""
dag_ingest_bank_transactions.py

Ingestion quotidienne des transactions bancaires vers Bronze — version K8s
(Spark Operator). Planifié chaque jour à 01h00 UTC : le "logical_date" de
chaque exécution correspond automatiquement à la VEILLE (J-1).

Pour chaque pays : gate (sélection) -> check_file (vérifie la présence du
CSV du jour) -> submit (soumet le SparkApplication) -> wait (sensor qui
attend la fin réelle du job dans le pod driver K8s, supprimé automatiquement
après TTL_SECONDS_AFTER_FINISHED).

Rattrapage sélectif : déclenchement manuel avec un paramètre country_codes.
"""
from datetime import datetime
from airflow.sdk import Asset
from airflow import DAG
from airflow.models import Param
from airflow.providers.standard.operators.python import (
    ShortCircuitOperator, PythonOperator,
)

from comon.waba_common import (
    make_spark_task, alert_on_failure, check_file_exists,
    COUNTRIES, FILE_PREFIXES,
)

DATA_TYPE = "bank_transactions"
BRONZE_OUTPUT = Asset(f"bronze://{DATA_TYPE}")


def check_country(country: str, **context) -> bool:
    selected = context["params"].get("country_codes", [])
    return not selected or country in selected


with DAG(
    dag_id=f"dag_ingest_{DATA_TYPE}",
    description=f"Ingestion quotidienne de {DATA_TYPE} vers Bronze (traite J-1) — K8s",
    start_date=datetime(2026, 5, 10),
    end_date=datetime(2026, 5, 12),
    schedule="0 1 * * *",
    catchup=True,
    max_active_runs=2,
    max_active_tasks=8,
    default_args={"on_failure_callback": alert_on_failure},
    tags=["waba", "level2", "bronze", DATA_TYPE, "k8s"],
    params={
        "country_codes": Param(
            default=[], type="array",
            items={"type": "string", "enum": COUNTRIES},
            description="Pays à traiter pour un rattrapage sélectif "
                         "(liste vide = tous les pays).",
        ),
    },
) as dag:

    for country in COUNTRIES:
        gate = ShortCircuitOperator(
            task_id=f"gate_{country}",
            python_callable=check_country,
            op_kwargs={"country": country},
        )

        check_file = PythonOperator(
            task_id=f"check_file_{country}",
            python_callable=check_file_exists,
            op_kwargs={
                "country": country,
                "data_type": DATA_TYPE,
                "prefix": FILE_PREFIXES[DATA_TYPE],
            },
        )

        # make_spark_task retourne maintenant DEUX tâches déjà chaînées
        # (submit >> wait). On branche l'amont sur `submit`.
        submit = make_spark_task(
            f"ingest_{DATA_TYPE}_{country}", DATA_TYPE, country,
            use_logical_date=True,
        )

        gate >> check_file >> submit