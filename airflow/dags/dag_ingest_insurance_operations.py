"""
dag_ingest_bank_transactions.py

Ingestion quotidienne des transactions bancaires vers Bronze.
Planifié chaque jour à 01h00 UTC 
"""
from datetime import datetime
from datetime import timedelta
from airflow.sdk import Asset
from airflow import DAG
from airflow.models import Param
from airflow.providers.standard.operators.python import (
    ShortCircuitOperator, PythonOperator,
)

from comon.waba_common import (
    make_spark_task, alert_on_failure, check_file_exists,
    COUNTRIES, FILE_PREFIXES,make_ensure_table_task
)

default_args = {
    "on_failure_callback": alert_on_failure,  
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}


RETRY_KWARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}



DATA_TYPE = "insurance_operations"

def check_country(country: str, **context) -> bool:
    selected = context["params"].get("country_codes", [])
    return not selected or country in selected


with DAG(
    dag_id=f"dag_ingest_{DATA_TYPE}",
    description=f"Ingestion quotidienne de {DATA_TYPE} vers Bronze (traite J-1)",
    start_date=datetime(2026, 5, 10),
    end_date=datetime(2026, 5, 12), 
    schedule="0 1 * * *",
    catchup=True,
    max_active_runs=2,
    max_active_tasks=8,
    default_args=default_args,
    tags=["waba", "level2", "bronze", DATA_TYPE],
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
            **RETRY_KWARGS,
        )

        check_file = PythonOperator(
            task_id=f"check_file_{country}",
            python_callable=check_file_exists,
            op_kwargs={
                "country": country,
                "data_type": DATA_TYPE,
                "prefix": FILE_PREFIXES[DATA_TYPE],
            },
            **RETRY_KWARGS,
        )

        txn_task = make_spark_task(
            f"ingest_{DATA_TYPE}_{country}", DATA_TYPE, country,
            use_logical_date=True,
        )

        gate >> check_file >> txn_task

