"""
dag_bronze_to_silver.py

Transforme les 4 tables Bronze en Silver : nettoyage, déduplication,
jointure avec les référentiels, conversion en EUR.

Planifié quotidiennement, à la MÊME cadence que les 4 DAGs d'ingestion
(dag_ingest_bank_transactions, dag_ingest_insurance_operations,
dag_ingest_mobile_money, dag_ingest_loan_repayments) -- son exécution
attend explicitement, via ExternalTaskSensor, que le DagRun du jour de
CHACUN de ces 4 DAGs soit terminé avec succès avant de lancer les
transformations Silver.
"""
from datetime import datetime

from airflow import DAG
from airflow.sdk import Asset
from airflow.utils.state import DagRunState
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from comon.waba_common import make_silver_task, alert_on_failure

DATA_TYPES = ["bank_transactions", "insurance_operations", "mobile_money", "loan_repayments"]
INGESTION_DAGS = [f"dag_ingest_{dt}" for dt in DATA_TYPES]


with DAG(
    dag_id="dag_bronze_to_silver",
    description="Transformation Bronze -> Silver (nettoyage, dédup, jointure, conversion EUR)",
    start_date=datetime(2026, 5, 10),
    end_date=datetime(2026, 5, 12),
    schedule="0 1 * * *",
    catchup=True,
    max_active_tasks=8,
    default_args={"on_failure_callback": alert_on_failure},
    tags=["waba", "level2", "silver"],
) as dag:

    wait_tasks = [
        ExternalTaskSensor(
            task_id=f"wait_for_{ingestion_dag}",
            external_dag_id=ingestion_dag,
            external_task_id=None,
            allowed_states=[DagRunState.SUCCESS],
            failed_states=[DagRunState.FAILED],
            timeout=60 * 60,
            poke_interval=60,
            mode="reschedule",
        )
        for ingestion_dag in INGESTION_DAGS
    ]

    silver_tasks = []
    for i, dt in enumerate(DATA_TYPES):
        silver_task = make_silver_task(
            f"silver_{dt}", dt,
            use_logical_date=True,  # plus de outlets= ici, retiré de l'appel
        )
        silver_tasks.append(silver_task)

    for wait_task in wait_tasks:
        for silver_task in silver_tasks:
            wait_task >> silver_task