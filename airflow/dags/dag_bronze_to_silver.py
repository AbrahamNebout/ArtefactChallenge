"""
dag_bronze_to_silver.py
Transforme les 4 tables Bronze en Silver : nettoyage, déduplication,
jointure avec les référentiels, conversion en EUR.
"""
from datetime import datetime

from airflow import DAG
from airflow.sdk import Asset
from airflow.utils.state import DagRunState
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from comon.waba_common import make_silver_task, alert_on_failure

DATA_TYPES = ["bank_transactions", "insurance_operations", "mobile_money", "loan_repayments"]
INGESTION_DAGS = [f"dag_ingest_{dt}" for dt in DATA_TYPES]

# Toujours publié en sortie, pour ne pas casser dag_silver_to_gold (qui reste
# pour l'instant déclenché par Asset -- à revoir quand on arrivera à son tour
# dans la revue étape par étape).
SILVER_READY = Asset("silver://all")

with DAG(
    dag_id="dag_bronze_to_silver",
    description="Transformation Bronze -> Silver (nettoyage, dédup, jointure, conversion EUR)",
    start_date=datetime(2026, 5, 10),
    end_date=datetime(2026, 5, 12),    # même période simulée que les DAGs d'ingestion
    schedule="0 1 * * *",            # même cadence que les 4 DAGs d'ingestion -> logical_date identiques
    catchup=True,
    max_active_tasks=8,
    default_args={"on_failure_callback": alert_on_failure},
    tags=["waba", "level2", "silver"],
) as dag:

    wait_tasks = [
        ExternalTaskSensor(
            task_id=f"wait_for_{ingestion_dag}",
            external_dag_id=ingestion_dag,
            external_task_id=None,   # surveille l'état du DagRun entier, pas une tâche précise
            allowed_states=[DagRunState.SUCCESS],
            failed_states=[DagRunState.FAILED],
            timeout=60 * 60,         # 1h de patience max, puis échec plutôt qu'attente indéfinie
            poke_interval=60,
            mode="reschedule",       # libère le slot worker pendant l'attente (pas de poke bloquant)
        )
        for ingestion_dag in INGESTION_DAGS
    ]

    silver_tasks = [
        make_silver_task(
            f"silver_{dt}", dt,
            outlets=[SILVER_READY] if i == len(DATA_TYPES) - 1 else None,
            use_logical_date=True,   # logical_date à nouveau fiable (cron, plus Asset) -> on peut cibler le jour explicitement
        )
        for i, dt in enumerate(DATA_TYPES)
    ]

    for wait_task in wait_tasks:
        for silver_task in silver_tasks:
            wait_task >> silver_task