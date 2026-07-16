"""
dag_silver_to_gold.py
...
"""
from datetime import datetime

from airflow import DAG
from airflow.sdk import Asset
from airflow.utils.state import DagRunState
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from comon.waba_common import make_gold_task, alert_on_failure

# Toujours publié en sortie (utile si un futur DAG venait s'y accrocher via
# Asset -- ex: dag_regulatory_report pourrait migrer vers ce pattern plus tard).

KPIS = [
    "daily_transaction_volume",
    "npl_ratio_by_country",
    "customer_arpu_monthly",
    "loss_ratio_by_product",
    "claims_processing_time",
    "mobile_money_daily_flow",
    "cross_border_transfers",
]

with DAG(
    dag_id="dag_silver_to_gold",
    description="Calcul des KPIs Gold (financiers et réglementaires) depuis Silver",
    start_date=datetime(2026, 5, 10),
    end_date=datetime(2026, 5, 12),
    schedule="0 1 * * *",
    catchup=True,
    max_active_tasks=7,
    default_args={"on_failure_callback": alert_on_failure},
    tags=["waba", "level2", "gold"],
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_dag_bronze_to_silver",
        external_dag_id="dag_bronze_to_silver",
        external_task_id=None,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        timeout=60 * 60,
        poke_interval=60,
        mode="reschedule",
    )

    for i, kpi in enumerate(KPIS):
        gold_task = make_gold_task(f"gold_{kpi}", kpi)
        wait_for_silver >> gold_task