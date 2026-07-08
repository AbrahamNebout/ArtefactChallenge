"""
dag_silver_to_gold.py

Calcule les 7 KPIs Gold (financiers et réglementaires) à partir des tables
Silver. Planifié quotidiennement, à la MÊME cadence que dag_bronze_to_silver
(et donc que les 4 DAGs d'ingestion) -- attend explicitement, via
ExternalTaskSensor, que le DagRun du jour de dag_bronze_to_silver soit
terminé avec succès avant de lancer les 7 calculs Gold.

Remplace l'ancien déclenchement par Asset (silver://all) : plus cohérent
maintenant que dag_bronze_to_silver lui-même est passé d'un déclenchement
par Asset à un cron quotidien + ExternalTaskSensor sur les 4 DAGs
d'ingestion (voir dag_bronze_to_silver.py -- même logique, un niveau plus
loin dans le pipeline).

Important : chaque KPI garde sa propre logique de détection des périodes en
attente (get_new_periods() dans transform_gold.py, volontairement
indépendante du logical_date Airflow) -- ça reste pertinent ici, contrairement
à dag_bronze_to_silver, parce que les 7 KPIs Gold ont des granularités
différentes (jour / semaine / mois). Un simple --date ne suffirait pas pour
les KPIs à grain semaine/mois (customer_arpu_monthly, loss_ratio_by_product,
claims_processing_time, cross_border_transfers) -- seul le DÉCLENCHEMENT du
DAG change ici, pas la logique interne des KPIs.
"""
from datetime import datetime

from airflow import DAG
from airflow.sdk import Asset
from airflow.utils.state import DagRunState
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from comon.waba_common import make_gold_task, alert_on_failure

# Toujours publié en sortie (utile si un futur DAG venait s'y accrocher via
# Asset -- ex: dag_regulatory_report pourrait migrer vers ce pattern plus tard).
GOLD_READY = Asset("gold://all")

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
    start_date=datetime(2026, 4, 10),
    end_date=datetime(2026, 4, 12),   # même période simulée que les DAGs en amont
    schedule="0 1 * * *",            # même cadence que dag_bronze_to_silver -> logical_date identiques
    catchup=True,
    max_active_tasks=7,
    default_args={"on_failure_callback": alert_on_failure},
    tags=["waba", "level2", "gold"],
) as dag:

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_dag_bronze_to_silver",
        external_dag_id="dag_bronze_to_silver",
        external_task_id=None,   # surveille l'état du DagRun entier
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        timeout=60 * 60,
        poke_interval=60,
        mode="reschedule",
    )

    for i, kpi in enumerate(KPIS):
        gold_task = make_gold_task(
            f"gold_{kpi}", kpi,
            outlets=[GOLD_READY] if i == len(KPIS) - 1 else None,
        )
        wait_for_silver >> gold_task