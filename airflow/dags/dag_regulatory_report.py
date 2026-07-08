"""
dag_regulatory_report.py

Génère quotidiennement le rapport réglementaire consolidé BCEAO/CIMA
(NPL par pays/type de prêt + loss ratio par pays/produit), à partir des
tables Gold déjà calculées par dag_silver_to_gold.

Planifié à J+1 00h30 UTC : le "logical_date" de chaque exécution correspond
à la VEILLE (J), comme pour les DAGs d'ingestion Bronze. Contrairement à
dag_bronze_to_silver et dag_silver_to_gold (déclenchés par Asset), celui-ci
est un cron classique avec catchup -> {{ ds_nodash }} est ici fiable et
correspond bien au jour réellement à traiter.
"""
from datetime import datetime

from airflow import DAG

from comon.waba_common import make_regulatory_report_task, alert_on_failure

with DAG(
    dag_id="dag_regulatory_report",
    description="Génère le rapport réglementaire quotidien BCEAO/CIMA (NPL + loss ratio)",
    start_date=datetime(2026, 4, 10),
    end_date=datetime(2026, 4, 12),
    schedule="30 0 * * *",
    catchup=True,
    max_active_runs=1,  # un seul rapport à la fois : pas de risque de conflit sur le DELETE+INSERT
    default_args={"on_failure_callback": alert_on_failure},
    tags=["waba", "level2", "regulatory"],
) as dag:

    make_regulatory_report_task("generate_regulatory_report")