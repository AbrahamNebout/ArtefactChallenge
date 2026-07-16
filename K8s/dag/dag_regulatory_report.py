from datetime import datetime

from airflow import DAG

from comon.waba_common import make_regulatory_report_task, alert_on_failure



with DAG(
    dag_id="dag_regulatory_report",
    description="Génère le rapport réglementaire quotidien BCEAO/CIMA (NPL + loss ratio)",
    start_date=datetime(2026, 5, 10),
    end_date=datetime(2026, 5, 12), 
    schedule="30 0 * * *",
    catchup=True,
    max_active_runs=1,
    default_args={"on_failure_callback": alert_on_failure},
    tags=["waba", "level2", "regulatory"],
) as dag:

    report_task = make_regulatory_report_task("generate_regulatory_report")