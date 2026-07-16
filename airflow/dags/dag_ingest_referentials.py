"""
dag_ingest_referentials.py

Ingestion des 4 référentiels partagés (customers, branches, products, accounts)
vers Bronze, dans l'ordre strict imposé par la cohérence référentielle.

Pour chaque référentiel : ensure_table (crée la table si absente) -> ingestion.
Même si ces 4 tâches s'exécutent déjà séquentiellement (pas de risque de race
condition de création concurrente ici), on garde le pattern ensure_table pour
la cohérence avec les DAGs de transactions et pour éviter tout raise inattendu
si la table Bronze devait être créée avec un schéma différent d'une exécution
à l'autre.

En terminant, ce DAG "publie" l'Asset bronze://referentials, ce qui déclenche
automatiquement les 4 DAGs de transactions qui en dépendent — que ce DAG ait
été lancé par le cron ou manuellement.
"""
from datetime import datetime

from airflow import DAG
from airflow.sdk import Asset

from comon.waba_common import make_spark_task, alert_on_failure, make_ensure_table_task


REFERENTIALS = ["customers", "branches", "products", "accounts"]

with DAG(
    dag_id="dag_ingest_referentials",
    description="Ingestion des référentiels (customers/branches/products/accounts) vers Bronze",
    start_date=datetime(2026, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_tasks=4,
    default_args={"on_failure_callback": alert_on_failure},
    tags=["waba", "level2", "bronze", "referentials"],
) as dag:

    for i, name in enumerate(REFERENTIALS):
        is_last = (i == len(REFERENTIALS) - 1)

       

        ingest_task = make_spark_task(
            f"ingest_{name}", name,
        )

        ingest_task