"""
transform_silver.py

Transforme les tables Bronze en tables Silver : dédoublonnage, jointure
avec les référentiels, conversion des montants en EUR, gestion des nulls.
Traitement incrémental jour par jour (cohérent avec l'ingestion Bronze).

Usage: spark-submit transform_silver.py --data_type bank_transactions --date 20260705
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Taux de conversion fixes vers EUR (hypothèse documentée : XOF est
# indexé sur l'EUR via un taux fixe historique ; GHS est une approximation
# raisonnable pour la démo, à remplacer par un flux de taux réel en prod) ---
EXCHANGE_RATES_TO_EUR = {
    "XOF": 1 / 655.957,
    "GHS": 1 / 13.5,
}

JOIN_SPECS = {
    "bank_transactions": ["customers", "accounts", "branches"],
    "insurance_operations": ["customers", "accounts"],
    "mobile_money": ["customers"],
    "loan_repayments": ["customers", "accounts"],
}

DEDUP_KEYS = {
    "bank_transactions": "transaction_id",
    "insurance_operations": "operation_id",
    "mobile_money": "payment_id",
    "loan_repayments": "repayment_id",
}

AMOUNT_COLUMNS = {
    "bank_transactions": ["amount", "fee_amount"],
    "insurance_operations": ["amount"],
    "mobile_money": ["amount", "fee_amount"],
    "loan_repayments": ["amount_due", "amount_paid"],
}


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("waba-transform-silver")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.lakehouse.uri", os.environ["ICEBERG_CATALOG_URI"])
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://lakehouse/")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", os.environ["MINIO_ENDPOINT"])
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.client.region", "us-east-1")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", os.environ["MINIO_ACCESS_KEY"])
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", os.environ["MINIO_SECRET_KEY"])
        .getOrCreate()
    )


def add_eur_columns(df, amount_cols: list):
    """Ajoute une colonne _eur pour chaque colonne de montant, selon la devise."""
    rate_expr = F.create_map(*[
        item for cur, rate in EXCHANGE_RATES_TO_EUR.items() for item in (F.lit(cur), F.lit(rate))
    ])
    for col in amount_cols:
        df = df.withColumn(f"{col}_eur",
                            F.round(F.col(col) * rate_expr[F.col("currency")], 2))
    return df


def clean_and_dedup(df, dedup_key: str):
    """
    Déduplication sur la clé métier (en gardant la ligne la plus récente par
    timestamp d'ingestion) — gère les doublons à l'intérieur d'un même batch,
    en complément du MERGE déjà fait en Bronze (qui gère les doublons entre
    exécutions successives).
    """
    from pyspark.sql import Window
    window = Window.partitionBy(dedup_key).orderBy(F.col("timestamp").desc())
    df = (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return df


def get_pending_dates(spark: SparkSession, data_type: str) -> list[str]:
    """
    Détermine les dates métier (YYYYMMDD) présentes en Bronze mais pas encore
    reflétées en Silver, en comparant MAX(date métier) déjà présent en Silver
    aux dates distinctes disponibles en Bronze.

    Volontairement DÉCOUPLÉ du logical_date Airflow : ce DAG est déclenché
    par Asset (AssetAll), donc son logical_date correspond au moment du
    déclenchement, pas à une date métier particulière. On ne peut donc pas
    se fier à {{ ds_nodash }} pour savoir quel jour traiter — on le déduit
    directement des données.
    """
    bronze_table = f"lakehouse.bronze.{data_type}"
    silver_table = f"lakehouse.silver.{data_type}"
    date_col = "due_date" if data_type == "loan_repayments" else "timestamp"

    if not spark.catalog.tableExists(bronze_table):
        print(f"ℹ️  Table {bronze_table} inexistante — rien à traiter.")
        return []

    max_date = None
    if spark.catalog.tableExists(silver_table):
        row = spark.table(silver_table).agg(F.max(F.to_date(F.col(date_col)))).collect()[0]
        max_date = row[0]

    bronze_dates_df = (
        spark.table(bronze_table)
        .select(F.to_date(F.col(date_col)).alias("d"))
        .filter(F.col("d").isNotNull())
        .distinct()
    )
    if max_date is not None:
        bronze_dates_df = bronze_dates_df.filter(F.col("d") > F.lit(max_date))

    rows = bronze_dates_df.orderBy("d").collect()
    pending = [r["d"].strftime("%Y%m%d") for r in rows]

    if pending:
        print(f"📋 {len(pending)} nouvelle(s) date(s) à traiter pour {data_type}: {pending}")
    else:
        print(f"ℹ️  Aucune nouvelle date à traiter pour {data_type} "
              f"(dernier jour déjà en Silver : {max_date}).")
    return pending


def transform(spark: SparkSession, data_type: str, date_str: str | None = None):
    """
    Transforme une table Bronze en Silver : filtre sur un jour précis (si fourni),
    déduplique, nettoie les valeurs aberrantes, convertit les montants en EUR,
    enrichit via jointure avec les référentiels, puis MERGE dans la table Silver.
    """
    bronze_table = f"lakehouse.bronze.{data_type}"
    silver_table = f"lakehouse.silver.{data_type}"
    dedup_key = DEDUP_KEYS[data_type]
    date_col = "due_date" if data_type == "loan_repayments" else "timestamp"

    df = spark.table(bronze_table)

    # --- 0. Filtre sur un seul jour, cohérent avec l'ingestion incrémentale ---
    if date_str:
        from datetime import datetime as dt
        target_date = dt.strptime(date_str, "%Y%m%d").date()
        df = df.filter(F.to_date(F.col(date_col)) == F.lit(target_date))
        n_raw = df.count()
        print(f"📅 Traitement du jour {target_date} uniquement ({n_raw} lignes brutes)")
        if n_raw == 0:
            print(f"ℹ️  Aucune donnée Bronze pour {data_type} le {target_date} — rien à transformer.")
            return

    # --- 1. Déduplication sur la clé métier ---
    df = clean_and_dedup(df, dedup_key)

    # --- 2. Gestion des valeurs nulles / aberrantes sur les colonnes de montant ---
    for col in AMOUNT_COLUMNS[data_type]:
        if col in df.columns:
            df = df.withColumn(col, F.when(F.col(col) < 0, None).otherwise(F.col(col)))
    main_amount_col = AMOUNT_COLUMNS[data_type][0]
    df = df.filter(F.col(main_amount_col).isNotNull())

    # --- 3. Conversion des montants en EUR ---
    df = add_eur_columns(df, AMOUNT_COLUMNS[data_type])

    # --- 4. Jointure avec les référentiels ---
    # Important : "accounts" est joint AVANT "customers". Certains types de
    # données (bank_transactions) ne portent ni customer_id ni sender_id,
    # seulement account_id -> il faut d'abord remonter le customer_id via
    # accounts.customer_id avant de pouvoir joindre customers.
    if "accounts" in JOIN_SPECS[data_type]:
        accounts = spark.table("lakehouse.bronze.accounts").select(
            F.col("account_id").alias("_acc_id"),
            F.col("customer_id").alias("_acc_customer_id"),
            F.col("account_type"),
            F.col("status").alias("account_status"),
        )
        join_col = "account_id" if "account_id" in df.columns else "loan_account_id"
        df = df.join(accounts, df[join_col] == accounts["_acc_id"], "left") \
               .drop("_acc_id")

        if "_acc_customer_id" in df.columns:
            if "customer_id" not in df.columns:
                # pas de customer_id direct (ex: bank_transactions) -> on
                # utilise celui remonté via accounts
                df = df.withColumnRenamed("_acc_customer_id", "customer_id")
            else:
                # customer_id déjà présent directement, colonne redondante
                df = df.drop("_acc_customer_id")

    if "customers" in JOIN_SPECS[data_type]:
        customers = spark.table("lakehouse.bronze.customers").select(
            F.col("customer_id").alias("_cust_id"),
            F.col("segment"),
            F.col("kyc_level").alias("customer_kyc_level"),
        )
        if "customer_id" in df.columns:
            join_col = "customer_id"
        elif "sender_id" in df.columns:
            join_col = "sender_id"
        else:
            raise ValueError(
                f"Impossible de déterminer la clé de jointure client pour "
                f"{data_type} (colonnes disponibles : {df.columns})"
            )
        df = df.join(customers, df[join_col] == customers["_cust_id"], "left") \
               .drop("_cust_id")

    if "branches" in JOIN_SPECS[data_type]:
        branches = spark.table("lakehouse.bronze.branches").select(
            F.col("branch_id").alias("_branch_id"),
            F.col("branch_type"),
            F.col("region").alias("branch_region"),
        )
        df = df.join(branches, df["branch_id"] == branches["_branch_id"], "left") \
               .drop("_branch_id")

    # --- 5. Création de la table Silver si besoin (partitionnée pays + jour) ---
    if not spark.catalog.tableExists(silver_table):
        try:
            (
                df.limit(0)
                .writeTo(silver_table)
                .using("iceberg")
                .partitionedBy("country_code", F.days(date_col))
                .createOrReplace()
            )
            print(f"✅ Table {silver_table} créée.")
        except Exception as e:
            if "ALREADY_EXISTS" in str(e).upper():
                print(f"ℹ️  Table {silver_table} déjà créée par un job concurrent — on continue.")
            else:
                raise

    # --- 6. Upsert idempotent ---
    df.createOrReplaceTempView("source_silver")
    spark.sql(f"""
        MERGE INTO {silver_table} t
        USING source_silver s
        ON t.{dedup_key} = s.{dedup_key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    n_final = df.count()
    print(f"✅ {n_final} lignes traitées pour {silver_table}"
          + (f" (jour {date_str})" if date_str else " (table complète)"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_type", required=True, choices=list(DEDUP_KEYS.keys()))
    parser.add_argument("--date", default=None,
                         help="Jour YYYYMMDD à transformer explicitement (utile pour un rejeu "
                              "manuel/backfill ciblé). Si omis, le script détecte lui-même les "
                              "dates non encore traitées (mode incrémental automatique, "
                              "recommandé pour l'usage régulier déclenché par Airflow/Asset).")
    args = parser.parse_args()

    spark = get_spark_session()

    if args.date:
        # Rejeu explicite d'un jour précis (backfill manuel)
        transform(spark, args.data_type, args.date)
    else:
        # Mode incrémental auto : on ne dépend plus du logical_date du DAG
        # (qui, pour un DAG déclenché par Asset, ne correspond à aucune
        # date métier réelle) — on traite tout ce qui est en attente.
        pending_dates = get_pending_dates(spark, args.data_type)
        for d in pending_dates:
            transform(spark, args.data_type, d)

    spark.stop()


if __name__ == "__main__":
    main()