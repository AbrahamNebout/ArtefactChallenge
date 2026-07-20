"""
transform_gold.py

Calcule les 7 tables Gold définies au
cahier des charges, à partir des tables Silver.


Cas particulier : gold.npl_ratio_by_country est un indicateur de STOCK
(encours en cours, pas un flux du jour) -> à chaque nouvelle date détectée,
on recalcule le ratio sur l'ENSEMBLE de l'encours dû jusqu'à cette date,
pas uniquement sur les lignes de ce jour-là.

Usage: spark-submit transform_gold.py --kpi daily_transaction_volume
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


LOAN_DEFAULT_STATUSES = {"DEFAULT"}                            # loan_repayments.repayment_status
INSURANCE_PREMIUM_TYPE = "PREMIUM_PAYMENT"                      # insurance_operations.operation_type
INSURANCE_CLAIM_PAYMENT_TYPE = "CLAIM_PAYMENT"                  # insurance_operations.operation_type
INSURANCE_CLAIM_PAID_STATUS = "PAID"                            # insurance_operations.claim_status
MOBILE_MONEY_FAILED_STATUS = "FAILED"                           # mobile_money.status
UEMOA_COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ"]    # Ghana (GH) hors zone UEMOA


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("waba-transform-gold")
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



def get_new_periods(spark: SparkSession, source_table: str, gold_table: str,
                     date_col: str, granularity: str) -> list:
    """
    Renvoie la liste des périodes (jour / semaine / mois selon `granularity`)
    présentes dans `source_table` mais postérieures à la dernière période déjà
    agrégée dans `gold_table` (colonne "period"). Même logique que
    get_pending_dates() dans transform_silver.py, généralisée à toute
    granularité de KPI.
    """
    if not spark.catalog.tableExists(source_table):
        return []

    if granularity == "day":
        period_expr = F.to_date(F.col(date_col))
    elif granularity == "week":
        period_expr = F.date_trunc("week", F.col(date_col)).cast("date")
    elif granularity == "month":
        period_expr = F.trunc(F.col(date_col), "month")
    else:
        raise ValueError(f"Granularité inconnue: {granularity}")

    max_period = None
    if spark.catalog.tableExists(gold_table):
        row = spark.table(gold_table).agg(F.max(F.col("period"))).collect()[0]
        max_period = row[0]

    periods_df = (
        spark.table(source_table)
        .select(period_expr.alias("period"))
        .filter(F.col("period").isNotNull())
        .distinct()
    )
    if max_period is not None:
        periods_df = periods_df.filter(F.col("period") > F.lit(max_period))

    return [r["period"] for r in periods_df.orderBy("period").collect()]


def merge_gold(spark: SparkSession, df, gold_table: str, merge_keys: list):
    """
    Upsert générique dans une table Gold (création si absente). Les tables
    Gold sont petites et agrégées, requêtées principalement filtrées par
    country_code -> pas besoin de partitionnement physique ici.
    """
    if not spark.catalog.tableExists(gold_table):
        df.limit(0).writeTo(gold_table).using("iceberg").createOrReplace()
        print(f"✅ Table {gold_table} créée.")

    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    df.createOrReplaceTempView("source_gold")
    spark.sql(f"""
        MERGE INTO {gold_table} t
        USING source_gold s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    n = df.count()
    print(f"✅ {n} ligne(s) upsertée(s) dans {gold_table}")


# ============================================================================
# KPIs Bancaires
# ============================================================================

def build_daily_transaction_volume(spark: SparkSession, new_days: list):
    """Volume et montant total des transactions par jour, pays, entité et type."""
    df = spark.table("lakehouse.silver.bank_transactions")
    df = df.filter(F.to_date(F.col("timestamp")).isin(new_days))

    result = (
        df.groupBy(
            F.to_date(F.col("timestamp")).alias("period"),
            F.col("country_code"),
            F.col("entity_type"),
            F.col("transaction_type"),
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount_eur"), 2).alias("total_amount_eur"),
        )
    )
    return result, ["period", "country_code", "entity_type", "transaction_type"]


def build_npl_ratio_by_country(spark: SparkSession, new_days: list):
    """
    Taux de créances douteuses (NPL) par pays et type de prêt.
    Indicateur de STOCK : pour chaque date, on recalcule le ratio sur la
    totalité de l'encours dû jusqu'à cette date (due_date <= date), pas
    seulement sur les nouvelles lignes du jour.
    """
    base = spark.table("lakehouse.silver.loan_repayments")

    snapshots = []
    for d in new_days:
        snap = (
            base.filter(F.col("due_date") <= F.lit(d))
            .groupBy("country_code", "loan_type")
            .agg(
                F.sum(
                    F.when(F.col("repayment_status").isin(list(LOAN_DEFAULT_STATUSES)),
                           F.col("amount_due_eur")).otherwise(0.0)
                ).alias("encours_defaut_eur"),
                F.sum("amount_due_eur").alias("encours_total_eur"),
            )
            .withColumn("period", F.lit(d))
        )
        snapshots.append(snap)

    result = snapshots[0]
    for s in snapshots[1:]:
        result = result.unionByName(s)

    result = (
        result
        .withColumn("npl_ratio", F.round(F.col("encours_defaut_eur") / F.col("encours_total_eur"), 4))
        .select("period", "country_code", "loan_type", "npl_ratio", "encours_defaut_eur", "encours_total_eur")
    )
    return result, ["period", "country_code", "loan_type"]


def build_customer_arpu_monthly(spark: SparkSession, new_months: list):
    """
    ARPU mensuel par pays et segment client.
    Formule : Somme(commissions + intérêts perçus) / COUNT(DISTINCT customer_id)
    Commissions = fee_amount_eur (bank_transactions).
    Intérêts perçus = amount_paid_eur (loan_repayments), approximation
    documentée en l'absence d'une colonne "intérêts" dédiée dans le générateur.
    """
    bank = spark.table("lakehouse.silver.bank_transactions")
    loans = spark.table("lakehouse.silver.loan_repayments")

    bank_m = (
        bank.withColumn("month", F.trunc(F.col("timestamp"), "month"))
        .filter(F.col("month").isin(new_months))
        .groupBy("month", "country_code", "segment", "customer_id")
        .agg(F.sum(F.coalesce(F.col("fee_amount_eur"), F.lit(0.0))).alias("commissions_eur"))
    )
    loans_m = (
        loans.withColumn("month", F.trunc(F.col("due_date"), "month"))
        .filter(F.col("month").isin(new_months))
        .groupBy("month", "country_code", "segment", "customer_id")
        .agg(F.sum(F.coalesce(F.col("amount_paid_eur"), F.lit(0.0))).alias("interets_eur"))
    )

    combined = (
        bank_m.join(loans_m, ["month", "country_code", "segment", "customer_id"], "outer")
        .fillna(0.0, subset=["commissions_eur", "interets_eur"])
        .withColumn("revenue_eur", F.col("commissions_eur") + F.col("interets_eur"))
    )

    result = (
        combined.groupBy("month", "country_code", "segment")
        .agg(
            F.round(F.sum("revenue_eur") / F.countDistinct("customer_id"), 2).alias("arpu_eur"),
            F.countDistinct("customer_id").alias("nb_customers"),
        )
        .withColumnRenamed("month", "period")
    )
    return result, ["period", "country_code", "segment"]


# ============================================================================
# KPIs Assurance
# ============================================================================

def build_loss_ratio_by_product(spark: SparkSession, new_months: list):
    """Ratio sinistres/primes par produit d'assurance, pays et mois."""
    df = (
        spark.table("lakehouse.silver.insurance_operations")
        .withColumn("month", F.trunc(F.col("timestamp"), "month"))
        .filter(F.col("month").isin(new_months))
    )

    claims = (
        df.filter((F.col("operation_type") == INSURANCE_CLAIM_PAYMENT_TYPE) &
                   (F.col("claim_status") == INSURANCE_CLAIM_PAID_STATUS))
        .groupBy("month", "country_code", "product_line")
        .agg(F.sum("amount_eur").alias("claims_paid_eur"))
    )
    premiums = (
        df.filter(F.col("operation_type") == INSURANCE_PREMIUM_TYPE)
        .groupBy("month", "country_code", "product_line")
        .agg(F.sum("amount_eur").alias("premiums_eur"))
    )

    result = (
        claims.join(premiums, ["month", "country_code", "product_line"], "outer")
        .fillna(0.0, subset=["claims_paid_eur", "premiums_eur"])
        .withColumn("loss_ratio", F.round(F.col("claims_paid_eur") / F.col("premiums_eur"), 4))
        .withColumnRenamed("month", "period")
    )
    return result, ["period", "country_code", "product_line"]


def build_claims_processing_time(spark: SparkSession, new_months: list):
    """Délai moyen de traitement des sinistres (jours) par pays et ligne produit."""
    df = (
        spark.table("lakehouse.silver.insurance_operations")
        .withColumn("month", F.trunc(F.col("timestamp"), "month"))
        .filter(F.col("month").isin(new_months))
        .filter(F.col("processing_days").isNotNull())
    )
    result = (
        df.groupBy("month", "country_code", "product_line")
        .agg(F.round(F.avg("processing_days"), 1).alias("avg_processing_days"))
        .withColumnRenamed("month", "period")
    )
    return result, ["period", "country_code", "product_line"]


# ============================================================================
# KPIs Mobile Money
# ============================================================================

def build_mobile_money_daily_flow(spark: SparkSession, new_days: list):
    """Flux journalier de paiements mobiles par pays."""
    df = spark.table("lakehouse.silver.mobile_money")
    df = df.filter(F.to_date(F.col("timestamp")).isin(new_days))

    result = (
        df.groupBy(F.to_date(F.col("timestamp")).alias("period"), F.col("country_code"))
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount_eur"), 2).alias("total_amount_eur"),
            F.round(
                F.sum(F.when(F.col("status") == MOBILE_MONEY_FAILED_STATUS, 1).otherwise(0))
                / F.count("*"), 4
            ).alias("failure_rate"),
            F.countDistinct("sender_id").alias("active_users"),
        )
    )
    return result, ["period", "country_code"]


def build_cross_border_transfers(spark: SparkSession, new_weeks: list):
    """Transferts transfrontaliers entre pays UEMOA, par corridor et semaine."""
    df = (
        spark.table("lakehouse.silver.mobile_money")
        .filter(
            (F.col("sender_country") != F.col("receiver_country")) &
            F.col("sender_country").isin(UEMOA_COUNTRIES) &
            F.col("receiver_country").isin(UEMOA_COUNTRIES)
        )
        .withColumn("week", F.date_trunc("week", F.col("timestamp")).cast("date"))
        .filter(F.col("week").isin(new_weeks))
    )
    result = (
        df.groupBy(F.col("week").alias("period"), F.col("sender_country"), F.col("receiver_country"))
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.avg("amount_eur"), 2).alias("avg_amount_eur"),
            F.round(F.sum("amount_eur"), 2).alias("total_amount_eur"),
        )
    )
    return result, ["period", "sender_country", "receiver_country"]


# ============================================================================
# Registre des KPIs : source pour la détection des périodes + fonction de build
# ============================================================================

KPI_SPECS = {
    "daily_transaction_volume": {
        "source_table": "lakehouse.silver.bank_transactions",
        "date_col": "timestamp", "granularity": "day",
        "build_fn": build_daily_transaction_volume,
    },
    "npl_ratio_by_country": {
        "source_table": "lakehouse.silver.loan_repayments",
        "date_col": "due_date", "granularity": "day",
        "build_fn": build_npl_ratio_by_country,
    },
    "customer_arpu_monthly": {
        "source_table": "lakehouse.silver.bank_transactions",
        "date_col": "timestamp", "granularity": "month",
        "build_fn": build_customer_arpu_monthly,
    },
    "loss_ratio_by_product": {
        "source_table": "lakehouse.silver.insurance_operations",
        "date_col": "timestamp", "granularity": "month",
        "build_fn": build_loss_ratio_by_product,
    },
    "claims_processing_time": {
        "source_table": "lakehouse.silver.insurance_operations",
        "date_col": "timestamp", "granularity": "month",
        "build_fn": build_claims_processing_time,
    },
    "mobile_money_daily_flow": {
        "source_table": "lakehouse.silver.mobile_money",
        "date_col": "timestamp", "granularity": "day",
        "build_fn": build_mobile_money_daily_flow,
    },
    "cross_border_transfers": {
        "source_table": "lakehouse.silver.mobile_money",
        "date_col": "timestamp", "granularity": "week",
        "build_fn": build_cross_border_transfers,
    },
}


def process_kpi(spark: SparkSession, kpi_name: str):
    spec = KPI_SPECS[kpi_name]
    gold_table = f"lakehouse.gold.{kpi_name}"

    periods = get_new_periods(spark, spec["source_table"], gold_table,
                               spec["date_col"], spec["granularity"])
    if not periods:
        print(f"ℹ️  Aucune nouvelle période à traiter pour {kpi_name}.")
        return

    print(f"📋 {len(periods)} nouvelle(s) période(s) à traiter pour {kpi_name}: {periods}")
    df, merge_keys = spec["build_fn"](spark, periods)
    merge_gold(spark, df, gold_table, merge_keys)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kpi", required=True, choices=list(KPI_SPECS.keys()))
    args = parser.parse_args()

    spark = get_spark_session()
    process_kpi(spark, args.kpi)
    spark.stop()


if __name__ == "__main__":
    main()