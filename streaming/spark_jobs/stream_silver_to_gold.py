"""
stream_silver_to_gold.py

Level 3 — Job 2 : Spark Structured Streaming Silver -> Gold.
Consomme les topics silver-* depuis Kafka et détecte, en quasi temps réel :
  - fraud_multiple_txn   : transactions multiples > 500 000 XOF sur un même
                           compte en moins de 5 min (fenêtre glissante 5 min,
                           slide 1 min)              -> gold-fraud-alerts
  - fraud_unusual_country: paiement mobile money émis depuis un pays différent
                           du pays d'enregistrement du client (jointure
                           stream-statique avec le référentiel customers)
                                                       -> gold-fraud-alerts
  - fraud_claim_ratio    : sinistre payé > 3x la somme des primes versées par
                           le client sur les 12 derniers mois
                                                       -> gold-fraud-alerts
  - aml_threshold        : virement dépassant le seuil déclaratif BCEAO/CIMA
                           (1 000 000 XOF zone UEMOA, 5 000 GHS Ghana)
                                                       -> gold-aml-events
  - liquidity_alerts     : solde net glissant (1 jour) par pays passant sous
                           un seuil de couverture minimal
                                                       -> gold-liquidity-alerts

Chaque règle tourne comme un job Spark indépendant (voir docker-compose,
un conteneur par --job), pour rester simple à surveiller/redémarrer
individuellement -- leçon tirée du debug du Job 1.

Usage: spark-submit stream_silver_to_gold.py --job fraud_multiple_txn
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

KAFKA_BROKERS = "kafka:29092"

# ============================================================================
# Seuils réglementaires / métier -- à vérifier/ajuster avec le métier
# ============================================================================
FRAUD_MULTI_TXN_THRESHOLD_XOF = 500_000
FRAUD_MULTI_TXN_WINDOW = "5 minutes"
FRAUD_MULTI_TXN_SLIDE = "1 minute"

CLAIM_TO_PREMIUM_RATIO = 3

AML_THRESHOLD_XOF = 1_000_000   # zone UEMOA
AML_THRESHOLD_GHS = 5_000       # Ghana

# Seuil de liquidité : ARBITRAIRE, à ajuster -- pas de valeur précisée au
# cahier des charges. Solde net glissant sur 1 jour, par pays. Négatif =
# plus de sorties (retraits/paiements/virements) que d'entrées (dépôts).
LIQUIDITY_MIN_COVERAGE_XOF = -50_000_000
LIQUIDITY_MIN_COVERAGE_GHS = -1_000_000
UEMOA_COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ"]


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
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
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_silver_stream(spark: SparkSession, topic: str):
    """Lit un topic silver-* : c'est du JSON déjà propre (produit par le Job 1),
    donc on infère juste la structure une fois via un read batch ponctuel pour
    obtenir le schéma, plutôt que de le redéclarer en dur ici (le Job 1 est la
    source de vérité du schéma Silver)."""
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    sample = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .limit(1)
        .select(F.col("value").cast("string").alias("v"))
        .collect()
    )
    if not sample:
        raise RuntimeError(
            f"Impossible d'inférer le schéma de {topic} : aucun message présent. "
            f"Assure-toi que le Job 1 a déjà publié au moins un message dans ce topic."
        )
    schema = spark.read.json(spark.sparkContext.parallelize([sample[0]["v"]])).schema

    return raw_stream.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")


def write_alerts_to_kafka(df, topic: str, checkpoint_dir: str, trigger_seconds: int = 30):
    return (
        df.select(F.to_json(F.struct(*df.columns)).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("topic", topic)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("update")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


# ============================================================================
# Règle 1 : transactions multiples > seuil sur un même compte / 5 min glissant
# ============================================================================
def run_fraud_multiple_txn(spark: SparkSession):
    df = read_silver_stream(spark, "silver-bank-transactions")
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))

    alerts = (
        df.filter(F.col("currency") == "XOF")
        .withWatermark("timestamp", "10 minutes")
        .groupBy(
            F.window("timestamp", FRAUD_MULTI_TXN_WINDOW, FRAUD_MULTI_TXN_SLIDE),
            F.col("account_id"),
            F.col("country_code"),
        )
        .agg(F.sum("amount").alias("total_amount_xof"), F.count("*").alias("txn_count"))
        .filter(F.col("total_amount_xof") > FRAUD_MULTI_TXN_THRESHOLD_XOF)
        .select(
            F.lit("MULTIPLE_TRANSACTIONS_THRESHOLD").alias("alert_type"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("account_id"),
            F.col("country_code"),
            F.col("total_amount_xof"),
            F.col("txn_count"),
            F.current_timestamp().alias("detected_at"),
        )
    )
    return write_alerts_to_kafka(alerts, "gold-fraud-alerts", "/tmp/checkpoints/fraud_multiple_txn")


# ============================================================================
# Règle 2 : paiement mobile money depuis un pays inhabituel pour le client
# ============================================================================
def run_fraud_unusual_country(spark: SparkSession):
    df = read_silver_stream(spark, "silver-mobile-money")
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))

    def _write(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return
        # Référentiel statique, relu à chaque micro-batch (petit volume,
        # coût négligeable) -- pas de jointure stream-stream nécessaire ici.
        customers = spark.table("lakehouse.bronze.customers").select(
            F.col("customer_id").alias("_cust_id"),
            F.col("country_code").alias("registered_country"),
        )
        joined = batch_df.join(customers, batch_df["sender_id"] == customers["_cust_id"], "left")
        alerts = (
            joined.filter(
                F.col("registered_country").isNotNull()
                & (F.col("sender_country") != F.col("registered_country"))
            )
            .select(
                F.lit("UNUSUAL_COUNTRY_MOBILE_MONEY").alias("alert_type"),
                F.col("payment_id"),
                F.col("sender_id"),
                F.col("sender_country"),
                F.col("registered_country"),
                F.col("amount"),
                F.col("currency"),
                F.current_timestamp().alias("detected_at"),
            )
        )
        if alerts.rdd.isEmpty():
            return
        (
            alerts.select(F.to_json(F.struct(*alerts.columns)).alias("value"))
            .write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("topic", "gold-fraud-alerts")
            .save()
        )
        print(f"✅ Batch {batch_id} — {alerts.count()} alerte(s) pays inhabituel détectée(s)")

    return (
        df.writeStream
        .foreachBatch(_write)
        .option("checkpointLocation", "/tmp/checkpoints/fraud_unusual_country")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )


# ============================================================================
# Règle 3 : sinistre payé > 3x la somme des primes versées (12 derniers mois)
# ============================================================================
def run_fraud_claim_ratio(spark: SparkSession):
    df = read_silver_stream(spark, "silver-insurance-operations")
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
    claims = df.filter(
        (F.col("operation_type") == "CLAIM_PAYMENT") & (F.col("claim_status") == "PAID")
    )

    def _write(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return
        # Agrégat des primes versées par client sur 12 mois, recalculé à
        # chaque micro-batch depuis la table Silver batch (Level 2) --
        # approximation raisonnable pour la démo (pas de vrai référentiel
        # "premium annuel" précalculé en Gold).
        one_year_ago = F.date_sub(F.current_date(), 365)
        premiums = (
            spark.table("lakehouse.silver.insurance_operations")
            .filter(
                (F.col("operation_type") == "PREMIUM_PAYMENT")
                & (F.to_date(F.col("timestamp")) >= one_year_ago)
            )
            .groupBy(F.col("customer_id").alias("_cust_id"))
            .agg(F.sum("amount_eur").alias("annual_premium_eur"))
        )

        joined = batch_df.join(premiums, batch_df["customer_id"] == premiums["_cust_id"], "left")
        alerts = (
            joined.filter(
                F.col("annual_premium_eur").isNotNull()
                & (F.col("amount_eur") > CLAIM_TO_PREMIUM_RATIO * F.col("annual_premium_eur"))
            )
            .select(
                F.lit("CLAIM_EXCEEDS_PREMIUM_RATIO").alias("alert_type"),
                F.col("operation_id"),
                F.col("customer_id"),
                F.col("country_code"),
                F.col("amount_eur").alias("claim_amount_eur"),
                F.col("annual_premium_eur"),
                F.current_timestamp().alias("detected_at"),
            )
        )
        if alerts.rdd.isEmpty():
            return
        (
            alerts.select(F.to_json(F.struct(*alerts.columns)).alias("value"))
            .write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("topic", "gold-fraud-alerts")
            .save()
        )
        print(f"✅ Batch {batch_id} — {alerts.count()} alerte(s) ratio sinistre/prime détectée(s)")

    return (
        claims.writeStream
        .foreachBatch(_write)
        .option("checkpointLocation", "/tmp/checkpoints/fraud_claim_ratio")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )


# ============================================================================
# AML : virement dépassant le seuil déclaratif BCEAO/CIMA
# ============================================================================
def run_aml_threshold(spark: SparkSession):
    df = read_silver_stream(spark, "silver-bank-transactions")
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))

    alerts = (
        df.filter(F.col("transaction_type") == "TRANSFER")
        .filter(
            ((F.col("currency") == "XOF") & (F.col("amount") > AML_THRESHOLD_XOF))
            | ((F.col("currency") == "GHS") & (F.col("amount") > AML_THRESHOLD_GHS))
        )
        .select(
            F.lit("AML_DECLARATIVE_THRESHOLD").alias("alert_type"),
            F.col("transaction_id"),
            F.col("account_id"),
            F.col("beneficiary_account"),
            F.col("country_code"),
            F.col("amount"),
            F.col("currency"),
            F.col("timestamp"),
            F.current_timestamp().alias("detected_at"),
        )
    )
    return write_alerts_to_kafka(alerts, "gold-aml-events", "/tmp/checkpoints/aml_threshold")


# ============================================================================
# Alertes de liquidité : solde net glissant (1 jour) par pays
# ============================================================================
def run_liquidity_alerts(spark: SparkSession):
    df = read_silver_stream(spark, "silver-bank-transactions")
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))

    # Signé : DEPOSIT = entrée (+), tout le reste = sortie (-)
    signed_amount = F.when(F.col("transaction_type") == "DEPOSIT", F.col("amount")) \
        .otherwise(-F.col("amount"))

    balances = (
        df.withColumn("signed_amount", signed_amount)
        .withWatermark("timestamp", "1 hour")
        .groupBy(
            F.window("timestamp", "1 day"),
            F.col("country_code"),
            F.col("currency"),
        )
        .agg(F.sum("signed_amount").alias("net_balance"))
    )

    def threshold_for(currency_col, country_col):
        return F.when(currency_col == "GHS", F.lit(LIQUIDITY_MIN_COVERAGE_GHS)) \
            .otherwise(F.lit(LIQUIDITY_MIN_COVERAGE_XOF))

    alerts = (
        balances
        .withColumn("min_threshold", threshold_for(F.col("currency"), F.col("country_code")))
        .filter(F.col("net_balance") < F.col("min_threshold"))
        .select(
            F.lit("LIQUIDITY_COVERAGE_BREACH").alias("alert_type"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("country_code"),
            F.col("currency"),
            F.col("net_balance"),
            F.col("min_threshold"),
            F.current_timestamp().alias("detected_at"),
        )
    )
    return write_alerts_to_kafka(alerts, "gold-liquidity-alerts", "/tmp/checkpoints/liquidity_alerts")


JOB_REGISTRY = {
    "fraud_multiple_txn": run_fraud_multiple_txn,
    "fraud_unusual_country": run_fraud_unusual_country,
    "fraud_claim_ratio": run_fraud_claim_ratio,
    "aml_threshold": run_aml_threshold,
    "liquidity_alerts": run_liquidity_alerts,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, choices=list(JOB_REGISTRY.keys()))
    args = parser.parse_args()

    spark = get_spark_session(f"waba-stream-{args.job}")
    query = JOB_REGISTRY[args.job](spark)
    print(f"🚀 Job 2 démarré : {args.job}")
    query.awaitTermination()


if __name__ == "__main__":
    main()