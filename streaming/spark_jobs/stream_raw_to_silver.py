"""
stream_raw_to_silver.py

Level 3 — Job 1 : Spark Structured Streaming Raw -> Silver.

Pour un type de donnée donné (bank_transactions / insurance_operations /
mobile_money) :
  1. Consomme le topic Kafka raw-* correspondant (publié par NiFi)
  2. Valide le schéma JSON : un message qui ne parse pas correctement (champ
     clé manquant/mal typé) part vers dlq-financial-events, PAS silencieusement
     ignoré
  3. Déduplique par clé métier (transaction_id/operation_id/payment_id) sur
     une fenêtre de 10 minutes (watermark) — équivalent temps réel du
     clean_and_dedup() de transform_silver.py (Level 2 batch)
  4. Convertit les montants en EUR (mêmes taux fixes que le Level 2, pour
     rester cohérent entre batch et streaming — cf architecture Lambda)
  5. Double sink : écrit le résultat à la fois dans le topic Kafka silver-*
     ET dans la table Iceberg silver.* (foreachBatch, pattern standard Spark
     pour un sink multiple depuis une seule requête de streaming)

Usage: spark-submit stream_raw_to_silver.py --data_type bank_transactions
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)

KAFKA_BROKERS = "kafka:29092"
DLQ_TOPIC = "dlq-financial-events"

# --- Taux de conversion fixes vers EUR (mêmes hypothèses que Level 2 batch,
#     cf transform_silver.py) ---
EXCHANGE_RATES_TO_EUR = {
    "XOF": 1 / 655.957,
    "GHS": 1 / 13.5,
}

RAW_TOPICS = {
    "bank_transactions": "raw-bank-transactions",
    "insurance_operations": "raw-insurance-operations",
    "mobile_money": "raw-mobile-money-payments",
}

SILVER_TOPICS = {
    "bank_transactions": "silver-bank-transactions",
    "insurance_operations": "silver-insurance-operations",
    "mobile_money": "silver-mobile-money",
}

DEDUP_KEYS = {
    "bank_transactions": "transaction_id",
    "insurance_operations": "operation_id",
    "mobile_money": "payment_id",
}

AMOUNT_COLUMNS = {
    "bank_transactions": ["amount", "fee_amount"],
    "insurance_operations": ["amount"],
    "mobile_money": ["amount", "fee_amount"],
}

# Schémas des messages JSON publiés par NiFi : les colonnes métier d'origine
# (cf schema.py du Level 1) + les 2 champs d'enrichissement ajoutés par NiFi
# (ingestion_timestamp, source_file).
RAW_SCHEMAS = {
    "bank_transactions": StructType([
        StructField("transaction_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("account_id", StringType()),
        StructField("beneficiary_account", StringType()),
        StructField("branch_id", StringType()),
        StructField("country_code", StringType()),
        StructField("transaction_type", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("channel", StringType()),
        StructField("transaction_status", StringType()),
        StructField("fee_amount", DoubleType()),
        StructField("entity_type", StringType()),
        StructField("source_file", StringType()),
        StructField("ingestion_timestamp", StringType()),
    ]),
    "insurance_operations": StructType([
        StructField("operation_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("customer_id", StringType()),
        StructField("account_id", StringType()),
        StructField("country_code", StringType()),
        StructField("operation_type", StringType()),
        StructField("product_line", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("claim_status", StringType()),
        StructField("processing_days", IntegerType()),
        StructField("entity_type", StringType()),
        StructField("source_file", StringType()),
        StructField("ingestion_timestamp", StringType()),
    ]),
    "mobile_money": StructType([
        StructField("payment_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("sender_id", StringType()),
        StructField("receiver_id", StringType()),
        StructField("sender_country", StringType()),
        StructField("receiver_country", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("payment_type", StringType()),
        StructField("operator", StringType()),
        StructField("status", StringType()),
        StructField("fee_amount", DoubleType()),
        StructField("entity_type", StringType()),
        StructField("source_file", StringType()),
        StructField("ingestion_timestamp", StringType()),
    ]),
}


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("waba-stream-raw-to-silver")
        # Note : spark.jars.packages (connecteur Kafka + Iceberg) n'est PAS
        # ici mais dans conf/spark-defaults.conf -- un .config() à cet endroit
        # arrive trop tard, la JVM est déjà lancée par spark-submit avant que
        # ce code Python ne s'exécute.
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
        .config("spark.sql.shuffle.partitions", "4")  # petit volume de démo, pas besoin de 200 partitions par défaut
        .getOrCreate()
    )


def add_eur_columns(df, amount_cols: list):
    rate_expr = F.create_map(*[
        item for cur, rate in EXCHANGE_RATES_TO_EUR.items() for item in (F.lit(cur), F.lit(rate))
    ])
    for col in amount_cols:
        df = df.withColumn(f"{col}_eur",
                            F.round(F.col(col) * rate_expr[F.col("currency")], 2))
    return df


def ensure_silver_table_exists(spark: SparkSession, data_type: str, sample_df):
    """Crée la table Silver si elle n'existe pas déjà (le batch Level 2 l'a
    probablement déjà fait — ce garde-fou permet au streaming de tourner
    même en tout premier, avant tout run du DAG bronze_to_silver)."""
    table_name = f"lakehouse.silver.{data_type}"
    if not spark.catalog.tableExists(table_name):
        sample_df.limit(0).writeTo(table_name).using("iceberg") \
            .partitionedBy("country_code", F.days("timestamp")).createOrReplace()
        print(f"✅ Table {table_name} créée (par le streaming, premier démarrage).")


def make_write_microbatch(data_type: str):
    """Fabrique la fonction foreachBatch pour ce type de donnée (double sink :
    Iceberg + Kafka), avec conversion EUR appliquée à chaque micro-batch."""
    amount_cols = AMOUNT_COLUMNS[data_type]
    silver_topic = SILVER_TOPICS[data_type]
    table_name = f"lakehouse.silver.{data_type}"

    def _write(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        enriched_df = add_eur_columns(batch_df, amount_cols).cache()

        # mobile_money n'a pas de country_code natif dans son schéma JSON
        # (seulement sender_country/receiver_country) -- même règle que
        # Bronze (ingest_raw.py, Level 2) pour rester cohérent avec le
        # schéma déjà établi de lakehouse.silver.mobile_money.
        if data_type == "mobile_money" and "country_code" not in enriched_df.columns:
            enriched_df = enriched_df.withColumn("country_code", F.col("sender_country"))

        ensure_silver_table_exists(batch_df.sparkSession, data_type, enriched_df)

        # --- Sink 1 : Iceberg (silver.*) ---
        enriched_df.writeTo(table_name).append()

        # --- Sink 2 : Kafka (silver-*) ---
        (
            enriched_df
            .select(F.to_json(F.struct(*enriched_df.columns)).alias("value"))
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("topic", silver_topic)
            .save()
        )

        n = enriched_df.count()
        print(f"✅ Batch {batch_id} — {n} ligne(s) écrites dans {table_name} et {silver_topic}")
        enriched_df.unpersist()

    return _write


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_type", required=True, choices=list(RAW_TOPICS.keys()))
    args = parser.parse_args()
    data_type = args.data_type

    spark = get_spark_session()
    schema = RAW_SCHEMAS[data_type]
    dedup_key = DEDUP_KEYS[data_type]

    # --- Lecture du topic raw-* ---
    kafka_raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", RAW_TOPICS[data_type])
        .option("startingOffsets", "earliest")  # démo : on veut rejouer l'historique déjà publié
        .load()
    )

    parsed = kafka_raw_stream.select(
        F.col("value").cast("string").alias("raw_value"),
        F.from_json(F.col("value").cast("string"), schema).alias("data"),
    )

    # --- Validation de schéma : clé métier absente/mal typée = malformé ---
    valid_stream = parsed.filter(F.col(f"data.{dedup_key}").isNotNull()).select("data.*")
    invalid_stream = parsed.filter(F.col(f"data.{dedup_key}").isNull()) \
        .select(F.col("raw_value").alias("value"))

    # --- DLQ : passthrough simple, pas de watermark/dédup nécessaire ---
    dlq_query = (
        invalid_stream.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("topic", DLQ_TOPIC)
        .option("checkpointLocation", f"/tmp/checkpoints/{data_type}_dlq")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # --- Déduplication par fenêtre de 10 min (watermark) ---
    dedup_stream = (
        valid_stream
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
        .withWatermark("timestamp", "10 minutes")
        .dropDuplicates([dedup_key, "timestamp"])
    )

    silver_query = (
        dedup_stream.writeStream
        .foreachBatch(make_write_microbatch(data_type))
        .option("checkpointLocation", f"/tmp/checkpoints/{data_type}_silver")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print(f"🚀 Job 1 démarré pour {data_type} — "
          f"{RAW_TOPICS[data_type]} -> [{SILVER_TOPICS[data_type]} + {'lakehouse.silver.' + data_type}]")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()