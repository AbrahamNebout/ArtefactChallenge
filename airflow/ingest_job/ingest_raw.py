"""
ingest_raw.py

Lit les fichiers CSV bruts depuis MinIO bucket raw-landing, les valide,
et les écrit dans des tables Iceberg schéma raw.* via le REST Catalog.
Usage: spark-submit ingest_raw.py --data_type bank_transactions --country CI
"""
import argparse
import os
import sys

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
    IntegerType, BooleanType, DateType,
)

from schema import SCHEMAS , DEDUP_KEYS , FILE_PREFIXES


def get_spark_session() -> SparkSession:

    return (
        SparkSession.builder
        .appName("waba-ingest-raw")
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

        .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def ensure_table_exists(spark: SparkSession, data_type: str):
    """
    Crée la table dans le Bronze si elle n'existe pas
    """
    schema = SCHEMAS[data_type]
    table_name = f"lakehouse.bronze.{data_type}"

    effective_schema = schema
    if data_type == "mobile_money" and "country_code" not in schema.names:
        effective_schema = StructType(
            schema.fields + [StructField("country_code", StringType(), True)]
        )

    if spark.catalog.tableExists(table_name):
        print(f"ℹ️  Table {table_name} existe déjà.")
        return

    has_country = "country_code" in effective_schema.names
    date_col = _date_column_for(data_type)

    if has_country and date_col:
        partition_clause = f"PARTITIONED BY (country_code, days({date_col}))"
    elif has_country:
        partition_clause = "PARTITIONED BY (country_code)"
    else:
        partition_clause = ""

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} ({_schema_to_ddl(effective_schema)})
        USING iceberg
        {partition_clause}
    """
    spark.sql(create_sql)
    print(f"✅ Table {table_name} créée.")


REFERENTIAL_TYPES = {"customers", "accounts", "branches", "products"}


def archive_processed_files(input_files: list, source_bucket: str = "raw-landing"):
    """
    Déplace les fichiers traittés vers le bucket archive
    """
    if not input_files:
        return

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )

    for file_path in input_files:
        key = file_path.replace(f"s3a://{source_bucket}/", "")
        s3.copy_object(
            Bucket="archive",
            Key=key,
            CopySource={"Bucket": source_bucket, "Key": key},
        )
        s3.delete_object(Bucket=source_bucket, Key=key)
        print(f"📦 Archivé : {source_bucket}/{key} -> archive/{key}")


def ingest_file(spark: SparkSession, data_type: str, s3_path: str):
    schema = SCHEMAS[data_type]
    dedup_key = DEDUP_KEYS[data_type]

    schema_with_corrupt = StructType(
        schema.fields + [StructField("_corrupt_record", StringType(), True)]
    )
    raw_df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema_with_corrupt)
        .csv(s3_path)
        .withColumn("_source_file", F.input_file_name())
    )

    input_files = [row["_source_file"] for row in raw_df.select("_source_file").distinct().collect()]

    corrupt_count = raw_df.filter(F.col("_corrupt_record").isNotNull()).count()
    if corrupt_count > 0:
        print(f"⚠️  {corrupt_count} lignes malformées rejetées dans {s3_path}")

    clean_df = raw_df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record", "_source_file")
    clean_df = clean_df.filter(F.col(dedup_key).isNotNull()).cache()

    if data_type == "mobile_money":
        clean_df = clean_df.withColumn("country_code", F.col("sender_country"))

    table_name = f"lakehouse.bronze.{data_type}"

    clean_df.createOrReplaceTempView("source_batch")
    merge_sql = f"""
        MERGE INTO {table_name} t
        USING source_batch s
        ON t.{dedup_key} = s.{dedup_key}
        WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_sql)
    n_inserted = clean_df.count()

    if data_type not in REFERENTIAL_TYPES:
        archive_processed_files(input_files)

    print(f"✅ {n_inserted} lignes traitées pour {table_name} depuis {s3_path}")
    return n_inserted, corrupt_count



def _date_column_for(data_type: str):
    return {
        "bank_transactions": "timestamp",
        "insurance_operations": "timestamp",
        "mobile_money": "timestamp",
        "loan_repayments": "due_date",
    }.get(data_type)


def _schema_to_ddl(schema: StructType) -> str:
    """Convertit un StructType PySpark en DDL SQL pour le CREATE TABLE Iceberg."""
    type_map = {
        "string": "STRING", "double": "DOUBLE", "int": "INT",
        "boolean": "BOOLEAN", "date": "DATE", "timestamp": "TIMESTAMP",
    }
    fields = []
    for f in schema.fields:
        sql_type = type_map[f.dataType.simpleString()]
        fields.append(f"{f.name} {sql_type}")
    return ", ".join(fields)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_type", required=True, choices=list(SCHEMAS.keys()))
    parser.add_argument("--country", default=None,
                         help="Code pays (CI, SN, ...). Omis pour les référentiels.")
    parser.add_argument("--date", default=None,
                         help="Date YYYYMMDD à ingérer (données transactionnelles uniquement). "
                              "Si omis, lit tous les fichiers du dossier.")
    parser.add_argument("--ensure-table-only", action="store_true",
                         help="Crée uniquement la table Bronze si absente, sans ingérer de données.")
    args = parser.parse_args()
    spark = get_spark_session()
    if args.ensure_table_only:
        ensure_table_exists(spark, args.data_type)
        spark.stop()
        return

    ensure_table_exists(spark, args.data_type)
    is_referential = args.data_type in REFERENTIAL_TYPES
    if is_referential:
        s3_path = f"s3a://raw-landing/shared/referentials/{args.data_type}.csv"
    else:
        if not args.country:
            print("❌ --country est requis pour les données transactionnelles")
            sys.exit(1)

        prefix = FILE_PREFIXES[args.data_type]
        if args.date:
            s3_path = f"s3a://raw-landing/{args.country}/{args.data_type}/{args.date}/{prefix}_{args.country}_{args.date}_*.csv"
        else:
            s3_path = f"s3a://raw-landing/{args.country}/{args.data_type}/*/*.csv"

    ingest_file(spark, args.data_type, s3_path)
    spark.stop()


if __name__ == "__main__":
    main()