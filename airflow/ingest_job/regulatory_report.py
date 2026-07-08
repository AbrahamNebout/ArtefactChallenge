"""
regulatory_report.py

Génère le rapport réglementaire consolidé BCEAO/CIMA pour un jour donné,
à partir des tables Gold déjà calculées (gold.npl_ratio_by_country,
gold.loss_ratio_by_product) :
  - NPL par pays/type de prêt vs seuil réglementaire BCEAO (< 5%)
  - Loss ratio par pays/produit vs seuil de vigilance CIMA (> 70%)

Écrit le résultat consolidé dans gold.regulatory_report (table Iceberg) ET
exporte un CSV dans MinIO (bucket lakehouse, dossier reports/) pour la
déclaration réglementaire proprement dite.

Usage: spark-submit regulatory_report.py --date 20260502
"""
import argparse
import io
import os
from datetime import datetime

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, BooleanType

BCEAO_NPL_THRESHOLD = 0.05     # NPL doit rester < 5%
CIMA_LOSS_RATIO_THRESHOLD = 0.70  # seuil de VIGILANCE (pas un plafond dur) : > 70%

REPORT_SCHEMA = StructType([
    StructField("report_date", DateType(), False),
    StructField("country_code", StringType(), False),
    StructField("indicator_type", StringType(), False),   # "NPL" ou "LOSS_RATIO"
    StructField("dimension", StringType(), False),        # loan_type ou product_line
    StructField("value", DoubleType(), False),
    StructField("threshold", DoubleType(), False),
    StructField("breach", BooleanType(), False),
])


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("waba-regulatory-report")
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


def build_report(spark: SparkSession, report_date):
    """
    Construit le rapport consolidé pour `report_date` (objet date Python).
    Lève une exception explicite si les données Gold attendues ne sont pas
    encore disponibles (cas d'une course avec dag_silver_to_gold, piloté par
    Asset et donc sans horaire fixe garanti) -> déclenche un retry Airflow
    plutôt qu'un échec silencieux.
    """
    month_start = report_date.replace(day=1)

    # --- NPL (grain jour) : comparaison directe sur le jour exact ---
    npl_table = "lakehouse.gold.npl_ratio_by_country"
    if not spark.catalog.tableExists(npl_table):
        raise RuntimeError(f"Table {npl_table} inexistante — Gold pas encore calculé.")

    npl = spark.table(npl_table).filter(F.col("period") == F.lit(report_date))
    if npl.count() == 0:
        raise RuntimeError(
            f"Aucune donnée NPL pour le {report_date} dans {npl_table} — "
            f"dag_silver_to_gold n'a probablement pas encore tourné pour ce jour."
        )

    npl_rows = npl.select(
        F.lit(report_date).alias("report_date"),
        F.col("country_code"),
        F.lit("NPL").alias("indicator_type"),
        F.col("loan_type").alias("dimension"),
        F.col("npl_ratio").alias("value"),
        F.lit(BCEAO_NPL_THRESHOLD).alias("threshold"),
        (F.col("npl_ratio") >= F.lit(BCEAO_NPL_THRESHOLD)).alias("breach"),
    )

    # --- Loss ratio (grain mois) : on prend le mois du jour traité ---
    loss_table = "lakehouse.gold.loss_ratio_by_product"
    if not spark.catalog.tableExists(loss_table):
        raise RuntimeError(f"Table {loss_table} inexistante — Gold pas encore calculé.")

    loss = spark.table(loss_table).filter(F.col("period") == F.lit(month_start))
    if loss.count() == 0:
        raise RuntimeError(
            f"Aucune donnée loss_ratio pour le mois {month_start} dans {loss_table} — "
            f"dag_silver_to_gold n'a probablement pas encore tourné pour cette période."
        )

    loss_rows = loss.select(
        F.lit(report_date).alias("report_date"),
        F.col("country_code"),
        F.lit("LOSS_RATIO").alias("indicator_type"),
        F.col("product_line").alias("dimension"),
        F.col("loss_ratio").alias("value"),
        F.lit(CIMA_LOSS_RATIO_THRESHOLD).alias("threshold"),
        (F.col("loss_ratio") > F.lit(CIMA_LOSS_RATIO_THRESHOLD)).alias("breach"),
    )

    return npl_rows.unionByName(loss_rows).coalesce(1)


def merge_report(spark: SparkSession, df, report_date):
    """Upsert idempotent : rejouer le même jour remplace le rapport existant."""
    gold_table = "lakehouse.gold.regulatory_report"
    if not spark.catalog.tableExists(gold_table):
        spark.createDataFrame([], REPORT_SCHEMA).writeTo(gold_table).using("iceberg").createOrReplace()
        print(f"✅ Table {gold_table} créée.")

    df.createOrReplaceTempView("source_report")
    spark.sql(f"""
        DELETE FROM {gold_table} WHERE report_date = DATE('{report_date}')
    """)
    spark.sql(f"""
        INSERT INTO {gold_table}
        SELECT report_date, country_code, indicator_type, dimension, value, threshold, breach
        FROM source_report
    """)


def export_csv_to_minio(df, report_date):
    """Exporte le rapport en CSV vers MinIO (bucket lakehouse, dossier reports/)
    pour la déclaration réglementaire proprement dite."""
    pdf = df.orderBy("country_code", "indicator_type", "dimension").toPandas()
    buffer = io.StringIO()
    pdf.to_csv(buffer, index=False)

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )
    key = f"reports/regulatory_report_{report_date.strftime('%Y%m%d')}.csv"
    s3.put_object(Bucket="lakehouse", Key=key, Body=buffer.getvalue().encode("utf-8"))
    print(f"✅ Rapport exporté vers s3://lakehouse/{key}")

    n_breaches = pdf["breach"].sum()
    if n_breaches > 0:
        print(f"🚨 {n_breaches} dépassement(s) de seuil détecté(s) pour le {report_date} !")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Jour YYYYMMDD à traiter (J-1 par rapport à l'exécution).")
    args = parser.parse_args()

    report_date = datetime.strptime(args.date, "%Y%m%d").date()

    spark = get_spark_session()
    df = build_report(spark, report_date)
    merge_report(spark, df, report_date)
    export_csv_to_minio(df, report_date)
    spark.stop()


if __name__ == "__main__":
    main()