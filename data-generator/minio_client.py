"""
minio_client.py

Gère la connexion à MinIO et l'upload des fichiers générés.
Utilise boto3 (SDK AWS) car MinIO est compatible avec l'API S3.
"""
import os
import io
import boto3
import pandas as pd

from config import BUCKET_RAW_LANDING


def get_minio_client():
    """
    Crée un client S3 pointant vers MinIO.
    Les credentials viennent des variables d'environnement (jamais en dur),
    injectées via docker-compose.yml.
    """
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


def upload_dataframe(client, df: pd.DataFrame, filename: str, country: str, data_type: str):
    """
    Upload un DataFrame vers le bucket raw-landing, organisé par sous-dossiers
    pays/type comme demandé dans le cahier des charges ("Envoi direct vers
    MinIO, organisé par pays et type de données").

    Ex: raw-landing/CI/bank_transactions/bank_txn_CI_20260101_01.csv
    """
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    key = f"{country}/{data_type}/{filename}"

    client.put_object(
        Bucket=BUCKET_RAW_LANDING,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
    )
    return key


def list_uploaded_files(client, prefix: str = ""):
    """Liste les fichiers déjà présents dans raw-landing (utile pour l'UI)."""
    response = client.list_objects_v2(Bucket=BUCKET_RAW_LANDING, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", [])]