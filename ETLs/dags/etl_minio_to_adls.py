# Bibliotecas
# -----------------------------
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from azure.storage.blob import BlobServiceClient
import os
import tempfile

# -----------------------------
# Configurações
# -----------------------------
MINIO_ENDPOINT = "http://192.168.1.6:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "********"
MINIO_BUCKET = "cozedores-curated-daily"
MINIO_PREFIX = "curated/cozedores"

# ADLS Gen2 – container "bronze"
ADLS_ACCOUNT_URL = "https://lakecozedores.blob.core.windows.net"
ADLS_CONTAINER = "bronze"
ADLS_DIRECTORY = "cozedores"

# SAS gerada no Storage Account
ADLS_SAS_TOKEN = os.getenv("ADLS_SAS_TOKEN", "sv=2026-02-06&ss=bfqt&srt=sco&sp=rwdlacupyx&se...")

# -----------------------------
# FUNÇÃO PRINCIPAL
# -----------------------------
def copy_minio_to_adls():

    # MinIO (S3 compatible)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # BlobServiceClient com SAS
    blob_service = BlobServiceClient(
        account_url=ADLS_ACCOUNT_URL,
        credential=ADLS_SAS_TOKEN
    )
    container_client = blob_service.get_container_client(ADLS_CONTAINER)

    # Listar objetos no MinIO
    response = s3.list_objects_v2(
        Bucket=MINIO_BUCKET,
        Prefix=MINIO_PREFIX
    )

    if "Contents" not in response:
        print("Nenhum arquivo encontrado no MinIO.")
        return

    for obj in response["Contents"]:
        key = obj["Key"]

        if key.endswith("/"):
            continue
        if not key.endswith(".csv"):
            continue

        print(f"Encontrado arquivo no MinIO: {key}")

        filename = key.split("/")[-1]
        blob_path = f"{ADLS_DIRECTORY}/{filename}"

        print(f"Copiando para ADLS (blob): {blob_path}")

        # Baixar para arquivo temporário
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_name = tmp.name
            s3.download_file(MINIO_BUCKET, key, tmp_name)

        # Upload para Blob
        blob_client = container_client.get_blob_client(blob_path)
        with open(tmp_name, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)

        print(f"Arquivo copiado com sucesso: {filename}")

# -----------------------------
# DAG
# -----------------------------
default_args = {
    "owner": "Cristiano",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 31),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="minio_to_adls",
    default_args=default_args,
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["minio", "adls", "etl"],
) as dag:

    copy_task = PythonOperator(
        task_id="copy_minio_to_adls_task",
        python_callable=copy_minio_to_adls,
    )
