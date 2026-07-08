# BIBLIOTECAS
# ============================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import re

# ============================
# CONFIGURAÇÕES
# ============================
ADLS_ACCOUNT_NAME = "lakecozedores"
ADLS_SAS_TOKEN = "sv=2026-02-06&ss=bfqt&srt=sco&sp=rwdlacupyx&se=..."
bronze_container = "bronze"
silver_container = "silver"
bronze_path = "cozedores/"
silver_path = "cozedores"

def get_adls_client():
    return DataLakeServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=ADLS_SAS_TOKEN
    )

# ============================
# FUNÇÃO PRINCIPAL
# ============================
def bronze_to_silver(**context):
    client = get_adls_client()
    bronze_fs = client.get_file_system_client(bronze_container)
    silver_fs = client.get_file_system_client(silver_container)

    # Lista arquivos no Bronze
    paths = bronze_fs.get_paths(bronze_path)

    for file in paths:
        if file.is_directory:
            continue
        file_name = file.name.split("/")[-1]
        logging.info(f"Processando arquivo: {file_name}")

        # ============================
        # LEITURA DO BRONZE
        # ============================
        file_client = bronze_fs.get_file_client(file.name)
        stream = file_client.download_file().readall()

        # CSV separado por ponto e vírgula
        df = pd.read_csv(io.BytesIO(stream), sep=";", encoding="utf-8", engine="python")

        # Normalizar nomes das colunas
        df.columns = df.columns.str.strip().str.lower()

        print("COLUNAS LIDAS:", df.columns.tolist())

        # ============================
        # TRANSFORMAÇÕES ROBUSTAS
        # ============================
        if "data_ref" in df.columns:
            df["data_ref"] = pd.to_datetime(df["data_ref"])
        elif "data" in df.columns:
            df["data_ref"] = pd.to_datetime(df["data"])
        else:
            raise ValueError(f"Nenhuma coluna de data encontrada. Colunas disponíveis: {df.columns.tolist()}")

        df["ano"] = df["data_ref"].dt.year
        df["mes"] = df["data_ref"].dt.month
        df["dia"] = df["data_ref"].dt.day

        for colname in ["id", "data"]:
            if colname in df.columns:
                df = df.drop(columns=[colname])

        # ============================
        # ESCRITA NO SILVER (PARQUET)
        # ============================
        for cozedor, df_part in df.groupby("cozedor"):    

            # Normalizar nome do arquivo
            safe_name = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', file_name)
            safe_name = safe_name.replace(".csv", ".parquet")
            output_path = f"{silver_path}/cozedor={cozedor}/{safe_name}"
            output_path = output_path.replace("//", "/")  # remover barras duplas
            logging.info(f"Gravando Silver: {output_path}")
            table = pa.Table.from_pandas(df_part)
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            file_client = silver_fs.get_file_client(output_path)
            file_client.upload_data(buffer.read(), overwrite=True)

    logging.info("Processo Bronze → Silver finalizado com sucesso!")

# ============================
# DAG
# ============================
default_args = {
    "owner": "Cristiano",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="bronze_to_silver",
    default_args=default_args,
    start_date=datetime(2026, 5, 4),
    schedule_interval="@daily",
    catchup=False,
    tags=["adls", "bronze", "silver"]
) as dag:

    task_bronze_to_silver = PythonOperator(
        task_id="process_bronze_to_silver",
        python_callable=bronze_to_silver
    )

    task_bronze_to_silver
