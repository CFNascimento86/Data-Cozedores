# BIBLIOTECAS
# ============================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import json
import logging
import re

# ============================
# CONFIGURAÇÕES
# ============================
ADLS_ACCOUNT_NAME = "lakecozedores"
ADLS_SAS_TOKEN = "sv=2026-02-06&ss=bfqt&srt=sco&sp=rwdlacupyx&se=..."
silver_container = "silver"
gold_container = "gold"
silver_path = "cozedores"
gold_path = "cozedores"
watermark_path = "_control/watermark.json"

def get_adls_client():
    return DataLakeServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=ADLS_SAS_TOKEN
    )

# ============================
# FUNÇÃO PRINCIPAL
# ============================
def silver_to_gold(**context):
    client = get_adls_client()
    silver_fs = client.get_file_system_client(silver_container)
    gold_fs = client.get_file_system_client(gold_container)

    # ============================
    # 1. LER WATERMARK
    # ============================
    try:
        wm_file = gold_fs.get_file_client(watermark_path)
        wm_data = wm_file.download_file().readall()
        watermark = json.loads(wm_data)
        last_day = watermark["last_day"]
        last_day = datetime.strptime(last_day, "%Y-%m-%d").date()
        logging.info(f"Watermark atual: {last_day}")
    except Exception:
        # Se não existir, começa do zero
        last_day = datetime(1900, 1, 1).date()
        logging.info("Watermark não encontrado. Iniciando do zero.")

    # ============================
    # 2. LISTAR ARQUIVOS DO SILVER
    # ============================
    paths = silver_fs.get_paths(silver_path)
    arquivos_para_processar = []

    for file in paths:
        if file.is_directory:
            continue
        # Extrair ano/mes/dia do caminho
        partes = file.name.split("/")
        # Exemplo: cozedores/cozedor=1/2026-06-12.parquet
        nome = partes[-1]
        # Extrair data do nome do arquivo
        match = re.search(r"(\d{4})-(\d{2})-(\d{2})", nome)
        if not match:
            continue
        ano, mes, dia = match.groups()
        file_date = datetime(int(ano), int(mes), int(dia)).date()
        # Comparar com watermark
        if file_date > last_day:
            arquivos_para_processar.append((file_date, file.name))
    if not arquivos_para_processar:
        logging.info("Nenhum arquivo novo para processar.")
        return
    # Ordenar por data
    arquivos_para_processar.sort()

    # ============================
    # 3. PROCESSAR ARQUIVOS NOVOS
    # ============================
    novo_watermark = last_day
    for file_date, file_path in arquivos_para_processar:
        logging.info(f"Processando arquivo Silver: {file_path}")
        file_client = silver_fs.get_file_client(file_path)
        stream = file_client.download_file().readall()
        table = pq.read_table(io.BytesIO(stream))
        df = table.to_pandas()

        # ============================
        # REORGANIZAÇÃO DE COLUNAS
        # ============================
        colunas_ordenadas = [
            "ano", "mes", "dia", "hora", "turno", "cozedor",
            "brix", "temperatura", "pressao"
        ]

        colunas_existentes = [c for c in colunas_ordenadas if c in df.columns]
        df = df[colunas_existentes]

        # ============================
        # CÁLCULO DE MÉDIAS E KPIs
        # ============================
        df["brix_media_cozedor"] = df.groupby("cozedor")["brix"].transform("mean")
      
        if "turno" in df.columns:
            df["brix_media_turno"] = df.groupby("turno")["brix"].transform("mean")
          
        df["kpi_producao"] = df.groupby(["ano", "mes", "dia"])["brix"].transform("count")

        # ============================
        # ESCRITA NO GOLD
        # ============================
        ano, mes, dia = df["ano"].iloc[0], df["mes"].iloc[0], df["dia"].iloc[0]

        safe_name = file_path.split("/")[-1].replace(".parquet", "_gold.parquet")
        safe_name = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', safe_name)

        output_path = f"{gold_path}/ano={ano}/mes={mes}/dia={dia}/{safe_name}"
        output_path = output_path.replace("//", "/")

        logging.info(f"Gravando Gold: {output_path}")

        table = pa.Table.from_pandas(df)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        file_client = gold_fs.get_file_client(output_path)
        file_client.upload_data(buffer.read(), overwrite=True)

        # Atualizar watermark
        novo_watermark = max(novo_watermark, file_date)

    # ============================
    # 4. ATUALIZAR WATERMARK
    # ============================
    wm_data = json.dumps({"last_day": novo_watermark.strftime("%Y-%m-%d")})
    wm_file = gold_fs.get_file_client(watermark_path)
    wm_file.upload_data(wm_data, overwrite=True)
    logging.info(f"Novo watermark salvo: {novo_watermark}")

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
    dag_id="silver_to_gold,
    default_args=default_args,
    start_date=datetime(2026, 5, 4),
    schedule_interval="@daily",
    catchup=False,
    tags=["cozedores", "silver", "gold", "watermark"]
) as dag:

    task_silver_to_gold = PythonOperator(
        task_id="process_silver_to_gold",
        python_callable=silver_to_gold
    )

    task_silver_to_gold
