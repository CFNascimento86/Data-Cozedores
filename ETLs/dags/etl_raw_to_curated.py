# Bibliotecas
# ---------------------------------------------------------
import os
import json
import logging
from datetime import datetime, timedelta
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------
# CONFIGURAÇÕES
# ---------------------------------------------------------
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "192.168.1.6")
SQLSERVER_DB = os.getenv("SQLSERVER_DB", "Cozedores")
SQLSERVER_USER = os.getenv("SQLSERVER_USER", "LadrilhandoData")
SQLSERVER_PWD = os.getenv("SQLSERVER_PWD", "**********")
ETL_BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "120000"))

# ---------------------------------------------------------
# LOG JSON
# ---------------------------------------------------------
def log_json(level: str, message: str, extra: dict | None = None) -> None:
    payload = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level,
        "message": message,
        "extra": extra or {}
    }
    logging.info(json.dumps(payload, default=str, ensure_ascii=False))

# ---------------------------------------------------------
# CONEXÃO SQL SERVER
# ---------------------------------------------------------
def criar_conexao_sql():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SQLSERVER_HOST};"
        f"DATABASE={SQLSERVER_DB};"
        f"UID={SQLSERVER_USER};"
        f"PWD={SQLSERVER_PWD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

# ---------------------------------------------------------
# VALIDAÇÃO DA ESTRUTURA RAW/CURATED
# ---------------------------------------------------------
def validar_estrutura_curated():
    conn = criar_conexao_sql()
    cursor = conn.cursor()

    try:
        curated_colunas_obrigatorias = {
            "id",
            "data",
            "hora",
            "turno",
            "fluxo",
            "cozedor",
            "temperatura",
            "pressao_vacuo",
            "brix",
            "nivel",
            "vazao_vapor",
            "vazao_alimentacao",
            "estado_batelada",
            "pureza",
            "condensado",
            "id_raw",
        }

        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo'
              AND TABLE_NAME = 'curated_cozedores';
        """)

        curated_colunas_existentes = {row[0] for row in cursor.fetchall()}
        curated_faltantes = curated_colunas_obrigatorias - curated_colunas_existentes

        if curated_faltantes:
            raise RuntimeError(
                f"Tabela dbo.curated_cozedores incompleta. "
                f"Colunas faltantes: {sorted(curated_faltantes)}"
            )

        raw_colunas_obrigatorias = {
            "id",
            "timestamp_utc",
            "db_number",
            "fluxo",
            "cozedor",
            "temperatura",
            "pressao_vacuo",
            "brix",
            "nivel",
            "vazao_vapor",
            "vazao_alimentacao",
            "estado_batelada",
            "pureza",
            "condensado",
        }

        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo'
              AND TABLE_NAME = 'raw_cozedores';
        """)

        raw_colunas_existentes = {row[0] for row in cursor.fetchall()}
        raw_faltantes = raw_colunas_obrigatorias - raw_colunas_existentes

        if raw_faltantes:
            raise RuntimeError(
                f"Tabela dbo.raw_cozedores incompleta. "
                f"Colunas faltantes: {sorted(raw_faltantes)}"
            )

        log_json("info", "Estrutura RAW/CURATED validada com sucesso")

    finally:
        cursor.close()
        conn.close()

# ---------------------------------------------------------
# ETL INCREMENTAL RAW -> CURATED
# ---------------------------------------------------------
def etl_raw_to_curated():
    conn = criar_conexao_sql()
    cursor = conn.cursor()

    try:
        log_json("info", "Iniciando ETL incremental RAW -> CURATED")

        cursor.execute("""
            SELECT ISNULL(MAX(id_raw), 0)
            FROM dbo.curated_cozedores;
        """)
        ultimo_raw_id = cursor.fetchone()[0]

        log_json("info", "Último raw_id processado identificado", {
            "ultimo_raw_id": ultimo_raw_id
        })

        cursor.execute("""
            INSERT INTO dbo.curated_cozedores (
                data,
                hora,
                turno,
                fluxo,
                cozedor,
                temperatura,
                pressao_vacuo,
                brix,
                nivel,
                vazao_vapor,
                vazao_alimentacao,
                estado_batelada,
                pureza,
                condensado,
                id_raw
            )
            SELECT TOP (?)
                CAST(r.timestamp_utc AS DATE) AS data,
                CAST(r.timestamp_utc AS TIME) AS hora,

                CASE
                    WHEN CAST(r.timestamp_utc AS TIME) >= '06:00:00'
                     AND CAST(r.timestamp_utc AS TIME) <= '14:00:00'
                        THEN 'A'

                    WHEN CAST(r.timestamp_utc AS TIME) >= '14:01:00'
                     AND CAST(r.timestamp_utc AS TIME) <= '22:00:00'
                        THEN 'B'

                    ELSE 'C'
                END AS turno,

                r.fluxo,

                CASE
                    WHEN r.fluxo = 2 THEN r.cozedor + 5
                    ELSE r.cozedor
                END AS cozedor,

                r.temperatura,
                r.pressao_vacuo,
                r.brix,
                r.nivel,
                r.vazao_vapor,
                r.vazao_alimentacao,
                r.estado_batelada,
                r.pureza,
                r.condensado,
                r.id AS id_raw

            FROM dbo.raw_cozedores r
            WHERE r.id > ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM dbo.curated_cozedores c
                  WHERE c.id_raw = r.id
              )
            ORDER BY r.id ASC;
        """, ETL_BATCH_SIZE, ultimo_raw_id)

        linhas_inseridas = cursor.rowcount
        conn.commit()

        log_json("info", "ETL incremental finalizado com sucesso", {
            "linhas_inseridas": linhas_inseridas,
            "batch_size": ETL_BATCH_SIZE
        })

    except Exception as exc:
        conn.rollback()
        log_json("error", "Erro no ETL incremental RAW -> CURATED", {
            "exception": str(exc)
        })
        raise

    finally:
        cursor.close()
        conn.close()

# ---------------------------------------------------------
# DEFINIÇÃO DA DAG
# ---------------------------------------------------------
default_args = {
    "owner": "Cristiano",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="etl_raw_to_curated",
    description="Carga incremental RAW para CURATED dos cozedores",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cozedores", "sqlserver", "curated", "industrial-data"],
) as dag:

    validar_estrutura = PythonOperator(
        task_id="validar_estrutura_raw_curated",
        python_callable=validar_estrutura_curated,
    )

    executar_etl = PythonOperator(
        task_id="executar_etl_incremental_raw_to_curated",
        python_callable=etl_raw_to_curated,
    )

    validar_estrutura >> executar_etl
