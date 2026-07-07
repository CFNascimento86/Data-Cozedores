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
ETL_BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "10000"))

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
# VALIDAÇÃO DA ESTRUTURA CURATED/GOLD
# ---------------------------------------------------------
def validar_estrutura_gold():
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

        gold_colunas_obrigatorias = {
            "id",
            "data",
            "turno",
            "fluxo",
            "cozedor",
            "qtd_registros",
            "temperatura_media",
            "pressao_vacuo_media",
            "brix_medio",
            "nivel_medio",
            "vazao_vapor_media",
            "vazao_alimentacao_media",
            "pureza_media",
            "condensado_medio",
            "ultimo_raw_id",
            "data_processamento",
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

        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo'
              AND TABLE_NAME = 'gold_kpi_cozedores_turno';
        """)
        gold_colunas_existentes = {row[0] for row in cursor.fetchall()}
        gold_faltantes = gold_colunas_obrigatorias - gold_colunas_existentes

        if gold_faltantes:
            raise RuntimeError(
                f"Tabela dbo.gold_kpi_cozedores_turno incompleta. "
                f"Colunas faltantes: {sorted(gold_faltantes)}"
            )

        log_json("info", "Estrutura CURATED/GOLD validada com sucesso")

    finally:
        cursor.close()
        conn.close()

# ---------------------------------------------------------
# ETL INCREMENTAL CURATED -> GOLD
# ---------------------------------------------------------
def etl_curated_to_gold():
    conn = criar_conexao_sql()
    cursor = conn.cursor()

    try:
        log_json("info", "Iniciando ETL incremental CURATED -> GOLD")

        cursor.execute("""
            SELECT ISNULL(MAX(ultimo_raw_id), 0)
            FROM dbo.gold_kpi_cozedores_turno;
        """)
        ultimo_raw_id_gold = cursor.fetchone()[0]

        log_json("info", "Último raw_id consolidado na GOLD identificado", {
            "ultimo_raw_id_gold": ultimo_raw_id_gold
        })

        # Criação explícita da tabela temporária em comando separado.
        cursor.execute("""
            IF OBJECT_ID('tempdb..#grupos_impactados') IS NOT NULL
                DROP TABLE #grupos_impactados;
        """)

        cursor.execute("""
            CREATE TABLE #grupos_impactados (
                data DATE NOT NULL,
                turno CHAR(1) NOT NULL,
                fluxo INT NOT NULL,
                cozedor INT NOT NULL
            );
        """)

        cursor.execute("""
            INSERT INTO #grupos_impactados (
                data,
                turno,
                fluxo,
                cozedor
            )
            SELECT DISTINCT TOP (?)
                data,
                turno,
                fluxo,
                cozedor
            FROM dbo.curated_cozedores
            WHERE id_raw > ?
            ORDER BY data, turno, fluxo, cozedor;
        """, ETL_BATCH_SIZE, ultimo_raw_id_gold)

        cursor.execute("SELECT COUNT(*) FROM #grupos_impactados;")
        qtd_grupos = cursor.fetchone()[0]

        if qtd_grupos == 0:
            conn.commit()
            log_json("info", "Nenhum novo grupo para processar na GOLD")
            return

        log_json("info", "Grupos impactados identificados", {
            "qtd_grupos": qtd_grupos
        })

        cursor.execute("""
            DELETE g
            FROM dbo.gold_kpi_cozedores_turno g
            INNER JOIN #grupos_impactados i
                ON g.data = i.data
               AND g.turno = i.turno
               AND g.fluxo = i.fluxo
               AND g.cozedor = i.cozedor;
        """)
        grupos_removidos = cursor.rowcount

        cursor.execute("""
            INSERT INTO dbo.gold_kpi_cozedores_turno (
                data,
                turno,
                fluxo,
                cozedor,
                qtd_registros,
                temperatura_media,
                pressao_vacuo_media,
                brix_medio,
                nivel_medio,
                vazao_vapor_media,
                vazao_alimentacao_media,
                pureza_media,
                condensado_medio,
                ultimo_raw_id,
                data_processamento
            )
            SELECT
                c.data,
                c.turno,
                c.fluxo,
                c.cozedor,
                COUNT(*) AS qtd_registros,
                ROUND(AVG(c.temperatura), 3) AS temperatura_media,
                ROUND(AVG(c.pressao_vacuo), 3) AS pressao_vacuo_media,
                ROUND(AVG(c.brix), 3) AS brix_medio,
                ROUND(AVG(c.nivel), 3) AS nivel_medio,
                ROUND(AVG(c.vazao_vapor), 3) AS vazao_vapor_media,
                ROUND(AVG(c.vazao_alimentacao), 3) AS vazao_alimentacao_media,
                ROUND(AVG(c.pureza), 3) AS pureza_media,
                ROUND(AVG(c.condensado), 3) AS condensado_medio,
                MAX(c.id_raw) AS ultimo_raw_id,
                CAST(SYSDATETIME() AS DATETIME2(0)) AS data_processamento
            FROM dbo.curated_cozedores c
            INNER JOIN #grupos_impactados i
                ON c.data = i.data
               AND c.turno = i.turno
               AND c.fluxo = i.fluxo
               AND c.cozedor = i.cozedor
            GROUP BY
                c.data,
                c.turno,
                c.fluxo,
                c.cozedor;
        """)
        grupos_inseridos = cursor.rowcount

        conn.commit()

        log_json("info", "ETL incremental CURATED -> GOLD finalizado com sucesso", {
            "grupos_impactados": qtd_grupos,
            "grupos_removidos": grupos_removidos,
            "grupos_inseridos": grupos_inseridos,
            "batch_size": ETL_BATCH_SIZE
        })

    except Exception as exc:
        conn.rollback()
        log_json("error", "Erro no ETL incremental CURATED -> GOLD", {
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
    dag_id="etl_curated_to_gold",
    description="Carga incremental da CURATED para GOLD.",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cozedores", "sqlserver", "gold", "kpi", "industrial-data"],
) as dag:

    validar_estrutura = PythonOperator(
        task_id="validar_estrutura_curated_gold",
        python_callable=validar_estrutura_gold,
    )

    executar_etl = PythonOperator(
        task_id="executar_etl_incremental_curated_to_gold",
        python_callable=etl_curated_to_gold,
    )

    validar_estrutura >> executar_etl
