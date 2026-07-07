# Bibliotecas
# ---------------------------------------------------------
import os
import csv
import json
import logging
import tempfile
from datetime import datetime, timedelta
import pyodbc
from minio import Minio
from minio.error import S3Error
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------
# CONFIGURAÇÕES SQL SERVER
# ---------------------------------------------------------
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "192.168.1.6")
SQLSERVER_DB = os.getenv("SQLSERVER_DB", "Cozedores")
SQLSERVER_USER = os.getenv("SQLSERVER_USER", "LadrilhandoData")
SQLSERVER_PWD = os.getenv("SQLSERVER_PWD", "**********")

# ---------------------------------------------------------
# CONFIGURAÇÕES MINIO
# ---------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "********")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "cozedores-curated-daily")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
# Prefixo/pasta lógica dentro do bucket
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "curated/cozedores")


# ---------------------------------------------------------
# CONTROLE DE EXPORTAÇÃO
# ---------------------------------------------------------

# Se vazio, exporta todas as datas existentes na CURATED.
# Se preenchido, exporta somente datas >= EXPORT_START_DATE.
# Formato: YYYY-MM-DD
EXPORT_START_DATE = os.getenv("EXPORT_START_DATE", "")

# Se preenchido, exporta somente datas <= EXPORT_END_DATE.
# Formato: YYYY-MM-DD
EXPORT_END_DATE = os.getenv("EXPORT_END_DATE", "")


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
# CONEXÕES
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

def criar_cliente_minio():
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

# ---------------------------------------------------------
# VALIDAÇÃO
# ---------------------------------------------------------
def validar_estrutura_e_destino():
    conn = criar_conexao_sql()
    cursor = conn.cursor()

    try:
        colunas_obrigatorias = {
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

        colunas_existentes = {row[0] for row in cursor.fetchall()}
        faltantes = colunas_obrigatorias - colunas_existentes

        if faltantes:
            raise RuntimeError(
                f"Tabela dbo.curated_cozedores incompleta. "
                f"Colunas faltantes: {sorted(faltantes)}"
            )

        minio_client = criar_cliente_minio()

        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            log_json("info", "Bucket MinIO criado", {"bucket": MINIO_BUCKET})
        else:
            log_json("info", "Bucket MinIO validado", {"bucket": MINIO_BUCKET})

        log_json("info", "Validação SQL Server/MinIO concluída com sucesso")

    finally:
        cursor.close()
        conn.close()

# ---------------------------------------------------------
# DATAS A EXPORTAR
# ---------------------------------------------------------
def listar_datas_para_exportacao(cursor):
    filtros = []
    parametros = []

    if EXPORT_START_DATE:
        filtros.append("data >= ?")
        parametros.append(EXPORT_START_DATE)

    if EXPORT_END_DATE:
        filtros.append("data <= ?")
        parametros.append(EXPORT_END_DATE)

    where_clause = ""
    if filtros:
        where_clause = "WHERE " + " AND ".join(filtros)

    sql = f"""
        SELECT DISTINCT data
        FROM dbo.curated_cozedores
        {where_clause}
        ORDER BY data;
    """

    cursor.execute(sql, parametros)
    return [row[0] for row in cursor.fetchall()]

# ---------------------------------------------------------
# EXPORTAÇÃO CSV PARTICIONADA POR DATA
# ---------------------------------------------------------
def exportar_curated_para_minio():
    conn = criar_conexao_sql()
    cursor = conn.cursor()
    minio_client = criar_cliente_minio()

    try:
        log_json("info", "Iniciando exportação CURATED -> MinIO")

        datas = listar_datas_para_exportacao(cursor)

        if not datas:
            log_json("info", "Nenhuma data encontrada para exportação")
            return

        total_arquivos = 0
        total_linhas = 0

        for data_particao in datas:
            data_str = data_particao.strftime("%Y-%m-%d")

            cursor.execute("""
                SELECT
                    id,
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
                FROM dbo.curated_cozedores
                WHERE data = ?
                ORDER BY fluxo, cozedor, hora, id_raw;
            """, data_particao)

            colunas = [column[0] for column in cursor.description]
            linhas = cursor.fetchall()

            if not linhas:
                continue

            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".csv",
                prefix=f"cozedores_{data_str}_",
                newline="",
                encoding="utf-8",
                delete=False,
            ) as tmp_file:
                writer = csv.writer(
                    tmp_file,
                    delimiter=";",
                    quotechar='"',
                    quoting=csv.QUOTE_MINIMAL
                )
                writer.writerow(colunas)

                for row in linhas:
                    writer.writerow([
                        valor.isoformat() if hasattr(valor, "isoformat") else valor
                        for valor in row
                    ])

                tmp_path = tmp_file.name

            object_name = (
                f"{MINIO_PREFIX}/"
                f"data={data_str}/"
                f"cozedores_curated_{data_str}.csv"
            )

            minio_client.fput_object(
                bucket_name=MINIO_BUCKET,
                object_name=object_name,
                file_path=tmp_path,
                content_type="text/csv",
            )

            try:
                os.remove(tmp_path)
            except OSError:
                pass

            qtd_linhas = len(linhas)
            total_arquivos += 1
            total_linhas += qtd_linhas

            log_json("info", "Arquivo CSV exportado para MinIO", {
                "data": data_str,
                "bucket": MINIO_BUCKET,
                "object_name": object_name,
                "linhas": qtd_linhas
            })

        log_json("info", "Exportação CURATED -> MinIO finalizada com sucesso", {
            "arquivos_exportados": total_arquivos,
            "linhas_exportadas": total_linhas,
            "bucket": MINIO_BUCKET,
            "prefix": MINIO_PREFIX
        })

    except S3Error as exc:
        log_json("error", "Erro MinIO/S3 durante exportação", {
            "exception": str(exc)
        })
        raise

    except Exception as exc:
        log_json("error", "Erro geral durante exportação CURATED -> MinIO", {
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
    dag_id="export_curated_cozedores_to_minio",
    description="Exporta curated para MinIO em CSV particionado.",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cozedores", "curated", "minio", "csv", "industrial-data"],
) as dag:

    validar = PythonOperator(
        task_id="validar_estrutura_sqlserver_minio",
        python_callable=validar_estrutura_e_destino,
    )

    exportar = PythonOperator(
        task_id="exportar_curated_para_minio_csv_particionado",
        python_callable=exportar_curated_para_minio,
    )

    validar >> exportar
