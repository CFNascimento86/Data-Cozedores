import os
import snap7
from snap7.util import get_real, get_int
from snap7.error import Snap7Exception
import pyodbc
import json
import time
from datetime import datetime

# ---------------------------------------------------------
# CONFIGURAÇÕES VIA VARIÁVEIS DE AMBIENTE
# ---------------------------------------------------------

PLC_IP = os.getenv("PLC_IP", "192.168.0.10")
RACK = int(os.getenv("PLC_RACK", "0"))
SLOT = int(os.getenv("PLC_SLOT", "2"))

SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "DESKTOP-9PICUGC\\SQLEXPRESS")
SQLSERVER_DB = os.getenv("SQLSERVER_DB", "Cozedores")
SQLSERVER_USER = os.getenv("SQLSERVER_USER", "LadrilhandoData")
SQLSERVER_PWD = os.getenv("SQLSERVER_PWD", "**********")

DB_LIST = list(range(101, 106)) + list(range(201, 206))
DB_SIZE = 34

# ---------------------------------------------------------
# CONEXÃO SQL SERVER
# ---------------------------------------------------------

def criar_conexao_sql():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SQLSERVER_HOST},1433;"
        f"DATABASE={SQLSERVER_DB};"
        f"UID={SQLSERVER_USER};"
        f"PWD={SQLSERVER_PWD};"
    )
    return pyodbc.connect(conn_str)

conn = criar_conexao_sql()
cursor = conn.cursor()

# ---------------------------------------------------------
# LOG JSON
# ---------------------------------------------------------

def log_json(level, message, extra=None):
    log = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level,
        "message": message,
        "extra": extra
    }
    print(json.dumps(log), flush=True)

# ---------------------------------------------------------
# LEITURA DO DB
# ---------------------------------------------------------

def ler_db(client, db_number):
    try:
        data = client.db_read(db_number, 0, DB_SIZE)
        fluxo = 1 if db_number < 200 else 2
        cozedor_local = db_number % 100
        cozedor_id = cozedor_local if fluxo == 1 else cozedor_local + 5

        return {
            "timestamp_utc": datetime.utcnow(),
            "db": db_number,
            "cozedor": cozedor_id,
            "fluxo": fluxo,
            "temperatura": get_real(data, 0),
            "pressao_vacuo": get_real(data, 4),
            "brix": get_real(data, 8),
            "nivel": get_real(data, 12),
            "vazao_vapor": get_real(data, 16),
            "vazao_alimentacao": get_real(data, 20),
            "estado_batelada": get_int(data, 24),
            "pureza": get_real(data, 26),
            "condensado": get_real(data, 30)
        }

    except Snap7Exception as e:
        log_json("error", f"Erro ao ler DB{db_number}", {"exception": str(e)})
        return None

# ---------------------------------------------------------
# INSERÇÃO NO SQL SERVER
# ---------------------------------------------------------

def inserir_raw_sqlserver(dados):
    global conn, cursor
    try:
        cursor.execute("""
            INSERT INTO dbo.raw_cozedores (
                timestamp_utc, db, cozedor, fluxo, temperatura, pressao_vacuo,
                brix, nivel, vazao_vapor, vazao_alimentacao,
                estado_batelada, pureza, condensado
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            dados["timestamp_utc"], dados["db"], dados["cozedor"], dados["fluxo"],
            dados["temperatura"], dados["pressao_vacuo"], dados["brix"],
            dados["nivel"], dados["vazao_vapor"], dados["vazao_alimentacao"],
            dados["estado_batelada"], dados["pureza"], dados["condensado"]
        ))
        conn.commit()
    except Exception as e:
        log_json("error", "Erro ao inserir no SQL Server", {"exception": str(e)})
        # tenta recriar conexão
        try:
            conn.close()
        except:
            pass
        time.sleep(3)
        nova = criar_conexao_sql()
        globals()["conn"] = nova
        globals()["cursor"] = nova.cursor()

# ---------------------------------------------------------
# LOOP PRINCIPAL
# ---------------------------------------------------------

def main():
    client = snap7.client.Client()
    log_json("info", "Serviço Edge Snap7 iniciado")

    while True:
        try:
            if not client.get_connected():
                log_json("info", f"Conectando ao CLP {PLC_IP}...")
                client.connect(PLC_IP, RACK, SLOT)
                log_json("info", "Conectado ao CLP")

            for db in DB_LIST:
                dados = ler_db(client, db)
                if dados:
                    inserir_raw_sqlserver(dados)
                    log_json("info", "Registro inserido", {
                        "fluxo": dados["fluxo"],
                        "cozedor": dados["cozedor"],
                        "db": dados["db"]
                    })

            time.sleep(1)

        except Exception as e:
            log_json("error", "Erro geral no loop", {"exception": str(e)})
            time.sleep(5)
            try:
                client.disconnect()
            except:
                pass

if __name__ == "__main__":
    main()
