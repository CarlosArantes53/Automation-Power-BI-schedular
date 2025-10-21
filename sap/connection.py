import logging
from hdbcli import dbapi

def conectar_sap(dados):
    address = dados.get("HOST")
    port = dados.get("PORT")
    user = dados.get("USUARIO")
    password = dados.get("SENHA")

    if not all([address, port, user, password]):
        raise RuntimeError("Credenciais SAP incompletas. Verifique address/port/user/password.")

    logging.info("Conectando ao SAP HANA...")
    conn = dbapi.connect(address=address, port=int(port), user=user, password=password)
    logging.info("Conexão SAP estabelecida.")
    return conn

def executar_consulta(conn, consulta):
    cursor = conn.cursor()
    try:
        cursor.execute(consulta)
        cols = [d[0] for d in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return cols, rows
    except dbapi.Error as e:
        logging.error(f"Erro de SQL ao executar a consulta. Detalhes: {e}")
        raise
    finally:
        cursor.close()

def executar_consulta_em_chunks(conn, consulta, chunk_size=10000):
    cursor = conn.cursor()
    try:
        cursor.execute(consulta)
        cols = [d[0] for d in cursor.description] if cursor.description else []
        
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield cols, rows
            
    except dbapi.Error as e:
        logging.error(f"Erro de SQL ao executar a consulta em chunks. Detalhes: {e}")
        raise
    finally:
        cursor.close()