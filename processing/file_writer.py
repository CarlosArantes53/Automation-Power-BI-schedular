import os
import logging
import tempfile
import pandas as pd
import sqlite3
from sqlite3 import Error as SqliteError
import shutil

def salvar_xlsx_atomic(path, df_chunks, sheet_name):

    tmp_file = None
    total_rows = 0
    sheet_name_to_use = sheet_name or 'data'
    
    try:
        full_df = pd.concat(df_chunks, ignore_index=True)
        total_rows = len(full_df)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx", prefix="sync_") as tmp:
            tmp_file = tmp.name

        file_exists = os.path.exists(path)
        if file_exists:
            shutil.copy(path, tmp_file)
            
        mode = 'a' if file_exists else 'w'
        
        with pd.ExcelWriter(tmp_file, 
                              engine='openpyxl', 
                              mode=mode, 
                              if_sheet_exists='replace', 
                              datetime_format='YYYY-MM-DD HH:MM:SS') as writer:
            
            full_df.to_excel(writer, index=False, sheet_name=sheet_name_to_use, header=True)
            
            if not file_exists and 'Sheet' in writer.book.sheetnames and sheet_name_to_use != 'Sheet':
                del writer.book['Sheet']
        
        os.makedirs(os.path.dirname(path), exist_ok=True)
        os.replace(tmp_file, path)
        logging.info(f"Arquivo salvo: {path} (Planilha: '{sheet_name_to_use}', total de linhas={total_rows})")
        return True
    except Exception as e:
        logging.exception(f"Falha ao salvar o arquivo XLSX '{path}' (Planilha: '{sheet_name_to_use}'): {e}")
        return False
    finally:
        if tmp_file and os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except OSError:
                pass

def salvar_csv_atomic(path, df_chunks):
    tmp_file = None
    total_rows = 0
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".csv", prefix="sync_", encoding='utf-8') as tmp:
            tmp_file = tmp.name

        header = True
        for df_chunk in df_chunks:
            df_chunk.to_csv(tmp_file, index=False, header=header, mode='a', lineterminator='\n')
            total_rows += len(df_chunk)
            header = False

        os.replace(tmp_file, path)
        logging.info(f"Arquivo salvo: {path} (total de linhas={total_rows})")
        return True
    except Exception as e:
        logging.exception(f"Falha ao salvar o arquivo CSV em chunks '{path}': {e}")
        return False
    finally:
        if tmp_file and os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except OSError:
                pass

def salvar_parquet_atomic(path, df_chunks):
    tmp_file = None
    total_rows = 0
    try:
        full_df = pd.concat(df_chunks, ignore_index=True)
        total_rows = len(full_df)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet", prefix="sync_") as tmp:
            tmp_file = tmp.name
        
        full_df.to_parquet(tmp_file, index=False, engine='pyarrow')

        os.makedirs(os.path.dirname(path), exist_ok=True)

        os.replace(tmp_file, path)
        logging.info(f"Arquivo salvo: {path} (total de linhas={total_rows})")
        return True
    except Exception as e:
        logging.exception(f"Falha ao salvar o arquivo Parquet '{path}': {e}")
        return False
    finally:
        if tmp_file and os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except OSError:
                pass

def salvar_db_atomic(path, df_chunks, table_name):
    tmp_file = None
    conn_src = None
    conn_dst = None
    conn = None
    total_rows = 0
    table_name_to_use = table_name or 'data'

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db", prefix="sync_") as tmp:
            tmp_file = tmp.name

        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        if os.path.exists(path) and os.path.getsize(path) > 0:
            conn_src = sqlite3.connect(path)
            conn_dst = sqlite3.connect(tmp_file)
            conn_src.backup(conn_dst)
            conn_src.close()
            conn_dst.close()
            conn_src, conn_dst = None, None

        conn = sqlite3.connect(tmp_file)
        
        header = True
        
        for df_chunk in df_chunks:
            action = 'replace' if header else 'append'
            
            df_chunk.to_sql(table_name_to_use, conn, if_exists=action, index=False)
            
            total_rows += len(df_chunk)
            header = False
        
        conn.commit()
        conn.close()
        conn = None

        os.replace(tmp_file, path)
        logging.info(f"Arquivo salvo: {path} (Tabela: '{table_name_to_use}', total de linhas={total_rows})")
        return True

    except (SqliteError, pd.io.sql.DatabaseError) as e:
        logging.exception(f"Falha ao salvar o arquivo DB '{path}' (Tabela: '{table_name_to_use}'): {e}")
        return False
    except Exception as e:
        logging.exception(f"Falha genérica ao salvar o arquivo DB '{path}': {e}")
        return False
    finally:
        if conn_src:
            conn_src.close()
        if conn_dst:
            conn_dst.close()
        if conn:
            conn.close()
        if tmp_file and os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except OSError:
                pass

def salvar_atomicamente(path, df_chunks, formato, target_name='data'):
    if formato == 'xlsx':
        return salvar_xlsx_atomic(path, df_chunks, target_name)
    elif formato == 'csv':
        return salvar_csv_atomic(path, df_chunks)
    elif formato == 'parquet':
        return salvar_parquet_atomic(path, df_chunks)
    elif formato == 'db':
        return salvar_db_atomic(path, df_chunks, target_name)
    else:
        logging.error(f"Formato de arquivo '{formato}' não suportado.")
        return False