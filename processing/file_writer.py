import os
import logging
import tempfile
import pandas as pd
import sqlite3
from sqlite3 import Error as SqliteError

def salvar_xlsx_atomic(path, df_chunks):
    tmp_file = None
    total_rows = 0
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx", prefix="sync_") as tmp:
            tmp_file = tmp.name

        with pd.ExcelWriter(tmp_file, engine='openpyxl', datetime_format='YYYY-MM-DD HH:MM:SS') as writer:
            header = True
            writer.book.create_sheet('data')
            
            start_row = 0
            for df_chunk in df_chunks:
                sheet_name = 'data'
                if not header:
                    start_row = writer.sheets[sheet_name].max_row
                
                df_chunk.to_excel(writer, index=False, sheet_name=sheet_name, header=header, startrow=start_row)
                total_rows += len(df_chunk)
                header = False
        
        if 'Sheet' in writer.book.sheetnames and 'data' in writer.book.sheetnames:
             del writer.book['Sheet']
        
        writer.save()

        os.replace(tmp_file, path)
        logging.info(f"Arquivo salvo: {path} (total de linhas={total_rows})")
        return True
    except Exception as e:
        logging.exception(f"Falha ao salvar o arquivo XLSX em chunks '{path}': {e}")
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

def salvar_db_atomic(path, df_chunks):
    tmp_file = None
    conn = None
    total_rows = 0
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db", prefix="sync_") as tmp:
            tmp_file = tmp.name

        conn = sqlite3.connect(tmp_file)
        
        header = True
        table_name = 'data'
        
        for df_chunk in df_chunks:
            action = 'replace' if header else 'append'
            
            df_chunk.to_sql(table_name, conn, if_exists=action, index=False)
            
            total_rows += len(df_chunk)
            header = False
        
        conn.commit()
        conn.close()
        conn = None

        os.replace(tmp_file, path)
        logging.info(f"Arquivo salvo: {path} (total de linhas={total_rows})")
        return True

    except (SqliteError, pd.io.sql.DatabaseError) as e:
        logging.exception(f"Falha ao salvar o arquivo DB em chunks '{path}': {e}")
        return False
    except Exception as e:
        logging.exception(f"Falha genérica ao salvar o arquivo DB '{path}': {e}")
        return False
    finally:
        if conn:
            try:
                conn.close()
            except SqliteError:
                pass
        if tmp_file and os.path.exists(tmp_file):
            try:
                os.remove(tmp_file)
            except OSError:
                pass

def salvar_atomicamente(path, df_chunks, formato):
    if formato == 'xlsx':
        return salvar_xlsx_atomic(path, df_chunks)
    elif formato == 'csv':
        return salvar_csv_atomic(path, df_chunks)
    elif formato == 'parquet':
        return salvar_parquet_atomic(path, df_chunks)
    elif formato == 'db':
        return salvar_db_atomic(path, df_chunks)
    else:
        logging.error(f"Formato de arquivo '{formato}' não suportado.")
        return False