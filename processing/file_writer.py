import os
import logging
import tempfile
import pandas as pd

def salvar_xlsx_atomic(path, df_chunks):
    tmp_file = None
    total_rows = 0
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx", prefix="sync_") as tmp:
            tmp_file = tmp.name

        with pd.ExcelWriter(tmp_file, engine='openpyxl', datetime_format='YYYY-MM-DD HH:MM:SS') as writer:
            header = True
            for df_chunk in df_chunks:
                df_chunk.to_excel(writer, index=False, sheet_name='data', header=header, startrow=writer.sheets['data'].max_row if not header else 0)
                total_rows += len(df_chunk)
                header = False

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

def salvar_atomicamente(path, df_chunks, formato):
    if formato == 'xlsx':
        return salvar_xlsx_atomic(path, df_chunks)
    elif formato == 'csv':
        return salvar_csv_atomic(path, df_chunks)
    elif formato == 'parquet':
        return salvar_parquet_atomic(path, df_chunks)
    else:
        logging.error(f"Formato de arquivo '{formato}' não suportado.")
        return False