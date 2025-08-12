import time
import logging
import datetime
import pandas as pd
import threading

from config.settings import carregar_tarefas, TAREFAS_JSON_FILE, HORARIO_PERMITIDO
from config.credentials import obter_credenciais_sap
from sap.connection import conectar_sap, executar_consulta_em_chunks
from processing.dataframe_handler import aplicar_formatacoes_df
from processing.excel_writer import salvar_xlsx_em_chunks_atomic
from utils.scheduler import dentro_janela_permitida, proxima_janela_inicio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

ERROR_RETRY_INTERVAL = 60
JSON_CHECK_INTERVAL = 3600
processamento_lock = threading.Lock()

def calcular_proxima_execucao_agendada(agora_dt, horarios):
    horarios_ordenados = sorted([datetime.datetime.strptime(h, '%H:%M').time() for h in horarios])

    for horario in horarios_ordenados:
        proximo_dt = agora_dt.replace(hour=horario.hour, minute=horario.minute, second=0, microsecond=0)
        if proximo_dt > agora_dt:
            return proximo_dt.timestamp()

    proximo_dia = agora_dt + datetime.timedelta(days=1)
    while proximo_dia.weekday() not in HORARIO_PERMITIDO['dias']:
        proximo_dia += datetime.timedelta(days=1)

    primeiro_horario = horarios_ordenados[0]
    return proximo_dia.replace(hour=primeiro_horario.hour, minute=primeiro_horario.minute, second=0, microsecond=0).timestamp()

def processar_tarefa(dados_conn, tarefa):
    tabela = tarefa["tabela"]
    consulta = tarefa["consulta_sap"]
    colunas_esperadas = tarefa.get("colunas")
    xlsx_opts = tarefa.get('xlsx_options', {})
    chunk_size = tarefa.get('chunk_size', 10000)
    filename = f"{tabela}.xlsx"
    conn = None

    try:
        logging.info(f"Iniciando processamento da tarefa: '{tabela}'")
        conn = conectar_sap(dados_conn)

        def processar_chunks():
            data_iterator = executar_consulta_em_chunks(conn, consulta, chunk_size)
            primeiro_chunk = True
            for cols, rows in data_iterator:
                if primeiro_chunk and not rows:
                    logging.warning(f"Consulta retornou 0 linhas para '{tabela}'. Nenhum arquivo será gerado.")
                    raise StopIteration
                
                df_novo = pd.DataFrame(rows, columns=cols)
                if colunas_esperadas:
                    df_novo = df_novo.reindex(columns=colunas_esperadas)
                
                df_formatado = aplicar_formatacoes_df(df_novo, xlsx_opts)
                yield df_formatado
                primeiro_chunk = False
        
        chunk_generator = processar_chunks()
        try:
            primeiro_item = next(chunk_generator)
        except StopIteration:
            return True

        import itertools
        salvar_xlsx_em_chunks_atomic(filename, itertools.chain([primeiro_item], chunk_generator), xlsx_opts)
        
        return True

    except Exception as e:
        logging.error(f"Falha ao processar a tarefa '{tabela}'. Causa: {e}")
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def main():
    logging.info("Iniciando sincronizador XLSX (CTRL+C para parar).")

    try:
        dados_conn = obter_credenciais_sap()
    except Exception:
        logging.exception("Falha crítica ao obter credenciais do Firebase. Abortando.")
        return

    tarefas_ativas = []
    proximo_check_json_ts = 0

    while True:
        try:
            if not processamento_lock.acquire(blocking=False):
                time.sleep(1)
                continue

            try:
                agora_ts = time.time()
                agora_dt = datetime.datetime.fromtimestamp(agora_ts)

                if agora_ts >= proximo_check_json_ts:
                    logging.info(f"Verificando o arquivo '{TAREFAS_JSON_FILE}' para atualizações...")
                    novas_tarefas_config = carregar_tarefas()
                    
                    if novas_tarefas_config is not None:
                        configs_atuais = [t['config'] for t in tarefas_ativas]
                        if novas_tarefas_config != configs_atuais:
                            logging.info("Detectada nova configuração. Atualizando a lista de tarefas.")
                            tarefas_ativas = []
                            for t in novas_tarefas_config:
                                horarios = t.get("horarios_execucao")
                                proxima_execucao = (
                                    calcular_proxima_execucao_agendada(agora_dt, horarios)
                                    if horarios
                                    else agora_ts
                                )
                                tarefas_ativas.append({'config': t, 'proxima_execucao': proxima_execucao})

                    elif not tarefas_ativas:
                        logging.warning(f"Nenhuma tarefa configurada. Tentando novamente em {JSON_CHECK_INTERVAL}s.")

                    proximo_check_json_ts = agora_ts + JSON_CHECK_INTERVAL

                if not tarefas_ativas:
                    time.sleep(5)
                    continue

                if not dentro_janela_permitida(agora_dt):
                    next_start_ts = proxima_janela_inicio(agora_dt)
                    logging.info(f"Fora da janela permitida. Próxima execução às {datetime.datetime.fromtimestamp(next_start_ts)}.")
                    sleep_secs = max(1, next_start_ts - agora_ts)
                    time.sleep(sleep_secs)
                    continue
                
                tarefa_executada_neste_ciclo = False
                for item in tarefas_ativas:
                    if agora_ts >= item['proxima_execucao']:
                        tarefa_executada_neste_ciclo = True
                        tarefa_config = item['config']
                        sucesso = processar_tarefa(dados_conn, tarefa_config)
                        
                        horarios = tarefa_config.get("horarios_execucao")
                        if sucesso:
                            if horarios:
                                proxima_exec = calcular_proxima_execucao_agendada(agora_dt, horarios)
                                item['proxima_execucao'] = proxima_exec
                                proxima_exec_dt = datetime.datetime.fromtimestamp(proxima_exec)
                                logging.info(f"Tarefa '{tarefa_config['tabela']}' concluída. Próxima execução agendada para {proxima_exec_dt.strftime('%Y-%m-%d %H:%M:%S')}.")
                            else:
                                intervalo = tarefa_config.get("intervalo", 300)
                                proxima_exec_ts = agora_ts + intervalo
                                item['proxima_execucao'] = proxima_exec_ts
                                proxima_exec_dt = datetime.datetime.fromtimestamp(proxima_exec_ts)
                                logging.info(f"Tarefa '{tarefa_config['tabela']}' concluída. Próxima execução às {proxima_exec_dt.strftime('%Y-%m-%d %H:%M:%S')}.")
                        else:
                            item['proxima_execucao'] = agora_ts + ERROR_RETRY_INTERVAL
                            logging.error(f"Tarefa '{tarefa_config['tabela']}' falhou. Nova tentativa agendada em {ERROR_RETRY_INTERVAL}s.")
                
                if tarefa_executada_neste_ciclo:
                    if tarefas_ativas:
                        proxima_tarefa_agendada = min(tarefas_ativas, key=lambda t: t['proxima_execucao'])
                        nome_tarefa = proxima_tarefa_agendada['config']['tabela']
                        proximo_ts = proxima_tarefa_agendada['proxima_execucao']
                        proximo_dt = datetime.datetime.fromtimestamp(proximo_ts)
                        logging.info(f"Próxima tarefa na fila: '{nome_tarefa}', agendada para {proximo_dt.strftime('%Y-%m-%d %H:%M:%S')}.")

            finally:
                processamento_lock.release()

            time.sleep(1)

        except KeyboardInterrupt:
            logging.info("Interrupção pelo usuário. Encerrando.")
            break
        except Exception:
            logging.exception("Erro inesperado no loop principal. O processo continuará.")
            time.sleep(10)

if __name__ == "__main__":
    main()