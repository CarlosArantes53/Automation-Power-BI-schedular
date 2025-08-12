import json
import logging
import os
from dotenv import load_dotenv

load_dotenv()

HORARIO_PERMITIDO = {
    "dias": [0, 1, 2, 3, 4],
    "hora_inicio": 6,
    "hora_fim": 18,
}

FIREBASE_CRED_JSON = os.getenv("FIREBASE_CRED_JSON")
SECRET_KEY_FILE = os.getenv("SECRET_KEY_FILE")
TAREFAS_JSON_FILE = os.getenv("TAREFAS_JSON_FILE")

if not all([FIREBASE_CRED_JSON, SECRET_KEY_FILE, TAREFAS_JSON_FILE]):
    raise ValueError("Uma ou mais variáveis de ambiente essenciais não foram definidas!")


def carregar_tarefas():
    if not TAREFAS_JSON_FILE:
        logging.error("A variável de ambiente 'TAREFAS_JSON_FILE' não está configurada.")
        return None

    try:
        with open(TAREFAS_JSON_FILE, 'r', encoding='utf-8') as f:
            tarefas = json.load(f)
        logging.info(f"Configuração de tarefas carregada com sucesso de '{TAREFAS_JSON_FILE}'.")
        return tarefas
    except FileNotFoundError:
        logging.error(f"Arquivo de tarefas '{TAREFAS_JSON_FILE}' não encontrado.")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao decodificar o arquivo JSON de tarefas: {e}")
        return None
    except Exception as e:
        logging.exception(f"Erro inesperado ao carregar as tarefas: {e}")
        return None

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    tarefas_carregadas = carregar_tarefas()
    if tarefas_carregadas:
        print("Tarefas carregadas:", tarefas_carregadas)
    else:
        print("Falha ao carregar as tarefas.")