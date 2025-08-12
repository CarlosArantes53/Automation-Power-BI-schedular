import json
import logging

HORARIO_PERMITIDO = {
    "dias": [0, 1, 2, 3, 4],
    "hora_inicio": 7,
    "hora_fim": 18,
}

FIREBASE_CRED_JSON = "sgdd-riofer-firebase-adminsdk-fbsvc-5ea260b23b.json"
SECRET_KEY_FILE = "secret.key"
TAREFAS_JSON_FILE = "tarefas.json"

def carregar_tarefas():
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