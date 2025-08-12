import logging
import firebase_admin
from firebase_admin import credentials, firestore
from cryptography.fernet import Fernet
from .settings import FIREBASE_CRED_JSON, SECRET_KEY_FILE

def obter_credenciais_sap():
    logging.info("Inicializando conexão com Firebase para obter credenciais SAP...")
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_CRED_JSON)
        firebase_admin.initialize_app(cred)

    db = firestore.client()

    with open(SECRET_KEY_FILE, "rb") as key_file:
        key = key_file.read()
    fernet = Fernet(key)

    doc_ref = db.collection("configuracoes").document("conexao")
    doc = doc_ref.get()
    if not doc.exists:
        raise RuntimeError("Documento 'configuracoes/conexao' não encontrado no Firestore.")
    
    dados_enc = doc.to_dict()
    dados = {}
    for k, v in dados_enc.items():
        if v is None:
            dados[k] = None
        else:
            dados[k] = fernet.decrypt(v.encode()).decode()
            
    logging.info("Credenciais SAP obtidas e descriptografadas.")
    return dados