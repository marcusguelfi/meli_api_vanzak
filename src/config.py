# src/config.py
from pathlib import Path
from dotenv import load_dotenv
import os
import logging

# Raiz do projeto: .../meli_api_vanzak
ROOT_DIR = Path(__file__).resolve().parents[1]
DOTENV_PATH = ROOT_DIR / ".env"

# Carrega .env explicitamente (não depende do diretório onde você roda o python)
load_dotenv(dotenv_path=str(DOTENV_PATH), override=False)

def _env_any(*keys: str):
    """Lê a primeira variável disponível entre várias chaves alternativas."""
    for k in keys:
        v = os.getenv(k)
        if v:
            return v.strip()
    return None

def _warn_missing(name: str, value):
    if not value:
        logging.warning(f"[config] Variável {name} ausente. .env existe? {DOTENV_PATH.exists()}  caminho: {DOTENV_PATH}")

# Aceita tanto MELI_* quanto ML_* por garantia
ML_CLIENT_ID      = _env_any("MELI_CLIENT_ID", "ML_CLIENT_ID", "CLIENT_ID")
ML_CLIENT_SECRET  = _env_any("MELI_CLIENT_SECRET", "ML_CLIENT_SECRET", "CLIENT_SECRET")
ML_REDIRECT_URI   = _env_any("MELI_REDIRECT_URI", "ML_REDIRECT_URI", "REDIRECT_URI")

RAW_DIR = str(ROOT_DIR / "data" / "raw")
LOG_DIR = str(ROOT_DIR / "data" / "logs")

def assert_config():
    _warn_missing("MELI_CLIENT_ID", ML_CLIENT_ID)
    _warn_missing("MELI_CLIENT_SECRET", ML_CLIENT_SECRET)
    _warn_missing("MELI_REDIRECT_URI", ML_REDIRECT_URI)
    if not (ML_CLIENT_ID and ML_CLIENT_SECRET and ML_REDIRECT_URI):
        raise RuntimeError("Config incompleta: defina MELI_CLIENT_ID, MELI_CLIENT_SECRET e MELI_REDIRECT_URI no .env")

def debug_print():
    print("ROOT_DIR     :", ROOT_DIR)
    print(".env path    :", DOTENV_PATH, "| exists:", DOTENV_PATH.exists())
    print("CLIENT_ID    :", bool(ML_CLIENT_ID))
    print("CLIENT_SECRET:", bool(ML_CLIENT_SECRET))
    print("REDIRECT_URI :", ML_REDIRECT_URI)
    print("RAW_DIR      :", RAW_DIR)
    print("LOG_DIR      :", LOG_DIR)
