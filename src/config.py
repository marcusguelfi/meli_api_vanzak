# src/config.py
from __future__ import annotations
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urlparse
import os
import logging

# Raiz do projeto: .../meli_api_vanzak
ROOT_DIR = Path(__file__).resolve().parents[1]
DOTENV_PATH = ROOT_DIR / ".env"

# Carrega .env explicitamente (env do sistema continua tendo precedência, override=False)
load_dotenv(dotenv_path=str(DOTENV_PATH), override=False)

def _env_any(*keys: str, default: str | None = None):
    """Lê a primeira variável disponível entre várias chaves alternativas."""
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return v.strip()
    return default

def _warn_missing(name: str, value):
    if not value:
        logging.warning(
            "[config] Variável %s ausente. .env existe? %s  caminho: %s",
            name, DOTENV_PATH.exists(), DOTENV_PATH
        )

# --- Credenciais Mercado Livre ---
ML_CLIENT_ID     = _env_any("MELI_CLIENT_ID", "ML_CLIENT_ID", "CLIENT_ID")
ML_CLIENT_SECRET = _env_any("MELI_CLIENT_SECRET", "ML_CLIENT_SECRET", "CLIENT_SECRET")
ML_REDIRECT_URI  = _env_any("MELI_REDIRECT_URI", "ML_REDIRECT_URI", "REDIRECT_URI")

# --- Execução do pipeline ---
ADVERTISER_ID  = _env_any("ADVERTISER_ID")
SITE_ID        = _env_any("SITE_ID", default="MLB")
BACKFILL_DAYS  = int(_env_any("BACKFILL_DAYS", default="30"))

# --- Upload para Google Apps Script ---
GOOGLE_APPSCRIPT_URL   = _env_any("GOOGLE_APPSCRIPT_URL")
GOOGLE_APPSCRIPT_TOKEN = _env_any("GOOGLE_APPSCRIPT_TOKEN")

# --- Logging ---
LOG_LEVEL = _env_any("LOG_LEVEL", default="INFO")

# --- Paths auxiliares ---
RAW_DIR = str(ROOT_DIR / "data" / "raw")
LOG_DIR = str(ROOT_DIR / "data" / "logs")
PROCESSED_DIR = str(ROOT_DIR / "data" / "processed")

def ensure_dirs(create: bool = True):
    """Garante que diretórios de trabalho existam."""
    for p in (RAW_DIR, LOG_DIR, PROCESSED_DIR):
        path = Path(p)
        if create:
            path.mkdir(parents=True, exist_ok=True)

def _valid_redirect(uri: str) -> bool:
    try:
        u = urlparse(uri)
        return bool(u.scheme and u.netloc)
    except Exception:
        return False

def assert_config():
    # OAuth
    _warn_missing("MELI_CLIENT_ID", ML_CLIENT_ID)
    _warn_missing("MELI_CLIENT_SECRET", ML_CLIENT_SECRET)
    _warn_missing("MELI_REDIRECT_URI", ML_REDIRECT_URI)
    if not (ML_CLIENT_ID and ML_CLIENT_SECRET and ML_REDIRECT_URI):
        raise RuntimeError("Config incompleta: defina MELI_CLIENT_ID, MELI_CLIENT_SECRET e MELI_REDIRECT_URI no .env")
    if not _valid_redirect(ML_REDIRECT_URI):
        raise RuntimeError(f"REDIRECT_URI inválido: {ML_REDIRECT_URI}")

    # Pipeline mínimos
    _warn_missing("ADVERTISER_ID", ADVERTISER_ID)
    if not ADVERTISER_ID:
        logging.warning("[config] ADVERTISER_ID ausente — alguns jobs podem falhar sem essa info.")

    # Upload (opcional; jobs pulam upload se vazio)
    if not GOOGLE_APPSCRIPT_URL:
        logging.info("[config] GOOGLE_APPSCRIPT_URL ausente — uploads para Sheets serão pulados.")

    # Logging
    os.environ.setdefault("LOG_LEVEL", LOG_LEVEL)

def _mask(s: str, keep: int = 3) -> str:
    if not s:
        return ""
    if len(s) <= keep:
        return "*" * len(s)
    return s[:keep] + "…" + f"({len(s)}c)"

def debug_print(show_values: bool = False):
    print("ROOT_DIR     :", ROOT_DIR)
    print(".env path    :", DOTENV_PATH, "| exists:", DOTENV_PATH.exists())
    if show_values:
        print("CLIENT_ID    :", _mask(ML_CLIENT_ID))
        print("CLIENT_SECRET:", _mask(ML_CLIENT_SECRET))
        print("REDIRECT_URI :", ML_REDIRECT_URI)
        print("APPSCRIPT_URL:", GOOGLE_APPSCRIPT_URL)
        print("APPSCRIPT_TK :", _mask(GOOGLE_APPSCRIPT_TOKEN))
        print("ADVERTISER_ID:", ADVERTISER_ID)
        print("SITE_ID      :", SITE_ID)
        print("BACKFILL_DAYS:", BACKFILL_DAYS)
    else:
        print("CLIENT_ID    :", bool(ML_CLIENT_ID))
        print("CLIENT_SECRET:", bool(ML_CLIENT_SECRET))
        print("REDIRECT_URI :", bool(ML_REDIRECT_URI))
        print("APPSCRIPT_URL:", bool(GOOGLE_APPSCRIPT_URL))
        print("APPSCRIPT_TK :", bool(GOOGLE_APPSCRIPT_TOKEN))
        print("ADVERTISER_ID:", bool(ADVERTISER_ID))
        print("SITE_ID      :", SITE_ID)
        print("BACKFILL_DAYS:", BACKFILL_DAYS)
    print("RAW_DIR      :", RAW_DIR)
    print("LOG_DIR      :", LOG_DIR)
    print("PROCESSED    :", PROCESSED_DIR)
