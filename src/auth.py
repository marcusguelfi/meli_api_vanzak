# src/auth.py
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

import requests

# Lê credenciais do config.py (você já testou que está OK)
from .config import ML_CLIENT_ID, ML_CLIENT_SECRET, ML_REDIRECT_URI

# Constantes OAuth
AUTH_BASE_URL = "https://auth.mercadolibre.com/authorization"
TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
SCOPE = (
    "offline_access "
    "read "
    "urn:ml:mktp:ads:/read-write "
    "urn:ml:mktp:orders-shipments:/read-write "
    "urn:ml:mktp:publish-sync:/read-write "
    "urn:ml:mktp:offers:/read-write "
    "urn:ml:mktp:metrics:/read-only "
    "urn:ml:mktp:invoices:/read-write "
    "urn:ml:mktp:comunication:/read-write "
    "write"
)

# Caminho único do tokens.json (sempre dentro de src/)
TOKENS_PATH = Path(__file__).parent / "tokens.json"


# ---------------------------
# Utilidades de armazenamento
# ---------------------------
def load_tokens() -> Dict[str, Any] | None:
    if not TOKENS_PATH.exists():
        return None
    with TOKENS_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_tokens(tokens: Dict[str, Any]) -> None:
    TOKENS_PATH.write_text(json.dumps(tokens, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info("💾 Tokens salvos em %s", TOKENS_PATH)


# ---------------------------
# URL de autorização (opcional – útil para debug)
# ---------------------------
def get_auth_url(state: str = "meli_state") -> str:
    """
    Monta a URL para o usuário autorizar o app e retornar um `code`.
    """
    from urllib.parse import urlencode

    params = {
        "response_type": "code",
        "client_id": ML_CLIENT_ID,
        "redirect_uri": ML_REDIRECT_URI,
        "state": state,
        # escopo é opcional na tela de consentimento do ML; mantemos aqui por clareza
        # "scope": SCOPE,
    }
    return f"{AUTH_BASE_URL}?{urlencode(params)}"


# ---------------------------
# Troca de authorization_code
# ---------------------------
def exchange_code_for_token(authorization_code: str) -> Dict[str, Any]:
    """
    Troca o `authorization_code` por access/refresh token.
    Salva tokens com `created_at` e `expires_at` normalizados.
    """
    data = {
        "grant_type": "authorization_code",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "code": authorization_code,
        "redirect_uri": ML_REDIRECT_URI,
    }
    logging.info("🔐 Solicitando troca de authorization_code por tokens...")
    resp = requests.post(TOKEN_URL, data=data, timeout=60)
    if resp.status_code >= 400:
        logging.error("Erro ao trocar authorization_code: %s %s", resp.status_code, resp.text[:400])
        resp.raise_for_status()

    payload = resp.json()

    # Normaliza campos de tempo
    now_utc = datetime.now(timezone.utc)
    payload["created_at"] = now_utc.isoformat()
    expires_in = int(payload.get("expires_in", 0) or 0)
    if expires_in:
        payload["expires_at"] = (now_utc + timedelta(seconds=expires_in)).isoformat()

    save_tokens(payload)
    return payload


# ---------------------------
# Refresh token
# ---------------------------
def refresh_access_token(refresh_token: str | None = None) -> Dict[str, Any]:
    """
    Atualiza o access_token via refresh_token.
    """
    tokens = load_tokens() or {}
    if not refresh_token:
        refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise RuntimeError("Nenhum refresh_token disponível. Refaça o OAuth.")

    data = {
        "grant_type": "refresh_token",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "refresh_token": refresh_token,
    }
    logging.info("🔄 Fazendo refresh do access_token...")
    resp = requests.post(TOKEN_URL, data=data, timeout=60)
    if resp.status_code >= 400:
        logging.error("Erro no refresh_token: %s %s", resp.status_code, resp.text[:400])
        resp.raise_for_status()

    payload = resp.json()
    now_utc = datetime.now(timezone.utc)
    payload["created_at"] = now_utc.isoformat()
    expires_in = int(payload.get("expires_in", 0) or 0)
    if expires_in:
        payload["expires_at"] = (now_utc + timedelta(seconds=expires_in)).isoformat()

    # Mantém user_id anterior, se não vier no refresh
    if "user_id" not in payload and "user_id" in tokens:
        payload["user_id"] = tokens["user_id"]

    save_tokens(payload)
    return payload


# ---------------------------
# Fornecedor de access_token
# ---------------------------
def get_access_token() -> str:
    """
    Retorna um access_token válido. Se estiver perto de expirar, faz refresh.
    Tolera tokens.json antigos (sem expires_at) calculando via created_at+expires_in.
    """
    tokens = load_tokens()
    if not tokens:
        raise RuntimeError("tokens.json não encontrado. Execute o fluxo OAuth para gerar os tokens.")

    # Backfill de expires_at se estiver faltando
    expires_at_iso = tokens.get("expires_at")
    if not expires_at_iso:
        created = tokens.get("created_at")
        expires_in = tokens.get("expires_in")
        if created and expires_in:
            try:
                created_dt = datetime.fromisoformat(created)
                expires_at_dt = created_dt + timedelta(seconds=int(expires_in))
                expires_at_iso = expires_at_dt.astimezone(timezone.utc).isoformat()
                tokens["expires_at"] = expires_at_iso
                save_tokens(tokens)
            except Exception:
                # Se não conseguir calcular, usa o token como está (pode funcionar) e deixa o refresh para erro 401
                return tokens["access_token"]

    # Se ainda não temos expires_at, retorna o token atual
    if not expires_at_iso:
        return tokens["access_token"]

    # Verifica tempo restante
    try:
        expires_at_dt = datetime.fromisoformat(expires_at_iso)
    except Exception:
        # Formato inesperado: força refresh
        return refresh_access_token(tokens.get("refresh_token"))["access_token"]

    now = datetime.now(timezone.utc)
    # margenzinha de 60s
    if (expires_at_dt - now).total_seconds() < 60:
        return refresh_access_token(tokens.get("refresh_token"))["access_token"]

    return tokens["access_token"]
