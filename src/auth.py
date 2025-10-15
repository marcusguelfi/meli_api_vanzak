# src/auth.py (topo)
import json, os, time
from urllib.parse import urlencode
import requests
from .config import ML_CLIENT_ID, ML_CLIENT_SECRET, ML_REDIRECT_URI, TOKEN_STORE_PATH


OAUTH_AUTH_URL = "https://auth.mercadolibre.com/authorization"
OAUTH_TOKEN_URL = "https://api.mercadolibre.com/oauth/token"

def _save_tokens(tokens: dict):
    with open(TOKEN_STORE_PATH, "w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)

def _load_tokens():
    if not os.path.exists(TOKEN_STORE_PATH):
        return None
    with open(TOKEN_STORE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def build_authorize_url(state: str = "meli_state", scope: str | None = None):
    params = {
        "response_type": "code",
        "client_id": ML_CLIENT_ID,
        "redirect_uri": ML_REDIRECT_URI,
        "state": state,
    }
    # Se precisar de escopos específicos, adicione `scope`
    if scope:
        params["scope"] = scope
    return f"{OAUTH_AUTH_URL}?{urlencode(params)}"

def exchange_code_for_token(code: str):
    data = {
        "grant_type": "authorization_code",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "code": code,
        "redirect_uri": ML_REDIRECT_URI,
    }
    resp = requests.post(OAUTH_TOKEN_URL, data=data, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    # Normaliza e adiciona timestamp de expiração
    tokens = {
        "access_token": payload["access_token"],
        "token_type": payload.get("token_type", "Bearer"),
        "expires_in": payload["expires_in"],   # em segundos
        "scope": payload.get("scope", ""),
        "user_id": payload.get("user_id"),
        "refresh_token": payload.get("refresh_token"),
        "created_at": int(time.time()),
    }
    _save_tokens(tokens)
    return tokens

def refresh_access_token():
    tokens = _load_tokens()
    if not tokens or not tokens.get("refresh_token"):
        raise RuntimeError("Refresh token não encontrado. Faça a autorização inicial.")
    data = {
        "grant_type": "refresh_token",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "refresh_token": tokens["refresh_token"],
    }
    resp = requests.post(OAUTH_TOKEN_URL, data=data, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    tokens.update({
        "access_token": payload["access_token"],
        "expires_in": payload["expires_in"],
        "scope": payload.get("scope", tokens.get("scope", "")),
        "created_at": int(time.time()),
        "refresh_token": payload.get("refresh_token", tokens.get("refresh_token")),
    })
    _save_tokens(tokens)
    return tokens

def get_valid_token(min_ttl: int = 120):
    """
    Retorna um access_token válido. Se faltarem menos que `min_ttl` segundos, faz refresh.
    """
    tokens = _load_tokens()
    if not tokens:
        raise RuntimeError("Tokens não encontrados. Execute a autorização (pegar code).")
    age = int(time.time()) - tokens["created_at"]
    if age >= tokens["expires_in"] - min_ttl:
        tokens = refresh_access_token()
    return tokens["access_token"]


