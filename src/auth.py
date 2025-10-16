import os
import json
import logging
from datetime import datetime, timedelta, timezone
import requests

# Caminho fixo do token
TOKEN_PATH = os.path.join(os.path.dirname(__file__), "tokens.json")

# Credenciais da aplicação (substitua pelos seus dados do app Meli)
CLIENT_ID = os.getenv("MELI_CLIENT_ID", "YOUR_CLIENT_ID")
CLIENT_SECRET = os.getenv("MELI_CLIENT_SECRET", "YOUR_CLIENT_SECRET")
REDIRECT_URI = os.getenv("MELI_REDIRECT_URI", "https://auth.mercadolibre.com.br")

BASE_URL = "https://api.mercadolibre.com"


def save_tokens(data):
    """Salva os tokens no arquivo tokens.json"""
    with open(TOKEN_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    logging.info(f"💾 Tokens salvos em {TOKEN_PATH}")


def load_tokens():
    """Carrega tokens do arquivo local"""
    if not os.path.exists(TOKEN_PATH):
        raise FileNotFoundError("tokens.json não encontrado.")
    with open(TOKEN_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_auth_url():
    """Gera o link de autorização para o usuário"""
    return f"https://auth.mercadolibre.com/authorization?response_type=code&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"


def exchange_code_for_token(auth_code: str):
    """Troca o authorization code por access e refresh tokens"""
    url = f"{BASE_URL}/oauth/token"
    payload = {
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": auth_code,
        "redirect_uri": REDIRECT_URI,
    }

    resp = requests.post(url, data=payload)
    if resp.status_code != 200:
        logging.error(f"Erro ao trocar authorization_code: {resp.status_code} {resp.text}")
        resp.raise_for_status()

    data = resp.json()
    data["expires_at"] = (datetime.now(timezone.utc) + timedelta(seconds=data["expires_in"])).isoformat()
    save_tokens(data)
    return data


def refresh_token():
    """Atualiza o token quando expirado"""
    tokens = load_tokens()
    refresh = tokens.get("refresh_token")

    url = f"{BASE_URL}/oauth/token"
    payload = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh,
    }

    resp = requests.post(url, data=payload)
    if resp.status_code != 200:
        logging.error(f"Erro ao atualizar token: {resp.status_code} {resp.text}")
        resp.raise_for_status()

    new_tokens = resp.json()
    new_tokens["refresh_token"] = refresh
    new_tokens["expires_at"] = (datetime.now(timezone.utc) + timedelta(seconds=new_tokens["expires_in"])).isoformat()
    save_tokens(new_tokens)
    return new_tokens


def get_access_token():
    """Retorna o access_token válido"""
    tokens = load_tokens()
    expires_at = datetime.fromisoformat(tokens["expires_at"])
    if datetime.now(timezone.utc) >= expires_at:
        logging.info("🔄 Token expirado, atualizando...")
        tokens = refresh_token()
    return tokens["access_token"]
