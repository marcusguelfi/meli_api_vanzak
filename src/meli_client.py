import os
import logging
import requests
from src.auth import get_access_token

BASE_URL = "https://api.mercadolibre.com"


def meli_get(path: str, params=None, headers=None):
    """Faz chamadas GET autenticadas à API do Mercado Livre."""
    access_token = get_access_token()

    if headers is None:
        headers = {}

    headers.update({
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    })

    url = f"{BASE_URL}{path}"
    logging.info(f"➡️ GET {url}")

    resp = requests.get(url, headers=headers, params=params)
    try:
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error(f"❌ Erro {resp.status_code} ao chamar {url}: {resp.text}")
        raise
