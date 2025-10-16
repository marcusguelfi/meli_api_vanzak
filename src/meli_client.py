# src/meli_get.py
import logging
import time
from typing import Dict, Optional

import requests

from src.auth import get_access_token

BASE_URL = "https://api.mercadolibre.com"
RETRY_STATUS = {429, 500, 502, 503, 504}


def meli_get(
    path: str,
    params: Optional[Dict] = None,
    headers: Optional[Dict] = None,
    *,
    timeout: int = 60,
    max_retries: int = 3,
    backoff_base: float = 1.5,
):
    """
    Faz chamadas GET autenticadas à API do Mercado Livre.

    - Injeta automaticamente Api-Version: 2 para rotas /advertising/ (Product Ads).
    - Aceita `headers` customizados e permite override dos padrões.
    - Retry com backoff para status 429/5xx.
    """
    access_token = get_access_token()

    # Params seguros
    params = dict(params or {})

    # Cabeçalhos padrão
    default_headers: Dict[str, str] = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # Para rotas Product Ads, a doc exige Api-Version: 2
    if path.startswith("/advertising/"):
        # O backend aceita 'Api-Version' ou 'api-version'; mantemos 'Api-Version'
        default_headers["Api-Version"] = "2"

    # Merge com headers do caller (caller sobrescreve)
    if headers:
        default_headers.update(headers)

    url = f"{BASE_URL}{path}"

    attempt = 0
    while True:
        attempt += 1
        logging.info(f"➡️ GET {url}")
        try:
            resp = requests.get(url, headers=default_headers, params=params, timeout=timeout)
            if resp.status_code in RETRY_STATUS and attempt <= max_retries:
                # Backoff exponencial simples
                wait = backoff_base ** (attempt - 1)
                logging.warning(
                    f"⚠️ {resp.status_code} em {url} — tentativa {attempt}/{max_retries}. "
                    f"Tentando novamente em {wait:.1f}s..."
                )
                time.sleep(wait)
                continue

            # Erros não-retriáveis ou estourou tentativas
            resp.raise_for_status()

            # Tenta JSON, cai para texto se não for JSON
            try:
                return resp.json()
            except ValueError:
                return resp.text

        except requests.HTTPError:
            # Log de corpo de erro para depuração
            body = resp.text if "resp" in locals() else "<sem resposta>"
            logging.error(f"❌ Erro {resp.status_code if 'resp' in locals() else '?'} ao chamar {url}: {body}")
            raise
        except requests.RequestException as e:
            # Erros de rede também podem merecer retry
            if attempt <= max_retries:
                wait = backoff_base ** (attempt - 1)
                logging.warning(
                    f"⚠️ Erro de rede em {url}: {e} — tentativa {attempt}/{max_retries}. "
                    f"Tentando novamente em {wait:.1f}s..."
                )
                time.sleep(wait)
                continue
            logging.error(f"❌ Falha de rede ao chamar {url}: {e}")
            raise

