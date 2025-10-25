# src/meli_client.py
from __future__ import annotations

import logging
import time
import random
from typing import Dict, Optional, Any, Tuple

import requests

from src.auth import get_access_token, refresh_access_token

BASE_URL = "https://api.mercadolibre.com"
RETRY_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}  # inclui 408 e 409/425 (transientes)

# timeouts (connect, read) — evita travar em conexões ruins
DEFAULT_TIMEOUT: Tuple[int, int] = (10, 60)

log = logging.getLogger(__name__)


def _is_advertising_route(path_or_url: str) -> bool:
    return "/advertising/" in path_or_url


def _build_url(path_or_url: str) -> str:
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    return f"{BASE_URL}{path_or_url}"


def meli_request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    json: Any = None,
    data: Any = None,
    timeout: Tuple[int, int] | int = DEFAULT_TIMEOUT,
    max_retries: int = 3,
    backoff_base: float = 1.5,
) -> Any:
    """
    Chamada autenticada à API do Mercado Livre com retry/backoff + refresh on 401.

    - Injeta access token e Api-Version: 2 para rotas Product Ads.
    - Respeita Retry-After quando presente.
    - Retenta 429/5xx (e alguns transientes) com backoff exponencial + jitter.
    - Em 401, tenta UMA vez fazer refresh do token e repete.
    - Aceita path relativo (prefixa BASE_URL) ou URL absoluta.
    """
    url = _build_url(path)
    attempt = 0
    did_refresh = False

    # headers padrão
    access_token = get_access_token()
    merged_headers: Dict[str, str] = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    if _is_advertising_route(url):
        merged_headers["Api-Version"] = "2"
    if headers:
        merged_headers.update(headers)

    # normaliza params para str (evita booleans/dates esquisitos)
    safe_params = None
    if params:
        safe_params = {k: ("" if v is None else str(v)) for k, v in params.items()}

    while True:
        attempt += 1
        log.info("➡️ %s %s", method.upper(), url)
        try:
            resp = requests.request(
                method=method.upper(),
                url=url,
                headers=merged_headers,
                params=safe_params,
                json=json,
                data=data,
                timeout=timeout,
            )

            # 401 → tenta refresh UMA vez
            if resp.status_code == 401 and not did_refresh:
                log.warning("🔒 401 recebido — tentando refresh do token e repetindo uma vez…")
                try:
                    refresh_access_token()
                except Exception as e:
                    log.error("❌ Falha no refresh token: %s", e)
                    resp.raise_for_status()
                # atualiza header com novo token e repete sem consumir retry quota
                new_token = get_access_token()
                merged_headers["Authorization"] = f"Bearer {new_token}"
                did_refresh = True
                continue

            # retry transientes
            if resp.status_code in RETRY_STATUS and attempt <= max_retries:
                # honra Retry-After se presente
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        wait = backoff_base ** (attempt - 1)
                else:
                    # backoff exponencial com jitter (±20%)
                    base = backoff_base ** (attempt - 1)
                    wait = base * random.uniform(0.8, 1.2)

                log.warning(
                    "⚠️ %s em %s — tentativa %d/%d. Aguardando %.2fs…",
                    resp.status_code, url, attempt, max_retries, wait
                )
                time.sleep(wait)
                continue

            # lança para códigos não-OK
            resp.raise_for_status()

            # tenta JSON; se falhar, devolve texto
            try:
                return resp.json()
            except ValueError:
                return resp.text

        except requests.HTTPError:
            body = resp.text if "resp" in locals() else "<sem resposta>"
            log.error("❌ HTTP %s em %s: %s", resp.status_code if "resp" in locals() else "?", url, body[:800])
            raise
        except requests.RequestException as e:
            # erros de rede também merecem retry
            if attempt <= max_retries:
                base = backoff_base ** (attempt - 1)
                wait = base * random.uniform(0.8, 1.2)
                log.warning(
                    "⚠️ Erro de rede em %s: %s — tentativa %d/%d. Aguardando %.2fs…",
                    url, e, attempt, max_retries, wait
                )
                time.sleep(wait)
                continue
            log.error("❌ Falha de rede ao chamar %s: %s", url, e)
            raise


def meli_get(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    *,
    timeout: Tuple[int, int] | int = DEFAULT_TIMEOUT,
    max_retries: int = 3,
    backoff_base: float = 1.5,
) -> Any:
    return meli_request(
        "GET",
        path,
        params=params,
        headers=headers,
        timeout=timeout,
        max_retries=max_retries,
        backoff_base=backoff_base,
    )


def meli_post(
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    json: Any = None,
    data: Any = None,
    timeout: Tuple[int, int] | int = DEFAULT_TIMEOUT,
    max_retries: int = 3,
    backoff_base: float = 1.5,
) -> Any:
    # garante Content-Type quando for JSON
    headers = dict(headers or {})
    if json is not None and "Content-Type" not in {k.title(): v for k, v in headers.items()}:
        headers.setdefault("Content-Type", "application/json")
    return meli_request(
        "POST",
        path,
        params=params,
        headers=headers,
        json=json,
        data=data,
        timeout=timeout,
        max_retries=max_retries,
        backoff_base=backoff_base,
    )
