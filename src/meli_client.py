from __future__ import annotations

import logging
import time
import random
from typing import Dict, Optional, Any, Tuple, List
import pandas as pd
from pathlib import Path
import requests
import os

from src.auth import get_access_token, refresh_access_token
from src.csv_utils import upsert_csv

BASE_URL = "https://api.mercadolibre.com"
RETRY_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}
DEFAULT_TIMEOUT: Tuple[int, int] = (10, 60)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ===============================================================
# === núcleo de requisições Mercado Livre ========================
# ===============================================================

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
    """Chamada autenticada com retry/backoff + refresh automático em 401."""
    url = _build_url(path)
    attempt = 0
    did_refresh = False

    access_token = get_access_token()
    merged_headers: Dict[str, str] = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    if _is_advertising_route(url):
        merged_headers["Api-Version"] = "2"
    if headers:
        merged_headers.update(headers)

    safe_params = {k: ("" if v is None else str(v)) for k, v in (params or {}).items()}

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

            if resp.status_code == 401 and not did_refresh:
                log.warning("🔒 401 recebido — tentando refresh do token…")
                refresh_access_token()
                new_token = get_access_token()
                merged_headers["Authorization"] = f"Bearer {new_token}"
                did_refresh = True
                continue

            if resp.status_code in RETRY_STATUS and attempt <= max_retries:
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else backoff_base ** (attempt - 1)
                wait *= random.uniform(0.8, 1.2)
                log.warning(
                    "⚠️ %s em %s — tentativa %d/%d. Aguardando %.2fs…",
                    resp.status_code, url, attempt, max_retries, wait
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            try:
                return resp.json()
            except ValueError:
                return resp.text

        except requests.RequestException as e:
            if attempt <= max_retries:
                wait = backoff_base ** (attempt - 1) * random.uniform(0.8, 1.2)
                log.warning("⚠️ Erro de rede em %s: %s — tentativa %d/%d. Esperando %.2fs…",
                            url, e, attempt, max_retries, wait)
                time.sleep(wait)
                continue
            log.error("❌ Falha de rede em %s: %s", url, e)
            raise


def meli_get(path: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None, *,
             timeout: Tuple[int, int] | int = DEFAULT_TIMEOUT,
             max_retries: int = 3, backoff_base: float = 1.5) -> Any:
    return meli_request("GET", path, params=params, headers=headers,
                        timeout=timeout, max_retries=max_retries, backoff_base=backoff_base)


def meli_post(path: str, *, params: Optional[Dict[str, Any]] = None,
              headers: Optional[Dict[str, str]] = None, json: Any = None,
              data: Any = None, timeout: Tuple[int, int] | int = DEFAULT_TIMEOUT,
              max_retries: int = 3, backoff_base: float = 1.5) -> Any:
    headers = dict(headers or {})
    if json is not None and "Content-Type" not in {k.title(): v for k, v in headers.items()}:
        headers.setdefault("Content-Type", "application/json")
    return meli_request("POST", path, params=params, headers=headers, json=json, data=data,
                        timeout=timeout, max_retries=max_retries, backoff_base=backoff_base)


# ===============================================================
# === função de alto nível: puxar orders completas ===============
# ===============================================================

def get_orders_full(
    seller_id: str | int,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 50,
    max_orders: int = 9999,
    out_path: str = "data/processed/orders_full.csv",
) -> pd.DataFrame:
    """
    Busca pedidos detalhados (orders + payments + shipping).
    Pode filtrar por data de criação (date_from, date_to) e faz append incremental.
    """
    all_orders: List[dict] = []
    offset = 0

    log.info("📦 Coletando pedidos completos para seller_id=%s", seller_id)

    while len(all_orders) < max_orders:
        params = {
            "seller": seller_id,
            "sort": "date_desc",
            "limit": limit,
            "offset": offset,
        }
        if date_from:
            params["order.date_created.from"] = date_from
        if date_to:
            params["order.date_created.to"] = date_to

        data = meli_get("/orders/search", params=params)
        results = data.get("results", [])
        if not results:
            break
        all_orders.extend(results)
        offset += limit
        log.info("→ Coletados %d pedidos até agora (offset=%d)", len(all_orders), offset)
        time.sleep(0.2)
        if len(results) < limit:
            break

    # deduplica
    seen = set()
    unique_orders = [o for o in all_orders if not (o["id"] in seen or seen.add(o["id"]))]

    enriched = []
    for order in unique_orders:
        oid = order["id"]
        try:
            detail = meli_get(f"/orders/{oid}")
            enriched_order = {
                "order_id": oid,
                "status": detail.get("status"),
                "date_created": detail.get("date_created"),
                "date_closed": detail.get("date_closed"),
                "total_amount": detail.get("total_amount"),
                "currency_id": detail.get("currency_id"),
                "buyer_id": detail.get("buyer", {}).get("id"),
                "buyer_nickname": detail.get("buyer", {}).get("nickname"),
                "buyer_email": detail.get("buyer", {}).get("email"),
                "shipping_id": detail.get("shipping", {}).get("id"),
                "shipping_status": detail.get("shipping", {}).get("status"),
                "shipping_mode": detail.get("shipping", {}).get("mode"),
                "payment_total": sum(p.get("total_paid_amount", 0) for p in detail.get("payments", [])),
                "payment_methods": ", ".join({p.get("payment_type", "") for p in detail.get("payments", [])}),
                "tags": ", ".join(detail.get("tags", [])),
            }

            if detail.get("order_items"):
                item = detail["order_items"][0]
                enriched_order.update({
                    "item_id": item["item"].get("id"),
                    "item_title": item["item"].get("title"),
                    "item_category": item["item"].get("category_id"),
                    "quantity": item.get("quantity"),
                    "unit_price": item.get("unit_price"),
                    "full_unit_price": item.get("full_unit_price"),
                    "sku": item.get("seller_custom_field"),
                })

            enriched.append(enriched_order)
            time.sleep(0.15)
        except Exception as e:
            log.warning("⚠️ Falha no pedido %s: %s", oid, e)

    if not enriched:
        log.warning("Nenhum pedido detalhado encontrado.")
        return pd.DataFrame()

    df = pd.DataFrame(enriched)

    # Upsert incremental (sem duplicar pedidos)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    if os.path.exists(out_path):
        log.info("🔁 Atualizando CSV existente com novos pedidos (upsert por order_id)...")
        rows = df.to_dict(orient="records")
        upsert_csv(out_path, rows, key_fields=["order_id"], schema=list(df.columns))
    else:
        df.to_csv(out_path, index=False)
        log.info("✅ Novo arquivo salvo em %s", out_path)

    log.info("✅ Pedidos completos processados (%d linhas novas)", len(df))
    return df
