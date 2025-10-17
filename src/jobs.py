# src/jobs.py
from __future__ import annotations

import os
import csv
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

from .meli_client import meli_get

# ---------------------------------------------------------------------
# Cabeçalhos e variáveis globais
# ---------------------------------------------------------------------
HDR_V1 = {"Api-Version": "1"}  # /advertising/advertisers?product_id=PADS
HDR_V2 = {"Api-Version": "2"}  # endpoints Product Ads v2

APPSCRIPT_URL = os.getenv("GOOGLE_APPSCRIPT_URL", "").strip().strip('"').strip("'")
APPSCRIPT_TOKEN = os.getenv("GOOGLE_APPSCRIPT_TOKEN", "").strip()

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _ts_now() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def save_json(data: Any, path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path

def write_csv(rows: List[Dict[str, Any]], base_name: str, folder: str = "data/processed") -> str:
    """
    Mantém 1 arquivo por chamada (sem timestamp) e faz append.
    Cabeçalho dinâmico com as chaves vistas no lote atual.
    """
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{base_name}.csv")

    # chaves dinâmicas de forma estável
    fieldnames: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    file_exists = os.path.exists(path)
    mode = "a" if file_exists else "w"

    with open(path, mode, encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    logging.info(f"💾 CSV atualizado: {path}")
    return path

# ---------------------------------------------------------------------
# Envio para Google Sheets via Apps Script
# ---------------------------------------------------------------------
def enviar_para_google_sheets(caminho_csv: str, sheet: Optional[str] = None) -> None:
    """
    Envia um CSV para o Apps Script WebApp configurado via .env (GOOGLE_APPSCRIPT_URL).
    Cria/limpa a aba automaticamente se não existir. Remove qualquer fragmento da URL.
    """
    if not APPSCRIPT_URL:
        logging.info("GOOGLE_APPSCRIPT_URL não configurado; pulando envio ao Apps Script.")
        return

    parts = urlsplit(APPSCRIPT_URL)
    query = dict(parse_qsl(parts.query))

    if APPSCRIPT_TOKEN:
        query["token"] = APPSCRIPT_TOKEN
    if sheet:
        query["sheet"] = sheet

    file_name = os.path.basename(caminho_csv).replace(" ", "_")
    query["name"] = file_name

    # url sem fragmento
    url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))

    try:
        headers = {
            "Content-Type": "text/csv",
            "X-Filename": file_name,
        }
        size = os.path.getsize(caminho_csv)
        logging.info(f"⬆️ Enviando {caminho_csv} ({size} bytes) → {url}")
        with open(caminho_csv, "rb") as f:
            resp = requests.post(url, data=f.read(), headers=headers, timeout=120)
        resp.raise_for_status()
        logging.info(f"✅ Upload OK ({resp.status_code}) – {sheet or 'dados'} atualizado.")
    except Exception as e:
        logging.exception(f"❌ Falha ao enviar CSV para Apps Script: {e}")

# ---------------------------------------------------------------------
# Métricas básicas (válidas nos endpoints /search)
# ---------------------------------------------------------------------
def _basic_metrics() -> List[str]:
    return [
        "clicks", "prints", "ctr", "cost", "cpc", "acos",
        "organic_units_quantity", "organic_units_amount", "organic_items_quantity",
        "direct_items_quantity", "indirect_items_quantity", "advertising_items_quantity",
        "cvr", "roas", "sov",
        "direct_units_quantity", "indirect_units_quantity", "units_quantity",
        "direct_amount", "indirect_amount", "total_amount",
    ]

# ---------------------------------------------------------------------
# Users / Orders
# ---------------------------------------------------------------------
def job_user_me() -> str:
    data = meli_get("/users/me")
    row = {
        "user_id": data.get("id"),
        "nickname": data.get("nickname"),
        "registration_date": data.get("registration_date"),
        "country_id": data.get("country_id"),
        "permalink": data.get("permalink"),
        "status_site_status": (data.get("status") or {}).get("site_status"),
        "ts_local": datetime.now().isoformat(timespec="seconds"),
    }
    path = write_csv([row], "users_me")
    enviar_para_google_sheets(path, sheet="users_me")
    return path

def job_orders_recent(seller_id: str, date_from_iso: Optional[str] = None) -> str:
    params: Dict[str, Any] = {"seller": seller_id}
    if date_from_iso:
        params["order.date_created.from"] = date_from_iso

    data = meli_get("/orders/search", params=params)
    rows: List[Dict[str, Any]] = []
    for o in data.get("results", []):
        rows.append({
            "id": o.get("id"),
            "date_created": o.get("date_created"),
            "status": o.get("status"),
            "total_amount": o.get("total_amount"),
            "currency_id": o.get("currency_id"),
            "buyer_id": (o.get("buyer") or {}).get("id"),
        })
    path = write_csv(rows, "orders")
    enviar_para_google_sheets(path, sheet="orders")
    return path

# ---------------------------------------------------------------------
# Product Ads — Advertiser / Campaigns / Ads
# ---------------------------------------------------------------------
def job_get_advertiser(product_id: str = "PADS") -> Dict[str, Any]:
    """
    /advertising/advertisers?product_id=PADS (Api-Version: 1)
    """
    data = meli_get("/advertising/advertisers", params={"product_id": product_id}, headers=HDR_V1)
    save_json(data, f"data/processed/advertiser_{_ts_now()}.json")
    return data

# ---- Campaigns (DAILY & SUMMARY)
def job_campaigns_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"
    params = {
        "limit": 50,
        "offset": 0,
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(_basic_metrics()),
        "aggregation_type": "DAILY",
    }
    data = meli_get(endpoint, params=params, headers=HDR_V2)
    rows = []
    for r in data.get("results", []):
        if isinstance(r, dict):
            base = {"advertiser_id": advertiser_id, "site_id": site_id}
            for k, v in r.items():
                if not isinstance(v, (list, dict)):
                    base[k] = v
            rows.append(base)
    path = write_csv(rows, f"campaigns_daily_{advertiser_id}")
    enviar_para_google_sheets(path, sheet="campaigns_daily")
    return path

def job_campaigns_summary(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"
    params = {
        "limit": 50,
        "offset": 0,
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(_basic_metrics()),
        "metrics_summary": "true",
    }
    data = meli_get(endpoint, params=params, headers=HDR_V2)
    rows: List[Dict[str, Any]] = []
    if isinstance(data, dict) and "metrics_summary" in data:
        row = {"advertiser_id": advertiser_id, "site_id": site_id}
        for k, v in data["metrics_summary"].items():
            if not isinstance(v, (list, dict)):
                row[k] = v
        rows.append(row)
    path = write_csv(rows, f"campaigns_summary_{advertiser_id}")
    enviar_para_google_sheets(path, sheet="campaigns_summary")
    return path

# ---- Ads (DAILY & SUMMARY)
def job_ads_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"
    params = {
        "limit": 50,
        "offset": 0,
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(_basic_metrics()),
        "aggregation_type": "DAILY",
    }
    data = meli_get(endpoint, params=params, headers=HDR_V2)
    rows = []
    for r in data.get("results", []):
        if isinstance(r, dict):
            base = {"advertiser_id": advertiser_id, "site_id": site_id}
            for k, v in r.items():
                if not isinstance(v, (list, dict)):
                    base[k] = v
            rows.append(base)
    path = write_csv(rows, f"ads_daily_{advertiser_id}")
    enviar_para_google_sheets(path, sheet="ads_daily")
    return path

def job_ads_summary(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"
    params = {
        "limit": 50,
        "offset": 0,
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(_basic_metrics()),
        "metrics_summary": "true",
    }
    data = meli_get(endpoint, params=params, headers=HDR_V2)
    rows: List[Dict[str, Any]] = []
    if isinstance(data, dict) and "metrics_summary" in data:
        row = {"advertiser_id": advertiser_id, "site_id": site_id}
        for k, v in data["metrics_summary"].items():
            if not isinstance(v, (list, dict)):
                row[k] = v
        rows.append(row)
    path = write_csv(rows, f"ads_summary_{advertiser_id}")
    enviar_para_google_sheets(path, sheet="ads_summary")
    return path

# ---- Listagem simples de anúncios (sem métricas)
def job_list_ads_basic(advertiser_id: str, site_id: str, limit: int = 10, offset: int = 0) -> str:
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"
    params = {"limit": limit, "offset": offset}
    data = meli_get(endpoint, params=params, headers=HDR_V2)
    rows: List[Dict[str, Any]] = []

    for r in data.get("results", []):
        if not isinstance(r, dict):
            continue
        ad_id = (r.get("ad") or {}).get("id") or r.get("ad_id") or r.get("id")
        item_id = (r.get("item") or {}).get("id") or r.get("item_id") or r.get("id")
        status = r.get("status")
        rows.append({"ad_id": ad_id, "item_id": item_id, "status": status})

    path = write_csv(rows, f"ads_list_{advertiser_id}")
    save_json(data, f"data/processed/ads_list_{advertiser_id}_{_ts_now()}.json")
    enviar_para_google_sheets(path, sheet="ads_list")
    return path

# ---------------------------------------------------------------------
# Product Ads — Item detail & metrics (fallback: items → ads)
# ---------------------------------------------------------------------
def job_item_detail(site_id: str, item_id: str) -> str:
    try_order = [
        f"/advertising/{site_id}/product_ads/items/{item_id}",
        f"/advertising/{site_id}/product_ads/ads/{item_id}",
    ]
    last_err: Optional[Exception] = None
    for ep in try_order:
        try:
            data = meli_get(ep, headers=HDR_V2)
            rows = []
            if isinstance(data, dict):
                flat = {k: v for k, v in data.items() if not isinstance(v, (list, dict))}
                flat["item_id"] = item_id
                rows.append(flat)
            else:
                rows.append({"item_id": item_id, "payload_json": json.dumps(data, ensure_ascii=False)})

            csv_path = write_csv(rows, f"item_detail_{item_id}")
            save_json(data, f"data/processed/item_detail_{item_id}_{_ts_now()}.json")
            enviar_para_google_sheets(csv_path, sheet=f"item_detail_{item_id}")
            return csv_path

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            last_err = e
            if status == 404:
                continue
            break
        except Exception as e:
            last_err = e
            break
    raise last_err or RuntimeError(f"Falha ao buscar detalhe do item {item_id}")

def job_item_metrics(site_id: str, item_id: str, date_from: str, date_to: str, aggregation_type: Optional[str] = None) -> str:
    metrics = _basic_metrics()
    params: Dict[str, Any] = {
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(metrics),
    }
    if aggregation_type:
        params["aggregation_type"] = aggregation_type  # e.g. "DAILY"

    try_order = [
        f"/advertising/{site_id}/product_ads/items/{item_id}",
        f"/advertising/{site_id}/product_ads/ads/{item_id}",
    ]
    last_err: Optional[Exception] = None
    for ep in try_order:
        try:
            data = meli_get(ep, params=params, headers=HDR_V2)

            rows: List[Dict[str, Any]] = []
            if isinstance(data, dict) and "metrics_summary" in data:
                row = {"item_id": item_id}
                for k, v in data["metrics_summary"].items():
                    if not isinstance(v, (list, dict)):
                        row[k] = v
                rows.append(row)
            elif isinstance(data, dict) and isinstance(data.get("results"), list):
                for r in data["results"]:
                    base: Dict[str, Any] = {"item_id": item_id}
                    if isinstance(r, dict):
                        for k, v in r.items():
                            if not isinstance(v, (list, dict)):
                                base[k] = v
                    rows.append(base)
            else:
                rows.append({"item_id": item_id, "payload_json": json.dumps(data, ensure_ascii=False)})

            csv_path = write_csv(rows, f"item_metrics_{item_id}")
            save_json(data, f"data/processed/item_metrics_{item_id}_{_ts_now()}.json")
            enviar_para_google_sheets(csv_path, sheet=f"item_metrics_{item_id}")
            return csv_path

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            last_err = e
            if status == 404:
                continue
            break
        except Exception as e:
            last_err = e
            break
    raise last_err or RuntimeError(f"Falha ao obter métricas para item {item_id}")
