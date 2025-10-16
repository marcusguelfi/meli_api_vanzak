# src/jobs.py
from __future__ import annotations

import os
import json
import csv
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
import requests
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

from .meli_client import meli_get

# ---------------------------------------------------------------------
# Cabeçalhos e variáveis globais
# ---------------------------------------------------------------------
HDR_V1 = {"Api-Version": "1"}
HDR_V2 = {"Api-Version": "2"}

APPSCRIPT_URL = os.getenv("GOOGLE_APPSCRIPT_URL", "").strip()
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
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{base_name}.csv")

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
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})

    logging.info(f"💾 CSV atualizado: {path}")
    return path

# ---------------------------------------------------------------------
# Envio para Google Sheets via Apps Script
# ---------------------------------------------------------------------
def enviar_para_google_sheets(caminho_csv: str, sheet: Optional[str] = None) -> None:
    """
    Envia um CSV para o Apps Script WebApp configurado via .env (GOOGLE_APPSCRIPT_URL).
    Cria a aba automaticamente se não existir.
    """
    if not APPSCRIPT_URL:
        logging.warning("⚠️ GOOGLE_APPSCRIPT_URL não configurado. Pulando envio ao Google Sheets.")
        return

    q = dict(parse_qsl(urlsplit(APPSCRIPT_URL).query))
    if APPSCRIPT_TOKEN:
        q["token"] = APPSCRIPT_TOKEN
    if sheet:
        q["sheet"] = sheet
    q["name"] = os.path.basename(caminho_csv)

    parts = list(urlsplit(APPSCRIPT_URL))
    parts[3] = urlencode(q)
    url = urlunsplit(parts)

    try:
        headers = {
            "Content-Type": "text/csv",
            "X-Filename": os.path.basename(caminho_csv),
        }
        with open(caminho_csv, "rb") as f:
            data = f.read()
        logging.info(f"⬆️ Enviando {caminho_csv} → {url}")
        resp = requests.post(url, data=data, headers=headers, timeout=120)
        resp.raise_for_status()
        logging.info(f"✅ Upload OK ({resp.status_code}) – {sheet or 'dados'} atualizado.")
    except Exception as e:
        logging.exception(f"❌ Falha ao enviar CSV para Apps Script: {e}")

# ---------------------------------------------------------------------
# Métricas básicas
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
    params = {"seller": seller_id}
    if date_from_iso:
        params["order.date_created.from"] = date_from_iso

    data = meli_get("/orders/search", params=params)
    rows = [
        {
            "id": o.get("id"),
            "date_created": o.get("date_created"),
            "status": o.get("status"),
            "total_amount": o.get("total_amount"),
            "currency_id": o.get("currency_id"),
            "buyer_id": (o.get("buyer") or {}).get("id"),
        }
        for o in data.get("results", [])
    ]
    path = write_csv(rows, "orders")
    enviar_para_google_sheets(path, sheet="orders")
    return path

# ---------------------------------------------------------------------
# Product Ads — Advertiser / Campaigns / Ads
# ---------------------------------------------------------------------
def job_get_advertiser(product_id: str = "PADS") -> Dict[str, Any]:
    data = meli_get("/advertising/advertisers", params={"product_id": product_id}, headers=HDR_V1)
    save_json(data, f"data/processed/advertiser_{_ts_now()}.json")
    return data

def job_campaigns_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"
    params = {
        "limit": 50, "offset": 0, "date_from": date_from, "date_to": date_to,
        "metrics": ",".join(_basic_metrics()), "aggregation_type": "DAILY",
    }
    data = meli_get(endpoint, params=params, headers=HDR_V2)
    rows = [
        {**r, "advertiser_id": advertiser_id, "site_id": site_id}
        for r in data.get("results", [])
        if isinstance(r, dict)
    ]
    path = write_csv(rows, f"campaigns_daily_{advertiser_id}")
    enviar_para_google_sheets(path, sheet="campaigns_daily")
    return path

def job_ads_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"
    params = {
        "limit": 50, "offset": 0, "date_from": date_from, "date_to": date_to,
        "metrics": ",".join(_basic_metrics()), "aggregation_type": "DAILY",
    }
    data = meli_get(endpoint, params=params, headers=HDR_V2)
    rows = [
        {**r, "advertiser_id": advertiser_id, "site_id": site_id}
        for r in data.get("results", [])
        if isinstance(r, dict)
    ]
    path = write_csv(rows, f"ads_daily_{advertiser_id}")
    enviar_para_google_sheets(path, sheet="ads_daily")
    return path
