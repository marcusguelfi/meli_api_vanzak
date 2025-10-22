# src/jobs.py
from __future__ import annotations

import os
import csv
import json
import shutil
import tempfile
import logging
from typing import Any, Dict, List, Optional, Iterable, Tuple
from datetime import date, datetime, timedelta

import requests

from .meli_client import meli_get

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

HDR_V1 = {"Api-Version": "1"}
HDR_V2 = {"Api-Version": "2"}

APPSCRIPT_URL = os.getenv("GOOGLE_APPSCRIPT_URL", "").strip().strip('"').strip("'")
APPSCRIPT_TOKEN = os.getenv("GOOGLE_APPSCRIPT_TOKEN", "").strip()

DATA_DIR = "data/processed"


# ---------------------------------------------------------------------
# util
# ---------------------------------------------------------------------
def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _read_csv(path: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    if not os.path.exists(path):
        return [], []
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = [dict(row) for row in r]
        return (r.fieldnames or []), rows


def _write_atomic(path: str, header: List[str], rows: Iterable[Dict[str, Any]]) -> str:
    _ensure_dir(path)
    fd, tmp = tempfile.mkstemp(prefix=os.path.basename(path) + "_", suffix=".tmp")
    os.close(fd)
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in header})
    shutil.move(tmp, path)
    return path


def _union_header(existing_header: List[str], new_rows: List[Dict[str, Any]]) -> List[str]:
    order = list(existing_header)
    seen = set(order)
    for r in new_rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                order.append(k)
    return order


def write_csv_upsert_flexible(path: str, new_rows: List[Dict[str, Any]], key_fields: Tuple[str, ...]) -> str:
    header_old, existing = _read_csv(path)

    index: Dict[Tuple[str, ...], int] = {}
    for i, r in enumerate(existing):
        key = tuple(str(r.get(k, "")) for k in key_fields)
        index[key] = i

    merged = list(existing)
    for r in new_rows:
        k = tuple(str(r.get(kf, "")) for kf in key_fields)
        if k in index:
            pos = index[k]
            merged[pos] = {**merged[pos], **r}
        else:
            index[k] = len(merged)
            merged.append(r)

    header = _union_header(header_old, merged)
    _write_atomic(path, header, merged)
    log.info(f"💾 CSV (upsert): {path} (+{len(new_rows)} linhas novas/atualizadas)")
    return path


def enviar_para_google_sheets(caminho_csv: str, sheet: Optional[str] = None) -> None:
    if not APPSCRIPT_URL:
        log.info("GOOGLE_APPSCRIPT_URL não configurado — pulando upload.")
        return

    from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

    parts = urlsplit(APPSCRIPT_URL)
    q = dict(parse_qsl(parts.query))
    if APPSCRIPT_TOKEN:
        q["token"] = APPSCRIPT_TOKEN
    if sheet:
        q["sheet"] = sheet
    name = os.path.basename(caminho_csv)
    q["name"] = name

    url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), ""))

    try:
        size = os.path.getsize(caminho_csv)
        log.info(f"⬆️ Enviando {caminho_csv} ({size} bytes) → {url}")
        with open(caminho_csv, "rb") as f:
            resp = requests.post(
                url,
                data=f.read(),
                headers={"Content-Type": "text/csv", "X-Filename": name},
                timeout=120,
            )
        resp.raise_for_status()
        log.info(f"✅ Upload OK ({resp.status_code}) – aba {sheet or 'dados'}")
    except Exception as e:
        log.exception(f"❌ Falha no upload ao Apps Script: {e}")


# ---------------------------------------------------------------------
# chamadas meli (paginadas)
# ---------------------------------------------------------------------
def search_all(endpoint: str, params: Dict[str, Any], headers: Dict[str, str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    limit = int(params.get("limit", 200))
    offset = 0
    while True:
        page = meli_get(endpoint, params={**params, "limit": limit, "offset": offset}, headers=headers)
        batch = page.get("results", []) or []
        out.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return out


# ---------------------------------------------------------------------
# flatten “cru”: copia todas as chaves simples e adiciona IDs/nome
# ---------------------------------------------------------------------
def _flatten_raw_daily(r: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mantém nomes originais das métricas e adiciona chaves convenientes:
    campaign_id/name, ad_id, item_id, item_title, seller_sku, status, date.
    Compatível com payloads diários (aggregation_type=DAILY), que às vezes não incluem IDs.
    """
    out: Dict[str, Any] = {}

    for k, v in r.items():
        if not isinstance(v, (list, dict)):
            out[k] = v

    out["date"] = out.get("date") or r.get("day") or r.get("report_date")

    campaign = r.get("campaign") or {}
    ad = r.get("ad") or {}
    item = r.get("item") or {}

    out["campaign_id"] = campaign.get("id") or r.get("campaign_id") or out.get("campaign_id")
    out["campaign_name"] = campaign.get("name") or r.get("campaign_name") or out.get("campaign_name")
    out["ad_id"] = ad.get("id") or r.get("ad_id") or out.get("ad_id")
    out["item_id"] = item.get("id") or r.get("item_id") or out.get("item_id")
    out["item_title"] = item.get("title") or ad.get("title") or r.get("title") or out.get("item_title")
    out["seller_sku"] = item.get("seller_sku") or r.get("seller_sku") or out.get("seller_sku")
    out["status"] = r.get("status") or campaign.get("status") or ad.get("status") or out.get("status")

    return out


def _with_meta(row: Dict[str, Any], advertiser_id: str, site_id: str) -> Dict[str, Any]:
    row["advertiser_id"] = advertiser_id
    row["site_id"] = site_id
    return row


# ---------------------------------------------------------------------
# helpers de enriquecimento (join com summary local)
# ---------------------------------------------------------------------
def _load_campaign_name_to_id(advertiser_id: str) -> Dict[str, str]:
    """
    Lê campaign_summary_<advertiser_id>.csv e retorna {nome_lower: id}.
    """
    path = os.path.join(DATA_DIR, f"campaign_summary_{advertiser_id}.csv")
    _, rows = _read_csv(path)
    mapping: Dict[str, str] = {}
    for r in rows:
        # o summary pode ter "campaign_id" ou "id" dependendo do schema salvo
        cid = str(r.get("campaign_id") or r.get("id") or "").strip()
        name = str(r.get("campaign_name") or r.get("name") or "").strip()
        if cid and name:
            mapping[name.lower()] = cid
    return mapping


# ---------------------------------------------------------------------
# JOBS — daily (métricas) + summary
# ---------------------------------------------------------------------
def job_campaigns_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    """
    campaigns_daily: métricas diárias agregadas POR CAMPANHA (aggregation_type=DAILY),
    enriquecidas com campaign_id via campaign_summary local quando necessário.
    """
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"
    params = {
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join([
            "clicks","prints","cost","cpc","acos",
            "organic_units_quantity","organic_units_amount","organic_items_quantity",
            "direct_items_quantity","indirect_items_quantity","advertising_items_quantity",
            "cvr","roas","sov",
            "direct_units_quantity","indirect_units_quantity","units_quantity",
            "direct_amount","indirect_amount","total_amount",
        ]),
        "aggregation_type": "DAILY",
        "limit": 200,
        "offset": 0,
    }
    # mapeia name->id do summary (se existir)
    name_to_id = _load_campaign_name_to_id(advertiser_id)

    raw = search_all(endpoint, params, HDR_V2)

    rows: List[Dict[str, Any]] = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        flat = _flatten_raw_daily(r)

        # se não veio campaign_id, tenta resolver pelo nome
        if not flat.get("campaign_id") and flat.get("campaign_name"):
            cid = name_to_id.get(str(flat["campaign_name"]).lower())
            if cid:
                flat["campaign_id"] = cid
                log.debug(f"↔️ campaign_id preenchido via summary: {flat['campaign_name']} → {cid}")

        # precisa ter data; campaign_id é desejável, mas se não tiver, ainda salvamos (para join posterior)
        if not flat.get("date"):
            continue

        rows.append(_with_meta(flat, advertiser_id, site_id))

    out_path = os.path.join(DATA_DIR, f"campaign_daily_{advertiser_id}.csv")
    write_csv_upsert_flexible(out_path, rows, key_fields=("advertiser_id", "campaign_id", "date"))
    enviar_para_google_sheets(out_path, sheet="campaign_daily")
    return out_path


def job_ads_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    """
    ads_daily: métricas diárias agregadas POR ANÚNCIO (aggregation_type=DAILY),
    enriquecidas ao máximo com chaves disponíveis (campaign_id via campaign_name quando possível).
    """
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"
    params = {
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join([
            "clicks","prints","cost","cpc","acos",
            "organic_units_quantity","organic_units_amount","organic_items_quantity",
            "direct_items_quantity","indirect_items_quantity","advertising_items_quantity",
            "cvr","roas","sov",
            "direct_units_quantity","indirect_units_quantity","units_quantity",
            "direct_amount","indirect_amount","total_amount",
        ]),
        "aggregation_type": "DAILY",
        "limit": 200,
        "offset": 0,
    }
    name_to_id = _load_campaign_name_to_id(advertiser_id)

    raw = search_all(endpoint, params, HDR_V2)

    rows: List[Dict[str, Any]] = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        flat = _flatten_raw_daily(r)

        # tenta preencher campaign_id pelo nome da campanha, se existir
        if not flat.get("campaign_id") and flat.get("campaign_name"):
            cid = name_to_id.get(str(flat["campaign_name"]).lower())
            if cid:
                flat["campaign_id"] = cid
                log.debug(f"↔️ campaign_id preenchido via summary (ads): {flat['campaign_name']} → {cid}")

        # queremos pelo menos data e (idealmente) ad_id
        if not flat.get("date"):
            continue

        rows.append(_with_meta(flat, advertiser_id, site_id))

    out_path = os.path.join(DATA_DIR, f"ads_daily_{advertiser_id}.csv")
    write_csv_upsert_flexible(out_path, rows, key_fields=("advertiser_id", "ad_id", "date"))
    enviar_para_google_sheets(out_path, sheet="ads_daily")
    return out_path


def job_campaigns_summary(advertiser_id: str, site_id: str) -> str:
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"
    params = {"limit": 200, "offset": 0}
    raw = search_all(endpoint, params, HDR_V2)

    rows: List[Dict[str, Any]] = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        out: Dict[str, Any] = {}
        for k, v in r.items():
            if not isinstance(v, (list, dict)):
                out[k] = v

        camp = r.get("campaign") or {}
        out["campaign_id"] = camp.get("id") or r.get("campaign_id") or out.get("campaign_id")
        out["campaign_name"] = camp.get("name") or r.get("campaign_name") or out.get("campaign_name")
        out["status"] = out.get("status") or camp.get("status")
        rows.append(_with_meta(out, advertiser_id, site_id))

    out_path = os.path.join(DATA_DIR, f"campaign_summary_{advertiser_id}.csv")
    write_csv_upsert_flexible(out_path, rows, key_fields=("advertiser_id", "campaign_id"))
    enviar_para_google_sheets(out_path, sheet="campaign_summary")
    return out_path


def job_ads_summary(advertiser_id: str, site_id: str) -> str:
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"
    params = {"limit": 200, "offset": 0}
    raw = search_all(endpoint, params, HDR_V2)

    rows: List[Dict[str, Any]] = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        out: Dict[str, Any] = {}
        for k, v in r.items():
            if not isinstance(v, (list, dict)):
                out[k] = v

        ad = r.get("ad") or {}
        item = r.get("item") or {}
        camp = r.get("campaign") or {}

        out["ad_id"] = ad.get("id") or r.get("ad_id") or out.get("ad_id")
        out["campaign_id"] = camp.get("id") or r.get("campaign_id") or out.get("campaign_id")
        out["item_id"] = item.get("id") or r.get("item_id") or out.get("item_id")
        out["item_title"] = item.get("title") or ad.get("title") or r.get("title") or out.get("item_title")
        out["seller_sku"] = item.get("seller_sku") or r.get("seller_sku") or out.get("seller_sku")
        rows.append(_with_meta(out, advertiser_id, site_id))

    out_path = os.path.join(DATA_DIR, f"ads_summary_{advertiser_id}.csv")
    write_csv_upsert_flexible(out_path, rows, key_fields=("advertiser_id", "ad_id"))
    enviar_para_google_sheets(out_path, sheet="ads_summary")
    return out_path


# ---------------------------------------------------------------------
# pipeline conveniente
# ---------------------------------------------------------------------
def run_product_ads_pipeline(advertiser_id: str, site_id: str, backfill_days: int = 30) -> None:
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=max(1, backfill_days) - 1)
    df, dt = start.isoformat(), end.isoformat()

    log.info(f"🏃 Product Ads (orig metrics) {df} → {dt}")
    p1 = job_campaigns_daily(advertiser_id, site_id, df, dt)
    p2 = job_ads_daily(advertiser_id, site_id, df, dt)
    p3 = job_campaigns_summary(advertiser_id, site_id)
    p4 = job_ads_summary(advertiser_id, site_id)
    log.info(f"✔ campaign_daily → {p1}")
    log.info(f"✔ ads_daily → {p2}")
    log.info(f"✔ campaign_summary → {p3}")
    log.info(f"✔ ads_summary → {p4}")


if __name__ == "__main__":
    adv = os.getenv("ADVERTISER_ID", "").strip()
    site = os.getenv("SITE_ID", "MLB").strip()
    backfill_days = int(os.getenv("BACKFILL_DAYS", "30"))
    if not adv:
        raise SystemExit("Defina ADVERTISER_ID no ambiente.")
    run_product_ads_pipeline(adv, site, backfill_days)
