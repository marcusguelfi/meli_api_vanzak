# src/jobs.py
from __future__ import annotations

import os
import csv
import shutil
import tempfile
import logging
from typing import Any, Dict, List, Optional, Iterable, Tuple
from datetime import date, datetime, timedelta
from collections import defaultdict

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

# ordem “fixa” no cabeçalho; o restante vai depois em ordem alfabética
PRIMARY_COL_ORDER = [
    "advertiser_id", "site_id",
    "date",
    "campaign_id", "campaign_name",
    "ad_id",
    "item_id", "item_title", "seller_sku",
    "status",
]

# ---------------------------------------------------------------------
# utils
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
    # newline="\n" evita linhas em branco quando o Apps Script lê
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in header})
    shutil.move(tmp, path)
    return path


def _stable_header_from_rows(existing_header: List[str], rows: List[Dict[str, Any]]) -> List[str]:
    keys = set(existing_header)
    for r in rows:
        keys.update(r.keys())
    header: List[str] = [c for c in PRIMARY_COL_ORDER if c in keys]
    remaining = sorted(k for k in keys if k not in header and k)
    header.extend(remaining)
    return header


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

    header = _stable_header_from_rows(header_old, merged)
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
                headers={"Content-Type": "text/csv; charset=utf-8", "X-Filename": name},
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
        if not isinstance(page, dict):
            log.warning("⚠️ Resposta não-JSON em %s (offset=%s). Encerrando paginação.", endpoint, offset)
            break
        batch = page.get("results", []) or []
        out.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return out

# ---------------------------------------------------------------------
# flatten “cru”
# ---------------------------------------------------------------------
def _flatten_raw_daily(r: Dict[str, Any]) -> Dict[str, Any]:
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
# helpers de dimensão/enriquecimento
# ---------------------------------------------------------------------
def _load_campaign_dim(advertiser_id: str) -> Dict[str, Dict[str, str]]:
    path = os.path.join(DATA_DIR, f"campaign_summary_{advertiser_id}.csv")
    _, rows = _read_csv(path)
    dim: Dict[str, Dict[str, str]] = {}
    for r in rows:
        cid = str(r.get("campaign_id") or r.get("id") or "").strip()
        name = str(r.get("campaign_name") or r.get("name") or "").strip()
        if cid:
            dim[cid] = {"name": name}
    return dim


def _load_ads_dim_maps(advertiser_id: str) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    """
    Retorna dois mapas:
      - por item_id
      - por ad_id
    Cada meta inclui: ad_id, item_id, campaign_id, item_title, seller_sku, status
    """
    path = os.path.join(DATA_DIR, f"ads_summary_{advertiser_id}.csv")
    _, rows = _read_csv(path)
    by_item: Dict[str, Dict[str, str]] = {}
    by_ad: Dict[str, Dict[str, str]] = {}
    for r in rows:
        item_id = str(r.get("item_id") or "").strip()
        ad_id = str(r.get("ad_id") or "").strip()
        campaign_id = str(r.get("campaign_id") or "").strip()
        item_title = str(r.get("item_title") or r.get("title") or "").strip()
        seller_sku = str(r.get("seller_sku") or "").strip()
        status = str(r.get("status") or "").strip()
        meta = {
            "ad_id": ad_id,
            "item_id": item_id,
            "campaign_id": campaign_id,
            "item_title": item_title,
            "seller_sku": seller_sku,
            "status": status,
        }
        if item_id:
            by_item[item_id] = meta
        if ad_id:
            by_ad[ad_id] = meta
    return by_item, by_ad


_campaign_name_cache: Dict[str, str] = {}


def _fetch_campaign_name(site_id: str, campaign_id: str) -> Optional[str]:
    if not campaign_id:
        return None
    if campaign_id in _campaign_name_cache:
        return _campaign_name_cache[campaign_id]
    try:
        path = f"/advertising/{site_id}/product_ads/campaigns/{campaign_id}"
        resp = meli_get(path, headers=HDR_V2)
        name = None
        if isinstance(resp, dict):
            name = (
                resp.get("name")
                or resp.get("campaign", {}).get("name")
                or resp.get("data", {}).get("name")
            )
        if name:
            name = str(name)
            _campaign_name_cache[campaign_id] = name
            return name
    except Exception as e:
        log.debug(f"⚠️ Falha ao buscar nome da campanha {campaign_id}: {e}")
    return None

# ---------------------------------------------------------------------
# JOBS — daily + summary
# ---------------------------------------------------------------------
def job_campaigns_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    base_endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"

    dim = _load_campaign_dim(advertiser_id)
    if not dim:
        log.info("ℹ️ campaign dimension vazia — executando job_campaigns_summary para preencher…")
        job_campaigns_summary(advertiser_id, site_id)
        dim = _load_campaign_dim(advertiser_id)

    if not dim:
        log.warning("⚠️ Sem campanhas no summary; abortando campaign_daily.")
        return os.path.join(DATA_DIR, f"campaign_daily_{advertiser_id}.csv")

    ids = list(dim.keys())
    CHUNK = 50

    rows: List[Dict[str, Any]] = []
    enriched, total = 0, 0

    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i + CHUNK]
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
            "filters[campaign_ids]": ",".join(chunk),
        }

        page = meli_get(base_endpoint, params=params, headers=HDR_V2)
        data_results = page.get("results", []) if isinstance(page, dict) else (page or [])
        for r in data_results:
            if not isinstance(r, dict):
                continue
            flat = _flatten_raw_daily(r)
            if not flat.get("date"):
                continue

            cid = str(flat.get("campaign_id") or "").strip()
            cname = str(flat.get("campaign_name") or "").strip()

            if not cid and len(chunk) == 1:
                cid = chunk[0]

            if cid and not cname:
                cname = dim.get(cid, {}).get("name") or _fetch_campaign_name(site_id, cid) or cname

            if cid:
                enriched += 1

            flat["campaign_id"] = cid or flat.get("campaign_id")
            flat["campaign_name"] = cname or flat.get("campaign_name")

            rows.append(_with_meta(flat, advertiser_id, site_id))
            total += 1

    out_path = os.path.join(DATA_DIR, f"campaign_daily_{advertiser_id}.csv")
    write_csv_upsert_flexible(out_path, rows, key_fields=("advertiser_id", "campaign_id", "date"))
    log.info(f"📌 campaign_daily: {enriched}/{total} linhas com campaign_id garantido (via filtro/lookup).")
    enviar_para_google_sheets(out_path, sheet="campaign_daily")
    return out_path


def job_ads_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    """
    Enriquecimento do DAILY por ad_id (prioridade) + fallback por item_id,
    trazendo item_title, seller_sku, status e ids coerentes.
    """
    base_endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"

    by_item, by_ad = _load_ads_dim_maps(advertiser_id)
    campaign_dim = _load_campaign_dim(advertiser_id)
    if not campaign_dim:
        log.info("ℹ️ campaign dimension vazia — executando job_campaigns_summary para preencher…")
        job_campaigns_summary(advertiser_id, site_id)
        campaign_dim = _load_campaign_dim(advertiser_id)

    rows: List[Dict[str, Any]] = []

    # Se não houver dimensão de ads, itera só por campanha
    if not by_item and not by_ad:
        log.warning("⚠️ Sem ads no summary; iterando por campanhas com filtro de campanha.")
        camp_ids = list(campaign_dim.keys())
        if not camp_ids:
            log.warning("⚠️ Sem campanhas; abortando ads_daily.")
            out_path = os.path.join(DATA_DIR, f"ads_daily_{advertiser_id}.csv")
            write_csv_upsert_flexible(out_path, rows, key_fields=("advertiser_id", "ad_id", "item_id", "date"))
            enviar_para_google_sheets(out_path, sheet="ads_daily")
            return out_path
        camp_chunks = [camp_ids[i:i+50] for i in range(0, len(camp_ids), 50)]
        for cchunk in camp_chunks:
            base_params = {
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
                "filters[campaign_ids]": ",".join(cchunk),
            }
            raw = search_all(base_endpoint, base_params, HDR_V2)
            for r in raw:
                flat = _flatten_raw_daily(r)
                if not flat.get("date"):
                    continue
                cid = str(flat.get("campaign_id") or "").strip()
                if cid and not flat.get("campaign_name"):
                    flat["campaign_name"] = campaign_dim.get(cid, {}).get("name") or _fetch_campaign_name(site_id, cid)

                # enriquecer por ad_id > item_id
                aid = str(flat.get("ad_id") or "").strip()
                iid = str(flat.get("item_id") or "").strip()
                meta = (aid and by_ad.get(aid)) or (iid and by_item.get(iid)) or {}

                if meta:
                    flat.setdefault("ad_id", meta.get("ad_id", ""))
                    flat.setdefault("item_id", meta.get("item_id", ""))
                    if not flat.get("item_title"): flat["item_title"] = meta.get("item_title", "")
                    if not flat.get("seller_sku"): flat["seller_sku"] = meta.get("seller_sku", "")
                    if not flat.get("status"):     flat["status"]     = meta.get("status", "")

                rows.append(_with_meta(flat, advertiser_id, site_id))
    else:
        # Mapa campanha -> itens a partir de by_item
        camp_to_items: Dict[str, List[str]] = defaultdict(list)
        for iid, meta in by_item.items():
            cid = meta.get("campaign_id")
            if cid:
                camp_to_items[cid].append(iid)
        for cid in list(campaign_dim.keys()):
            camp_to_items.setdefault(cid, [])

        for cid, items in camp_to_items.items():
            if items:
                for i in range(0, len(items), 100):
                    item_chunk = items[i:i+100]
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
                        "filters[campaign_ids]": cid,
                        "filters[item_ids]": ",".join(item_chunk),
                    }
                    page = meli_get(base_endpoint, params=params, headers=HDR_V2)
                    data_results = page.get("results", []) if isinstance(page, dict) else (page or [])
                    for r in data_results:
                        if not isinstance(r, dict):
                            continue
                        flat = _flatten_raw_daily(r)
                        if not flat.get("date"):
                            continue

                        # garantir campaign
                        flat["campaign_id"] = flat.get("campaign_id") or cid
                        if not flat.get("campaign_name"):
                            flat["campaign_name"] = campaign_dim.get(cid, {}).get("name") or _fetch_campaign_name(site_id, cid)

                        # enriquecer por ad_id (prioridade) e depois item_id
                        aid = str(flat.get("ad_id") or "").strip()
                        iid = str(flat.get("item_id") or "").strip()
                        meta = (aid and by_ad.get(aid)) or (iid and by_item.get(iid)) or {}

                        if meta:
                            if not flat.get("ad_id"):       flat["ad_id"] = meta.get("ad_id", "")
                            if not flat.get("item_id"):     flat["item_id"] = meta.get("item_id", "")
                            if not flat.get("item_title"):  flat["item_title"] = meta.get("item_title", "")
                            if not flat.get("seller_sku"):  flat["seller_sku"] = meta.get("seller_sku", "")
                            if not flat.get("status"):      flat["status"] = meta.get("status", "")

                        rows.append(_with_meta(flat, advertiser_id, site_id))
            else:
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
                    "filters[campaign_ids]": cid,
                }
                page = meli_get(base_endpoint, params=params, headers=HDR_V2)
                data_results = page.get("results", []) if isinstance(page, dict) else (page or [])
                for r in data_results:
                    if not isinstance(r, dict):
                        continue
                    flat = _flatten_raw_daily(r)
                    if not flat.get("date"):
                        continue

                    flat["campaign_id"] = flat.get("campaign_id") or cid
                    if not flat.get("campaign_name"):
                        flat["campaign_name"] = campaign_dim.get(cid, {}).get("name") or _fetch_campaign_name(site_id, cid)

                    # enriquecer por ad_id (prioridade) e depois item_id
                    aid = str(flat.get("ad_id") or "").strip()
                    iid = str(flat.get("item_id") or "").strip()
                    meta = (aid and by_ad.get(aid)) or (iid and by_item.get(iid)) or {}

                    if meta:
                        if not flat.get("ad_id"):       flat["ad_id"] = meta.get("ad_id", "")
                        if not flat.get("item_id"):     flat["item_id"] = meta.get("item_id", "")
                        if not flat.get("item_title"):  flat["item_title"] = meta.get("item_title", "")
                        if not flat.get("seller_sku"):  flat["seller_sku"] = meta.get("seller_sku", "")
                        if not flat.get("status"):      flat["status"] = meta.get("status", "")

                    rows.append(_with_meta(flat, advertiser_id, site_id))

    out_path = os.path.join(DATA_DIR, f"ads_daily_{advertiser_id}.csv")
    # chave inclui ad_id e item_id (e campaign_id + date) para estabilidade
    write_csv_upsert_flexible(out_path, rows, key_fields=("advertiser_id", "campaign_id", "ad_id", "item_id", "date"))
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
        out["status"] = out.get("status") or ad.get("status") or camp.get("status")
        rows.append(_with_meta(out, advertiser_id, site_id))

    out_path = os.path.join(DATA_DIR, f"ads_summary_{advertiser_id}.csv")
    # chave inclui item_id para não colapsar linhas quando ad_id vier vazio
    write_csv_upsert_flexible(out_path, rows, key_fields=("advertiser_id", "ad_id", "item_id"))
    enviar_para_google_sheets(out_path, sheet="ads_summary")
    return out_path

# ---------------------------------------------------------------------
# pipeline — summaries + dailies
# ---------------------------------------------------------------------
def run_product_ads_pipeline(advertiser_id: str, site_id: str, backfill_days: int = 30, date_from: Optional[str] = None, date_to: Optional[str] = None) -> None:
    if date_from and date_to:
        df, dt = date_from, date_to
    else:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=max(1, backfill_days) - 1)
        df, dt = start.isoformat(), end.isoformat()

    log.info(f"🏃 Product Ads (orig metrics) {df} → {dt}")

    p3 = job_campaigns_summary(advertiser_id, site_id)
    p4 = job_ads_summary(advertiser_id, site_id)

    p1 = job_campaigns_daily(advertiser_id, site_id, df, dt)
    p2 = job_ads_daily(advertiser_id, site_id, df, dt)

    log.info(f"✔ campaign_summary → {p3}")
    log.info(f"✔ ads_summary → {p4}")
    log.info(f"✔ campaign_daily → {p1}")
    log.info(f"✔ ads_daily → {p2}")


if __name__ == "__main__":
    adv = os.getenv("ADVERTISER_ID", "").strip()
    site = os.getenv("SITE_ID", "MLB").strip()
    backfill_days = int(os.getenv("BACKFILL_DAYS", "30"))
    if not adv:
        raise SystemExit("Defina ADVERTISER_ID no ambiente.")
    run_product_ads_pipeline(adv, site, backfill_days)
