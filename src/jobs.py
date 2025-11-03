# src/jobs.py
from __future__ import annotations

import os
import csv
import shutil
import tempfile
import logging
import time
import random
from typing import Any, Dict, List, Optional, Iterable, Tuple
from datetime import date, timedelta
from collections import defaultdict
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

import requests

from .meli_client import meli_get

# ---------------------------------------------------------------------
# Logging e constantes
# ---------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

HDR_V1 = {"Api-Version": "1"}
HDR_V2 = {"Api-Version": "2"}

APPSCRIPT_URL = os.getenv("GOOGLE_APPSCRIPT_URL", "").strip().strip('"').strip("'")
APPSCRIPT_TOKEN = os.getenv("GOOGLE_APPSCRIPT_TOKEN", "").strip()

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
DATA_DIR = PROCESSED_DIR  # compatibilidade
RESET_CSVS = os.getenv("RESET_CSVS", "").strip() in ("1", "true", "True")

# Ordem base do cabeçalho; demais colunas seguem em ordem alfabética
PRIMARY_COL_ORDER = [
    "advertiser_id", "site_id",
    "date",
    "campaign_id", "campaign_name",
    "ad_id",
    "item_id", "item_title", "seller_sku",
    "status",
]

# Métricas padrão para os relatórios DAILY
METRICS_DAILY = [
    "clicks", "prints", "cost", "cpc", "acos",
    "organic_units_quantity", "organic_units_amount", "organic_items_quantity",
    "direct_items_quantity", "indirect_items_quantity", "advertising_items_quantity",
    "cvr", "roas", "sov",
    "direct_units_quantity", "indirect_units_quantity", "units_quantity",
    "direct_amount", "indirect_amount", "total_amount",
]

# ---------------------------------------------------------------------
# Utils de arquivo/CSV
# ---------------------------------------------------------------------
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
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in header})
    shutil.move(tmp, path)
    return path

def _stable_header_from_rows(
    existing_header: List[str],
    rows: List[Dict[str, Any]],
    strict: bool = False,
) -> List[str]:
    """
    strict=False (default): mantém colunas antigas do arquivo + novas dos rows.
    strict=True: usa SOMENTE as colunas presentes nos rows (não herda cabeçalho antigo).
    """
    keys: set = set()
    if not strict:
        keys.update(existing_header or [])
    for r in rows:
        keys.update(k for k in r.keys() if k)

    header: List[str] = [c for c in PRIMARY_COL_ORDER if c in keys]
    remaining = sorted(k for k in keys if k not in header)
    header.extend(remaining)
    return header

def _sort_rows(rows: List[Dict[str, Any]], sort_by: Optional[Tuple[str, ...]]) -> List[Dict[str, Any]]:
    if not sort_by:
        return rows
    def key_func(r: Dict[str, Any]):
        return tuple(str(r.get(k, "")) for k in sort_by)
    return sorted(rows, key=key_func)

def write_csv_upsert_flexible(
    path: str,
    new_rows: List[Dict[str, Any]],
    key_fields: Tuple[str, ...],
    *,
    strict_header: bool = False,
    drop_fields: Optional[Iterable[str]] = None,
    sort_by: Optional[Tuple[str, ...]] = None,
    reset_file: bool = False,
    fallback_header: Optional[Iterable[str]] = None,
) -> str:
    """
    Upsert idempotente e flexível.
    - strict_header=True: cabeçalho reflete SOMENTE colunas presentes nas linhas finais.
    - drop_fields: remove campos indesejados de todas as linhas antes de escrever.
    - sort_by: ordena linhas pelo(s) campo(s) informado(s).
    - reset_file: ignora conteúdo anterior (recria o arquivo).
    - fallback_header: header base caso não exista arquivo (ou reset_file=True).
    """
    if reset_file and os.path.exists(path):
        try:
            os.remove(path)
            log.info("🧹 Reset do CSV solicitado: %s", path)
        except Exception as e:
            log.warning("Não foi possível apagar %s: %s", path, e)

    header_old, existing = _read_csv(path)

    if not header_old and fallback_header:
        header_old = list(fallback_header)

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

    # remove campos indesejados
    if drop_fields:
        drop_fields = tuple(drop_fields)
        for r in merged:
            for f in drop_fields:
                r.pop(f, None)

    # ordena
    merged = _sort_rows(merged, sort_by)

    # escreve
    header = _stable_header_from_rows(header_old, merged, strict=strict_header)
    _write_atomic(path, header, merged)
    log.info("💾 CSV atualizado: %s (+%d linhas novas/atualizadas)", path, len(new_rows))
    return path

# ---------------------------------------------------------------------
# Upload para Apps Script (streaming + retry + URL mascarada)
# ---------------------------------------------------------------------
def _post_with_retry(url: str, path: str, headers: Dict[str, str], tries: int = 3, base: float = 1.8):
    for a in range(1, tries + 1):
        try:
            with open(path, "rb") as f:
                resp = requests.post(url, data=f, headers=headers, timeout=(10, 180))
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if a == tries:
                raise
            wait = base ** (a - 1) + random.uniform(0, 0.5)
            log.warning("Retry upload %d/%d em %.2fs: %s", a, tries, wait, e)
            time.sleep(wait)

def enviar_para_google_sheets(caminho_csv: str, sheet: Optional[str] = None) -> None:
    if not APPSCRIPT_URL:
        log.info("GOOGLE_APPSCRIPT_URL não configurado — pulando upload.")
        return

    parts = urlsplit(APPSCRIPT_URL)
    q = dict(parse_qsl(parts.query))
    if APPSCRIPT_TOKEN:
        q["token"] = APPSCRIPT_TOKEN
    if sheet:
        q["sheet"] = sheet
    name = os.path.basename(caminho_csv)
    q["name"] = name
    url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), ""))

    masked_url = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

    try:
        size = os.path.getsize(caminho_csv)
        headers = {"Content-Type": "text/csv; charset=utf-8", "X-Filename": name}
        log.info("⬆️ Enviando %s (%s bytes) → %s", name, size, masked_url)
        resp = _post_with_retry(url, caminho_csv, headers)
        log.info("✅ Upload OK (%s) – aba %s", resp.status_code, sheet or "dados")
    except Exception as e:
        log.exception("❌ Falha no upload ao Apps Script: %s", e)

# ---------------------------------------------------------------------
# Chamadas Meli (paginadas)
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
# Flatten diário e meta
# ---------------------------------------------------------------------
def _flatten_raw_daily(r: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
    # data
    out["date"] = out.get("date") or r.get("day") or r.get("report_date")
    if out.get("date"):
        out["date"] = str(out["date"])[:10]  # YYYY-MM-DD

    # aninhados
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
# Dimensões e cache
# ---------------------------------------------------------------------
def _load_campaign_dim(advertiser_id: str) -> Dict[str, Dict[str, str]]:
    path = os.path.join(PROCESSED_DIR, f"campaign_summary_{advertiser_id}.csv")
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
    Retorna dois mapas (por item_id e por ad_id) com meta:
    ad_id, item_id, campaign_id, item_title, seller_sku, status
    """
    path = os.path.join(PROCESSED_DIR, f"ads_summary_{advertiser_id}.csv")
    _, rows = _read_csv(path)
    by_item: Dict[str, Dict[str, str]] = {}
    by_ad: Dict[str, Dict[str, str]] = {}
    for r in rows:
        item_id = str(r.get("item_id") or "").strip()
        ad_id = str(r.get("ad_id") or "").strip()
        meta = {
            "ad_id": ad_id,
            "item_id": item_id,
            "campaign_id": str(r.get("campaign_id") or "").strip(),
            "item_title": str(r.get("item_title") or r.get("title") or "").strip(),
            "seller_sku": str(r.get("seller_sku") or "").strip(),
            "status": str(r.get("status") or "").strip(),
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
            name = resp.get("name") or resp.get("campaign", {}).get("name") or resp.get("data", {}).get("name")
        if name:
            name = str(name)
            _campaign_name_cache[campaign_id] = name
            return name
    except Exception as e:
        log.debug("⚠️ Falha ao buscar nome da campanha %s: %s", campaign_id, e)
    return None

# ---------------------------------------------------------------------
# Helpers de promoção RAW → PROCESSED
# ---------------------------------------------------------------------
def _promote_to_processed(path_raw: str, sheet_name: str, key_fields: Tuple[str, ...],
                          ensure_date_sorted: bool = True) -> str:
    header, rows = _read_csv(path_raw)
    if ensure_date_sorted and rows:
        rows = _sort_rows(rows, tuple(k for k in key_fields if k in ("advertiser_id", "campaign_id", "date", "ad_id", "item_id")))
    out_path = os.path.join(PROCESSED_DIR, os.path.basename(path_raw))
    header_final = _stable_header_from_rows([], rows, strict=False)
    _write_atomic(out_path, header_final, rows)
    enviar_para_google_sheets(out_path, sheet=sheet_name)
    log.info("📦 Promovido RAW → PROCESSED: %s", out_path)
    return out_path

# ---------------------------------------------------------------------
# JOBS — summaries
# ---------------------------------------------------------------------
def job_campaigns_summary(advertiser_id: str, site_id: str) -> str:
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"
    raw = search_all(endpoint, {"limit": 200}, HDR_V2)

    rows: List[Dict[str, Any]] = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        out: Dict[str, Any] = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
        camp = r.get("campaign") or {}
        out["campaign_id"] = camp.get("id") or r.get("campaign_id") or out.get("campaign_id")
        out["campaign_name"] = camp.get("name") or r.get("campaign_name") or out.get("campaign_name")
        out["status"] = out.get("status") or camp.get("status")
        rows.append(_with_meta(out, advertiser_id, site_id))

    out_path = os.path.join(PROCESSED_DIR, f"campaign_summary_{advertiser_id}.csv")
    write_csv_upsert_flexible(
        out_path, rows, key_fields=("advertiser_id", "campaign_id"),
        sort_by=("advertiser_id", "campaign_id"),
        reset_file=RESET_CSVS,
        fallback_header=("advertiser_id","site_id","campaign_id","campaign_name","status"),
    )
    enviar_para_google_sheets(out_path, sheet="campaign_summary")
    return out_path

def job_ads_summary(advertiser_id: str, site_id: str) -> str:
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"
    raw = search_all(endpoint, {"limit": 200}, HDR_V2)

    rows: List[Dict[str, Any]] = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        out: Dict[str, Any] = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
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

    out_path = os.path.join(PROCESSED_DIR, f"ads_summary_{advertiser_id}.csv")
    write_csv_upsert_flexible(
        out_path, rows,
        key_fields=("advertiser_id", "ad_id", "item_id"),
        sort_by=("advertiser_id", "campaign_id", "ad_id", "item_id"),
        reset_file=RESET_CSVS,
        fallback_header=("advertiser_id","site_id","campaign_id","ad_id","item_id","item_title","seller_sku","status"),
    )
    enviar_para_google_sheets(out_path, sheet="ads_summary")
    return out_path

# ---------------------------------------------------------------------
# JOBS — dailies
# ---------------------------------------------------------------------
def job_campaigns_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    base_endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"

    dim = _load_campaign_dim(advertiser_id)
    if not dim:
        log.info("ℹ️ campaign dimension vazia — preenchendo via summary…")
        job_campaigns_summary(advertiser_id, site_id)
        dim = _load_campaign_dim(advertiser_id)

    if not dim:
        log.warning("⚠️ Sem campanhas no summary; abortando campaign_daily.")
        return os.path.join(PROCESSED_DIR, f"campaign_daily_{advertiser_id}.csv")

    ids = list(dim.keys())
    CHUNK = 50
    rows: List[Dict[str, Any]] = []

    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i + CHUNK]
        params = {
            "date_from": date_from,
            "date_to": date_to,
            "metrics": ",".join(METRICS_DAILY),
            "aggregation_type": "DAILY",
            "limit": 200,
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

            flat["campaign_id"] = cid or flat.get("campaign_id")
            flat["campaign_name"] = cname or flat.get("campaign_name")

            rows.append(_with_meta(flat, advertiser_id, site_id))

    # remove colunas de nível ad/item + header estrito + ordenação por data
    out_path = os.path.join(PROCESSED_DIR, f"campaign_daily_{advertiser_id}.csv")
    write_csv_upsert_flexible(
        out_path,
        rows,
        key_fields=("advertiser_id", "campaign_id", "date"),
        strict_header=True,
        drop_fields=("ad_id", "item_id", "item_title", "seller_sku", "status"),
        sort_by=("advertiser_id", "campaign_id", "date"),
        reset_file=RESET_CSVS,
        fallback_header=("advertiser_id","site_id","date","campaign_id","campaign_name", *METRICS_DAILY),
    )
    enviar_para_google_sheets(out_path, sheet="campaign_daily")
    return out_path

def job_ads_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    """
    DAILY de Ads via:
      /advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search

    Estrutura simplificada:
      - Apenas campaign_id e item_id como chaves de identificação
      - Sem enrichment (ad_id, item_title, seller_sku, status ficam no summary)
      - Mantém todas as métricas de performance
    """
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"

    campaign_dim = _load_campaign_dim(advertiser_id)
    if not campaign_dim:
        log.info("ℹ️ campaign dimension vazia — preenchendo via summary…")
        job_campaigns_summary(advertiser_id, site_id)
        campaign_dim = _load_campaign_dim(advertiser_id)

    rows: List[Dict[str, Any]] = []
    base_params = {
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(METRICS_DAILY),
        "aggregation_type": "DAILY",
        "limit": 200,
    }

    for cid in campaign_dim.keys():
        params = {**base_params, "filters[campaign_id]": cid}
        data = search_all(endpoint, params, HDR_V2)
        for r in (data or []):
            if not isinstance(r, dict):
                continue

            flat = _flatten_raw_daily(r)
            if not flat.get("date"):
                continue

            # mantemos apenas campaign_id, item_id e métricas
            filtered = {
                "advertiser_id": advertiser_id,
                "site_id": site_id,
                "date": flat.get("date"),
                "campaign_id": flat.get("campaign_id") or cid,
                "item_id": flat.get("item_id") or "",
            }

            # adiciona todas as métricas disponíveis
            for m in METRICS_DAILY:
                if m in flat:
                    filtered[m] = flat[m]

            rows.append(filtered)

    # grava RAW simplificado
    out_path_raw = os.path.join(RAW_DIR, f"ads_daily_{advertiser_id}.csv")
    write_csv_upsert_flexible(
        out_path_raw,
        rows,
        key_fields=("advertiser_id", "campaign_id", "item_id", "date"),
        sort_by=("advertiser_id", "campaign_id", "item_id", "date"),
        reset_file=RESET_CSVS,
        fallback_header=("advertiser_id", "site_id", "date", "campaign_id", "item_id", *METRICS_DAILY),
    )

    log.info("📊 ads_daily (simplificado): total de linhas para %s = %d", advertiser_id, len(rows))

    # promove RAW → PROCESSED
    return _promote_to_processed(
        out_path_raw, "ads_daily",
        ("advertiser_id", "campaign_id", "item_id", "date"),
        ensure_date_sorted=True,
    )

# ---------------------------------------------------------------------
# Pipeline principal (range com backfill de N dias; default termina em D-1)
# ---------------------------------------------------------------------
def run_product_ads_pipeline(advertiser_id: str, site_id: str, backfill_days: int = 30,
                             date_from: Optional[str] = None, date_to: Optional[str] = None) -> None:
    if date_from and date_to:
        df, dt = date_from, date_to
    else:
        end = date.today() - timedelta(days=1)               # fecha em D-1
        start = end - timedelta(days=max(1, backfill_days) - 1)
        df, dt = start.isoformat(), end.isoformat()

    log.info("🏃 Product Ads %s → %s", df, dt)

    p3 = job_campaigns_summary(advertiser_id, site_id)
    p4 = job_ads_summary(advertiser_id, site_id)

    p1 = job_campaigns_daily(advertiser_id, site_id, df, dt)
    p2 = job_ads_daily(advertiser_id, site_id, df, dt)

    log.info("✔ campaign_summary → %s", p3)
    log.info("✔ ads_summary → %s", p4)
    log.info("✔ campaign_daily → %s", p1)
    log.info("✔ ads_daily → %s", p2)

# Execução direta via env (útil p/ cron/container)
if __name__ == "__main__":
    adv = os.getenv("ADVERTISER_ID", "").strip()
    site = os.getenv("SITE_ID", "MLB").strip()
    backfill_days = int(os.getenv("BACKFILL_DAYS", "30"))
    if not adv:
        raise SystemExit("Defina ADVERTISER_ID no ambiente.")
    run_product_ads_pipeline(adv, site, backfill_days)
