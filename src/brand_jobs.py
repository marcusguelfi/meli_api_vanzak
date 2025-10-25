# src/brand_jobs.py
from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from requests.exceptions import HTTPError

from .report_utils import (
    log, HDR_V2,
    RAW_DIR, PROCESSED_DIR, RESET_CSVS,
    METRICS_DAILY,
    _read_csv,
    write_csv_upsert_flexible,
    enviar_para_google_sheets,
    _flatten_raw_daily, _with_meta,
    _promote_to_processed,
)
from .meli_client import meli_get

# Se quiser isolar métricas de Brand Ads, troque aqui:
METRICS_DAILY_BRAND = METRICS_DAILY

# Cache do "base path" válido para Brand Ads por advertiser
# Ex.: "/advertising/advertisers/{advertiser_id}/brand_ads"
_brand_base_cache: Dict[str, str] = {}


def _candidate_bases(advertiser_id: str, site_id: str) -> List[str]:
    """
    Tenta em ordem:
      1) /advertising/advertisers/{advertiser_id}/brand_ads
      2) /advertising/advertisers/{advertiser_id}/brands_ads          (plural)
      3) /advertising/{site_id}/advertisers/{advertiser_id}/brand_ads
      4) /advertising/{site_id}/advertisers/{advertiser_id}/brands_ads (plural)
    """
    return [
        f"/advertising/advertisers/{advertiser_id}/brand_ads",
        f"/advertising/advertisers/{advertiser_id}/brands_ads",
        f"/advertising/{site_id}/advertisers/{advertiser_id}/brand_ads",
        f"/advertising/{site_id}/advertisers/{advertiser_id}/brands_ads",
    ]


def _resolve_brand_base(advertiser_id: str, site_id: str) -> str:
    """
    Descobre e cacheia qual base path de Brand Ads funciona no tenant.
    Faz um GET leve em /campaigns/search?limit=1 para testar.
    """
    key = f"{site_id}:{advertiser_id}"
    if key in _brand_base_cache:
        return _brand_base_cache[key]

    for base in _candidate_bases(advertiser_id, site_id):
        test_ep = f"{base}/campaigns/search"
        try:
            log.info("➡️ Testando base de Brand Ads: %s", test_ep)
            _ = meli_get(test_ep, params={"limit": 1}, headers=HDR_V2)
            # Se não lança exceção, funcionou
            _brand_base_cache[key] = base
            log.info("✅ Base Brand Ads selecionada: %s", base)
            return base
        except HTTPError as e:
            status = getattr(e.response, "status_code", None)
            # Só troca de base em 400/404; outros erros interrompem
            if status not in (400, 404):
                raise
            log.info("… base %s não disponível (status %s), tentando próxima…", base, status)

    raise RuntimeError("Nenhum endpoint de Brand Ads funcionou neste tenant (testadas 4 variações).")


def _brand_search_all(advertiser_id: str, site_id: str, suffix: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Paginação usando a base resolvida dinamicamente.
    """
    base = _resolve_brand_base(advertiser_id, site_id)
    out: List[Dict[str, Any]] = []
    limit = int(params.get("limit", 200))
    offset = 0
    while True:
        ep = f"{base}{suffix}"
        page = meli_get(ep, params={**params, "limit": limit, "offset": offset}, headers=HDR_V2)
        if not isinstance(page, dict):
            log.warning("⚠️ Resposta não-JSON em %s (offset=%s). Encerrando paginação.", ep, offset)
            break
        batch = page.get("results", []) or []
        out.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return out


# ---------------------------------------------------------------------
# Dimensões (Brand)
# ---------------------------------------------------------------------
def _load_brand_campaign_dim(advertiser_id: str) -> Dict[str, Dict[str, str]]:
    path = os.path.join(PROCESSED_DIR, f"brand_campaign_summary_{advertiser_id}.csv")
    _, rows = _read_csv(path)
    dim: Dict[str, Dict[str, str]] = {}
    for r in rows:
        cid = str(r.get("campaign_id") or r.get("id") or "").strip()
        name = str(r.get("campaign_name") or r.get("name") or "").strip()
        if cid:
            dim[cid] = {"name": name}
    return dim


def _load_brand_ads_dim_maps(advertiser_id: str) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    path = os.path.join(PROCESSED_DIR, f"brand_ads_summary_{advertiser_id}.csv")
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


# ---------------------------------------------------------------------
# SUMMARY (Brand)
# ---------------------------------------------------------------------
def job_brand_campaigns_summary(advertiser_id: str, site_id: str) -> str:
    """
    Summary de campanhas (Brand Ads).
    """
    rows: List[Dict[str, Any]] = []
    data = _brand_search_all(advertiser_id, site_id, "/campaigns/search", {"limit": 200})

    for r in data or []:
        if not isinstance(r, dict):
            continue
        out: Dict[str, Any] = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
        camp = r.get("campaign") or {}
        out["campaign_id"] = camp.get("id") or r.get("campaign_id") or out.get("campaign_id")
        out["campaign_name"] = camp.get("name") or r.get("campaign_name") or out.get("campaign_name")
        out["status"] = out.get("status") or camp.get("status")
        rows.append(_with_meta(out, advertiser_id, site_id))

    out_path = os.path.join(PROCESSED_DIR, f"brand_campaign_summary_{advertiser_id}.csv")
    write_csv_upsert_flexible(
        out_path, rows,
        key_fields=("advertiser_id", "campaign_id"),
        sort_by=("advertiser_id", "campaign_id"),
        reset_file=RESET_CSVS,
        fallback_header=("advertiser_id","site_id","campaign_id","campaign_name","status"),
    )
    enviar_para_google_sheets(out_path, sheet="brand_campaign_summary")
    return out_path


def job_brand_ads_summary(advertiser_id: str, site_id: str) -> str:
    """
    Summary de ads (Brand Ads).
    """
    rows: List[Dict[str, Any]] = []
    data = _brand_search_all(advertiser_id, site_id, "/ads/search", {"limit": 200})

    for r in data or []:
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

    out_path = os.path.join(PROCESSED_DIR, f"brand_ads_summary_{advertiser_id}.csv")
    write_csv_upsert_flexible(
        out_path, rows,
        key_fields=("advertiser_id", "ad_id", "item_id"),
        sort_by=("advertiser_id", "campaign_id", "ad_id", "item_id"),
        reset_file=RESET_CSVS,
        fallback_header=("advertiser_id","site_id","campaign_id","ad_id","item_id","item_title","seller_sku","status"),
    )
    enviar_para_google_sheets(out_path, sheet="brand_ads_summary")
    return out_path


# ---------------------------------------------------------------------
# DAILY (Brand)
# ---------------------------------------------------------------------
def job_brand_campaigns_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    """
    Métricas diárias de campanhas (Brand Ads).
    """
    dim = _load_brand_campaign_dim(advertiser_id)
    if not dim:
        log.info("ℹ️ brand campaign dimension vazia — preenchendo via summary…")
        job_brand_campaigns_summary(advertiser_id, site_id)
        dim = _load_brand_campaign_dim(advertiser_id)
    if not dim:
        log.warning("⚠️ Sem campanhas de Brand; abortando brand_campaign_daily.")
        return os.path.join(PROCESSED_DIR, f"brand_campaign_daily_{advertiser_id}.csv")

    ids = list(dim.keys())
    CHUNK = 50
    rows: List[Dict[str, Any]] = []

    metrics = ",".join(METRICS_DAILY_BRAND)
    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i+CHUNK]
        params = {
            "date_from": date_from,
            "date_to": date_to,
            "metrics": metrics,
            "aggregation_type": "DAILY",
            "limit": 200,
            "filters[campaign_ids]": ",".join(chunk),
        }
        data = _brand_search_all(advertiser_id, site_id, "/campaigns/search", params)
        for r in data or []:
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
                cname = dim.get(cid, {}).get("name") or cname

            flat["campaign_id"] = cid or flat.get("campaign_id")
            flat["campaign_name"] = cname or flat.get("campaign_name")
            rows.append(_with_meta(flat, advertiser_id, site_id))

    out_path = os.path.join(PROCESSED_DIR, f"brand_campaign_daily_{advertiser_id}.csv")
    write_csv_upsert_flexible(
        out_path, rows,
        key_fields=("advertiser_id", "campaign_id", "date"),
        strict_header=True,
        drop_fields=("ad_id", "item_id", "item_title", "seller_sku", "status"),
        sort_by=("advertiser_id", "campaign_id", "date"),
        reset_file=RESET_CSVS,
        fallback_header=("advertiser_id","site_id","date","campaign_id","campaign_name", *METRICS_DAILY_BRAND),
    )
    enviar_para_google_sheets(out_path, sheet="brand_campaign_daily")
    return out_path


def job_brand_ads_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    """
    Métricas diárias de ads (Brand Ads) por campanha.
    Mantém linhas mesmo se a API não devolver ad_id/item_id; enriquece quando possível.
    """
    # tentar resolver base antes — evita 404 repetido e dá log claro
    _ = _resolve_brand_base(advertiser_id, site_id)

    rows: List[Dict[str, Any]] = []
    base_params = {
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(METRICS_DAILY_BRAND),
        "aggregation_type": "DAILY",
        "limit": 200,
    }

    # Sem depender de summary/ads: iteramos por campanha a partir do summary de brand
    dim = _load_brand_campaign_dim(advertiser_id)
    if not dim:
        job_brand_campaigns_summary(advertiser_id, site_id)
        dim = _load_brand_campaign_dim(advertiser_id)

    for cid in dim.keys():
        params = {**base_params, "filters[campaign_id]": cid}
        data = _brand_search_all(advertiser_id, site_id, "/ads/search", params)
        for r in (data or []):
            if not isinstance(r, dict):
                continue
            flat = _flatten_raw_daily(r)
            if not flat.get("date"):
                continue
            flat["campaign_id"] = flat.get("campaign_id") or cid
            flat["campaign_name"] = flat.get("campaign_name") or dim.get(cid, {}).get("name", "")
            rows.append(_with_meta(flat, advertiser_id, site_id))

    # RAW completo
    out_path_raw = os.path.join(RAW_DIR, f"brand_ads_daily_{advertiser_id}.csv")
    write_csv_upsert_flexible(
        out_path_raw, rows,
        key_fields=("advertiser_id", "campaign_id", "ad_id", "item_id", "date"),
        sort_by=("advertiser_id", "campaign_id", "date", "ad_id", "item_id"),
        reset_file=RESET_CSVS,
        fallback_header=("advertiser_id","site_id","date","campaign_id","campaign_name",
                         "ad_id","item_id","item_title","seller_sku","status", *METRICS_DAILY_BRAND),
    )

    # PROCESSED: promove mantendo ordenação por data
    return _promote_to_processed(
        out_path_raw, "brand_ads_daily",
        ("advertiser_id", "campaign_id", "ad_id", "item_id", "date"),
        ensure_date_sorted=True
    )


# ---------------------------------------------------------------------
# Runner (Brand Ads)
# ---------------------------------------------------------------------
def run_brand_ads_pipeline(advertiser_id: str, site_id: str, backfill_days: int = 30,
                           date_from: Optional[str] = None, date_to: Optional[str] = None) -> None:
    if date_from and date_to:
        df, dt = date_from, date_to
    else:
        end = date.today() - timedelta(days=1)               # fecha em D-1
        start = end - timedelta(days=max(1, backfill_days) - 1)
        df, dt = start.isoformat(), end.isoformat()

    log.info("🏃 Brand Ads %s → %s", df, dt)

    p3 = job_brand_campaigns_summary(advertiser_id, site_id)
    p4 = job_brand_ads_summary(advertiser_id, site_id)

    p1 = job_brand_campaigns_daily(advertiser_id, site_id, df, dt)
    p2 = job_brand_ads_daily(advertiser_id, site_id, df, dt)

    log.info("✔ brand_campaign_summary → %s", p3)
    log.info("✔ brand_ads_summary → %s", p4)
    log.info("✔ brand_campaign_daily → %s", p1)
    log.info("✔ brand_ads_daily → %s", p2)


if __name__ == "__main__":
    adv = os.getenv("ADVERTISER_ID", "").strip()
    site = os.getenv("SITE_ID", "MLB").strip()
    backfill_days = int(os.getenv("BACKFILL_DAYS", "30"))
    if not adv:
        raise SystemExit("Defina ADVERTISER_ID no ambiente.")
    run_brand_ads_pipeline(adv, site, backfill_days)
