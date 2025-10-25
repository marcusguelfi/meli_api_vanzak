# src/diagnose_ads_routes.py
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta
from typing import Any, Dict, Optional, Tuple, List

import requests

from src.meli_client import meli_get
from src.product_ads_endpoints import ENDPOINTS

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("diagnose_ads_routes")

MIN_METRICS = ["prints"]  # métrica leve para validar agregações

def _date_range(days: int = 2) -> Tuple[str, str]:
    days = max(1, min(days, 30))
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    return start.isoformat(), end.isoformat()

def _status_from_exc(e: Exception) -> Optional[int]:
    if isinstance(e, requests.HTTPError) and getattr(e, "response", None) is not None:
        try:
            return e.response.status_code  # type: ignore[attr-defined]
        except Exception:
            return None
    return None

def _api_causes(e: Exception) -> str:
    if isinstance(e, requests.HTTPError) and getattr(e, "response", None) is not None:
        try:
            js = e.response.json()  # type: ignore[attr-defined]
            causes = js.get("cause") or []
            if isinstance(causes, list) and causes:
                msg = "; ".join(str(c.get("description") or c) for c in causes)
                return f" | cause: {msg}"
        except Exception:
            pass
    return ""

def _pretty(label: str, status: int | str, note: str = "", ok: bool = False) -> None:
    flag = "✅" if ok else ("⚠️" if isinstance(status, int) and 400 <= status < 500 else "❌")
    note = f" — {note}" if note else ""
    print(f"{flag} {label:<30} → {status}{note}")

def _format_path(template: str, **kwargs) -> str:
    # Só injeta chaves que realmente aparecem no template
    safe_kwargs = {k: v for k, v in kwargs.items() if f"{{{k}}}" in template}
    return template.format(**safe_kwargs)

def _try_get(label: str, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    # meli_get já loga o GET; aqui apenas reportamos o status/nota
    try:
        data = meli_get(path, params=params)
        n = len(data.get("results", []) or []) if isinstance(data, dict) else None
        _pretty(label, 200, (f"results={n}" if n is not None else ""), ok=True)
        return data if isinstance(data, dict) else None
    except Exception as e:
        st = _status_from_exc(e) or "ERR"
        _pretty(label, st, _api_causes(e))
        return None

def _first_result_id(payload: Optional[Dict[str, Any]], *keys: str) -> Optional[str]:
    """Extrai um id do primeiro resultado: tenta nested (ex.: r['ad']['id']) e flat (ex.: r['ad_id'])."""
    if not isinstance(payload, dict):
        return None
    rs = payload.get("results") or []
    if not rs:
        return None
    r0 = rs[0]
    if not isinstance(r0, dict):
        return None
    # tenta nested primeiro (ex.: ad.id, campaign.id), depois flat (ad_id, campaign_id, item_id)
    for k in keys:
        if "." in k:
            a, b = k.split(".", 1)
            v = (r0.get(a) or {}).get(b) if isinstance(r0.get(a), dict) else None
        else:
            v = r0.get(k)
        if v:
            return str(v)
    return None

def diagnose_routes(advertiser_id: str, site_id: str, days: int,
                    campaign_id: Optional[str], ad_id: Optional[str], item_id: Optional[str]) -> None:
    print("🔍 Testando rotas Product Ads do Mercado Livre\n")
    df, dt = _date_range(days)

    # ---------- campaigns_search ----------
    # CAMPAIGN (agregado/summary) e DAILY
    camp_ids: List[str] = []
    for agg in ("CAMPAIGN", "DAILY"):
        label = f"campaigns_search {agg}"
        path = _format_path(ENDPOINTS["campaigns_search"], site_id=site_id, advertiser_id=advertiser_id)
        params: Dict[str, Any] = {
            "aggregation_type": agg,
            "limit": 1,
            "metrics": ",".join(MIN_METRICS),
            "date_from": df,
            "date_to": dt,
        }
        data = _try_get(label, path, params=params)
        cid = _first_result_id(data, "campaign.id", "campaign_id", "id")
        if cid:
            camp_ids.append(cid)

    # campaign_detail (manual e automático)
    if "campaign_detail" in ENDPOINTS:
        # manual (se fornecido)
        if campaign_id:
            cpath = _format_path(ENDPOINTS["campaign_detail"], site_id=site_id, campaign_id=campaign_id)
            _try_get("campaign_detail (manual)", cpath)
        # automático (do resultado CAMPAIGN)
        auto_cid = (camp_ids[0] if camp_ids else None)
        if auto_cid:
            cpath_auto = _format_path(ENDPOINTS["campaign_detail"], site_id=site_id, campaign_id=auto_cid)
            _try_get("campaign_detail (auto)", cpath_auto)

    # ---------- ads_search ----------
    # ITEM (agregado/summary) e DAILY
    ad_ids: List[str] = []
    for agg in ("ITEM", "DAILY"):
        label = f"ads_search {agg}"
        path = _format_path(ENDPOINTS["ads_search"], site_id=site_id, advertiser_id=advertiser_id)
        params = {
            "aggregation_type": agg,
            "limit": 1,
            "metrics": ",".join(MIN_METRICS),
            "date_from": df,
            "date_to": dt,
        }
        data = _try_get(label, path, params=params)
        aid = _first_result_id(data, "ad.id", "ad_id", "id")
        if aid:
            ad_ids.append(aid)

    # ad_detail (manual e automático)
    if "ad_detail" in ENDPOINTS:
        tpl = ENDPOINTS["ad_detail"]
        # manual
        if ad_id:
            if "{ad_id}" in tpl:
                apath = _format_path(tpl, site_id=site_id, ad_id=ad_id)
            else:
                apath = _format_path(tpl, site_id=site_id, item_id=item_id or ad_id)
            _try_get("ad_detail (manual)", apath)
        # automático
        auto_ad = (ad_ids[0] if ad_ids else None)
        if auto_ad:
            if "{ad_id}" in tpl:
                apath_auto = _format_path(tpl, site_id=site_id, ad_id=auto_ad)
            else:
                apath_auto = _format_path(tpl, site_id=site_id, item_id=auto_ad)
            _try_get("ad_detail (auto)", apath_auto)

    # ---------- advertiser_search ----------
    if "advertiser_search" in ENDPOINTS:
        # 1) sem site_id (preferível/atual)
        path1 = "/advertising/advertisers?product_id=PADS"
        ok1 = _try_get("advertiser_search (no-site)", path1) is not None

        # 2) com site_id (se o template tiver {site_id}) — pode ser 404 em algumas contas
        tpl = ENDPOINTS["advertiser_search"]
        ok2 = False
        if "{site_id}" in tpl:
            path2 = _format_path(tpl, site_id=site_id, advertiser_id=advertiser_id)
            ok2 = _try_get("advertiser_search (with-site)", path2) is not None

        if not ok1 and not ok2:
            _pretty("advertiser_search", "ERR", "nenhuma variante respondeu")

    print(f"\n🗓️ Período usado: {df} → {dt} (dias={days})")
    print("ℹ️ Dica: passe --campaign-id/--ad-id para testar detalhes com IDs reais.")

def main():
    p = argparse.ArgumentParser(description="Diagnóstico das rotas de Product Ads (Mercado Livre).")
    p.add_argument("--advertiser-id", default="731958")
    p.add_argument("--site-id", default="MLB")
    p.add_argument("--days", type=int, default=3, help="janela usada para testes (máx 30)")
    p.add_argument("--campaign-id", default=None)
    p.add_argument("--ad-id", default=None)
    p.add_argument("--item-id", default=None, help="se seu ad_detail ainda usa {item_id} no template")
    args = p.parse_args()

    diagnose_routes(
        advertiser_id=args.advertiser_id,
        site_id=args.site_id,
        days=args.days,
        campaign_id=args.campaign_id,
        ad_id=args.ad_id,
        item_id=args.item_id,
    )

if __name__ == "__main__":
    main()
