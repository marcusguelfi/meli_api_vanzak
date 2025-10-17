# src/backfill.py
from __future__ import annotations

import csv
import os
import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from .meli_client import meli_get

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# API headers / limites / paths
# -----------------------------------------------------------------------------
# Para Product Ads (search/ads, search/campaigns, items)
HDR_V2 = {"Api-Version": "2"}

# Janela máxima ~3 meses
BACKFILL_MAX_DAYS = 90

DATA_DIR = "data/processed"
CAMPAIGNS_DAILY_CSV = os.path.join(DATA_DIR, "campaigns_daily.csv")
ADS_DAILY_CSV = os.path.join(DATA_DIR, "ads_daily.csv")
ORDERS_ITEMS_DAILY_CSV = os.path.join(DATA_DIR, "orders_items_daily.csv")

# -----------------------------------------------------------------------------
# Helpers de data
# -----------------------------------------------------------------------------
def _today() -> datetime:
    return datetime.now()

def _iso(d: datetime | date) -> str:
    if isinstance(d, date) and not isinstance(d, datetime):
        d = datetime(d.year, d.month, d.day)
    return d.strftime("%Y-%m-%d")

def _clamp_to_3_months(start_date_str: str) -> Tuple[str, str]:
    """
    Garante que o intervalo [start, hoje] respeite no máximo BACKFILL_MAX_DAYS.
    """
    today = _today().date()
    hard_min = today - timedelta(days=BACKFILL_MAX_DAYS - 1)
    req_start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    start = max(req_start, hard_min)
    end = today
    return (start.isoformat(), end.isoformat())

def _iter_chunks(start_iso: str, end_iso: str, chunk_days: int) -> Iterable[Tuple[str, str]]:
    """
    Gera janelas [a,b] (inclusive) de no máx. 'chunk_days' dentro de [start_iso, end_iso].
    """
    start = datetime.strptime(start_iso, "%Y-%m-%d").date()
    end = datetime.strptime(end_iso, "%Y-%m-%d").date()
    cur = start
    while cur <= end:
        b = min(cur + timedelta(days=chunk_days - 1), end)
        yield (cur.isoformat(), b.isoformat())
        cur = b + timedelta(days=1)

def _day_bounds_iso(d: date | str) -> Tuple[str, str]:
    """
    Constrói intervalos RFC3339 (00:00:00.000Z – 23:59:59.999Z) para /orders/search.
    """
    if isinstance(d, str):
        d = datetime.strptime(d, "%Y-%m-%d").date()
    start = f"{d.isoformat()}T00:00:00.000-00:00"
    end   = f"{d.isoformat()}T23:59:59.999-00:00"
    return start, end

# -----------------------------------------------------------------------------
# Helpers de CSV c/ upsert
# -----------------------------------------------------------------------------
def _ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def _guess_date_key(row: Dict[str, Any]) -> Optional[str]:
    for k in ("date", "day", "period", "date_from"):
        if k in row and row[k] not in (None, ""):
            return k
    return None

def _guess_id_key(row: Dict[str, Any]) -> Optional[str]:
    for k in ("ad_id", "campaign_id", "item_id", "id"):
        if k in row and row[k] not in (None, ""):
            return k
    return None

def _collect_fieldnames(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[str]:
    fieldnames: List[str] = []
    seen = set()
    for r in existing + incoming:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    return fieldnames

def _load_csv(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            out.append(dict(row))
    return out

def _save_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    _ensure_dir(path)
    if not rows:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("")
        return
    fieldnames = _collect_fieldnames([], rows)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

def _upsert_rows(path: str, new_rows: List[Dict[str, Any]]) -> None:
    """
    Upsert por (date_key, id_key). Se não achar chaves, agrega.
    """
    existing = _load_csv(path)
    if not new_rows:
        _save_csv(path, existing)
        logger.info(f"💾 CSV atualizado (sem novidades): {path}")
        return

    index: Dict[Tuple[str, str], int] = {}
    for i, r in enumerate(existing):
        dk = _guess_date_key(r)
        ik = _guess_id_key(r)
        if dk and ik and r.get(dk) and r.get(ik):
            index[(str(r[dk]), str(r[ik]))] = i

    for nr in new_rows:
        dk = _guess_date_key(nr)
        ik = _guess_id_key(nr)
        if dk and ik and nr.get(dk) and nr.get(ik):
            key = (str(nr[dk]), str(nr[ik]))
            if key in index:
                pos = index[key]
                existing[pos] = {**existing[pos], **nr}
            else:
                index[key] = len(existing)
                existing.append(nr)
        else:
            existing.append(nr)

    _save_csv(path, existing)
    logger.info(f"💾 CSV atualizado: {path}")

# -----------------------------------------------------------------------------
# Métricas para /search (sem impression_share/bench)
# -----------------------------------------------------------------------------
def _basic_metrics() -> List[str]:
    return [
        "clicks", "prints", "ctr", "cost", "cpc", "acos",
        "organic_units_quantity", "organic_units_amount", "organic_items_quantity",
        "direct_items_quantity", "indirect_items_quantity", "advertising_items_quantity",
        "cvr", "roas", "sov",
        "direct_units_quantity", "indirect_units_quantity", "units_quantity",
        "direct_amount", "indirect_amount", "total_amount",
    ]

# -----------------------------------------------------------------------------
# Flatteners (preservam TODOS os campos brutos com prefixos)
# -----------------------------------------------------------------------------
def _flat_simple(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    Achata apenas o primeiro nível. Dicts aninhados viram prefixo__chave.
    """
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, dict):
            for k2, v2 in v.items():
                out[f"{prefix}{k}__{k2}"] = v2
        else:
            out[f"{prefix}{k}"] = v
    return out

def _flatten_search_payload(data: Any, day: str) -> List[Dict[str, Any]]:
    """
    Para endpoints /search de Product Ads.
    - Cada linha vem de results[i].
    - Mantém TODOS os campos brutos com prefixos (campaign__, ad__, item__, etc).
    - Adiciona colunas utilitárias: advertiser_id/site_id quando disponíveis e a 'date'.
    """
    rows: List[Dict[str, Any]] = []
    if not isinstance(data, dict):
        return rows

    base: Dict[str, Any] = {}
    # às vezes vêm no payload de topo
    for top in ("advertiser_id", "site_id"):
        if top in data:
            base[top] = data[top]

    results = data.get("results", [])
    if isinstance(results, list):
        for r in results:
            if not isinstance(r, dict):
                continue
            flat = {}
            for k, v in r.items():
                if isinstance(v, dict):
                    # sub-objetos (ex.: campaign, ad, item)
                    for kk, vv in v.items():
                        flat[f"{k}__{kk}"] = vv
                else:
                    flat[k] = v
            # sobrepõe dados de topo
            row = {**base, **flat}
            row["date"] = day  # importante pro upsert
            rows.append(row)

    # às vezes retorna metrics_summary sem results (não é o caso do DAILY)
    if not rows and "metrics_summary" in data and isinstance(data["metrics_summary"], dict):
        flat = _flat_simple(data["metrics_summary"])
        row = {**base, **flat}
        row["date"] = day
        rows.append(row)

    return rows

def _flatten_orders_day(payload: Dict[str, Any], the_day: str) -> List[Dict[str, Any]]:
    """
    Achata /orders/search para um dia, criando 1 linha por item vendido.
    Mantém o máximo de campos brutos relevantes.
    """
    out: List[Dict[str, Any]] = []
    results = payload.get("results", [])
    if not isinstance(results, list):
        return out

    for order in results:
        if not isinstance(order, dict):
            continue

        order_flat = _flat_simple(order, prefix="order__")
        order_id = order.get("id")
        # items
        for it in (order.get("order_items") or []):
            item_flat = _flat_simple(it, prefix="order_item__")
            # payments (pega o primeiro como referência; preserva contagem)
            pays = order.get("payments") or []
            pay_flat: Dict[str, Any] = {"payments__count": len(pays)}
            if pays and isinstance(pays[0], dict):
                for k, v in pays[0].items():
                    pay_flat[f"payments__0__{k}"] = v

            row = {
                "date": the_day,
                "order_id": order_id,
                **order_flat,
                **item_flat,
                **pay_flat,
            }

            # aliases úteis (ID / título / sku)
            row.setdefault("item_id", it.get("item", {}).get("id") if isinstance(it.get("item"), dict) else it.get("item", ""))
            row.setdefault("item_title", (it.get("item", {}) or {}).get("title") if isinstance(it.get("item"), dict) else "")
            row.setdefault("seller_sku",
                           (it.get("item", {}) or {}).get("seller_custom_field")
                           if isinstance(it.get("item"), dict) else "")

            out.append(row)

    return out

# -----------------------------------------------------------------------------
# Aliases "bonitinhos" para o Sheets
# -----------------------------------------------------------------------------
def _aliases_ads(rows: List[Dict[str, Any]]) -> None:
    for r in rows:
        r["ad_id"]         = r.get("ad_id")         or r.get("ad__id") or r.get("id")
        r["item_id"]       = r.get("item_id")       or r.get("item__id")
        r["item_title"]    = r.get("item_title")    or r.get("item__title") or r.get("title")
        r["seller_sku"]    = r.get("seller_sku")    or r.get("item__seller_sku") \
                                               or r.get("item__seller_custom_field") \
                                               or r.get("item__seller_sku_id")
        r["campaign_id"]   = r.get("campaign_id")   or r.get("campaign__id")
        r["campaign_name"] = r.get("campaign_name") or r.get("campaign__name") or r.get("name")
        r["status"]        = r.get("status")        or r.get("ad__status") or r.get("item__status")

def _aliases_campaigns(rows: List[Dict[str, Any]]) -> None:
    for r in rows:
        r["campaign_id"]   = r.get("campaign_id")   or r.get("campaign__id") or r.get("id")
        r["campaign_name"] = r.get("campaign_name") or r.get("campaign__name") or r.get("name")
        r["status"]        = r.get("status")        or r.get("campaign__status") or r.get("status")

def _aliases_orders_items(rows: List[Dict[str, Any]]) -> None:
    for r in rows:
        r["item_id"]    = r.get("item_id")    or r.get("order_item__item__id") or r.get("id")
        r["item_title"] = r.get("item_title") or r.get("order_item__item__title") or r.get("title")
        r["seller_sku"] = r.get("seller_sku") or r.get("order_item__item__seller_custom_field") \
                                           or r.get("order_item__item__seller_sku") \
                                           or r.get("variation_sku")

# -----------------------------------------------------------------------------
# Backfills Product Ads (DAILY)
# -----------------------------------------------------------------------------
def backfill_campaigns_daily(advertiser_id: str, site_id: str, start_date: str, chunk_days: int = 30) -> None:
    """
    Product Ads — campaigns DAILY. Upsert em campaigns_daily.csv
    """
    start_iso, end_iso = _clamp_to_3_months(start_date)
    out_csv = CAMPAIGNS_DAILY_CSV
    logger.info(f"🧮 Backfill CAMPAIGNS daily | {start_iso} → {end_iso} (máx {BACKFILL_MAX_DAYS}d)")

    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"

    for a, b in _iter_chunks(start_iso, end_iso, chunk_days):
        logger.info(f"🧭 campaigns_daily {a} → {b}")
        params = {
            "limit": 50,
            "offset": 0,
            "date_from": a,
            "date_to": b,
            "metrics": ",".join(_basic_metrics()),
            "aggregation_type": "DAILY",
        }
        try:
            data = meli_get(endpoint, params=params, headers=HDR_V2)
            rows = []
            # o /search DAILY retorna vários dias; queremos uma linha por dia
            for r in _flatten_search_payload(data, day=a):
                # se tiver campo 'date' na própria linha (algumas APIs devolvem), respeite
                if "date" not in r or not r["date"]:
                    r["date"] = a
                r["advertiser_id"] = advertiser_id
                r["site_id"] = site_id
                rows.append(r)

            # aliases úteis
            _aliases_campaigns(rows)

            if not rows:
                logger.info(f"⚠️ Sem dados para {a} → {b}")
                continue

            _ensure_dir(out_csv)
            _upsert_rows(out_csv, rows)

        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code == 404:
                logger.warning(f"⚠️ 404 (sem dados) em {a} → {b}; seguindo…")
                continue
            logger.exception(f"❌ HTTP {code} ao buscar campaigns {a} → {b}")
            raise
        except Exception as e:
            logger.exception(f"❌ Erro inesperado em campaigns {a} → {b}: {e}")
            raise

def backfill_ads_daily(advertiser_id: str, site_id: str, start_date: str, chunk_days: int = 30) -> None:
    """
    Product Ads — ads DAILY. Upsert em ads_daily.csv
    """
    start_iso, end_iso = _clamp_to_3_months(start_date)
    out_csv = ADS_DAILY_CSV
    logger.info(f"🧮 Backfill ADS daily | {start_iso} → {end_iso} (máx {BACKFILL_MAX_DAYS}d)")

    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"

    for a, b in _iter_chunks(start_iso, end_iso, chunk_days):
        logger.info(f"🧭 ads_daily {a} → {b}")
        params = {
            "limit": 50,
            "offset": 0,
            "date_from": a,
            "date_to": b,
            "metrics": ",".join(_basic_metrics()),
            "aggregation_type": "DAILY",
        }
        try:
            data = meli_get(endpoint, params=params, headers=HDR_V2)
            rows = []
            for r in _flatten_search_payload(data, day=a):
                if "date" not in r or not r["date"]:
                    r["date"] = a
                r["advertiser_id"] = advertiser_id
                r["site_id"] = site_id
                rows.append(r)

            # aliases úteis (id/título/sku/campaign)
            _aliases_ads(rows)

            if not rows:
                logger.info(f"⚠️ Sem dados para {a} → {b}")
                continue

            _ensure_dir(out_csv)
            _upsert_rows(out_csv, rows)

        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code == 404:
                logger.warning(f"⚠️ 404 (sem dados) em {a} → {b}; seguindo…")
                continue
            logger.exception(f"❌ HTTP {code} ao buscar ads {a} → {b}")
            raise
        except Exception as e:
            logger.exception(f"❌ Erro inesperado em ads {a} → {b}: {e}")
            raise

# -----------------------------------------------------------------------------
# Backfill Orders por item/dia (1 linha por item vendido por dia)
# -----------------------------------------------------------------------------
def backfill_orders_items_daily(seller_id: str, start_date: str) -> None:
    """
    Orders → 1 linha por item/dia (agrega todos os campos brutos possíveis).
    Upsert em orders_items_daily.csv
    """
    start_iso, end_iso = _clamp_to_3_months(start_date)
    out_csv = ORDERS_ITEMS_DAILY_CSV
    logger.info(f"🧮 Backfill ORDERS items daily | {start_iso} → {end_iso} (máx {BACKFILL_MAX_DAYS}d)")

    cur = datetime.strptime(start_iso, "%Y-%m-%d").date()
    last = datetime.strptime(end_iso, "%Y-%m-%d").date()

    while cur <= last:
        a_rfc, b_rfc = _day_bounds_iso(cur)
        logger.info(f"🧭 orders_items_daily {cur.isoformat()}")
        params = {
            "seller": seller_id,
            "order.date_created.from": a_rfc,
            "order.date_created.to": b_rfc,
        }
        try:
            data = meli_get("/orders/search", params=params)
            rows = _flatten_orders_day(data, the_day=cur.isoformat())
            _aliases_orders_items(rows)

            if not rows:
                logger.info(f"⚠️ Sem pedidos no dia {cur.isoformat()}")
                cur += timedelta(days=1)
                continue

            _ensure_dir(out_csv)
            _upsert_rows(out_csv, rows)

        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            logger.exception(f"❌ HTTP {code} ao buscar pedidos {cur.isoformat()}")
            raise
        except Exception as e:
            logger.exception(f"❌ Erro inesperado em pedidos {cur.isoformat()}: {e}")
            raise

        cur += timedelta(days=1)
