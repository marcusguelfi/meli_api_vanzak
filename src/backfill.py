# src/backfill.py
from __future__ import annotations

import csv
import os
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

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
# Headers & Config
# -----------------------------------------------------------------------------
HDR_V1 = {"Api-Version": "1"}  # não usamos aqui, mas deixo por consistência
HDR_V2 = {"Api-Version": "2"}

DATA_DIR = "data/processed"

# Apps Script (opcional)
APPSCRIPT_URL = os.getenv("GOOGLE_APPSCRIPT_URL", "").strip()
APPSCRIPT_TOKEN = os.getenv("GOOGLE_APPSCRIPT_TOKEN", "").strip()

def _send_to_sheets(caminho_csv: str, sheet: Optional[str] = None) -> None:
    """Envia CSV para Google Sheets via Apps Script (se configurado)."""
    if not APPSCRIPT_URL:
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

# -----------------------------------------------------------------------------
# Constantes
# -----------------------------------------------------------------------------
# Janela máxima de backfill: ~90 dias (limite usual da API)
BACKFILL_MAX_DAYS = 90

# -----------------------------------------------------------------------------
# Helpers de datas
# -----------------------------------------------------------------------------
def _today() -> datetime:
    return datetime.now()

def _iso(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")

def _clamp_to_3_months(start_date_str: str, end_inclusive: Optional[datetime] = None) -> Tuple[str, str]:
    """
    Garante que o intervalo [start, end] respeite no máximo BACKFILL_MAX_DAYS.
    """
    if end_inclusive is None:
        end_inclusive = _today()
    hard_min = end_inclusive.date() - timedelta(days=BACKFILL_MAX_DAYS - 1)
    req_start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    start = max(req_start, hard_min)
    end = end_inclusive.date()
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

# -----------------------------------------------------------------------------
# CSV helpers com upsert por (date_key, id_key)
# -----------------------------------------------------------------------------
def _ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def _guess_date_key(row: Dict[str, Any]) -> Optional[str]:
    for k in ("date", "day", "period", "date_from", "date_created"):
        if k in row and row[k]:
            return k
    return None

def _guess_id_key(row: Dict[str, Any]) -> Optional[str]:
    for k in ("campaign_id", "ad_id", "item_id", "id"):
        if k in row and row[k] not in (None, ""):
            return k
    return None

def _flatten_dicts(data: Any) -> List[Dict[str, Any]]:
    """
    Recebe payload de /search (results ou metrics_summary) e devolve linhas planas.
    """
    rows: List[Dict[str, Any]] = []
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        for r in data["results"]:
            if isinstance(r, dict):
                flat = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
                rows.append(flat)
    elif isinstance(data, dict) and "metrics_summary" in data:
        ms = data["metrics_summary"]
        if isinstance(ms, dict):
            flat = {k: v for k, v in ms.items() if not isinstance(v, (list, dict))}
            rows.append(flat)
    elif isinstance(data, dict):
        flat = {k: v for k, v in data.items() if not isinstance(v, (list, dict))}
        rows.append(flat)
    return rows

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
    Upsert por (date_key, id_key). Se não achar, apenas agrega.
    """
    existing = _load_csv(path)
    if not new_rows:
        _save_csv(path, existing)
        logger.info(f"💾 CSV atualizado (sem novidades): {path}")
        return

    # índice existente
    index: Dict[Tuple[str, str], int] = {}
    for i, r in enumerate(existing):
        dk = _guess_date_key(r)
        ik = _guess_id_key(r)
        if dk and ik and r.get(dk) and r.get(ik):
            index[(str(r[dk]), str(r[ik]))] = i

    # upsert
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
# Métricas básicas (compatíveis com /search)
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
# BACKFILL: Product Ads (Campaigns / Ads) – diárias
# -----------------------------------------------------------------------------
def backfill_campaigns_daily(advertiser_id: str, site_id: str, start_date: str, chunk_days: int = 30) -> None:
    """
    Busca dados diários de campanhas desde 'start_date' até HOJE (máx. 90d) e faz upsert em
    data/processed/campaigns_daily_<advertiser_id>.csv
    """
    start_iso, end_iso = _clamp_to_3_months(start_date)
    out_csv = os.path.join(DATA_DIR, f"campaigns_daily_{advertiser_id}.csv")
    logger.info(f"🧮 Backfill CAMPAIGNS daily | {start_iso} → {end_iso} (máx {BACKFILL_MAX_DAYS}d)")

    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"

    for a, b in _iter_chunks(start_iso, end_iso, chunk_days):
        params = {
            "limit": 50, "offset": 0,
            "date_from": a, "date_to": b,
            "metrics": ",".join(_basic_metrics()),
            "aggregation_type": "DAILY",
        }
        logger.info(f"🧭 campaigns_daily {a} → {b}")
        try:
            data = meli_get(endpoint, params=params, headers=HDR_V2)
            rows = _flatten_dicts(data)
            if not rows:
                logger.info(f"⚠️ Sem dados para {a} → {b}")
                continue
            _ensure_dir(out_csv)
            # adiciona colunas fixas
            for r in rows:
                r.setdefault("advertiser_id", advertiser_id)
                r.setdefault("site_id", site_id)
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

    _send_to_sheets(out_csv, sheet="campaigns_daily")

def backfill_ads_daily(advertiser_id: str, site_id: str, start_date: str, chunk_days: int = 30) -> None:
    """
    Busca dados diários de anúncios desde 'start_date' até HOJE (máx. 90d) e faz upsert em
    data/processed/ads_daily_<advertiser_id>.csv
    """
    start_iso, end_iso = _clamp_to_3_months(start_date)
    out_csv = os.path.join(DATA_DIR, f"ads_daily_{advertiser_id}.csv")
    logger.info(f"🧮 Backfill ADS daily | {start_iso} → {end_iso} (máx {BACKFILL_MAX_DAYS}d)")

    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"

    for a, b in _iter_chunks(start_iso, end_iso, chunk_days):
        params = {
            "limit": 50, "offset": 0,
            "date_from": a, "date_to": b,
            "metrics": ",".join(_basic_metrics()),
            "aggregation_type": "DAILY",
        }
        logger.info(f"🧭 ads_daily {a} → {b}")
        try:
            data = meli_get(endpoint, params=params, headers=HDR_V2)
            rows = _flatten_dicts(data)
            if not rows:
                logger.info(f"⚠️ Sem dados para {a} → {b}")
                continue
            _ensure_dir(out_csv)
            for r in rows:
                r.setdefault("advertiser_id", advertiser_id)
                r.setdefault("site_id", site_id)
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

    _send_to_sheets(out_csv, sheet="ads_daily")

# -----------------------------------------------------------------------------
# BACKFILL: Orders por item — diário (até ONTEM)
# -----------------------------------------------------------------------------
def _date_only(iso_ts: str) -> str:
    # pega AAAA-MM-DD de um ISO timestamp
    try:
        return iso_ts[:10]
    except Exception:
        return iso_ts

def _enrich_items_titles(item_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Busca detalhes básicos de /items/{id} para obter title, catalog_listing, official_store_id etc.
    Retorna dict por item_id.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for iid in item_ids:
        try:
            data = meli_get(f"/items/{iid}")  # endpoint público
            out[iid] = {
                "item_id": iid,
                "title": data.get("title"),
                "category_id": data.get("category_id"),
                "domain_id": data.get("domain_id"),
                "official_store_id": data.get("official_store_id"),
                "catalog_listing": data.get("catalog_listing"),
                "seller_id": (data.get("seller_id") or data.get("seller", {}).get("id")),
                "permalink": data.get("permalink"),
            }
        except Exception:
            logger.exception(f"⚠️ Falha ao enriquecer item {iid}")
    return out

def backfill_orders_items_daily(seller_id: str, start_date: str) -> None:
    """
    Agrega pedidos por item, por dia, desde 'start_date' até ONTEM (máx. 90d).
    Gera/atualiza:
      - data/processed/orders_items_daily.csv (linhas diárias por item)
      - data/processed/items_catalog.csv      (atributos do item)
    """
    # limite até ontem (conforme pedido)
    end_inclusive = datetime.now() - timedelta(days=1)
    start_iso, end_iso = _clamp_to_3_months(start_date, end_inclusive=end_inclusive)

    out_orders_csv = os.path.join(DATA_DIR, "orders_items_daily.csv")
    out_items_csv = os.path.join(DATA_DIR, "items_catalog.csv")

    logger.info(f"🧮 Backfill ORDERS items daily | {start_iso} → {end_iso} (máx {BACKFILL_MAX_DAYS}d)")

    # Itera dia a dia (agregação por dia + item)
    cur = datetime.strptime(start_iso, "%Y-%m-%d").date()
    end = datetime.strptime(end_iso, "%Y-%m-%d").date()

    all_rows: List[Dict[str, Any]] = []
    all_item_ids: set[str] = set()

    while cur <= end:
        a = cur.isoformat()
        b = cur.isoformat()
        logger.info(f"🧭 orders_items_daily {a}")

        params = {
            "seller": seller_id,
            "order.date_created.from": f"{a}T00:00:00.000Z",
            "order.date_created.to":   f"{b}T23:59:59.999Z",
        }
        try:
            data = meli_get("/orders/search", params=params)
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            logger.error(f"❌ HTTP {code} ao buscar pedidos {a}")
            raise
        except Exception as e:
            logger.exception(f"❌ Erro inesperado ao buscar pedidos {a}: {e}")
            raise

        # agregação por item nesse dia
        per_item: Dict[str, Dict[str, Any]] = {}

        for o in data.get("results", []):
            # data do pedido
            day = _date_only(o.get("date_created", a))
            order_status = o.get("status")
            currency_id = o.get("currency_id")
            total_amount = o.get("total_amount")

            items = o.get("order_items") or []
            if not isinstance(items, list):
                items = []

            for it in items:
                item = it.get("item") or {}
                iid = item.get("id") or item.get("seller_sku") or "UNKNOWN"
                qty = it.get("quantity") or 0
                unit_price = it.get("unit_price") or 0
                price = qty * unit_price

                row = per_item.setdefault(iid, {
                    "date": day,
                    "item_id": iid,
                    "units": 0,
                    "amount": 0.0,
                    "currency_id": currency_id,
                })
                row["units"] = (row.get("units") or 0) + qty
                row["amount"] = float(row.get("amount") or 0) + float(price or 0)

                all_item_ids.add(iid)

        all_rows.extend(per_item.values())
        cur = cur + timedelta(days=1)

    # upsert no CSV consolidado
    _ensure_dir(out_orders_csv)
    _upsert_rows(out_orders_csv, all_rows)
    _send_to_sheets(out_orders_csv, sheet="orders_items_daily")

    # enriquecer itens (título/sku/etc) e gravar catálogo
    # carrega catálogo atual para evitar refetch de itens já conhecidos
    existing_catalog = _load_csv(out_items_csv)
    known_ids = {r.get("item_id") for r in existing_catalog if r.get("item_id")}
    to_fetch = [iid for iid in all_item_ids if iid and iid not in known_ids and iid != "UNKNOWN"]

    if to_fetch:
        logger.info(f"🔎 Enriquecendo {len(to_fetch)} itens via /items/{{id}} …")
        enriched = list(_enrich_items_titles(to_fetch).values())
        # upsert por item_id (usa _upsert_rows – a chave será item_id; date_key inexistente → agrega/merge)
        _ensure_dir(out_items_csv)
        _upsert_rows(out_items_csv, enriched)

    _send_to_sheets(out_items_csv, sheet="items_catalog")
