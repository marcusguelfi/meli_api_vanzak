# src/backfill.py
from __future__ import annotations

import csv
import os
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from .meli_client import meli_get

# ---------------------------------------------------------------------------
# Logging (corrige o erro de "%Y" no format)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Limites e caminhos
# ---------------------------------------------------------------------------
# Janela máxima de backfill: 90 dias (3 meses). MLE geralmente limita a ~90d.
BACKFILL_MAX_DAYS = 90

DATA_DIR = "data/processed"
CAMPAIGNS_DAILY_CSV = os.path.join(DATA_DIR, "campaigns_daily.csv")
ADS_DAILY_CSV = os.path.join(DATA_DIR, "ads_daily.csv")

# ---------------------------------------------------------------------------
# Helpers de datas
# ---------------------------------------------------------------------------
def _today() -> datetime:
    # Use timezone local do host; se preferir UTC, troque por datetime.utcnow()
    return datetime.now()

def _iso(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")

def _clamp_to_3_months(start_date_str: str) -> Tuple[str, str]:
    """
    Garante que o intervalo [start, today] respeite no máximo BACKFILL_MAX_DAYS.
    Retorna (start_iso, end_iso).
    """
    today = _today().date()
    hard_min = today - timedelta(days=BACKFILL_MAX_DAYS - 1)  # inclusive
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

# ---------------------------------------------------------------------------
# Helper de CSV com upsert por chave (date_key + id_key flexível)
# ---------------------------------------------------------------------------
def _ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def _guess_date_key(row: Dict[str, Any]) -> Optional[str]:
    for k in ("date", "day", "period", "date_from"):
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
    Recebe o payload de /search (results ou metrics_summary).
    Devolve lista de linhas "planas" (apenas chaves-valor simples).
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
        # cria vazio com cabeçalho mínimo
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
    Faz upsert por (date_key, id_key). Se não achar chaves, apenas agrega.
    """
    existing = _load_csv(path)
    if not new_rows:
        _save_csv(path, existing)
        logger.info(f"💾 CSV atualizado (sem novidades): {path}")
        return

    # Mapa para deduplicar
    index: Dict[Tuple[str, str], int] = {}
    for i, r in enumerate(existing):
        dk = _guess_date_key(r)
        ik = _guess_id_key(r)
        if dk and ik and r.get(dk) and r.get(ik):
            index[(str(r[dk]), str(r[ik]))] = i

    # Upsert
    for nr in new_rows:
        dk = _guess_date_key(nr)
        ik = _guess_id_key(nr)
        if dk and ik and nr.get(dk) and nr.get(ik):
            key = (str(nr[dk]), str(nr[ik]))
            if key in index:
                # merge (sobrescreve campos vazios/com novos valores)
                pos = index[key]
                existing[pos] = {**existing[pos], **nr}
            else:
                index[key] = len(existing)
                existing.append(nr)
        else:
            # sem chaves confiáveis -> anexa
            existing.append(nr)

    _save_csv(path, existing)
    logger.info(f"💾 CSV atualizado: {path}")

# ---------------------------------------------------------------------------
# Chamadas de backfill
# ---------------------------------------------------------------------------
def _basic_metrics() -> List[str]:
    # Mesmas métricas que usamos nos jobs (sem impression_share / benchmark)
    return [
        "clicks", "prints", "ctr", "cost", "cpc", "acos",
        "organic_units_quantity", "organic_units_amount", "organic_items_quantity",
        "direct_items_quantity", "indirect_items_quantity", "advertising_items_quantity",
        "cvr", "roas", "sov",
        "direct_units_quantity", "indirect_units_quantity", "units_quantity",
        "direct_amount", "indirect_amount", "total_amount",
    ]

def backfill_campaigns_daily(advertiser_id: str, site_id: str, start_date: str, chunk_days: int = 30) -> None:
    """
    Busca dados diários de campanhas desde 'start_date' até HOJE, respeitando janela
    máxima de ~90 dias. Faz upsert em data/processed/campaigns_daily.csv
    """
    start_iso, end_iso = _clamp_to_3_months(start_date)
    logger.info(f"🧮 Backfill CAMPAIGNS daily | {start_iso} → {end_iso} (máx {BACKFILL_MAX_DAYS}d)")

    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"

    for a, b in _iter_chunks(start_iso, end_iso, chunk_days):
        params = {
            "limit": 50,
            "offset": 0,
            "date_from": a,
            "date_to": b,
            "metrics": ",".join(_basic_metrics()),
            "aggregation_type": "DAILY",
        }
        logger.info(f"🧭 campaigns_daily {a} → {b}")
        try:
            data = meli_get(endpoint, params=params)
            rows = _flatten_dicts(data)
            if not rows:
                logger.info(f"⚠️ Sem dados para {a} → {b}")
                continue
            _ensure_dir(CAMPAIGNS_DAILY_CSV)
            _upsert_rows(CAMPAIGNS_DAILY_CSV, rows)

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
    Busca dados diários de anúncios desde 'start_date' até HOJE, respeitando janela
    máxima de ~90 dias. Faz upsert em data/processed/ads_daily.csv
    """
    start_iso, end_iso = _clamp_to_3_months(start_date)
    logger.info(f"🧮 Backfill ADS daily | {start_iso} → {end_iso} (máx {BACKFILL_MAX_DAYS}d)")

    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"

    for a, b in _iter_chunks(start_iso, end_iso, chunk_days):
        params = {
            "limit": 50,
            "offset": 0,
            "date_from": a,
            "date_to": b,
            "metrics": ",".join(_basic_metrics()),
            "aggregation_type": "DAILY",
        }
        logger.info(f"🧭 ads_daily {a} → {b}")
        try:
            data = meli_get(endpoint, params=params)
            rows = _flatten_dicts(data)
            if not rows:
                logger.info(f"⚠️ Sem dados para {a} → {b}")
                continue
            _ensure_dir(ADS_DAILY_CSV)
            _upsert_rows(ADS_DAILY_CSV, rows)

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
