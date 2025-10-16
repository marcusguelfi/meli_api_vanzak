# src/jobs.py
from __future__ import annotations

import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from .meli_client import meli_get, write_csv

# -----------------------------------------------------------------------------
# Config: envio para Google Apps Script (opcional)
# -----------------------------------------------------------------------------
APPSCRIPT_URL = os.getenv("GOOGLE_APPSCRIPT_URL", "").strip()
APPSCRIPT_TOKEN = os.getenv("GOOGLE_APPSCRIPT_TOKEN", "").strip()  # opcional


def enviar_para_google_sheets(caminho_csv: str) -> None:
    """
    Envia o CSV gerado para o Apps Script (Web App) via POST.
    Se GOOGLE_APPSCRIPT_URL não estiver definido, apenas loga e segue.
    """
    if not APPSCRIPT_URL:
        logging.info("GOOGLE_APPSCRIPT_URL não configurado; pulando envio ao Apps Script.")
        return

    url = APPSCRIPT_URL
    if APPSCRIPT_TOKEN:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}token={APPSCRIPT_TOKEN}"

    try:
        with open(caminho_csv, "rb") as f:
            resp = requests.post(url, data=f, timeout=60)
        resp.raise_for_status()
        logging.info(f"Apps Script OK: {resp.status_code} {resp.text[:160]}")
    except Exception as e:
        logging.exception(f"Falha ao enviar CSV para Apps Script: {e}")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def save_json(data: Any, path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def _ts_now() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _basic_metrics() -> List[str]:
    """
    Conjunto de métricas aceitas em search/ads e search/campaigns (sem impression_share e afins).
    """
    return [
        "clicks", "prints", "ctr", "cost", "cpc", "acos",
        "organic_units_quantity", "organic_units_amount", "organic_items_quantity",
        "direct_items_quantity", "indirect_items_quantity", "advertising_items_quantity",
        "cvr", "roas", "sov",
        "direct_units_quantity", "indirect_units_quantity", "units_quantity",
        "direct_amount", "indirect_amount", "total_amount",
    ]


# -----------------------------------------------------------------------------
# Jobs “padrão” já existentes
# -----------------------------------------------------------------------------
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
    enviar_para_google_sheets(path)
    return path


def job_orders_recent(seller_id: str, date_from_iso: Optional[str] = None) -> str:
    params: Dict[str, Any] = {"seller": seller_id}
    if date_from_iso:
        params["order.date_created.from"] = date_from_iso

    data = meli_get("/orders/search", params=params)
    results = data.get("results", [])
    rows: List[Dict[str, Any]] = []
    for o in results:
        rows.append({
            "id": o.get("id"),
            "date_created": o.get("date_created"),
            "status": o.get("status"),
            "total_amount": o.get("total_amount"),
            "currency_id": o.get("currency_id"),
            "buyer_id": (o.get("buyer") or {}).get("id"),
        })

    path = write_csv(rows, "orders")
    enviar_para_google_sheets(path)
    return path


# -----------------------------------------------------------------------------
# Product Ads — Advertiser
# -----------------------------------------------------------------------------
def job_get_advertiser(product_id: str = "PADS") -> Dict[str, Any]:
    """
    Consulta o advertiser via /advertising/advertisers?product_id=PADS.
    Salva JSON em data/processed/advertiser_*.json
    """
    params = {"product_id": product_id}
    data = meli_get("/advertising/advertisers", params=params)
    advs = data.get("advertisers", [])
    if advs:
        logging.info(f"✅ Advertiser encontrado: {advs[0]}")
    else:
        logging.warning("⚠️ Nenhum advertiser retornado.")
    save_json(data, f"data/processed/advertiser_{_ts_now()}.json")
    return data


# -----------------------------------------------------------------------------
# Product Ads — Campaigns (search)
# -----------------------------------------------------------------------------
def job_campaigns_summary(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    """
    Métricas resumidas de campanhas (metrics_summary=true).
    """
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"
    params = {
        "limit": 50,
        "offset": 0,
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(_basic_metrics()),
        "metrics_summary": "true",
    }
    data = meli_get(endpoint, params=params)
    save_json(data, f"data/processed/campaigns_summary_{advertiser_id}_{_ts_now()}.json")

    # Normalização simples: se vier metrics_summary, vira uma linha
    rows: List[Dict[str, Any]] = []
    if isinstance(data, dict) and "metrics_summary" in data:
        row = {"advertiser_id": advertiser_id, "site_id": site_id}
        for k, v in data["metrics_summary"].items():
            if not isinstance(v, (list, dict)):
                row[k] = v
        rows.append(row)
    else:
        # fallback: joga payload em uma coluna
        rows.append({"advertiser_id": advertiser_id, "site_id": site_id, "payload_json": json.dumps(data, ensure_ascii=False)})

    path = write_csv(rows, f"campaigns_summary_{advertiser_id}")
    logging.info(f"✅ Dados salvos em {path}")
    enviar_para_google_sheets(path)
    return path


def job_campaigns_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    """
    Métricas diárias de campanhas (aggregation_type=DAILY).
    """
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search"
    params = {
        "limit": 50,
        "offset": 0,
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(_basic_metrics()),
        "aggregation_type": "DAILY",
    }
    data = meli_get(endpoint, params=params)
    save_json(data, f"data/processed/campaigns_daily_{advertiser_id}_{_ts_now()}.json")

    # Normalização: results (lista de dias/campanhas)
    rows: List[Dict[str, Any]] = []
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        for r in data["results"]:
            base: Dict[str, Any] = {"advertiser_id": advertiser_id, "site_id": site_id}
            if isinstance(r, dict):
                for k, v in r.items():
                    if not isinstance(v, (list, dict)):
                        base[k] = v
            rows.append(base)
    else:
        rows.append({"advertiser_id": advertiser_id, "site_id": site_id, "payload_json": json.dumps(data, ensure_ascii=False)})

    path = write_csv(rows, f"campaigns_daily_{advertiser_id}")
    logging.info(f"✅ Dados salvos em {path}")
    enviar_para_google_sheets(path)
    return path


# -----------------------------------------------------------------------------
# Product Ads — Ads (search)
# -----------------------------------------------------------------------------
def job_ads_summary(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    """
    Métricas de anúncios (resumo).
    """
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"
    params = {
        "limit": 50,
        "offset": 0,
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(_basic_metrics()),
        "metrics_summary": "true",
    }
    data = meli_get(endpoint, params=params)
    save_json(data, f"data/processed/ads_summary_{advertiser_id}_{_ts_now()}.json")

    rows: List[Dict[str, Any]] = []
    if isinstance(data, dict) and "metrics_summary" in data:
        row = {"advertiser_id": advertiser_id, "site_id": site_id}
        for k, v in data["metrics_summary"].items():
            if not isinstance(v, (list, dict)):
                row[k] = v
        rows.append(row)
    else:
        rows.append({"advertiser_id": advertiser_id, "site_id": site_id, "payload_json": json.dumps(data, ensure_ascii=False)})

    path = write_csv(rows, f"ads_summary_{advertiser_id}")
    logging.info(f"✅ Dados salvos em {path}")
    enviar_para_google_sheets(path)
    return path


def job_ads_daily(advertiser_id: str, site_id: str, date_from: str, date_to: str) -> str:
    """
    Métricas diárias de anúncios.
    """
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"
    params = {
        "limit": 50,
        "offset": 0,
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(_basic_metrics()),
        "aggregation_type": "DAILY",
    }
    data = meli_get(endpoint, params=params)
    save_json(data, f"data/processed/ads_daily_{advertiser_id}_{_ts_now()}.json")

    rows: List[Dict[str, Any]] = []
    if isinstance(data, dict) and isinstance(data.get("results"), list):
        for r in data["results"]:
            base: Dict[str, Any] = {"advertiser_id": advertiser_id, "site_id": site_id}
            if isinstance(r, dict):
                for k, v in r.items():
                    if not isinstance(v, (list, dict)):
                        base[k] = v
            rows.append(base)
    else:
        rows.append({"advertiser_id": advertiser_id, "site_id": site_id, "payload_json": json.dumps(data, ensure_ascii=False)})

    path = write_csv(rows, f"ads_daily_{advertiser_id}")
    logging.info(f"✅ Dados salvos em {path}")
    enviar_para_google_sheets(path)
    return path


def job_list_ads_basic(advertiser_id: str, site_id: str, limit: int = 10, offset: int = 0) -> str:
    """
    Lista básica de anúncios (sem métricas) para capturar item_id/status etc.
    """
    endpoint = f"/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search"
    params = {
        "limit": limit,
        "offset": offset,
    }
    data = meli_get(endpoint, params=params)
    results = data.get("results", [])
    rows: List[Dict[str, Any]] = []

    for idx, r in enumerate(results, start=1):
        ad_id = r.get("ad_id") or r.get("id")
        item_id = r.get("item_id") or r.get("id")  # dependendo do shape
        status = r.get("status")
        logging.info(f"• {idx}: ad_id={ad_id} item_id={item_id} status={status}")

        rows.append({
            "ad_id": ad_id,
            "item_id": item_id,
            "status": status,
        })

    logging.info(f"✅ {len(rows)} anúncios retornados")
    path = write_csv(rows, f"ads_list_{advertiser_id}")
    save_json(data, f"data/processed/ads_list_{advertiser_id}_{_ts_now()}.json")
    logging.info(f"📝 Lista salva em {path}")
    enviar_para_google_sheets(path)
    return path


# -----------------------------------------------------------------------------
# Product Ads — Item detail & metrics (com fallback ads → items)
# -----------------------------------------------------------------------------
def job_item_detail(site_id: str, item_id: str) -> str:
    """
    Detalhe de um anúncio por item_id.
    Tenta primeiro /product_ads/ads/{item_id} e faz fallback para /product_ads/items/{item_id}.
    """
    try_order = [
        f"/advertising/{site_id}/product_ads/ads/{item_id}",
        f"/advertising/{site_id}/product_ads/items/{item_id}",
    ]

    last_err: Optional[Exception] = None
    for ep in try_order:
        try:
            logging.info(f"➡️ GET {ep}")
            data = meli_get(ep)
            rows = []
            if isinstance(data, dict):
                flat = {k: v for k, v in data.items() if not isinstance(v, (list, dict))}
                flat["item_id"] = item_id
                rows.append(flat)
            else:
                rows.append({"item_id": item_id, "payload_json": json.dumps(data, ensure_ascii=False)})

            csv_path = write_csv(rows, f"item_detail_{item_id}")
            save_json(data, f"data/processed/item_detail_{item_id}_{_ts_now()}.json")
            logging.info(f"✅ Detalhe salvo em {csv_path}")
            enviar_para_google_sheets(csv_path)
            return csv_path

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            body = ""
            try:
                body = e.response.text
            except Exception:
                pass
            logging.error(f"❌ Erro {status} ao chamar https://api.mercadolibre.com{ep}: {body}")
            last_err = e
            if status == 404:
                continue
            break
        except Exception as e:
            logging.exception(f"❌ Erro inesperado no detalhe do item {item_id} em {ep}: {e}")
            last_err = e
            break

    raise last_err or RuntimeError(f"Falha ao buscar detalhe do item {item_id}")


def job_item_metrics(site_id: str, item_id: str, date_from: str, date_to: str, aggregation_type: Optional[str] = None) -> str:
    """
    Métricas de um anúncio por item_id.
    Tenta ADS e depois ITEMS. Remove métricas proibidas (impression_share e similares).
    """
    metrics = _basic_metrics()
    params: Dict[str, Any] = {
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(metrics),
    }
    if aggregation_type:
        params["aggregation_type"] = aggregation_type  # e.g. "DAILY"

    try_order = [
        f"/advertising/{site_id}/product_ads/ads/{item_id}",
        f"/advertising/{site_id}/product_ads/items/{item_id}",
    ]

    last_err: Optional[Exception] = None
    for ep in try_order:
        try:
            logging.info(f"➡️ GET {ep} (params={params})")
            data = meli_get(ep, params=params)

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
            logging.info(f"✅ Métricas do item salvas em {csv_path}")
            enviar_para_google_sheets(csv_path)
            return csv_path

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            body = ""
            try:
                body = e.response.text
            except Exception:
                pass
            logging.error(f"❌ Erro {status} ao chamar https://api.mercadolibre.com{ep}: {body}")
            last_err = e
            if status == 404:
                continue
            break
        except Exception as e:
            logging.exception(f"❌ Erro inesperado ao buscar métricas do item {item_id} em {ep}: {e}")
            last_err = e
            break

    raise last_err or RuntimeError(f"Falha ao obter métricas para item {item_id}")
