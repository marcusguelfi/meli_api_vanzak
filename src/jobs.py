import logging
from src.meli_client import meli_get
from src.product_ads_endpoints import ENDPOINTS
from src.product_ads_metrics import METRICS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def job_get_advertiser():
    """Consulta o advertiser ativo."""
    endpoint = ENDPOINTS.get("advertiser_search")
    if not endpoint:
        logging.error("❌ Erro ao consultar anunciante: Endpoint 'advertiser_search' não encontrado.")
        return

    try:
        data = meli_get(endpoint, headers={"Api-Version": "1"})
        logging.info(f"✅ Anunciante encontrado: {data}")
    except Exception as e:
        logging.error(f"❌ Falha ao buscar anunciante: {e}")


def job_campaigns_summary(advertiser_id, site_id, date_from, date_to):
    logging.info("📊 Coletando métricas resumidas de campanhas...")
    endpoint = ENDPOINTS["campaigns_search"].format(site_id=site_id, advertiser_id=advertiser_id)
    params = {
        "limit": 50,
        "offset": 0,
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(METRICS),
        "metrics_summary": "true"
    }

    try:
        data = meli_get(endpoint, params=params, headers={"Api-Version": "2"})
        logging.info(f"✅ Resultado: {data}")
    except Exception as e:
        logging.error(f"❌ Erro ao buscar dados para 'campaigns_summary': {e}")


def job_campaigns_daily(advertiser_id, site_id, date_from, date_to):
    logging.info("📅 Coletando métricas diárias de campanhas...")
    endpoint = ENDPOINTS["campaigns_daily"].format(site_id=site_id, advertiser_id=advertiser_id)
    params = {
        "limit": 50,
        "offset": 0,
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(METRICS),
    }

    try:
        data = meli_get(endpoint, params=params, headers={"Api-Version": "2"})
        logging.info(f"✅ Resultado: {data}")
    except Exception as e:
        logging.error(f"❌ Erro ao buscar dados para 'campaigns_daily': {e}")


def job_ads_summary(advertiser_id, site_id, date_from, date_to):
    logging.info("📈 Coletando métricas resumidas de anúncios...")
    endpoint = ENDPOINTS["ads_search"].format(site_id=site_id, advertiser_id=advertiser_id)
    params = {
        "limit": 50,
        "offset": 0,
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(METRICS),
        "metrics_summary": "true"
    }

    try:
        data = meli_get(endpoint, params=params, headers={"Api-Version": "2"})
        logging.info(f"✅ Resultado: {data}")
    except Exception as e:
        logging.error(f"❌ Erro ao buscar dados para 'ads_summary': {e}")


def job_ads_daily(advertiser_id, site_id, date_from, date_to):
    logging.info("📅 Coletando métricas diárias de anúncios...")
    endpoint = ENDPOINTS["ads_daily"].format(site_id=site_id, advertiser_id=advertiser_id)
    params = {
        "limit": 50,
        "offset": 0,
        "date_from": date_from,
        "date_to": date_to,
        "metrics": ",".join(METRICS),
    }

    try:
        data = meli_get(endpoint, params=params, headers={"Api-Version": "2"})
        logging.info(f"✅ Resultado: {data}")
    except Exception as e:
        logging.error(f"❌ Erro ao buscar dados para 'ads_daily': {e}")
