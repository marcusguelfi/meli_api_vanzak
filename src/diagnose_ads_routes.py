import logging
from src.meli_client import meli_get
from src.product_ads_endpoints import ENDPOINTS

logging.basicConfig(level=logging.INFO, format="%(message)s")


def diagnose_routes(advertiser_id="731958", site_id="MLB"):
    print("🔍 Testando rotas Product Ads disponíveis no Mercado Livre...\n")

    for name, endpoint in ENDPOINTS.items():
        path = endpoint.format(site_id=site_id, advertiser_id=advertiser_id, campaign_id="123", item_id="MLB123")
        try:
            meli_get(path)
            print(f"{path} → ✅ 200 OK")
        except Exception as e:
            err = str(e)
            if "404" in err:
                print(f"{path} → ❌ 404 (não encontrada)")
            elif "400" in err:
                print(f"{path} → ⚠️ 400 (Bad Request)")
            else:
                print(f"{path} → ⚠️ Erro inesperado")


if __name__ == "__main__":
    diagnose_routes()
