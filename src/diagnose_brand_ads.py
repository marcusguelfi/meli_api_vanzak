from src.meli_client import meli_get
import os
import logging

logging.basicConfig(level=logging.INFO)

def main():
    brand_id = os.getenv("BRAND_ID", "").strip()
    if not brand_id:
        raise SystemExit("❌ Defina a variável de ambiente BRAND_ID antes de rodar este teste.")

    print(f"🔎 Testando Brand Ads para brand_id={brand_id}")

    # endpoint principal de campanhas de Sponsored Brands
    endpoint = f"/advertising/brands/{brand_id}/campaigns/search"

    try:
        resp = meli_get(endpoint, params={"limit": 1})
        print("\n✅ Resposta recebida da API:")
        print(resp)
    except Exception as e:
        print(f"\n❌ Erro ao chamar {endpoint}: {e}")

if __name__ == "__main__":
    main()
