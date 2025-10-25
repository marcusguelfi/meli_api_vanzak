# src/product_ads_endpoints.py
"""
Mapeamento centralizado dos endpoints de Product Ads no Mercado Livre.
Esses templates são usados pelo diagnose_ads_routes.py e pelos jobs.
"""

ENDPOINTS = {
    # 🔍 Busca de anunciantes que usam Product Ads
    # ✅ confirmado: funciona sem {site_id}
    "advertiser_search": "/advertising/advertisers?product_id=PADS",

    # 📊 Campanhas: busca agregada (aggregation_type=CAMPAIGN/DAILY)
    "campaigns_search": "/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search",

    # 📄 Detalhe de campanha específica
    "campaign_detail": "/advertising/{site_id}/product_ads/campaigns/{campaign_id}",

    # 📊 Anúncios: busca agregada (aggregation_type=ITEM/DAILY/ADGROUP)
    "ads_search": "/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search",

    # 📄 Detalhe de anúncio específico (usa ad_id)
    "ad_detail": "/advertising/{site_id}/product_ads/ads/{ad_id}",
}
