ENDPOINTS = {
    # --- Advertiser ---
    "advertiser_search": "/advertising/advertisers?product_id=PADS",

    # --- Campaigns ---
    "campaigns_search": "/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search",
    "campaigns_daily": "/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/campaigns/search?aggregation_type=DAILY",
    "campaign_detail": "/advertising/{site_id}/product_ads/campaigns/{campaign_id}",

    # --- Ads ---
    "ads_search": "/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search",
    "ads_daily": "/advertising/{site_id}/advertisers/{advertiser_id}/product_ads/ads/search?aggregation_type=DAILY",
    "ad_detail": "/advertising/{site_id}/product_ads/ads/{item_id}",
}
