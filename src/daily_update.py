# src/daily_update.py
import os
import pandas as pd
from datetime import date
from src.jobs import enviar_para_google_sheets

ADS_PATH = "data/processed/ads_daily.csv"
ORDERS_PATH = "data/processed/orders_daily.csv"
MERGED_PATH = "data/processed/merged_product_daily.csv"

def load_or_empty(path: str):
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()

def update_daily_data():
    ads = load_or_empty(ADS_PATH)
    orders = load_or_empty(ORDERS_PATH)

    # === EXEMPLO DE JOIN SIMPLIFICADO ===
    # (ajuste as chaves conforme o que seus JSONs contêm)
    if "item_id" in ads.columns and "buyer_id" in orders.columns:
        merged = pd.merge(ads, orders, how="left", left_on="item_id", right_on="buyer_id")
    else:
        merged = ads.copy()

    # remove duplicatas
    merged.drop_duplicates(inplace=True)
    merged.sort_values(by="date", ascending=True, inplace=True)

    # salva cumulativo
    os.makedirs(os.path.dirname(MERGED_PATH), exist_ok=True)
    merged.to_csv(MERGED_PATH, index=False)
    print(f"✅ Atualizado: {MERGED_PATH} ({len(merged)} linhas)")

    # envia pro Google Sheets
    enviar_para_google_sheets(MERGED_PATH, sheet="merged_product_daily")

if __name__ == "__main__":
    update_daily_data()
