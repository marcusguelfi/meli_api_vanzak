# src/merge_ads_data.py
from __future__ import annotations
import os
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

PROCESSED_DIR = "data/processed"
OUTPUT_DIR = "data/merged"

def merge_ads_data(advertiser_id: str) -> str:
    """Une ads_summary + ads_daily em um CSV final pronto pro dashboard."""

    ads_summary_path = os.path.join(PROCESSED_DIR, f"ads_summary_{advertiser_id}.csv")
    ads_daily_path = os.path.join(PROCESSED_DIR, f"ads_daily_{advertiser_id}.csv")

    if not os.path.exists(ads_summary_path):
        raise FileNotFoundError(f"❌ Arquivo não encontrado: {ads_summary_path}")
    if not os.path.exists(ads_daily_path):
        raise FileNotFoundError(f"❌ Arquivo não encontrado: {ads_daily_path}")

    log.info("📂 Carregando arquivos:\n  - %s\n  - %s", ads_summary_path, ads_daily_path)

    df_summary = pd.read_csv(ads_summary_path)
    df_daily = pd.read_csv(ads_daily_path)

    log.info("🔗 Fazendo merge por ad_id e advertiser_id...")
    merged = df_daily.merge(
        df_summary[
            ["advertiser_id", "ad_id", "item_id", "item_title", "seller_sku", "status"]
        ],
        on=["advertiser_id", "ad_id"],
        how="left",
        suffixes=("", "_summary"),
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"ads_daily_enriched_{advertiser_id}.csv")
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")

    log.info("✅ Merge completo: %s (%d linhas)", out_path, len(merged))
    return out_path


if __name__ == "__main__":
    adv = os.getenv("ADVERTISER_ID", "").strip()
    if not adv:
        raise SystemExit("Defina ADVERTISER_ID no ambiente.")
    merge_ads_data(adv)
