# src/daily_update.py
import os
import logging
import pandas as pd
from pathlib import Path
from datetime import date
from src.jobs import enviar_para_google_sheets
from src.csv_utils import upsert_csv  # opcional, ver nota

ADS_PATH    = "data/processed/ads_daily.csv"          # alinhe com o padrão do pipeline
ORDERS_PATH = "data/processed/orders_daily.csv"
MERGED_PATH = "data/processed/merged_product_daily.csv"

log = logging.getLogger("daily_update")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def load_or_empty(path: str, parse_dates=None):
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, parse_dates=parse_dates or [])

def update_daily_data():
    ads    = load_or_empty(ADS_PATH, parse_dates=["date"] if Path(ADS_PATH).exists() else [])
    orders = load_or_empty(ORDERS_PATH, parse_dates=["date"] if Path(ORDERS_PATH).exists() else [])

    if ads.empty and orders.empty:
        log.warning("Nenhum dado para processar (ads/orders vazios).")
        return

    # --- validação de chaves para o merge ---
    # ajuste conforme seu schema real
    possible_join = [
        ("item_id", "item_id"),
        ("ad_id", "ad_id"),
        ("listing_id", "listing_id"),
        # ("item_id", "buyer_id"),  # geralmente NÃO faz sentido
    ]
    left_on = right_on = None
    for l, r in possible_join:
        if l in ads.columns and r in orders.columns:
            left_on, right_on = l, r
            break

    if left_on and right_on:
        merged = pd.merge(ads, orders, how="left", left_on=left_on, right_on=right_on, suffixes=("", "_ord"))
        log.info("Merge por %s↔%s: ads=%d, orders=%d → merged=%d",
                 left_on, right_on, len(ads), len(orders), len(merged))
    else:
        merged = ads.copy()
        log.warning("Sem colunas compatíveis para merge — copiando apenas ADS.")

    # --- normalização de datas ---
    if "date" in merged.columns:
        # garante string ISO para CSV/Sheets
        merged["date"] = pd.to_datetime(merged["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # --- deduplicação por chaves relevantes ---
    dedup_keys = [k for k in ("date", "seller_id", "campaign_id", "ad_id", "item_id") if k in merged.columns]
    if dedup_keys:
        before = len(merged)
        merged = merged.drop_duplicates(subset=dedup_keys)
        log.info("Dedup por %s: %d → %d linhas", dedup_keys, before, len(merged))
    else:
        merged = merged.drop_duplicates()

    # --- ordenação estável ---
    sort_keys = [k for k in ("seller_id", "campaign_id", "ad_id", "date") if k in merged.columns]
    if sort_keys:
        merged = merged.sort_values(by=sort_keys, ascending=True)

    # --- gravação atômica ---
    out_dir = Path(MERGED_PATH).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    # Opção A: escrita atômica manual
    tmp = out_dir / (Path(MERGED_PATH).name + ".tmp")
    merged.to_csv(tmp, index=False, encoding="utf-8", line_terminator="\n")
    os.replace(tmp, MERGED_PATH)

    # Opção B (alternativa): usar upsert_csv com chave (se quiser manter idempotência linha a linha)
    # from src.csv_utils import upsert_csv
    # rows = merged.to_dict(orient="records")
    # upsert_csv(MERGED_PATH, rows, key_fields=dedup_keys or merged.columns.tolist(),
    #            schema=list(merged.columns), allow_new_columns=False, atomic=True)

    log.info("✅ Atualizado: %s (%d linhas)", MERGED_PATH, len(merged))

    if not merged.empty:
        enviar_para_google_sheets(MERGED_PATH, sheet="merged_product_daily")
    else:
        log.warning("Arquivo vazio — não enviando para o Sheets.")

if __name__ == "__main__":
    update_daily_data()
