# src/build_orders_daily.py
import os
import glob
import pandas as pd
from src.jobs import enviar_para_google_sheets

RAW_GLOB = "data/raw/orders_*.csv"
OUT_PATH = "data/processed/orders_daily.csv"

def build_orders_daily():
    files = sorted(glob.glob(RAW_GLOB))
    if not files:
        print("⚠️ Nenhum arquivo em data/raw/orders_*.csv")
        return None

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            # normaliza nomes esperados (pelo seu screenshot)
            # id,date_created,status,total_amount,currency_id,buyer_id
            cols = {c.lower(): c for c in df.columns}
            # garante lower-case p/ trabalhar
            df.columns = [c.lower() for c in df.columns]

            # cria coluna date (YYYY-MM-DD) a partir de date_created
            if "date_created" in df.columns:
                df["date"] = pd.to_datetime(df["date_created"], errors="coerce").dt.date
            else:
                df["date"] = pd.NaT

            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Falha lendo {f}: {e}")

    if not dfs:
        print("⚠️ Nenhum dataframe válido.")
        return None

    full = pd.concat(dfs, ignore_index=True)
    # remove duplicatas por id (se existir)
    if "id" in full.columns:
        full = full.drop_duplicates(subset=["id"])

    # ordena por date
    if "date" in full.columns:
        full = full.sort_values("date")

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    full.to_csv(OUT_PATH, index=False, encoding="utf-8")
    print(f"✅ Atualizado: {OUT_PATH} ({len(full)} linhas)")
    return OUT_PATH

if __name__ == "__main__":
    path = build_orders_daily()
    if path:
        enviar_para_google_sheets(path, sheet="orders_daily")
