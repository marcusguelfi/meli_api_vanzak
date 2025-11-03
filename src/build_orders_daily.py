import os, glob, logging
import pandas as pd
from pathlib import Path
from src.csv_utils import upsert_csv
from src.jobs import enviar_para_google_sheets

# Caminhos e constantes
RAW_GLOB    = "data/raw/orders_*.csv"
OUT_PATH    = "data/processed/orders_daily.csv"
SHEET_NAME  = "orders_daily"

log = logging.getLogger("build_orders_daily")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def _recent_raw_files() -> list[str]:
    """Busca todos os CSVs que seguem o padrão 'orders_*.csv' na pasta data/raw."""
    files = glob.glob(RAW_GLOB)
    if not files:
        log.warning("Nenhum arquivo encontrado em %s", RAW_GLOB)
        return []
    files.sort(key=os.path.getmtime, reverse=False)
    return files


def _load_normalized(f: str) -> pd.DataFrame:
    """Lê e normaliza o CSV de orders, garantindo colunas mínimas e formato consistente."""
    df = pd.read_csv(f, dtype=str)
    # Limpa espaços de colunas e células
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Cria coluna 'date'
    if "date_created" in df.columns:
        dt = pd.to_datetime(df["date_created"], errors="coerce", utc=True)
        df["date"] = dt.dt.strftime("%Y-%m-%d")
    elif "date" not in df.columns:
        df["date"] = None

    # Colunas mínimas + adicionais caso existam
    keep = [
        c for c in (
            "id","date","status","total_amount","currency_id",
            "buyer_id","item_id","order_type","shipping_cost",
            "seller_id","payments_total","feedback","item_title"
        ) if c in df.columns
    ]
    return df[keep]


def build_orders_daily():
    """Consolida, enriquece e atualiza o dataset diário de pedidos."""
    files = _recent_raw_files()
    if not files:
        log.warning("Nenhum arquivo raw encontrado para processar.")
        return None

    frames = []
    for f in files:
        try:
            frames.append(_load_normalized(f))
            log.info("Arquivo processado: %s", f)
        except Exception as e:
            log.warning("Falha lendo %s: %s", f, e)

    if not frames:
        log.warning("Nenhum dataframe válido encontrado.")
        return None

    df = pd.concat(frames, ignore_index=True)

    # 🔹 Enriquecimento interno
    if "total_amount" in df.columns:
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")

    if "status" in df.columns:
        df["is_paid"] = df["status"].str.lower().eq("paid").astype(int)

    if "currency_id" in df.columns:
        df["revenue_brl"] = df["total_amount"].where(df["currency_id"] == "BRL", None)

    if "date" in df.columns:
        df["month"] = pd.to_datetime(df["date"], errors="coerce").dt.to_period("M").astype(str)

    # 🔹 Enriquecimento externo (join com product_info.csv)
    prod_path = "data/processed/product_info.csv"
    if os.path.exists(prod_path):
        try:
            prod = pd.read_csv(prod_path, dtype=str)
            prod.columns = [c.strip().lower() for c in prod.columns]
            if "item_id" in prod.columns:
                cols_to_join = [c for c in ("item_id","title","category_name") if c in prod.columns]
                df = df.merge(prod[cols_to_join], on="item_id", how="left")
                log.info("Join realizado com %s (%d colunas adicionadas)", prod_path, len(cols_to_join)-1)
        except Exception as e:
            log.warning("Falha ao enriquecer com %s: %s", prod_path, e)

    # 🔹 Deduplicação por ID
    subset = ["id"] if "id" in df.columns else None
    if subset:
        before = len(df)
        df = df.drop_duplicates(subset=subset)
        log.info("Deduplicado por %s: %d → %d", subset, before, len(df))

    # 🔹 Ordena por data e id
    sort_cols = [c for c in ("date","id") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    # 🔹 Upsert no CSV cumulativo
    rows = df.to_dict(orient="records")
    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    upsert_csv(
        OUT_PATH,
        rows,
        key_fields=subset or ["id"],
        schema=list(df.columns),
        allow_new_columns=True,
        atomic=True,
        lock_timeout_s=60,
    )

    log.info("✅ Atualizado: %s (linhas processadas: %d)", OUT_PATH, len(df))
    return OUT_PATH


if __name__ == "__main__":
    path = build_orders_daily()
    if path:
        enviar_para_google_sheets(path, sheet=SHEET_NAME)
