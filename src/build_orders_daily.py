# src/build_orders_daily.py
import os, glob, re, logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from src.csv_utils import upsert_csv
from src.jobs import enviar_para_google_sheets

RAW_GLOB   = "data/raw/orders_*.csv"
OUT_PATH   = "data/processed/orders_daily.csv"
SHEET_NAME = "orders_daily"
WINDOW_DAYS = 7  # janela de reprocessamento

log = logging.getLogger("build_orders_daily")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

_ts = re.compile(r"orders_(\d{8}T\d{6}Z)\.csv$")

def _recent_raw_files() -> list[str]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=WINDOW_DAYS + 1)
    files = []
    for f in glob.glob(RAW_GLOB):
        m = _ts.search(os.path.basename(f))
        if not m:
            continue
        try:
            ts = datetime.strptime(m.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            if ts >= cutoff:
                files.append((ts, f))
        except Exception:
            continue
    return [f for _, f in sorted(files)]

def _load_normalized(f: str) -> pd.DataFrame:
    df = pd.read_csv(f, dtype=str)  # evita coerção estranha
    df.columns = [c.lower() for c in df.columns]
    # date
    if "date_created" in df.columns:
        dt = pd.to_datetime(df["date_created"], errors="coerce", utc=True)
        df["date"] = dt.dt.strftime("%Y-%m-%d")
    elif "date" not in df.columns:
        df["date"] = None
    # colunas mínimas (ajuste conforme seu schema real)
    keep = [c for c in ("id","date","total_amount","currency_id","buyer_id","item_id","status") if c in df.columns]
    return df[keep]

def build_orders_daily():
    files = _recent_raw_files()
    if not files:
        log.warning("Nenhum raw recente para processar em %s", RAW_GLOB)
        return None

    frames = []
    for f in files:
        try:
            frames.append(_load_normalized(f))
        except Exception as e:
            log.warning("Falha lendo %s: %s", f, e)

    if not frames:
        log.warning("Nenhum dataframe válido.")
        return None

    df = pd.concat(frames, ignore_index=True)
    # dedup por id (ou id+date se fizer sentido)
    subset = ["id"] if "id" in df.columns else None
    if subset:
        before = len(df)
        df = df.drop_duplicates(subset=subset)
        log.info("Dedup por %s: %d → %d", subset, before, len(df))
    # ordenação opcional
    sort_cols = [c for c in ("date","id") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    # Upsert idempotente no cumulativo
    rows = df.to_dict(orient="records")
    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    upsert_csv(
        OUT_PATH,
        rows,
        key_fields=subset or ["id"],  # garante consistência
        schema=list(df.columns),      # congela ordem
        allow_new_columns=False,
        atomic=True,
        lock_timeout_s=60,
    )

    log.info("✅ Atualizado: %s (linhas novas/atualizadas: %d)", OUT_PATH, len(df))
    return OUT_PATH

if __name__ == "__main__":
    path = build_orders_daily()
    if path:
        enviar_para_google_sheets(path, sheet=SHEET_NAME)
