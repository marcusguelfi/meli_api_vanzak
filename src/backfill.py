# src/backfill.py
from __future__ import annotations

import os
import logging
from datetime import date, timedelta

# 🔧 Import corrigido (relativo)
from .jobs import run_product_ads_pipeline


# -------------------------------------------------------------
# Configuração de logs
# -------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)


# -------------------------------------------------------------
# Parâmetros
# -------------------------------------------------------------
ADVERTISER_ID = os.getenv("ADVERTISER_ID", "").strip()
SITE_ID = os.getenv("SITE_ID", "MLB").strip()
MAX_SPAN_DAYS = 90  # limite da API
START_DATE = os.getenv("BACKFILL_START_DATE", "2024-01-01").strip()
END_DATE = os.getenv("BACKFILL_END_DATE", date.today().isoformat()).strip()

if not ADVERTISER_ID:
    raise SystemExit("❌ Defina ADVERTISER_ID no ambiente antes de rodar o backfill.")


# -------------------------------------------------------------
# Função utilitária para gerar intervalos de até 90 dias
# -------------------------------------------------------------
def generate_periods(start_date: date, end_date: date, span_days: int = MAX_SPAN_DAYS):
    periods = []
    cursor = start_date
    while cursor < end_date:
        next_end = min(cursor + timedelta(days=span_days - 1), end_date)
        periods.append((cursor, next_end))
        cursor = next_end + timedelta(days=1)
    return periods


# -------------------------------------------------------------
# Execução principal
# -------------------------------------------------------------
def main():
    start_date = date.fromisoformat(START_DATE)
    end_date = date.fromisoformat(END_DATE)
    periods = generate_periods(start_date, end_date, MAX_SPAN_DAYS)

    log.info(f"🚀 Backfill de {start_date} até {end_date} ({len(periods)} blocos de até {MAX_SPAN_DAYS} dias)")

    for i, (df, dt) in enumerate(periods, start=1):
        log.info(f"🔹 [{i}/{len(periods)}] Executando período {df} → {dt}")
        try:
            run_product_ads_pipeline(
                advertiser_id=ADVERTISER_ID,
                site_id=SITE_ID,
                backfill_days=MAX_SPAN_DAYS,
                date_from=df.isoformat(),
                date_to=dt.isoformat(),
            )
        except Exception as e:
            log.exception(f"⚠️ Erro ao processar período {df} → {dt}: {e}")

    log.info("✅ Backfill concluído com sucesso!")


# -------------------------------------------------------------
# Execução direta
# -------------------------------------------------------------
if __name__ == "__main__":
    main()
