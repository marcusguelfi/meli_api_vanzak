# src/jobs_cleanup.py
from __future__ import annotations

import os
import re
import logging
from datetime import datetime
from typing import List

# Configuração básica de log
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

# Diretório padrão
PROCESSED_DIR = "data/processed"

# Regex para identificar os arquivos versionados do tipo ads_detail_daily_XXXX_YYYY-MM-DD.csv
PATTERN = re.compile(r"ads_detail_daily_(\d{4,})_(\d{4}-\d{2}-\d{2})\.csv$")


def cleanup_old_versions(keep_days: int = 7) -> None:
    """
    Mantém apenas os últimos N arquivos versionados de ads_detail_daily por advertiser_id.
    """
    files = [f for f in os.listdir(PROCESSED_DIR) if PATTERN.match(f)]
    by_adv: dict[str, List[str]] = {}

    for fname in files:
        match = PATTERN.match(fname)
        if not match:
            continue
        advertiser_id, date_str = match.groups()
        by_adv.setdefault(advertiser_id, []).append(date_str)

    for adv_id, dates in by_adv.items():
        # Ordena por data (mais recentes primeiro)
        sorted_dates = sorted(dates, reverse=True)
        to_keep = set(sorted_dates[:keep_days])
        to_delete = [d for d in sorted_dates if d not in to_keep]

        for d in to_delete:
            fname = f"ads_detail_daily_{adv_id}_{d}.csv"
            fpath = os.path.join(PROCESSED_DIR, fname)
            try:
                os.remove(fpath)
                log.info("🧹 Removido histórico antigo: %s", fname)
            except Exception as e:
                log.warning("⚠️ Falha ao apagar %s: %s", fname, e)

    log.info("✅ Limpeza concluída — mantendo últimos %d dias por advertiser_id.", keep_days)


if __name__ == "__main__":
    import sys
    keep_days = int(os.getenv("KEEP_HISTORY_DAYS", "7"))
    cleanup_old_versions(keep_days)
