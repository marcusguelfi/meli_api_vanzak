# src/cleanup_old_data.py
import os
import re
import logging
from pathlib import Path

LOG_FMT = "%(asctime)s | %(levelname)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)

DATA_DIR = Path("data/processed")
TIMESTAMP_PATTERN = re.compile(r"_\d{8}_\d{6}")

def cleanup_processed_folder():
    """
    Remove arquivos antigos (com timestamp no nome) dentro de data/processed,
    preservando CSVs fixos e o ads_store.json.
    """
    if not DATA_DIR.exists():
        logging.warning("⚠️ Pasta data/processed não existe.")
        return

    removed = 0
    skipped = 0

    for path in DATA_DIR.glob("*"):
        if path.is_dir():
            continue

        name = path.name

        # mantém os CSVs fixos e arquivos essenciais
        if name.endswith(".csv") and not TIMESTAMP_PATTERN.search(name):
            skipped += 1
            continue
        if name == "ads_store.json":
            skipped += 1
            continue

        # remove arquivos com timestamp no nome
        if TIMESTAMP_PATTERN.search(name):
            try:
                path.unlink()
                logging.info(f"🗑️ Removido: {name}")
                removed += 1
            except Exception as e:
                logging.error(f"❌ Erro ao remover {name}: {e}")
        else:
            skipped += 1

    logging.info(f"✅ Limpeza concluída — {removed} removidos, {skipped} preservados.")

if __name__ == "__main__":
    cleanup_processed_folder()
