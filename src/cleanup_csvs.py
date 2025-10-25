# src/cleanup_csvs.py
from __future__ import annotations
import os
import logging
from pathlib import Path
from typing import Sequence

FOLDER = Path("data/processed")

# nomes base (sem timestamp) que devem ser preservados
KEEP_DEFAULT = {
    "ads_daily.csv",
    "campaign_daily.csv",
    "orders_items_daily.csv",
    "ads_summary.csv",
    "campaign_summary.csv",
}

LOG_FMT = "%(asctime)s | %(levelname)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
log = logging.getLogger("cleanup_csvs")

def cleanup_csvs(folder: Path = FOLDER, keep: Sequence[str] | None = None, dry_run: bool = False) -> int:
    """
    Remove CSVs antigos do diretório 'data/processed', mantendo apenas os especificados.
    Retorna o número de arquivos removidos.
    """
    keep = set(keep or KEEP_DEFAULT)
    if not folder.exists():
        log.warning("📁 Pasta %s não existe — nada a fazer.", folder)
        return 0

    csv_files = [p for p in folder.glob("*.csv")]
    log.info("📦 Encontrados %d CSVs.", len(csv_files))

    to_delete = [p for p in csv_files if p.name not in keep]
    if not to_delete:
        log.info("✅ Nenhum arquivo fora da lista KEEP.")
        return 0

    log.info("🧹 Candidatos à exclusão (%d):", len(to_delete))
    for p in to_delete:
        log.info("  - %s", p.name)

    if dry_run:
        log.info("🧪 dry-run: nada foi removido.")
        return 0

    confirm = input("\n⚠️ Confirmar remoção? (s/n): ").strip().lower()
    if confirm not in {"s", "sim"}:
        log.info("🚫 Cancelado pelo usuário.")
        return 0

    removed = 0
    for p in to_delete:
        try:
            p.unlink(missing_ok=True)
            removed += 1
            log.warning("  ❌ removido: %s", p.name)
        except Exception as e:
            log.error("⚠️ Erro ao remover %s: %s", p.name, e)

    log.info("✅ Limpeza concluída. %d arquivo(s) removido(s).", removed)
    return removed

if __name__ == "__main__":
    cleanup_csvs()
