# src/cleanup_snapshots_raw.py
from __future__ import annotations
import os
from pathlib import Path
from typing import Iterable, Sequence
import logging

RAW_DIR = Path("data/raw")

# Padrões padrão (pode sobrepor ao chamar a função)
PATTERNS = [
    "users_me_*.csv",
    "orders_*.csv",
]

LOG_FMT = "%(asctime)s | %(levelname)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)

def _list_sorted(files: Iterable[Path]) -> list[Path]:
    # Ordena por mtime desc (mais recente primeiro)
    return sorted([f for f in files if f.is_file()],
                  key=lambda p: p.stat().st_mtime,
                  reverse=True)

def _cleanup_pattern(pattern: str, keep_last: int) -> int:
    paths = _list_sorted(RAW_DIR.glob(pattern))
    if not paths:
        logger.info(f"🔎 {pattern}: nenhum arquivo encontrado.")
        return 0

    keep = paths[:keep_last]
    trash = paths[keep_last:]
    logger.info(f"📦 {pattern}: {len(paths)} arquivo(s) | mantendo {len(keep)}, removendo {len(trash)}")

    removed = 0
    for p in trash:
        try:
            os.remove(p)
            removed += 1
            logger.warning(f"  ❌ removido: {p.name}")
        except Exception as e:
            logger.error(f"  ⚠️ erro ao remover {p.name}: {e}")
    return removed

def cleanup_noninteractive(*, keep_last: int = 1, patterns: Sequence[str] | None = None) -> int:
    """
    Limpa snapshots antigos em data/raw sem prompt.
    Retorna o total removido.
    """
    if not RAW_DIR.exists():
        logger.info(f"ℹ️ Pasta {RAW_DIR} não existe — nada a fazer.")
        return 0

    pats = list(patterns or PATTERNS)
    total = 0
    for pat in pats:
        total += _cleanup_pattern(pat, keep_last)
    return total

# Execução interativa opcional via CLI: mantém o prompt de confirmação
def main():
    if not RAW_DIR.exists():
        logger.error(f"❌ Pasta {RAW_DIR} não existe.")
        return

    logger.info(f"🧹 Limpando snapshots em {RAW_DIR.resolve()}")
    logger.info(f"Padrões: {', '.join(PATTERNS)} | KEEP_LAST=1 (padrão)")

    confirm = input("⚠️ Confirmar remoção (sim/não)? ").strip().lower()
    if confirm not in {"s", "sim"}:
        logger.info("🚫 Cancelado.")
        return

    removed = cleanup_noninteractive(keep_last=1, patterns=PATTERNS)
    logger.info(f"✅ Limpeza concluída. Arquivos removidos: {removed}")

if __name__ == "__main__":
    main()
