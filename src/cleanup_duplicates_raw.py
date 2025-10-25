# src/cleanup_duplicates_raw.py
from __future__ import annotations
import hashlib
import logging
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Iterable, Tuple, Set
import argparse

logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
EXCLUDE_SUFFIXES = (".lock", ".tmp", ".partial")

# -------- utils --------
def _iter_files(folder: Path) -> Iterable[Path]:
    if not folder.exists():
        return []
    for p in folder.iterdir():
        try:
            if p.is_file() and not p.is_symlink() and not p.name.endswith(EXCLUDE_SUFFIXES):
                yield p
        except Exception:
            continue

def hash_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """SHA256 por streaming (1MB/chunk)."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

# -------- deteção de duplicados --------
def find_duplicates(folder: Path) -> tuple[dict[str, list[Path]], dict[str, list[Path]]]:

    """
    Retorna (dups_por_nome, dups_por_conteudo).
    Conteúdo: agrupa por tamanho e só então calcula hash (otimiza custo).
    """
    if not folder.exists():
        logger.error("❌ Pasta %s não encontrada.", folder)
        return {}, {}

    files = list(_iter_files(folder))
    if not files:
        logger.info("📂 Nenhum arquivo encontrado em %s.", folder)
        return {}, {}

    # por nome
    name_map: Dict[str, List[Path]] = defaultdict(list)
    for f in files:
        name_map[f.name].append(f)
    name_dups = {k: v for k, v in name_map.items() if len(v) > 1}

    # por conteúdo (apenas se mesmo tamanho)
    size_map: Dict[int, List[Path]] = defaultdict(list)
    for f in files:
        try:
            size_map[f.stat().st_size].append(f)
        except Exception:
            continue

    hash_map: Dict[str, List[Path]] = defaultdict(list)
    for size, group in size_map.items():
        if len(group) < 2:
            continue
        for f in group:
            try:
                h = hash_file(f)
                hash_map[h].append(f)
            except Exception as e:
                logger.warning("⚠️ Falha ao hashear %s: %s", f.name, e)
    content_dups = {k: v for k, v in hash_map.items() if len(v) > 1}

    return name_dups, content_dups

# -------- remoção --------
def _choose_keep(paths: List[Path], keep_strategy: str = "newest") -> Tuple[Path, List[Path]]:
    """
    keep_strategy: 'newest' (mais novo) ou 'oldest' (mais antigo).
    """
    reverse = (keep_strategy == "newest")
    ordered = sorted(paths, key=lambda p: p.stat().st_mtime, reverse=reverse)
    return ordered[0], ordered[1:]

def cleanup_duplicates(dups_dict: Dict[str, List[Path]], mode: str,
                       *, dry_run: bool = False, keep_strategy: str = "newest",
                       already_removed: Set[Path] | None = None) -> int:
    """
    Remove duplicados mantendo um (por grupo). Evita remover duas vezes
    quando o mesmo arquivo aparece em múltiplos grupos (nome+conteúdo).
    """
    removed = 0
    already_removed = already_removed or set()

    for key, paths in dups_dict.items():
        # filtra os que já foram removidos em outra passada
        paths = [p for p in paths if p not in already_removed]
        if len(paths) < 2:
            continue

        keep, to_remove = _choose_keep(paths, keep_strategy)
        logger.info("🧩 Duplicados por %s — mantendo: %s", mode, keep.name)
        for r in to_remove:
            if r in already_removed:
                continue
            if dry_run:
                logger.warning("  🧪 dry-run: removeria %s", r.name)
                continue
            try:
                r.unlink(missing_ok=True)
                already_removed.add(r)
                removed += 1
                logger.warning("  ❌ removido: %s", r.name)
            except Exception as e:
                logger.error("  ⚠️ Erro ao remover %s: %s", r.name, e)
    return removed

# -------- CLI --------
def main():
    parser = argparse.ArgumentParser(description="Remove arquivos duplicados em data/raw.")
    parser.add_argument("--folder", default=str(RAW_DIR))
    parser.add_argument("--dry-run", action="store_true", help="Apenas simula; não remove nada.")
    parser.add_argument("--keep", choices=("newest", "oldest"), default="newest")
    parser.add_argument("--mode", choices=("all", "name", "content"), default="all")
    args = parser.parse_args()

    # logging básico só quando rodar via CLI
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    folder = Path(args.folder)
    name_dups, content_dups = find_duplicates(folder)
    if not name_dups and not content_dups:
        logger.info("✅ Nenhum duplicado encontrado.")
        return

    logger.info("🔠 Duplicados por nome: %d", len(name_dups))
    logger.info("💾 Duplicados por conteúdo: %d", len(content_dups))

    confirm = "sim" if args.dry_run else input("\n⚠️ Remover mantendo o mais %s? (sim/não): " % args.keep).strip().lower()
    if confirm not in {"s", "sim"}:
        logger.info("🚫 Operação cancelada.")
        return

    removed_total = 0
    already_removed: Set[Path] = set()

    if args.mode in {"all", "name"}:
        removed_total += cleanup_duplicates(name_dups, "nome", dry_run=args.dry_run,
                                            keep_strategy=args.keep, already_removed=already_removed)
    if args.mode in {"all", "content"}:
        removed_total += cleanup_duplicates(content_dups, "conteúdo", dry_run=args.dry_run,
                                            keep_strategy=args.keep, already_removed=already_removed)

    logger.info("✅ Limpeza concluída: %d arquivo(s) removido(s).", removed_total)

if __name__ == "__main__":
    main()
