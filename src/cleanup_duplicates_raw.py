# src/cleanup_duplicates_raw.py
import os
import hashlib
import logging
from pathlib import Path
from collections import defaultdict

# Configuração do log
LOG_FMT = "%(asctime)s | %(levelname)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)

RAW_DIR = Path("data/raw")

def hash_file(path: Path, chunk_size: int = 8192) -> str:
    """Calcula o hash SHA256 do arquivo."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

def find_duplicates(folder: Path):
    """Encontra arquivos duplicados por nome e conteúdo."""
    if not folder.exists():
        logging.error(f"❌ Pasta {folder} não encontrada.")
        return {}, {}

    logging.info(f"🔍 Analisando arquivos em: {folder.resolve()}")
    files = list(folder.glob("*"))
    if not files:
        logging.info("📂 Nenhum arquivo encontrado.")
        return {}, {}

    # Duplicados por nome
    name_map = defaultdict(list)
    for f in files:
        if f.is_file():
            name_map[f.name].append(f)
    name_dups = {k: v for k, v in name_map.items() if len(v) > 1}

    # Duplicados por conteúdo
    hash_map = defaultdict(list)
    for f in files:
        if f.is_file():
            file_hash = hash_file(f)
            hash_map[file_hash].append(f)
    content_dups = {k: v for k, v in hash_map.items() if len(v) > 1}

    return name_dups, content_dups

def cleanup_duplicates(dups_dict: dict[str, list[Path]], mode: str):
    """Remove duplicados mantendo o mais recente."""
    total_removed = 0
    for key, paths in dups_dict.items():
        # Ordena por data de modificação (mais recente no topo)
        paths_sorted = sorted(paths, key=lambda p: p.stat().st_mtime, reverse=True)
        keep = paths_sorted[0]
        remove = paths_sorted[1:]

        logging.info(f"\n🧩 Duplicados ({mode}) — mantendo: {keep.name}")
        for r in remove:
            logging.warning(f"  ❌ removendo {r.name}")
            try:
                os.remove(r)
                total_removed += 1
            except Exception as e:
                logging.error(f"Erro ao remover {r}: {e}")
    return total_removed

def main():
    name_dups, content_dups = find_duplicates(RAW_DIR)

    if not name_dups and not content_dups:
        logging.info("✅ Nenhum duplicado encontrado.")
        return

    logging.info("\nResumo:")
    logging.info(f"🔠 Duplicados por nome: {len(name_dups)}")
    logging.info(f"💾 Duplicados por conteúdo: {len(content_dups)}")

    confirm = input("\n⚠️ Deseja remover duplicados mantendo apenas o mais recente? (sim/não): ").strip().lower()
    if confirm not in {"s", "sim"}:
        logging.info("🚫 Operação cancelada.")
        return

    removed_name = cleanup_duplicates(name_dups, "nome")
    removed_content = cleanup_duplicates(content_dups, "conteúdo")
    logging.info(f"\n✅ Limpeza concluída: {removed_name + removed_content} arquivos removidos.")

if __name__ == "__main__":
    main()
