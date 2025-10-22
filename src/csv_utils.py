# src/csv_utils.py
from __future__ import annotations
import csv
import os
import time
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any, Iterable, List, Tuple, Optional, Callable

# ----------------------------
# utilidades internas
# ----------------------------
def _ensure_parent(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)

def _row_key(row: Dict[str, Any], key_fields: Iterable[str]) -> Tuple[str, ...]:
    out: List[str] = []
    for k in key_fields:
        v = row.get(k, "")
        out.append("" if v is None else str(v))
    return tuple(out)

class _FileLock:
    """Lock ingênuo baseado em arquivo .lock ao lado do CSV (best-effort)."""
    def __init__(self, path: str | Path, timeout_s: float = 30.0, poll_s: float = 0.1):
        self.lock_path = f"{str(path)}.lock"
        self.timeout_s = timeout_s
        self.poll_s = poll_s
        self.acquired = False

    def __enter__(self):
        start = time.time()
        while True:
            try:
                # os.O_EXCL garante exclusividade na criação
                fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                self.acquired = True
                return self
            except FileExistsError:
                if time.time() - start > self.timeout_s:
                    raise TimeoutError(f"Timeout aguardando lock: {self.lock_path}")
                time.sleep(self.poll_s)

    def __exit__(self, exc_type, exc, tb):
        if self.acquired and os.path.exists(self.lock_path):
            try:
                os.remove(self.lock_path)
            except FileNotFoundError:
                pass

# ----------------------------
# upsert principal
# ----------------------------
def upsert_csv(
    path: str | Path,
    rows: Iterable[Dict[str, Any]],
    key_fields: Iterable[str],
    *,
    # Se informado, congela o header nesta ordem (colunas fora do schema são ignoradas).
    schema: Optional[List[str]] = None,
    # Quando schema não for informado:
    # - True  -> permite adicionar novas colunas (união de colunas)
    # - False -> mantém exatamente o header atual do arquivo; ignora colunas novas
    allow_new_columns: bool = False,
    # Ordenar as linhas antes de gravar; ex.: lambda r: (r["seller_id"], r["campaign_id"], r["date"])
    sort_key: Optional[Callable[[Dict[str, Any]], Any]] = None,
    # Ativar lock e escrita atômica (tmp + rename)
    atomic: bool = True,
    lock_timeout_s: float = 30.0,
) -> str:
    """
    Upsert idempotente em um CSV fixo.

    - Usa key_fields como chave composta.
    - Se schema é passado, o header fica congelado nessa ordem.
    - Se schema é None:
        * Se o arquivo existe, o header atual é respeitado.
        * Se não existe:
            - allow_new_columns=False => header = chaves do primeiro row
            - allow_new_columns=True  => idem; nas próximas chamadas pode expandir.
    - Nunca cria um novo arquivo "com timestamp": sempre sobrescreve o arquivo alvo.
    """
    path = str(path)
    _ensure_parent(path)

    # lock (best-effort) para evitar escrita concorrente
    lock_ctx = _FileLock(path, timeout_s=lock_timeout_s) if atomic else None
    if lock_ctx:
        lock_ctx.__enter__()

    try:
        existing: List[Dict[str, Any]] = []
        header: List[str] = []
        idx: Dict[Tuple[str, ...], int] = {}

        # 1) leitura do arquivo atual (se existir)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8", newline="") as f:
                r = csv.DictReader(f)
                header = list(r.fieldnames or [])
                for i, row in enumerate(r):
                    row = dict(row)
                    existing.append(row)
                    idx[_row_key(row, key_fields)] = i

        # 2) definir header alvo
        if schema:
            header = list(schema)  # congela ordem
        else:
            if not header:
                # arquivo não existe (ou vazio): derive do 1º row
                rows_iter = iter(rows)
                boot_rows = list(rows_iter)
                if not boot_rows:
                    # nada a fazer
                    return path
                first = boot_rows[0]
                header = list(first.keys())
                # reusa a lista que já materializamos
                rows = boot_rows

            if allow_new_columns:
                # união de colunas novas (ordem: existentes + novas por chegada)
                seen = set(header)
                def _union_cols(cols: Iterable[str]):
                    nonlocal header, seen
                    for c in cols:
                        if c not in seen:
                            seen.add(c)
                            header.append(c)
            else:
                # trava no header atual (colunas fora serão ignoradas)
                def _union_cols(cols: Iterable[str]):  # no-op
                    return

        # 3) aplicar upserts
        incoming_list = list(rows)
        if not schema and allow_new_columns:
            # expande header conforme as linhas novas
            for row in incoming_list:
                _union_cols(row.keys())

        for row in incoming_list:
            k = _row_key(row, key_fields)
            if k in idx:
                # atualiza somente colunas conhecidas
                if schema or not allow_new_columns:
                    for c in header:
                        val = row.get(c, None)
                        if val not in ("", None):
                            existing[idx[k]][c] = val
                else:
                    # união já feita, pode atualizar tudo que estiver no header
                    for c in header:
                        val = row.get(c, None)
                        if val not in ("", None):
                            existing[idx[k]][c] = val
            else:
                # nova linha: respeita header
                new_row = {c: row.get(c, "") for c in header}
                idx[k] = len(existing)
                existing.append(new_row)

        # 4) ordenar (opcional)
        if sort_key:
            existing.sort(key=sort_key)

        # 5) escrita atômica
        if atomic:
            fd, tmp = tempfile.mkstemp(prefix=os.path.basename(path) + "_", suffix=".tmp")
            os.close(fd)
            with open(tmp, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=header)
                w.writeheader()
                for row in existing:
                    w.writerow({c: row.get(c, "") for c in header})
            shutil.move(tmp, path)
        else:
            with open(path, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=header)
                w.writeheader()
                for row in existing:
                    w.writerow({c: row.get(c, "") for c in header})

        return path

    finally:
        if lock_ctx:
            lock_ctx.__exit__(None, None, None)
