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
    schema: Optional[List[str]] = None,
    allow_new_columns: bool = False,
    sort_key: Optional[Callable[[Dict[str, Any]], Any]] = None,
    atomic: bool = True,
    lock_timeout_s: float = 30.0,
    drop_fields: Optional[Iterable[str]] = None,      # novo
    strict_header: bool = False,                      # novo
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
            header = list(schema)
        else:
            if not header:
                rows_iter = iter(rows)
                boot_rows = list(rows_iter)
                if not boot_rows:
                    return path
                first = boot_rows[0]
                header = list(first.keys())
                rows = boot_rows

            if allow_new_columns:
                seen = set(header)
                def _union_cols(cols: Iterable[str]):
                    nonlocal header, seen
                    for c in cols:
                        if c not in seen:
                            seen.add(c)
                            header.append(c)
            else:
                def _union_cols(cols: Iterable[str]):
                    return

        # 3) aplicar upserts
        incoming_list = list(rows)
        if not schema and allow_new_columns:
            for row in incoming_list:
                _union_cols(row.keys())

        for row in incoming_list:
            k = _row_key(row, key_fields)
            if k in idx:
                for c in header:
                    val = row.get(c, None)
                    if val not in ("", None):
                        existing[idx[k]][c] = val
            else:
                new_row = {c: row.get(c, "") for c in header}
                idx[k] = len(existing)
                existing.append(new_row)

        # 4) drop_fields opcional
        if drop_fields:
            drop_fields = tuple(drop_fields)
            for row in existing:
                for f in drop_fields:
                    row.pop(f, None)

        # 5) header estrito
        if strict_header and not schema:
            keys = []
            seen = set()
            for row in existing:
                for k in row.keys():
                    if k not in seen:
                        seen.add(k)
                        keys.append(k)
            header = keys

        # 6) ordenar
        if sort_key:
            existing.sort(key=sort_key)

        # 7) escrita
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
