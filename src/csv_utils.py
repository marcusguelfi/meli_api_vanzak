# src/csv_utils.py
from __future__ import annotations
import csv
import os
from pathlib import Path
from typing import Dict, Any, Iterable, List, Tuple

def _ensure_parent(path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

def _row_key(row: Dict[str, Any], key_fields: Iterable[str]) -> Tuple[str, ...]:
    out: List[str] = []
    for k in key_fields:
        v = row.get(k, "")
        out.append("" if v is None else str(v))
    return tuple(out)

def upsert_csv(path: str | Path, rows: Iterable[Dict[str, Any]], key_fields: Iterable[str]) -> str:
    """
    Upsert (insere/atualiza) linhas num CSV existente, usando key_fields como chave composta.
    Se não existir, cria com header = união de colunas novas + antigas.
    """
    path = str(path)
    _ensure_parent(path)

    existing: List[Dict[str, Any]] = []
    header: List[str] = []
    idx: Dict[Tuple[str, ...], int] = {}

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            header = list(r.fieldnames or [])
            for i, row in enumerate(r):
                row = dict(row)
                existing.append(row)
                idx[_row_key(row, key_fields)] = i

    # união de colunas
    def add_headers(new_cols: Iterable[str]) -> None:
        nonlocal header
        for c in new_cols:
            if c not in header:
                header.append(c)

    # aplicar upserts
    for row in rows:
        add_headers(row.keys())
        k = _row_key(row, key_fields)
        if k in idx:
            existing[idx[k]].update(row)
        else:
            idx[k] = len(existing)
            existing.append({**row})

    # escreve
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in existing:
            w.writerow({c: row.get(c, "") for c in header})

    return path
