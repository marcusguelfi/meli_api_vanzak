# src/meli_client.py
import requests
from datetime import datetime, timezone
from pathlib import Path

from .auth import get_valid_token
from .config import RAW_DIR

BASE_API = "https://api.mercadolibre.com"

def _ensure_dirs():
    p = Path(RAW_DIR)
    if p.exists() and p.is_file():
        raise RuntimeError(f"'{RAW_DIR}' existe como ARQUIVO. Apague/renomeie para criar o diretório.")
    p.mkdir(parents=True, exist_ok=True)

def meli_get(path: str, params: dict | None = None):
    token = get_valid_token()
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(f"{BASE_API}{path}", headers=headers, params=params or {}, timeout=60)
    resp.raise_for_status()
    return resp.json()

def write_csv(rows: list[dict], name_prefix: str):
    import csv
    _ensure_dirs()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = Path(RAW_DIR) / f"{name_prefix}_{ts}.csv"
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp_utc"])
            writer.writerow([ts])
        return str(path)
    header = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return str(path)
