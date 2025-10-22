# src/uploader.py
from __future__ import annotations

import os
import glob
import logging
import argparse
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

import requests

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("uploader")

# -----------------------------------------------------------------------------
# Config via env
# -----------------------------------------------------------------------------
APPSCRIPT_URL   = os.getenv("GOOGLE_APPSCRIPT_URL", "").strip()
APPSCRIPT_TOKEN = os.getenv("GOOGLE_APPSCRIPT_TOKEN", "").strip()

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def _pick_latest(pattern: str) -> str | None:
    """Retorna o arquivo mais novo que bate no padrão (glob)."""
    matches = glob.glob(pattern)
    if not matches:
        return None
    matches.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return matches[0]

def _with_query(url: str, **qadd) -> str:
    """Adiciona/mescla querystring na URL."""
    parts = list(urlsplit(url))
    q = dict(parse_qsl(parts[3]))
    for k, v in qadd.items():
        if v is not None:
            q[k] = v
    parts[3] = urlencode(q)
    return urlunsplit(parts)

def upload_csv(path: str, sheet: str) -> None:
    if not APPSCRIPT_URL:
        log.error("GOOGLE_APPSCRIPT_URL não está configurado.")
        raise SystemExit(2)

    if not os.path.exists(path):
        log.error("Arquivo não encontrado: %s", path)
        raise SystemExit(2)

    url = _with_query(
        APPSCRIPT_URL,
        sheet=sheet,
        token=(APPSCRIPT_TOKEN or None),
        name=os.path.basename(path),
    )

    size = os.path.getsize(path)
    headers = {
        "Content-Type": "text/csv",
        "X-Filename": os.path.basename(path),
    }

    log.info("⬆️ Enviando %s (%s bytes) → %s", path, size, url)
    with open(path, "rb") as f:
        resp = requests.post(url, data=f.read(), headers=headers, timeout=180)

    try:
        resp.raise_for_status()
    except Exception:
        log.error("❌ Falha HTTP %s: %s", resp.status_code, resp.text[:500])
        raise

    log.info("✅ OK %s — aba '%s' atualizada. Resposta: %s",
             resp.status_code, sheet, resp.text.strip()[:160])

# -----------------------------------------------------------------------------
# Default mapping (padrões → aba)
# -----------------------------------------------------------------------------
DEFAULT_JOBS = [
    # summaries (arquivos únicos)
    ("data/processed/campaign_summary.csv", "campaign_summary"),
    ("data/processed/ads_summary.csv",       "ads_summary"),
    # diários (um por advertiser; escolhemos o mais novo)
    ("data/processed/campaign_daily_*.csv",  "campaign_daily"),
    ("data/processed/ads_daily_*.csv",       "ads_daily"),
]

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Envia CSVs para o Apps Script → Google Sheets (uma aba por dataset)."
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Envia todos os datasets padrão (summary + daily).",
    )
    parser.add_argument(
        "--file",
        action="append",
        metavar="PATH",
        help="Arquivo CSV específico para enviar (pode repetir).",
    )
    parser.add_argument(
        "--sheet",
        action="append",
        metavar="SHEET",
        help="Nome da aba para cada --file (mesma ordem).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Log de debug.",
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    jobs: list[tuple[str, str]] = []

    if args.all:
        for pattern, sheet in DEFAULT_JOBS:
            path = pattern
            if "*" in pattern or "?" in pattern:
                latest = _pick_latest(pattern)
                if latest:
                    path = latest
                else:
                    log.warning("⚠️ Não há arquivos para '%s' — pulando.", pattern)
                    continue
            jobs.append((path, sheet))

    if args.file:
        if not args.sheet or len(args.sheet) != len(args.file):
            parser.error("Ao usar --file, informe --sheet na mesma quantidade/ordem.")
        for p, s in zip(args.file, args.sheet):
            jobs.append((p, s))

    if not jobs:
        parser.print_help()
        print("\nExemplos:")
        print("  python -m src.uploader --all")
        print("  python -m src.uploader --file data/processed/ads_daily_731958.csv --sheet ads_daily")
        raise SystemExit(1)

    for path, sheet in jobs:
        upload_csv(path, sheet)

if __name__ == "__main__":
    main()
