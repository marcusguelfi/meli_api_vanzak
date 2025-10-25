# src/make_fresh_csv.py
from __future__ import annotations

import os
import shutil
import argparse
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

from . import jobs  # reaproveita os jobs existentes

MAX_DAYS = 90

def _last_n_days(n: int) -> tuple[str, str]:
    n = max(1, min(n, MAX_DAYS))
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=n - 1)
    return start.isoformat(), end.isoformat()

@contextmanager
def _no_upload_to_sheets():
    old = jobs.APPSCRIPT_URL
    try:
        jobs.APPSCRIPT_URL = ""  # desliga upload só durante o with
        yield
    finally:
        jobs.APPSCRIPT_URL = old

def _ensure_parent(p: str | Path) -> None:
    Path(p).parent.mkdir(parents=True, exist_ok=True)

def main():
    parser = argparse.ArgumentParser(
        description="Gera um CSV 'fresh' (ads ou campaigns) sem subir para o Sheets."
    )
    parser.add_argument("--advertiser", default=os.getenv("ADVERTISER_ID", "").strip(), required=False)
    parser.add_argument("--site", default=os.getenv("SITE_ID", "MLB").strip())
    parser.add_argument("--days", type=int, default=int(os.getenv("DAYS", "90")))
    parser.add_argument("--kind", choices=("ads", "campaigns"), default=os.getenv("KIND", "ads").strip().lower())
    parser.add_argument("--out", dest="out_csv", default=os.getenv("NEW_CSV", "").strip())
    args = parser.parse_args()

    adv = args.advertiser
    if not adv:
        raise SystemExit("Defina --advertiser ou ADVERTISER_ID no ambiente.")

    days = max(1, min(args.days, MAX_DAYS))
    df, dt = _last_n_days(days)

    with _no_upload_to_sheets():
        # garante dimensões atualizadas antes do daily
        jobs.job_campaigns_summary(adv, args.site)
        jobs.job_ads_summary(adv, args.site)

        if args.kind == "ads":
            src_path = jobs.job_ads_daily(adv, args.site, df, dt)
            default_out = f"data/processed/ads_daily_fresh_{adv}.csv"
        else:
            src_path = jobs.job_campaigns_daily(adv, args.site, df, dt)
            default_out = f"data/processed/campaign_daily_fresh_{adv}.csv"

    final_out = args.out_csv or default_out
    _ensure_parent(final_out)

    # se destino for o mesmo arquivo, não duplicar
    if os.path.abspath(final_out) != os.path.abspath(src_path):
        tmp = f"{final_out}.tmp"
        shutil.copyfile(src_path, tmp)
        os.replace(tmp, final_out)  # cópia atômica
    else:
        final_out = src_path  # já está no lugar

    print(f"✅ CSV novo gerado: {final_out}")
    print(f"   (origem: {src_path} | período: {df} → {dt} | dias={days})")

if __name__ == "__main__":
    main()
