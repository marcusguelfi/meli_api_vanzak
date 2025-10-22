# src/make_fresh_csv.py
from __future__ import annotations

import os
import shutil
from datetime import date, timedelta

from . import jobs  # reaproveita os jobs existentes


def _last_n_days(n: int) -> tuple[str, str]:
    n = max(1, min(n, 90))  # API limita 90 dias
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=n - 1)
    return start.isoformat(), end.isoformat()


def main():
    adv = os.getenv("ADVERTISER_ID", "").strip()
    site = os.getenv("SITE_ID", "MLB").strip()
    days = int(os.getenv("DAYS", "90"))
    kind = (os.getenv("KIND", "ads").strip().lower())  # "ads" ou "campaigns"
    out_csv = os.getenv("NEW_CSV", "").strip()

    if not adv:
        raise SystemExit("Defina ADVERTISER_ID no ambiente.")

    # Desliga upload pro Sheets só neste script
    jobs.APPSCRIPT_URL = ""

    df, dt = _last_n_days(days)

    # Garante dimensões atualizadas
    jobs.job_campaigns_summary(adv, site)
    jobs.job_ads_summary(adv, site)

    if kind == "ads":
        src_path = jobs.job_ads_daily(adv, site, df, dt)
        default_out = f"data/processed/ads_daily_fresh_{adv}.csv"
    elif kind == "campaigns":
        src_path = jobs.job_campaigns_daily(adv, site, df, dt)
        default_out = f"data/processed/campaign_daily_fresh_{adv}.csv"
    else:
        raise SystemExit('KIND precisa ser "ads" ou "campaigns"')

    final_out = out_csv or default_out
    os.makedirs(os.path.dirname(final_out), exist_ok=True)
    shutil.copyfile(src_path, final_out)

    print(f"✅ CSV novo gerado: {final_out}")
    print(f"   (origem: {src_path} | período: {df} → {dt})")


if __name__ == "__main__":
    main()
