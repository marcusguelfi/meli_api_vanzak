# src/scheduler.py
from __future__ import annotations

import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

from .jobs import (
    job_user_me,
    job_orders_recent,
    job_get_advertiser,
    job_campaigns_summary,
    job_campaigns_daily,
    job_ads_summary,
    job_ads_daily,
)
from .cleanup_snapshots_raw import cleanup_noninteractive
from .backfill import backfill_ads_daily, backfill_campaigns_daily

# =============================================================================
# LOGGING (console + arquivo diário em logs/scheduler_YYYYMMDD.log)
# =============================================================================
def setup_logging() -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    log_fmt = "%Y-%m-%d %H:%M:%S | %(levelname)s | %(message)s"
    date_tag = datetime.now().strftime("%Y%m%d")
    log_file = os.path.join("logs", f"scheduler_{date_tag}.log")

    logger = logging.getLogger("scheduler")
    logger.setLevel(logging.INFO)

    # Evita handlers duplicados em re-imports
    if logger.handlers:
        return logger

    # Console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(log_fmt))

    # Arquivo (rotating por tamanho para evitar arquivos gigantes; 5 MB x 5 backups)
    fh = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter(log_fmt))

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


logger = setup_logging()

# =============================================================================
# CONFIG VIA .env (ou variáveis do ambiente)
# =============================================================================
RUN_ORDERS_JOB = os.getenv("RUN_ORDERS_JOB", "false").lower() in ("1", "true", "yes")

ADVERTISER_ID = os.getenv("ADVERTISER_ID", "731958")
SITE_ID = os.getenv("SITE_ID", "MLB")
DATE_FROM = os.getenv("DATE_FROM", "2025-10-01")
DATE_TO = os.getenv("DATE_TO", "2025-10-15")

# Limpeza automática do data/raw
ENABLE_RAW_CLEANUP = os.getenv("ENABLE_RAW_CLEANUP", "true").lower() in ("1", "true", "yes")
RAW_KEEP_LAST = int(os.getenv("RAW_KEEP_LAST", "1"))
RAW_PATTERNS = [p.strip() for p in os.getenv("RAW_PATTERNS", "users_me_*.csv,orders_*.csv").split(",") if p.strip()]

# Backfill na primeira execução
BACKFILL_ON_START = os.getenv("BACKFILL_ON_START", "false").lower() in ("1", "true", "yes")
BACKFILL_SINCE = os.getenv("BACKFILL_SINCE", "")
BACKFILL_CHUNK_DAYS = int(os.getenv("BACKFILL_CHUNK_DAYS", "30"))

# =============================================================================
# CICLO DE JOBS
# =============================================================================
def run_all_jobs():
    logger.info("🚀 Iniciando ciclo de atualização...")

    # 1) Backfill (opcional – roda só no primeiro ciclo)
    if BACKFILL_ON_START and BACKFILL_SINCE:
        logger.info(f"⏪ BACKFILL: since={BACKFILL_SINCE} | chunk={BACKFILL_CHUNK_DAYS}d")
        try:
            backfill_campaigns_daily(ADVERTISER_ID, SITE_ID, BACKFILL_SINCE, chunk_days=BACKFILL_CHUNK_DAYS)
            backfill_ads_daily(ADVERTISER_ID, SITE_ID, BACKFILL_SINCE, chunk_days=BACKFILL_CHUNK_DAYS)
        except Exception as e:
            logger.exception(f"⚠️ Falha no backfill: {e}")
        # desarma p/ ciclos futuros
        os.environ["BACKFILL_ON_START"] = "false"

    # 2) Perfil do usuário
    try:
        path_me = job_user_me()
        logger.info(f"✔ users_me → {path_me}")
    except Exception as e:
        logger.exception(f"⚠️ Erro em job_user_me: {e}")

    # 3) Pedidos recentes (opcional)
    if RUN_ORDERS_JOB:
        try:
            job_orders_recent(seller_id=ADVERTISER_ID)
        except Exception as e:
            logger.exception(f"❌ Erro em job_orders_recent: {e}")

    # 4) Advertiser
    try:
        job_get_advertiser()
    except Exception as e:
        logger.exception(f"⚠️ Erro em job_get_advertiser: {e}")

    # 5) Campaigns
    try:
        job_campaigns_summary(ADVERTISER_ID, SITE_ID, DATE_FROM, DATE_TO)
        job_campaigns_daily(ADVERTISER_ID, SITE_ID, DATE_FROM, DATE_TO)
    except Exception as e:
        logger.exception(f"⚠️ Erro nos jobs de campanhas: {e}")

    # 6) Ads
    try:
        job_ads_summary(ADVERTISER_ID, SITE_ID, DATE_FROM, DATE_TO)
        job_ads_daily(ADVERTISER_ID, SITE_ID, DATE_FROM, DATE_TO)
    except Exception as e:
        logger.exception(f"⚠️ Erro nos jobs de anúncios: {e}")

    # 7) Limpeza de snapshots em data/raw
    if ENABLE_RAW_CLEANUP:
        try:
            removed = cleanup_noninteractive(keep_last=RAW_KEEP_LAST, patterns=RAW_PATTERNS)
            logger.info(f"🧹 RAW cleanup: removidos {removed} arquivo(s) (keep_last={RAW_KEEP_LAST})")
        except Exception as e:
            logger.exception(f"⚠️ Falha na limpeza automática: {e}")

    logger.info("✅ Ciclo concluído.\n")

# =============================================================================
# SCHEDULER
# =============================================================================
def main():
    logger.info("🕒 Scheduler iniciado — executando jobs a cada 1h.")
    run_all_jobs()  # roda imediatamente

    scheduler = BlockingScheduler(timezone="America/Sao_Paulo")
    scheduler.add_job(run_all_jobs, "interval", hours=1, next_run_time=None)
    scheduler.start()

if __name__ == "__main__":
    main()
