import argparse, logging, os
from logging.handlers import RotatingFileHandler
from apscheduler.schedulers.blocking import BlockingScheduler
import pytz

from .auth import build_authorize_url, exchange_code_for_token
from .jobs import job_user_me, job_orders_recent
from .config import LOG_DIR, ML_SELLER_ID

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, "pipeline.log")
    handler = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[handler, logging.StreamHandler()])

def run_once():
    p1 = job_user_me()
    logging.info(f"users_me CSV -> {p1}")
    if ML_SELLER_ID:
        p2 = job_orders_recent(ML_SELLER_ID)
        logging.info(f"orders CSV -> {p2}")
    else:
        logging.info("ML_SELLER_ID não definido; pulando job de orders.")

def schedule_hourly():
    tz = pytz.timezone("America/Sao_Paulo")
    sched = BlockingScheduler(timezone=tz)
    # executa já e depois a cada hora
    run_once()
    sched.add_job(run_once, "cron", minute=0)  # minuto 0 de toda hora
    logging.info("Agendado para rodar de hora em hora (minuto 0).")
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Encerrando scheduler.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--print-auth-url", action="store_true",
                        help="Mostra a URL de autorização OAuth.")
    parser.add_argument("--auth-code", type=str,
                        help="Troca o authorization code por tokens e salva localmente.")
    parser.add_argument("--run-once", action="store_true",
                        help="Executa uma coleta única (sem scheduler).")
    parser.add_argument("--schedule-hourly", action="store_true",
                        help="Roda continuamente com agendamento horário.")
    args = parser.parse_args()

    setup_logging()

    if args.print_auth_url:
        url = build_authorize_url()
        print("\nAutorize aqui:\n", url, "\n")
        return

    if args.auth_code:
        tokens = exchange_code_for_token(args.auth_code)
        logging.info(f"Tokens salvos. user_id={tokens.get('user_id')}")
        return

    if args.run_once:
        run_once()
        return

    if args.schedule_hourly:
        schedule_hourly()
        return

    parser.print_help()

if __name__ == "__main__":
    main()
