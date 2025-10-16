import argparse
import logging
import sys
from datetime import datetime

# Importações locais
from src.auth import (
    get_auth_url,
    exchange_code_for_token,
    refresh_token,
    get_access_token,
)
from src.jobs import (
    job_get_advertiser,
    job_campaigns_summary,
    job_campaigns_daily,
    job_ads_summary,
    job_ads_daily,
)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ==============================================================
# CLI PRINCIPAL
# ==============================================================

def main():
    parser = argparse.ArgumentParser(
        description="🚀 Cliente de integração com a API de Product Ads do Mercado Livre."
    )

    parser.add_argument("--auth-url", action="store_true", help="Gera a URL de autenticação para obter o authorization_code.")
    parser.add_argument("--auth-code", type=str, help="Troca o authorization_code por tokens e salva localmente.")
    parser.add_argument("--refresh", action="store_true", help="Atualiza o access_token usando o refresh_token.")
    parser.add_argument("--job", type=str, help="Executa um job específico (ex: advertiser, campaigns_summary, ads_daily).")
    parser.add_argument("--advertiser-id", type=str, default="731958", help="ID do advertiser (padrão: 731958).")
    parser.add_argument("--site-id", type=str, default="MLB", help="Site ID (padrão: MLB).")
    parser.add_argument("--from-date", type=str, default="2025-10-01", help="Data inicial no formato YYYY-MM-DD.")
    parser.add_argument("--to-date", type=str, default="2025-10-15", help="Data final no formato YYYY-MM-DD.")

    args = parser.parse_args()

    # ==========================================================
    # 1️⃣ Geração do link de autenticação
    # ==========================================================
    if args.auth_url:
        url = get_auth_url()
        print(f"\n🔗 Acesse este link para autorizar o app:\n{url}\n")
        return

    # ==========================================================
    # 2️⃣ Troca de authorization_code por tokens
    # ==========================================================
    if args.auth_code:
        logging.info("🔐 Solicitando troca de authorization_code por tokens...")
        try:
            tokens = exchange_code_for_token(args.auth_code)
            logging.info(f"✅ Tokens obtidos e salvos com sucesso! user_id={tokens.get('user_id')}")
        except Exception as e:
            logging.error(f"❌ Falha ao trocar código: {e}")
        return

    # ==========================================================
    # 3️⃣ Atualizar token manualmente
    # ==========================================================
    if args.refresh:
        logging.info("🔄 Atualizando access_token manualmente...")
        try:
            new_tokens = refresh_token()
            logging.info("✅ Token atualizado com sucesso!")
            logging.info(f"Novo access_token: {new_tokens.get('access_token')[:20]}...")
        except Exception as e:
            logging.error(f"❌ Falha ao atualizar token: {e}")
        return

    # ==========================================================
    # 4️⃣ Executar um job
    # ==========================================================
    if args.job:
        job = args.job.lower()
        advertiser_id = args.advertiser_id
        site_id = args.site_id
        date_from = args.from_date
        date_to = args.to_date

        logging.info(f"🚀 Executando job '{job}' ({site_id}/{advertiser_id}) de {date_from} a {date_to}")

        try:
            if job == "advertiser":
                job_get_advertiser()
            elif job == "campaigns_summary":
                job_campaigns_summary(advertiser_id, site_id, date_from, date_to)
            elif job == "campaigns_daily":
                job_campaigns_daily(advertiser_id, site_id, date_from, date_to)
            elif job == "ads_summary":
                job_ads_summary(advertiser_id, site_id, date_from, date_to)
            elif job == "ads_daily":
                job_ads_daily(advertiser_id, site_id, date_from, date_to)
            else:
                logging.error(f"❌ Job '{job}' não reconhecido.")
        except Exception as e:
            logging.error(f"❌ Erro ao executar job '{job}': {e}")
        return

    # ==========================================================
    # 5️⃣ Nenhum argumento → mostra ajuda
    # ==========================================================
    parser.print_help()


# ==============================================================
# ENTRYPOINT
# ==============================================================

if __name__ == "__main__":
    logging.info("🚀 Iniciando cliente Mercado Livre Product Ads")
    main()
