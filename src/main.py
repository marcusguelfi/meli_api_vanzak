# src/main.py
import argparse
import logging
from src.auth import (
    exchange_code_for_token,
    get_auth_url,
    refresh_access_token,
    get_access_token,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def main():
    parser = argparse.ArgumentParser(description="Mercado Livre API Client CLI")
    parser.add_argument("--auth-url", action="store_true", help="Mostra a URL de autorização OAuth")
    parser.add_argument("--auth-code", type=str, help="Troca um authorization_code por tokens")
    parser.add_argument("--refresh", action="store_true", help="Força refresh do token")
    args = parser.parse_args()

    if args.auth_url:
        print("🔗 URL de autorização:")
        print(get_auth_url())
        return

    if args.auth_code:
        exchange_code_for_token(args.auth_code)
        print("✅ Tokens salvos em src/tokens.json")
        return

    if args.refresh:
        tokens = refresh_access_token()
        print("🔄 Novo token:", tokens.get("access_token")[:30], "...")
        return

    # Teste simples de token atual
    access_token = get_access_token()
    print("🔑 Access token ativo:", access_token[:30], "...")


if __name__ == "__main__":
    main()
