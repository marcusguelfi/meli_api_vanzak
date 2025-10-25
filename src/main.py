# src/main.py
import argparse
import json
import logging
import sys
from src.auth import (
    exchange_code_for_token,
    get_auth_url,
    refresh_access_token,
    get_access_token,
    # opcional: TOKENS_PATH  # se expuserem no auth
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def _mask(token: str, keep: int = 6) -> str:
    if not token:
        return "<empty>"
    return token[:keep] + "…" + f"({len(token)} chars)"

def main():
    parser = argparse.ArgumentParser(description="Mercado Livre API Client CLI")
    g = parser.add_mutually_exclusive_group(required=False)
    g.add_argument("--auth-url", action="store_true", help="Mostra a URL de autorização OAuth")
    g.add_argument("--auth-code", type=str, metavar="CODE", help="Troca um authorization_code por tokens")
    g.add_argument("--refresh", action="store_true", help="Força refresh do token")
    parser.add_argument("--json", action="store_true", help="Saída em JSON (para automação)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Log de debug")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        if args.auth_url:
            url = get_auth_url()
            if args.json:
                print(json.dumps({"auth_url": url}))
            else:
                print("🔗 URL de autorização:")
                print(url)
            return 0

        if args.auth_code:
            tokens = exchange_code_for_token(args.auth_code)
            masked = _mask(tokens.get("access_token", ""))
            out = {"ok": True, "access_token": masked}
            if args.json:
                print(json.dumps(out))
            else:
                # print(f"✅ Tokens salvos em {TOKENS_PATH}")  # se tiver
                print("✅ Tokens salvos.")
                print("🔑 Access token:", masked)
            return 0

        if args.refresh:
            tokens = refresh_access_token()
            masked = _mask(tokens.get("access_token", ""))
            out = {"ok": True, "access_token": masked}
            if args.json:
                print(json.dumps(out))
            else:
                print("🔄 Novo token:", masked)
            return 0

        # default: status do token atual
        access_token = get_access_token()
        masked = _mask(access_token)
        if args.json:
            print(json.dumps({"ok": True, "access_token": masked}))
        else:
            print("🔑 Access token ativo:", masked)
        return 0

    except KeyboardInterrupt:
        return 130
    except Exception as e:
        logging.error("❌ Erro: %s", e)
        if args.json:
            print(json.dumps({"ok": False, "error": str(e)}))
        return 1

if __name__ == "__main__":
    sys.exit(main())
