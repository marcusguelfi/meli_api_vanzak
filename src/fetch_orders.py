import os, json, time, logging
import pandas as pd
import requests
from datetime import datetime, timezone
from pathlib import Path

# Configurações
SELLER_ID   = os.getenv("MELI_SELLER_ID", "731958")   # pode fixar aqui se quiser
ACCESS_TOKEN = os.getenv("MELI_ACCESS_TOKEN")          # use variável de ambiente para segurança
LIMIT       = 50                                       # máximo por página
OUT_DIR     = "data/raw"

log = logging.getLogger("fetch_orders")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def _meli_request(url, params=None):
    """Executa requisição autenticada à API do Mercado Livre."""
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    r = requests.get(url, headers=headers, params=params)
    if r.status_code != 200:
        log.error("Erro %d em %s: %s", r.status_code, url, r.text)
        return None
    return r.json()


def fetch_orders(limit=LIMIT):
    """Busca todas as orders recentes e salva em CSV no diretório raw."""
    if not ACCESS_TOKEN:
        raise EnvironmentError("⚠️ Variável de ambiente MELI_ACCESS_TOKEN não definida.")

    base_url = f"https://api.mercadolibre.com/orders/search"
    params = {
        "seller": SELLER_ID,
        "sort": "date_desc",
        "limit": limit,
        "offset": 0
    }

    all_orders = []
    log.info("Iniciando busca de pedidos para seller_id=%s", SELLER_ID)

    while True:
        data = _meli_request(base_url, params)
        if not data or "results" not in data:
            break

        results = data["results"]
        if not results:
            break

        all_orders.extend(results)
        log.info("→ Página offset=%d | Total acumulado: %d", params["offset"], len(all_orders))

        if len(results) < limit:
            break  # fim da paginação
        params["offset"] += limit
        time.sleep(0.5)

    if not all_orders:
        log.warning("Nenhum pedido retornado.")
        return None

    # Normaliza estrutura JSON aninhada em colunas planas
    df = pd.json_normalize(all_orders)

    # Colunas principais e renomeadas para compatibilidade com build_orders_daily.py
    rename_cols = {
        "id": "id",
        "date_created": "date_created",
        "status": "status",
        "total_amount": "total_amount",
        "currency_id": "currency_id",
        "buyer.id": "buyer_id",
        "order_items[0].item.id": "item_id",
        "order_items[0].item.title": "item_title",
        "order_items[0].item.category_id": "category_name"
    }

    cols_exist = [c for c in rename_cols if c in df.columns]
    df = df[cols_exist].rename(columns=rename_cols)

    # Salva CSV bruto com timestamp
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
    out_path = f"{OUT_DIR}/orders_{ts}.csv"
    df.to_csv(out_path, index=False)

    log.info("✅ CSV salvo em: %s (linhas: %d)", out_path, len(df))
    return out_path


if __name__ == "__main__":
    path = fetch_orders()
    if path:
        log.info("Execução concluída. Pronto para rodar build_orders_daily.py.")
