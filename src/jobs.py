from datetime import datetime
from .meli_client import meli_get, write_csv

def job_user_me():
    data = meli_get("/users/me")
    # Normaliza pra uma única linha
    row = {
        "user_id": data.get("id"),
        "nickname": data.get("nickname"),
        "registration_date": data.get("registration_date"),
        "country_id": data.get("country_id"),
        "permalink": data.get("permalink"),
        "status_site_status": (data.get("status") or {}).get("site_status"),
        "ts_local": datetime.now().isoformat(timespec="seconds"),
    }
    path = write_csv([row], "users_me")
    return path

# Exemplo de esqueleto para outro dataset:
def job_orders_recent(seller_id: str, date_from_iso: str | None = None):
    # Ajuste os params conforme sua necessidade (filtros por data/status)
    params = {"seller": seller_id}
    if date_from_iso:
        params["order.date_created.from"] = date_from_iso
    data = meli_get("/orders/search", params=params)
    results = data.get("results", [])
    rows = []
    for o in results:
        rows.append({
            "id": o.get("id"),
            "date_created": o.get("date_created"),
            "status": o.get("status"),
            "total_amount": o.get("total_amount"),
            "currency_id": o.get("currency_id"),
            "buyer_id": (o.get("buyer") or {}).get("id"),
        })
    path = write_csv(rows, "orders")
    return path
