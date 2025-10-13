import requests
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.mercadolibre.com"

def get_brands(limit=50):
    headers = {"Authorization": f"Bearer {os.getenv('MELI_ACCESS_TOKEN')}"}
    response = requests.get(f"{BASE_URL}/brands/ads", headers=headers, params={"limit": limit})
    response.raise_for_status()
    return response.json()
