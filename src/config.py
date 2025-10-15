import os
from dotenv import load_dotenv

load_dotenv()

ML_CLIENT_ID = os.getenv("ML_CLIENT_ID")
ML_CLIENT_SECRET = os.getenv("ML_CLIENT_SECRET")
ML_REDIRECT_URI = os.getenv("ML_REDIRECT_URI")
ML_SITE_ID = os.getenv("ML_SITE_ID", "MLB")
ML_SELLER_ID = os.getenv("ML_SELLER_ID", "")
TOKEN_STORE_PATH = os.getenv("TOKEN_STORE_PATH", "token_store.json")
LOG_DIR = os.getenv("LOG_DIR", "data/logs")
RAW_DIR = os.getenv("RAW_DIR", "data/raw")
