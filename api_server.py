from fastapi import FastAPI
from meli_api import get_brands
from data_processing import filter_brands

app = FastAPI()

@app.get("/brands")
def brands(limit: int = 50, min_ads: int = 0):
    data = get_brands(limit)
    df = filter_brands(data, min_ads)
    return df.to_dict(orient="records")
