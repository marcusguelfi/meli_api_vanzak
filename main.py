from meli_api import get_brands
from data_processing import filter_brands
from export_utils import export_to_excel

def main():
    data = get_brands(limit=100)
    filtered = filter_brands(data, min_ads=1000)
    export_to_excel(filtered)

if __name__ == "__main__":
    main()
