import pandas as pd

def export_to_excel(df, filename="brands.xlsx"):
    df.to_excel(filename, index=False)
    print(f"Arquivo salvo em {filename}")