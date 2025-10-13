import pandas as pd

def filter_brands(data, min_ads=None):
    df = pd.DataFrame(data)
    if min_ads:
        df = df[df["ads_count"] >= min_ads]
    return df
