import io, boto3, pandas as pd
from functools import reduce

aws_access_key_id = "YOUR_KEY_ID"
aws_secret_access_key = "YOUR_ACCESS_KEY"
endpoint_url = "YOUR_ENDPOINT"
bucket_name = "fred"

SERIES = {
    "usd_krw":      ("DEXKOUS.csv",      "DEXKOUS"),
    "usd_china":    ("DEXCHUS.csv",      "DEXCHUS"),
    "usd_uk":       ("DEXUSUK.csv",      "DEXUSUK"),
    "prime_rate":   ("DPRIME.csv",       "DPRIME"),
    "sp500":        ("SP500.csv",        "SP500"),
    "usd_index":    ("DTWEXBGS.csv",     "DTWEXBGS"),
    "sofr_30d_avg": ("SOFR30DAYAVG.csv", "SOFR30DAYAVG"),
    "us_10y_yield": ("DGS10.csv",        "DGS10"),
    "crude_oil":    ("DCOILWTICO.csv",   "DCOILWTICO"),
}

s3 = boto3.resource(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    endpoint_url=endpoint_url,
)
bucket = s3.Bucket(bucket_name)

def find_date_col(df: pd.DataFrame) -> str:
    for cand in ("observation_date", "date"):
        for c in df.columns:
            if c.lower() == cand:
                return c
    raise ValueError("No date column found in DataFrame")

def tidy_series_df(df: pd.DataFrame, fred_col: str, new_name: str) -> pd.DataFrame:

    df.columns = df.columns.str.strip().str.lower()

    # locate date column
    date_col = next(c for c in df.columns if c in ("observation_date", "date"))

    # locate value column
    val_col = None
    if fred_col.lower() in df.columns:
        val_col = fred_col.lower()
    elif "value" in df.columns:
        val_col = "value"
    else:                                     
        val_col = next(c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]))

    out = (
        df[[date_col, val_col]]
        .rename(columns={date_col: "observation_date", val_col: new_name})
    )

    out[new_name] = pd.to_numeric(out[new_name], errors="coerce")
    return out.dropna()



def s3_csv_to_df(key: str) -> pd.DataFrame:

    body = bucket.Object(key).get()["Body"].read()
    return pd.read_csv(io.BytesIO(body))


def build_merged_macro(currency_code: str) -> pd.DataFrame:
  
    currency_code = currency_code.lower()
    if currency_code not in SERIES:
        raise ValueError(f"{currency_code} not found. Pick one of {list(SERIES)}")

    macro_vars = [k for k in SERIES if not k.startswith("usd_")]
    dfs = []

    fname, fred_col = SERIES[currency_code]
    df_currency = s3_csv_to_df(f"observations/{fname}")
    date_col = find_date_col(df_currency)
    df_currency = tidy_series_df(df_currency,fred_col, "currency_value")
    dfs.append(df_currency)

    for var in macro_vars:
        fname, fred_col = SERIES[var]
        try:
            df = s3_csv_to_df(f"observations/{fname}")
            df = tidy_series_df(df, fred_col, var)
            dfs.append(df)
        except Exception:
            print(f"Skipping {fname} (not found)")  

    df_merged = reduce(
        lambda l, r: pd.merge(l, r, on="observation_date", how="outer"), dfs
    ).sort_values("observation_date", ignore_index=True)
    df_merged["currency_code"] = currency_code
    return df_merged

print('hi')
