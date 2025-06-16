import pandas as pd
from functools import reduce
from fetch_fred_data import fetch_csv_from_s3

# Defined S3 keys and columns
kpi_files = {
    'CPIAUCSL.csv': 'cpi',
    'GDP.csv': 'gdp',
    'HOUST.csv': 'housing_starts',
    'M2REAL.csv': 'm2_money_supply',
    'MORTGAGE30US.csv': 'mortgage_rate',
    'PERMIT.csv': 'building_permits',
    'PPIACO.csv': 'ppi',
    'T10Y2Y.csv': 'yield_curve',
    'TLRESCONS.csv': 'construction_spending',
    'UNRATE.csv': 'unemployment_rate',
    'WPU0851.csv': 'lumber_price'
}

def load_and_merge_data():
    dfs = []
    for key, col_name in kpi_files.items():
        df = fetch_csv_from_s3(key)
        df.columns = ['date', col_name]
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').resample('W').ffill().reset_index()
        dfs.append(df)
    merged_df = reduce(lambda left, right: pd.merge(left, right, on='date', how='inner'), dfs)
    return merged_df.sort_values('date').reset_index(drop=True)
