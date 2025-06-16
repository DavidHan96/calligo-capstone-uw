# macro_model_s3.py
import pandas as pd, numpy as np
from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from .merge_fred_files import build_merged_macro  

def build_forecast(currency_code: str, horizon=14) -> pd.DataFrame:
    df = build_merged_macro(currency_code)
    df["observation_date"] = pd.to_datetime(df["observation_date"])
    currency_col = "currency_value"

    # ---------------- feature engineering ----------------
    df[f"{currency_col}_ma7"]        = df[currency_col].rolling(7).mean().shift(1)
    df[f"{currency_col}_return1"]    = df[currency_col].pct_change(fill_method=None).shift(1)
    df[f"{currency_col}_volatility7"] = df[currency_col].rolling(7).std().shift(1)

    if "sp500"      in df: df["sp500_return1"]   = df["sp500"].pct_change(fill_method=None).shift(1)
    if "prime_rate" in df: df["prime_rate_delta1"] = df["prime_rate"].diff().shift(1)

    lag_cols = ['prime_rate','usd_index','sp500','sofr_30d_avg','us_10y_yield',currency_col]
    for col in lag_cols:
        if col in df: df[f"{col}_lag1"] = df[col].shift(1)

    df.dropna(inplace=True)
    df[f"{currency_col}_tomorrow"] = df[currency_col].shift(-1)
    df["direction"] = (df[f"{currency_col}_tomorrow"] > df[currency_col]).astype(int)
    df.dropna(inplace=True)

    # ---------------- model training ----------------
    feature_cols = [c for c in df if any(k in c for k in ["_lag1","_ma","_return","_volatility","_delta"])]
    X, y_reg, y_clf = df[feature_cols], df[currency_col], df["direction"]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    reg  = Ridge(alpha=1.0).fit(X_scaled, y_reg)
    clf  = RandomForestClassifier(n_estimators=100, random_state=42).fit(X, y_clf)

    # ---------------- forecasting loop ----------------
    def forecast(df_hist):
        last = df_hist.copy()
        results = []
        base = last.iloc[-1][currency_col]

        results.append({
            "date": last.iloc[-1]["observation_date"],
            "predicted_value": base,
            "predicted_direction": "Today",
            "confidence": None,
        })

        feature_cols = [c for c in last if any(k in c for k in ["_lag1","_ma","_return","_volatility","_delta"])]
        for _ in range(horizon):
            row = last.iloc[-1]
            X_future = pd.DataFrame([{c: row[c] for c in feature_cols}])
            pred_val = reg.predict(scaler.transform(X_future))[0]
            pred_dir = "Up" if pred_val > base else "Down"
            prob     = clf.predict_proba(X_future)[0][1]

            next_date = row["observation_date"] + pd.Timedelta(days=1)
            results.append({
                "date": next_date,
                "predicted_value": pred_val,
                "predicted_direction": pred_dir,
                "confidence": round(100*prob,2),
            })

            # extend history for next iteration
            new_row = row.copy()
            new_row["observation_date"] = next_date
            new_row[currency_col] = pred_val
            last = pd.concat([last, pd.DataFrame([new_row])], ignore_index=True)

        return pd.DataFrame(results)

    return forecast(df)

# Expose dataframe so run_any() can grab it
forecast_df = build_forecast("usd_krw")
df = forecast_df