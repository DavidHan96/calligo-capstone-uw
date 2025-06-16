import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

# 1. LOAD DATA
df = pd.read_csv("data/merged_macro_data.csv", parse_dates=["observation_date"])
df = df[df["observation_date"] <= df["observation_date"].max()].sort_values("observation_date").reset_index(drop=True)

currency_col = "currency_value"

# 2. FEATURE ENGINEERING (with checks for missing columns)
df[f"{currency_col}_ma7"] = df[currency_col].rolling(7).mean().shift(1)
df[f"{currency_col}_return1"] = df[currency_col].pct_change(fill_method=None).shift(1)
df[f"{currency_col}_volatility7"] = df[currency_col].rolling(7).std().shift(1)
if "sp500" in df.columns:
    df["sp500_return1"] = df["sp500"].pct_change(fill_method=None).shift(1)
if "prime_rate" in df.columns:
    df["prime_rate_delta1"] = df["prime_rate"].diff().shift(1)

lag_cols = ['prime_rate', 'usd_index', 'sp500', 'sofr_30d_avg', 'us_10y_yield', currency_col]
for col in lag_cols:
    if col in df.columns:
        df[f"{col}_lag1"] = df[col].shift(1)

df.dropna(inplace=True)

# 3. BUILD TARGET (direction: tomorrow's value vs today's value)
df[f"{currency_col}_tomorrow"] = df[currency_col].shift(-1)
df["direction"] = (df[f"{currency_col}_tomorrow"] > df[currency_col]).astype(int)
df = df.dropna()

# 4. DEFINE FEATURES & SCALE
feature_cols = [col for col in df.columns if any(k in col for k in ["_lag1", "_ma", "_return", "_volatility", "_delta"])]
X = df[feature_cols]
y_reg = df[currency_col]
y_clf = df["direction"]

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

reg = Ridge(alpha=1.0).fit(X_scaled, y_reg)
clf = RandomForestClassifier(n_estimators=100, random_state=42).fit(X, y_clf)

# 5. FORECAST FUNCTION — **now always compares to start value**
def forecast_next_n_days(df, reg, clf, scaler, n_days=14, currency_col="currency_value"):
    last_known = df.copy()
    feature_cols = [col for col in last_known.columns if any(k in col for k in ["_lag1", "_ma", "_return", "_volatility", "_delta"])]
    results = []
    today_row = last_known.iloc[-1]
    base_value = today_row[currency_col]   # This is the value ALL future predictions compare to

    today_info = {
        "date": today_row["observation_date"],
        "predicted_value": base_value,
        "predicted_direction": "Today",
        "confidence": None,
    }
    results.append(today_info)
    for i in range(n_days):
        row = last_known.iloc[-1]
        future = {col: row[col] for col in feature_cols}
        X_future = pd.DataFrame([future])
        X_future_scaled = scaler.transform(X_future)
        pred_value = reg.predict(X_future_scaled)[0]
        pred_dir = clf.predict(X_future)[0]
        pred_prob = clf.predict_proba(X_future)[0][pred_dir]
        # Compare to **base_value only** (today's value)
        predicted_direction = "Up" if pred_value > base_value else "Down"
        next_date = row["observation_date"] + pd.Timedelta(days=1)
        results.append({
            "date": next_date,
            "predicted_value": pred_value,
            "predicted_direction": predicted_direction,
            "confidence": round(100 * pred_prob, 2),
        })
        # Simulate next day for rolling prediction
        next_row = row.copy()
        next_row["observation_date"] = next_date
        next_row[currency_col] = pred_value
        for col in feature_cols:
            if col.endswith("_lag1"):
                base_col = col.replace("_lag1", "")
                if base_col in row:
                    next_row[col] = row[base_col]
        last_known = pd.concat([last_known, pd.DataFrame([next_row])], ignore_index=True)
    return pd.DataFrame(results)

forecast_df = forecast_next_n_days(df, reg, clf, scaler, n_days=14, currency_col=currency_col)
print("Forecast Results:")
print(forecast_df)

plt.figure(figsize=(10,5))
plt.plot(forecast_df["date"], forecast_df["predicted_value"], marker='o', color="red", label="Forecasted Value")
today_value = forecast_df.iloc[0]["predicted_value"]
plt.axhline(today_value, color='blue', linestyle='--', linewidth=1.5, label=f"Today's Value: {today_value:.2f}")
plt.title("Currency 14-Day Forecast (Prediction Period Only)")
plt.xlabel("Date")
plt.ylabel("Predicted Exchange Rate")
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
