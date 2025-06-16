import numpy as np
from xgboost import XGBClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from merge_fred_files import load_and_merge_data

# Load and prepare data
df = load_and_merge_data()
df['target'] = np.where(df['lumber_price'].shift(-1) > df['lumber_price'], 1, 0)
df['month'] = df['date'].dt.month
df['is_summer'] = df['month'].isin([6,7,8]).astype(int)
df['month_sin'] = np.sin(2 * np.pi * df['month']/12)
df['month_cos'] = np.cos(2 * np.pi * df['month']/12)
df['lumber_pct_change_1w'] = df['lumber_price'].pct_change(1)
df['lumber_pct_change_4w'] = df['lumber_price'].pct_change(4)
df['lumber_sma_4w'] = df['lumber_price'].rolling(4).mean()
df['lumber_sma_12w'] = df['lumber_price'].rolling(12).mean()
df['lumber_volatility_4w'] = df['lumber_price'].rolling(4).std()
df = df.dropna().reset_index(drop=True)

X = df.drop(columns=['date', 'target', 'lumber_price'])
y = df['target']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, shuffle=False)

# Model training
model = XGBClassifier(n_estimators=50, max_depth=3, learning_rate=0.1, scale_pos_weight=2, random_state=42)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# Report
print("Classification Report")
print(classification_report(y_test, y_pred, labels=[0,1]))

# Feature importance
import pandas as pd
importance = model.get_booster().get_score(importance_type='weight')
importance_df = pd.DataFrame(importance.items(), columns=['Feature', 'Importance']).sort_values(by='Importance', ascending=False)
print("\n=== Feature Importance ===")
print(importance_df)
