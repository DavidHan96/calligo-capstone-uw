import numpy as np
import matplotlib.pyplot as plt
from merge_fred_files import load_and_merge_data

df = load_and_merge_data()
lumber_returns = df['lumber_price'].pct_change().dropna()

np.random.seed(42)
num_simulations = 100000000
future_weeks = 4 # 1 = 7 Days 
final_prices = []

for _ in range(num_simulations):
    random_returns = np.random.choice(lumber_returns, size=future_weeks, replace=True)
    simulated_price = df['lumber_price'].iloc[-1] * np.prod(1 + random_returns)
    final_prices.append(simulated_price)

current_price = df['lumber_price'].iloc[-1]
up_prob = np.mean(np.array(final_prices) > current_price)
down_prob = 1 - up_prob

print(f"Probability of price increase: {up_prob:.2%}")
print(f"Probability of price decrease: {down_prob:.2%}")

plt.figure(figsize=(8,6))
plt.hist(final_prices, bins=50, color='skyblue', edgecolor='black')
plt.axvline(current_price, color='red', linestyle='--', label='Current Price')
plt.title(f"Lumber Price Distribution After {future_weeks} Weeks")
plt.xlabel('Simulated Price')
plt.ylabel('Frequency')
plt.legend()
plt.show()
