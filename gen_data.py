import pandas as pd
import numpy as np

# Set seed for reproducible data
np.random.seed(42)

# Generate 200 rows of synthetic data with 5 features
num_rows = 200
data = {
    "feature_1": np.random.randn(num_rows) * 10 + 50,
    "feature_2": np.random.randn(num_rows) * 2 + 5,
    "feature_3": np.random.randint(20, 70, size=num_rows),
    "feature_4": np.random.rand(num_rows) * 100,
    "feature_5": np.random.randn(num_rows) + 0.5,
}

# Convert to DataFrame
df = pd.DataFrame(data)

# Create a clean binary target (0 or 1) based on a linear combination of features
log_odds = 0.1 * df["feature_1"] + 0.5 * df["feature_2"] - 0.05 * df["feature_3"]
probability = 1 / (1 + np.exp(-log_odds))
df["target"] = (probability > 0.5).astype(int)

# Save to the exact filename your PySpark code reads
df.to_csv("dataset.csv", index=False)
print("Successfully generated 'dataset.csv' with columns:", list(df.columns))
