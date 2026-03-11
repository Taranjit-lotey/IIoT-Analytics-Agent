"""
Run this once after downloading the AI4I dataset from UCI/Kaggle.
Download from: https://www.kaggle.com/datasets/stephanmatzka/predictive-maintenance-dataset-ai4i-2020
Save as: data/ai4i2020.csv
Then run: python data/enrich_dataset.py
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Load raw dataset
df = pd.read_csv("data/ai4i2020.csv")

# Rename columns to snake_case
df.columns = [
    "udi", "product_id", "machine_type", "air_temperature",
    "process_temperature", "rotational_speed", "torque", "tool_wear",
    "machine_failure", "twf", "hdf", "pwf", "osf", "rnf"
]

# Derive failure_type from binary columns
def get_failure_type(row):
    if row["twf"] == 1:
        return "tool_wear"
    elif row["hdf"] == 1:
        return "heat"
    elif row["pwf"] == 1:
        return "power"
    elif row["osf"] == 1:
        return "overstrain"
    elif row["rnf"] == 1:
        return "random"
    else:
        return "none"

df["failure_type"] = df.apply(get_failure_type, axis=1)

# Add plant_id
plants = ["Plant_A", "Plant_B", "Plant_C"]
df["plant_id"] = np.random.choice(plants, size=len(df), p=[0.4, 0.35, 0.25])

# Add line_number
df["line_number"] = np.random.randint(1, 6, size=len(df))

# Add machine_id (combine plant + product_id)
df["machine_id"] = df["plant_id"].str.replace("Plant_", "M") + "_" + df["product_id"].astype(str)

# Add timestamps spread over last 30 days
base_time = datetime.now() - timedelta(days=30)
timestamps = [base_time + timedelta(minutes=i * 3) for i in range(len(df))]
df["timestamp"] = timestamps

# Keep only needed columns
df_final = df[[
    "timestamp", "machine_id", "plant_id", "line_number",
    "machine_type", "air_temperature", "process_temperature",
    "rotational_speed", "torque", "tool_wear", "failure_type"
]]

df_final.to_csv("data/ai4i_enriched.csv", index=False)
print(f"Enriched dataset saved: {len(df_final)} rows")
print(df_final.head())
print("\nFailure distribution:")
print(df_final["failure_type"].value_counts())
print("\nPlant distribution:")
print(df_final["plant_id"].value_counts())
