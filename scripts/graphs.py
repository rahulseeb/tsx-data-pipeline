from pathlib import Path
import pandas as pd

HISTORY_PATH = Path("../data/raw/historical_quotes.csv")

df = pd.read_csv(HISTORY_PATH)
df["timestamp_utc"] = df["timestamp_utc"].astype(str)

source = df[df["timestamp_utc"].str.startswith("2026-03-26")]

print("Total rows:", len(source))
print("Missing prevClose:", source["prevClose"].isna().sum())
print(source.groupby("timestamp_utc").size())