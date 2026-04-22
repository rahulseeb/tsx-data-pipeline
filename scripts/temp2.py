import pandas as pd
from pathlib import Path

HISTORY_PATH = Path("../data/raw/historical_quotes.csv")

df = pd.read_csv(HISTORY_PATH)

if "snapshot_type" not in df.columns:
    df["snapshot_type"] = "live"
    df.to_csv(HISTORY_PATH, index=False)
    print("Added snapshot_type column and set all existing rows to 'live'.")
else:
    print("snapshot_type column already exists.")