import sqlite3
from pathlib import Path
import pandas as pd

DB_PATH = Path("../data/market_data.db")
CSV_PATH = Path("../data/raw/historical_quotes.csv")


def clean_quotes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Drop junk columns created by trailing commas in CSV
    df = df.loc[:, ~df.columns.str.contains(r"^Unnamed")]

    # Normalize column names
    if "__typename" in df.columns:
        df = df.rename(columns={"__typename": "typename"})

    # Ensure optional columns exist
    if "raw_symbol" not in df.columns:
        df["raw_symbol"] = None

    if "patched_from_null" not in df.columns:
        df["patched_from_null"] = 0
    else:
        df["patched_from_null"] = pd.to_numeric(
            df["patched_from_null"], errors="coerce"
        ).fillna(0).astype(int)

    if "snapshot_type" not in df.columns:
        df["snapshot_type"] = "live"

    if "backfilled_from_date" not in df.columns:
        df["backfilled_from_date"] = None

    # Normalize blanks and whitespace
    text_cols = [
        "symbol",
        "longname",
        "raw_symbol",
        "snapshot_type",
        "backfilled_from_date",
        "timestamp_utc",
        "typename",
    ]

    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
            df[col] = df[col].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

    # Patch National Bank blank symbol
    na_mask = (
        df["symbol"].isna()
        & df["longname"].fillna("").str.contains("National Bank of Canada", case=False, na=False)
    )
    df.loc[na_mask, "symbol"] = "NA"
    df.loc[na_mask, "patched_from_null"] = 1

    # Drop only rows that are truly unusable
    df = df[df["longname"].notna()].copy()
    df = df[df["price"].notna()].copy()

    # Keep only the columns the DB table expects, in order
    expected_cols = [
        "symbol",
        "longname",
        "price",
        "volume",
        "openPrice",
        "priceChange",
        "percentChange",
        "dayHigh",
        "dayLow",
        "prevClose",
        "typename",
        "timestamp_utc",
        "raw_symbol",
        "patched_from_null",
        "snapshot_type",
        "backfilled_from_date",
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    df = df[expected_cols]

    # Optional consistency sort
    df = df.sort_values(
        by=["timestamp_utc", "symbol"],
        ascending=[True, True],
        na_position="last"
    ).reset_index(drop=True)

    return df


def recreate_table(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS historical_quotes")
    conn.execute("""
        CREATE TABLE historical_quotes (
            symbol TEXT,
            longname TEXT,
            price REAL,
            volume REAL,
            openPrice REAL,
            priceChange REAL,
            percentChange REAL,
            dayHigh REAL,
            dayLow REAL,
            prevClose REAL,
            typename TEXT,
            timestamp_utc TEXT,
            raw_symbol TEXT,
            patched_from_null INTEGER,
            snapshot_type TEXT,
            backfilled_from_date TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_historical_quotes_symbol
        ON historical_quotes(symbol)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_historical_quotes_timestamp
        ON historical_quotes(timestamp_utc)
    """)


def load_csv_to_sqlite(rewrite_clean_csv: bool = True) -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH.resolve()}")

    raw_df = pd.read_csv(CSV_PATH)
    print("Raw rows:", len(raw_df))
    print("Raw columns:", list(raw_df.columns))

    clean_df = clean_quotes(raw_df)
    print("Clean columns:", list(clean_df.columns))
    print("Rows to insert:", len(clean_df))

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        recreate_table(conn)
        clean_df.to_sql("historical_quotes", conn, if_exists="append", index=False)

        total_rows = conn.execute("SELECT COUNT(*) FROM historical_quotes").fetchone()[0]
        print("Rows now in DB:", total_rows)

    if rewrite_clean_csv:
        clean_df.to_csv(CSV_PATH, index=False)
        print(f"Rewrote cleaned CSV: {CSV_PATH.resolve()}")


if __name__ == "__main__":
    load_csv_to_sqlite(rewrite_clean_csv=True)