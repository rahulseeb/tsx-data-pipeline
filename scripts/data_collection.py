import sys
from pathlib import Path
import pandas as pd

from tsx_scraper import build_snapshot


RAW_DIR = Path("../data/raw")
LATEST_PATH = RAW_DIR / "trending_quotes_today.csv"
HISTORY_PATH = RAW_DIR / "april_quotes.csv"


def save_latest_snapshot(df: pd.DataFrame) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(LATEST_PATH, index=False)


def append_to_history(df: pd.DataFrame) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if HISTORY_PATH.exists():
        existing_df = pd.read_csv(HISTORY_PATH)

        # If older history doesn't have snapshot_type yet, add it
        if "snapshot_type" not in existing_df.columns:
            existing_df["snapshot_type"] = "live"

        combined_df = pd.concat([existing_df, df], ignore_index=True)
    else:
        combined_df = df.copy()

    combined_df.to_csv(HISTORY_PATH, index=False)


# python data_collecting.py rollback
# This run deletes latest batch of TSX data
def rollback_latest_snapshot() -> None:
    if not HISTORY_PATH.exists():
        print("No april_quotes.csv file found.")
        return

    history_df = pd.read_csv(HISTORY_PATH)

    if history_df.empty:
        print("april_quotes.csv is already empty.")
        return

    latest_timestamp = history_df["timestamp_utc"].max()
    cleaned_df = history_df[history_df["timestamp_utc"] != latest_timestamp].copy()

    cleaned_df.to_csv(HISTORY_PATH, index=False)

    removed_rows = len(history_df) - len(cleaned_df)
    print(f"Removed latest snapshot batch: {removed_rows} rows with timestamp {latest_timestamp}")


def collect_snapshot() -> None:
    snapshot_df = build_snapshot()

    # Mark all normal runs as live snapshots
    snapshot_df["snapshot_type"] = "live"

    save_latest_snapshot(snapshot_df)
    append_to_history(snapshot_df)

    print(f"Saved latest snapshot to {LATEST_PATH.resolve()}")
    print(f"Appended {len(snapshot_df)} rows to {HISTORY_PATH.resolve()}")


def backfill_close_from_prevclose(missing_date: str, source_date: str) -> None:
    """
    Backfill a missing trading day's close using the next day's prevClose values.

    Example:
        python data_collecting.py backfill 2026-03-25 2026-03-26

    This means:
        create rows for 2026-03-25
        using prevClose from rows collected on 2026-03-26
    """
    if not HISTORY_PATH.exists():
        print("No april_quotes.csv file found.")
        return

    history_df = pd.read_csv(HISTORY_PATH)

    if history_df.empty:
        print("april_quotes.csv is empty.")
        return

    if "timestamp_utc" not in history_df.columns:
        print("timestamp_utc column is missing.")
        return

    if "prevClose" not in history_df.columns:
        print("prevClose column is missing.")
        return

    if "snapshot_type" not in history_df.columns:
        history_df["snapshot_type"] = "live"

    # normalize timestamp to string just in case
    history_df["timestamp_utc"] = history_df["timestamp_utc"].astype(str)

    # find source rows from the next day's scrape
    source_df = history_df[
        history_df["timestamp_utc"].str.startswith(source_date)
    ].copy()

    if source_df.empty:
        print(f"No rows found for source date {source_date}.")
        return

    # remove rows where prevClose is missing
    source_df = source_df[source_df["prevClose"].notna()].copy()

    if source_df.empty:
        print(f"No usable prevClose values found for source date {source_date}.")
        return

    # create backfill rows
    backfill_df = source_df.copy()

    # use prevClose as the synthetic close price for the missing day
    backfill_df["price"] = backfill_df["prevClose"]

    # overwrite timestamps so this batch belongs to the missing day
    backfill_df["timestamp_utc"] = f"{missing_date} 20:05:00+00:00"

    # clearly mark as synthetic/backfilled
    backfill_df["snapshot_type"] = "backfill_close"

    # optional helper column
    backfill_df["backfilled_from_date"] = source_date

    # prevent duplicate backfills for same date
    existing_same_date = history_df[
        history_df["timestamp_utc"].str.startswith(missing_date)
    ].copy()

    if not existing_same_date.empty:
        existing_backfills = existing_same_date[
            existing_same_date["snapshot_type"] == "backfill_close"
        ]
        if not existing_backfills.empty:
            print(f"Backfill rows for {missing_date} already appear to exist. Aborting.")
            return

    append_to_history(backfill_df)

    print(
        f"Backfilled {len(backfill_df)} rows for {missing_date} "
        f"using prevClose values from {source_date}."
    )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()

        if command == "rollback":
            rollback_latest_snapshot()

        elif command == "backfill":
            if len(sys.argv) != 4:
                print("Usage: python data_collecting.py backfill <missing_date> <source_date>")
                print("Example: python data_collecting.py backfill 2026-03-25 2026-03-26")
            else:
                missing_date = sys.argv[2]
                source_date = sys.argv[3]
                backfill_close_from_prevclose(missing_date, source_date)

        else:
            print(f"Unknown command: {command}")
    else:
        collect_snapshot()