import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

GRAPHQL_URL = "https://app-money.tmx.com/graphql"

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0",
}

TRENDING_QUERY = """
query TrendingQuotes($symbols: [String]) {
  quote: getTrendingQuotes(symbols: $symbols) {
    symbol
    longname
    price
    volume
    openPrice
    priceChange
    percentChange
    dayHigh
    dayLow
    prevClose
    __typename
  }
}
"""

QUOTE_BY_SYMBOL_QUERY = """
query getQuoteBySymbol($symbol: String, $locale: String) {
  getQuoteBySymbol(symbol: $symbol, locale: $locale) {
    symbol
    name
    price
    volume
    openPrice
    priceChange
    percentChange
    dayHigh
    dayLow
    prevClose
    __typename
  }
}
"""
SYMBOL_ALIASES = {
    "BBD": "BBD.B",
    "BBD B": "BBD.B",
    "BBD.B": "BBD.B",
    "RCI": "RCI.B",
    "RCI B": "RCI.B",
    "RCI.B": "RCI.B",
    "GIB": "GIB.A",
    "GIB A": "GIB.A",
    "GIB.A": "GIB.A",
    "BEP": "BEP.UN",
    "TLG": "TLG",
    "TROILUS": "TLG",
    "CHE": "CHE.UN",
    "CHE.UN": "CHE.UN",
    "COGECO": "CCA",
    "BIP UN": "BIP.UN",
    "BIP": "BIP.UN",  
    "BIP.UN": "BIP.UN",
        
}

def get_utc_now() -> datetime:
    return datetime.now(timezone.utc)


def post_graphql(payload: dict) -> dict:
    response = requests.post(
        GRAPHQL_URL,
        json=payload,
        headers=HEADERS,
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def fetch_trending_quotes() -> pd.DataFrame:
    payload = {
        "operationName": "TrendingQuotes",
        "variables": {},
        "query": TRENDING_QUERY,
    }

    result = post_graphql(payload)
    data = result["data"]["quote"]

    return pd.DataFrame(data)


def fetch_quote_by_symbol(symbol: str, locale: str = "en") -> pd.DataFrame:
    payload = {
        "operationName": "getQuoteBySymbol",
        "variables": {
            "symbol": symbol,
            "locale": locale,
        },
        "query": QUOTE_BY_SYMBOL_QUERY,
    }

    result = post_graphql(payload)
    quote = result["data"]["getQuoteBySymbol"]

    if not quote:
        return pd.DataFrame()

    row = {
        "symbol": quote.get("symbol"),
        "longname": quote.get("name"),
        "price": quote.get("price"),
        "volume": quote.get("volume"),
        "openPrice": quote.get("openPrice"),
        "priceChange": quote.get("priceChange"),
        "percentChange": quote.get("percentChange"),
        "dayHigh": quote.get("dayHigh"),
        "dayLow": quote.get("dayLow"),
        "prevClose": quote.get("prevClose"),
        "__typename": quote.get("__typename"),
    }

    return pd.DataFrame([row])


def patch_alias_and_na_rows(df: pd.DataFrame) -> pd.DataFrame:
    patched_df = df.copy()

    if "raw_symbol" not in patched_df.columns:
        patched_df["raw_symbol"] = pd.NA

    if "patched_from_null" not in patched_df.columns:
        patched_df["patched_from_null"] = False

    # 1) Patch blank National Bank symbol in place
    na_null_mask = (
        patched_df["symbol"].fillna("").astype(str).str.strip().eq("")
        & patched_df["longname"].fillna("").astype(str).str.contains(
            "National Bank of Canada", case=False, na=False
        )
    )

    if na_null_mask.any():
        patched_df.loc[na_null_mask, "raw_symbol"] = patched_df.loc[na_null_mask, "symbol"]
        patched_df.loc[na_null_mask, "symbol"] = "NA"
        patched_df.loc[na_null_mask, "patched_from_null"] = True

    # 2) For any remaining row with null price, try alias lookup first
    null_price_mask = patched_df["price"].isna()

    for idx, row in patched_df[null_price_mask].iterrows():
        original_symbol = str(row.get("symbol", "")).strip()

        if not original_symbol:
            continue

        lookup_symbol = SYMBOL_ALIASES.get(original_symbol, original_symbol)

        replacement_df = fetch_quote_by_symbol(lookup_symbol)

        if replacement_df.empty or pd.isna(replacement_df.iloc[0]["price"]):
            continue

        replacement_row = replacement_df.iloc[0]

        for col in replacement_df.columns:
            patched_df.at[idx, col] = replacement_row[col]

        patched_df.at[idx, "raw_symbol"] = original_symbol
        patched_df.at[idx, "patched_from_null"] = True

    return patched_df


def clean_quotes(df: pd.DataFrame) -> pd.DataFrame:
    cleaned_df = df.copy()

    # Treat blank/whitespace symbol as missing
    cleaned_df["symbol"] = cleaned_df["symbol"].astype("string").str.strip()
    cleaned_df["symbol"] = cleaned_df["symbol"].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

    cleaned_df = cleaned_df[cleaned_df["symbol"].notna()]
    cleaned_df = cleaned_df[cleaned_df["price"].notna()]

    return cleaned_df.reset_index(drop=True)


def build_snapshot() -> pd.DataFrame:
    batch_timestamp = get_utc_now()

    df = fetch_trending_quotes()
    df = patch_alias_and_na_rows(df)
    df = clean_quotes(df)

    df["timestamp_utc"] = batch_timestamp

    if "raw_symbol" not in df.columns:
        df["raw_symbol"] = pd.NA

    if "patched_from_null" not in df.columns:
        df["patched_from_null"] = False

    return df


def save_snapshot(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def main() -> None:
    output_path = Path("../data/raw/trending_quotes_today.csv")

    raw_df = fetch_trending_quotes()
    print("Raw rows from TMX:", len(raw_df))
    print("Blank symbols in raw feed:",
          raw_df["symbol"].fillna("").astype(str).str.strip().eq("").sum())
    print("Null prices in raw feed:", raw_df["price"].isna().sum())

    patched_df = patch_alias_and_na_rows(raw_df)
    print("Rows after patching:", len(patched_df))
    print("Blank symbols after patching:",
          patched_df["symbol"].fillna("").astype(str).str.strip().eq("").sum())
    print("Null prices after patching:", patched_df["price"].isna().sum())

    df = clean_quotes(patched_df)
    print("Rows after cleaning:", len(df))

    batch_timestamp = get_utc_now()
    df["timestamp_utc"] = batch_timestamp

    if "raw_symbol" not in df.columns:
        df["raw_symbol"] = pd.NA

    if "patched_from_null" not in df.columns:
        df["patched_from_null"] = False

    save_snapshot(df, output_path)
    print(f"Saved {len(df)} rows to {output_path.resolve()}")

    print("\nRows with null price after patching:")
    print(
        patched_df[patched_df["price"].isna()][
            ["symbol", "longname", "price", "raw_symbol", "patched_from_null"]
        ]
    )


if __name__ == "__main__":
    main()