"""
STEP 1A: Stock Price Extraction
--------------------------------
Pulls daily OHLCV data for a list of tickers using yfinance.
"""

import yfinance as yf
import pandas as pd
import json
import os
from datetime import datetime, timedelta


# --- CONFIG ---
# Pick 5-10 tickers you find interesting. Mix of sectors = better portfolio project.
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "JPM", "XOM", "UNH"]
LOOKBACK_DAYS = 30  # How far back to pull on first run
OUTPUT_DIR = "data/raw/stocks"


def extract_stock_data(tickers: list[str], days_back: int = 30) -> list[dict]:
    """
    Pull daily stock data for each ticker.
    Returns a list of records (one per ticker per day).
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    all_records = []

    for ticker in tickers:
        print(f"  Pulling {ticker}...")
        try:
            # yf.download gives you a DataFrame with Open, High, Low, Close, Volume
            df = yf.download(
                ticker,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                progress=False,  # suppress the progress bar
            )

            if df.empty:
                print(f"  WARNING: No data returned for {ticker}")
                continue

            # Flatten MultiIndex columns if present (yfinance quirk with single ticker)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # Convert each row to a record with metadata
            for date, row in df.iterrows():
                record = {
                    # --- Business data ---
                    "ticker": ticker,
                    "date": date.strftime("%Y-%m-%d"),
                    "open": round(float(row["Open"]), 4),
                    "high": round(float(row["High"]), 4),
                    "low": round(float(row["Low"]), 4),
                    "close": round(float(row["Close"]), 4),
                    "volume": int(row["Volume"]),
                    # --- Metadata (you'll thank yourself later) ---
                    "source": "yahoo_finance",
                    "ingested_at": datetime.utcnow().isoformat(),
                }
                all_records.append(record)

            print(f"  ✓ {ticker}: {len(df)} days of data")

        except Exception as e:
            print(f"  ERROR pulling {ticker}: {e}")

    return all_records


def save_to_json(records: list[dict], output_dir: str) -> str:
    """
    Save records as a single JSON file, partitioned by date.
    In a real pipeline, you'd write to Azure Data Lake — we'll get there in Step 3.
    """
    os.makedirs(output_dir, exist_ok=True)

    # File naming convention: source_YYYYMMDD_HHMMSS.json
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"stocks_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        json.dump(records, f, indent=2)

    print(f"\n✓ Saved {len(records)} records to {filepath}")
    return filepath


# --- MAIN ---
if __name__ == "__main__":
    print("=" * 50)
    print("STOCK DATA EXTRACTION")
    print("=" * 50)

    records = extract_stock_data(TICKERS, LOOKBACK_DAYS)
    if records:
        save_to_json(records, OUTPUT_DIR)
    else:
        print("No records extracted!")