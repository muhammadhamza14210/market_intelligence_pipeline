"""
STEP 1A: Stock Price Extraction
--------------------------------
Pulls daily OHLCV data for a list of tickers using yfinance.
"""

import yfinance as yf
import pandas as pd
import json
import os
from datetime import datetime
from utils.constants import TICKERS

LOOKBACK_DAYS = 30 
OUTPUT_DIR = "data/raw/stocks"


def extract_stock_data(tickers: list[str], days_back: int = LOOKBACK_DAYS) -> list[dict]:
    all_records = []

    for ticker in tickers:
        print(f"\nPulling {ticker}...")
        try:
            ticker_obj = yf.Ticker(ticker)
            df = ticker_obj.history(period=f"{days_back}d", auto_adjust=False)

            print(f"{ticker} DataFrame shape: {df.shape}")

            if df.empty:
                print(f"WARNING: No data returned for {ticker}")
                continue

            df = df.reset_index()

            for _, row in df.iterrows():
                record = {
                    "ticker": ticker,
                    "date": row["Date"].strftime("%Y-%m-%d"),
                    "open": round(float(row["Open"]), 4),
                    "high": round(float(row["High"]), 4),
                    "low": round(float(row["Low"]), 4),
                    "close": round(float(row["Close"]), 4),
                    "volume": int(row["Volume"]),
                    "source": "yahoo_finance",
                    "ingested_at": datetime.utcnow().isoformat(),
                }
                all_records.append(record)

            print(f"✓ {ticker}: {len(df)} rows extracted")

        except Exception as e:
            print(f"ERROR pulling {ticker}: {e}")

    print(f"\nTotal records extracted: {len(all_records)}")
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