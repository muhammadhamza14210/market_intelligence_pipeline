"""
STEP 1C: Macro Economic Data from FRED
----------------------------------------
Pulls key economic indicators from the Federal Reserve (FRED).
"""

import json
import os
from datetime import datetime, timedelta
from fredapi import Fred
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
FRED_API_KEY = os.getenv("FRED_API_KEY")

SERIES = [
    ("DFF", "federal_funds_rate", "daily"),           # Fed funds rate (daily)
    ("UNRATE", "unemployment_rate", "monthly"),        # Unemployment rate
    ("CPIAUCSL", "consumer_price_index", "monthly"),   # CPI (inflation)
    ("GDP", "gross_domestic_product", "quarterly"),     # GDP
    ("T10Y2Y", "treasury_yield_spread", "daily"),      # 10Y-2Y spread (recession indicator)
    ("VIXCLS", "vix_volatility_index", "daily"),       # VIX (fear gauge)
]
LOOKBACK_DAYS = 30
OUTPUT_DIR = "data/raw/fred"


def extract_fred_data(
    series_list: list[tuple], days_back: int = LOOKBACK_DAYS
) -> list[dict]:
    """
    Pull each FRED series and return as flat records.
    """
    if not FRED_API_KEY:
        raise ValueError(
            "FRED_API_KEY not found!"
        )

    fred = Fred(api_key=FRED_API_KEY)
    start_date = datetime.now() - timedelta(days=days_back)
    all_records = []

    for series_id, name, frequency in series_list:
        print(f"  Pulling {name} ({series_id})...")
        try:
            # fred.get_series returns a pandas Series indexed by date
            data = fred.get_series(
                series_id,
                observation_start=start_date.strftime("%Y-%m-%d"),
            )

            if data.empty:
                print(f"  WARNING: No data for {series_id}")
                continue

            # Drop NaN values (FRED uses NaN for missing observations)
            data = data.dropna()

            for date, value in data.items():
                record = {
                    # --- Business data ---
                    "series_id": series_id,
                    "series_name": name,
                    "date": date.strftime("%Y-%m-%d"),
                    "value": round(float(value), 4),
                    "frequency": frequency,
                    # --- Metadata ---
                    "source": "fred",
                    "ingested_at": datetime.utcnow().isoformat(),
                }
                all_records.append(record)

            print(f"  ✓ {name}: {len(data)} observations")

        except Exception as e:
            print(f"  ERROR pulling {series_id}: {e}")

    return all_records


def save_to_json(records: list[dict], output_dir: str) -> str:
    """Save FRED data as JSON."""
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"fred_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        json.dump(records, f, indent=2)

    print(f"\n✓ Saved {len(records)} records to {filepath}")
    return filepath


# --- MAIN ---
if __name__ == "__main__":
    print("=" * 50)
    print("FRED ECONOMIC DATA EXTRACTION")
    print("=" * 50)

    records = extract_fred_data(SERIES, LOOKBACK_DAYS)
    if records:
        save_to_json(records, OUTPUT_DIR)
    else:
        print("No records extracted!")