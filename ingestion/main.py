"""
Extract All Sources
------------------------------------
Runs all three extractors in sequence.
"""

from datetime import datetime

from .extract_stocks import extract_stock_data, save_to_json as save_stocks
from .extract_stocks import TICKERS, LOOKBACK_DAYS as STOCK_LOOKBACK, OUTPUT_DIR as STOCK_DIR

from .extract_news import extract_news, save_to_json as save_news
from .extract_news import SEARCH_QUERIES, LOOKBACK_DAYS as NEWS_LOOKBACK, OUTPUT_DIR as NEWS_DIR

from .extract_fred import extract_fred_data, save_to_json as save_fred
from .extract_fred import SERIES, LOOKBACK_DAYS as FRED_LOOKBACK, OUTPUT_DIR as FRED_DIR


def main():
    start = datetime.now()
    print("=" * 60)
    print(f"  FULL EXTRACTION PIPELINE — {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    results = {}

    # --- 1. Stocks ---
    print("\n[1/3] STOCK DATA")
    print("-" * 40)
    stock_records = extract_stock_data(TICKERS, STOCK_LOOKBACK)
    if stock_records:
        results["stocks"] = save_stocks(stock_records, STOCK_DIR)
    else:
        print("  SKIPPED: No stock data")

    # --- 2. News ---
    print("\n[2/3] NEWS HEADLINES")
    print("-" * 40)
    news_records = extract_news(SEARCH_QUERIES, NEWS_LOOKBACK)
    if news_records:
        results["news"] = save_news(news_records, NEWS_DIR)
    else:
        print("  SKIPPED: No news data")

    # --- 3. FRED ---
    print("\n[3/3] FRED ECONOMIC DATA")
    print("-" * 40)
    fred_records = extract_fred_data(SERIES, FRED_LOOKBACK)
    if fred_records:
        results["fred"] = save_fred(fred_records, FRED_DIR)
    else:
        print("  SKIPPED: No FRED data")

    # --- Summary ---
    elapsed = (datetime.now() - start).total_seconds()
    print("\n" + "=" * 60)
    print("  EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"  Time: {elapsed:.1f}s")
    print(f"  Files created:")
    for source, path in results.items():
        print(f"    {source}: {path}")
    print(f"\nNext step: Upload these files (Step 2)")


if __name__ == "__main__":
    main()