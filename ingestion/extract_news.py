"""
STEP 1B: News Headlines Extraction (GNews API)
------------------------------------------------
"""

import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")
BASE_URL = "https://gnews.io/api/v4"

SEARCH_QUERIES = [
    "stock market",
    "federal reserve",
    "tech earnings",
    "Wall Street",
    "S&P 500",
    "NASDAQ",
    "inflation economy",
    "interest rates",
    "Apple stock",
    "NVIDIA stock",
    "Tesla stock",
    "Amazon stock",
    "Microsoft stock",
    "banking stocks",
    "oil prices energy",
    "jobs report unemployment",
    "GDP growth",
    "crypto bitcoin",
    "trade tariff",
    "IPO market",
]
LOOKBACK_DAYS = 30
OUTPUT_DIR = "data/raw/news"


def extract_news(queries: list[str], days_back: int = 30) -> list[dict]:
    """
    Pull news articles for each search query using GNews API.
    Free tier: 100 requests/day, 10 articles per request.
    """
    if not GNEWS_API_KEY:
        raise ValueError(
            "GNEWS_API_KEY not found!"
        )

    from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")
    to_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")
    all_articles = []
    seen_urls = set()

    for query in queries:
        print(f'  Searching: "{query}"...')
        try:
            response = requests.get(
                f"{BASE_URL}/search",
                params={
                    "q": query,
                    "from": from_date,
                    "to": to_date,
                    "lang": "en",
                    "max": 10,
                    "token": GNEWS_API_KEY,
                },
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            articles = data.get("articles", [])
            new_count = 0

            for article in articles:
                url = article.get("url", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                record = {
                    # --- Business data ---
                    "title": article.get("title"),
                    "description": article.get("description"),
                    "source_name": article.get("source", {}).get("name"),
                    "url": url,
                    "published_at": article.get("publishedAt"),
                    "content_snippet": article.get("content"),
                    # --- Search context ---
                    "search_query": query,
                    # --- Metadata ---
                    "source": "gnews",
                    "ingested_at": datetime.utcnow().isoformat(),
                }
                all_articles.append(record)
                new_count += 1

            print(f"  ✓ Found {new_count} new articles for '{query}'")

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print("  ERROR: Invalid API key. Check your .env file.")
            elif e.response.status_code == 403:
                print("  ERROR: Forbidden. Your key may have expired.")
            elif e.response.status_code == 429:
                print("  ERROR: Rate limit hit. Free tier = 100 requests/day.")
            else:
                print(f"  ERROR: {e}")
        except Exception as e:
            print(f"  ERROR: {e}")

    return all_articles


def save_to_json(records: list[dict], output_dir: str) -> str:
    """Save articles as JSON."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"news_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        json.dump(records, f, indent=2)

    print(f"\n✓ Saved {len(records)} articles to {filepath}")
    return filepath


# --- MAIN ---
if __name__ == "__main__":
    print("=" * 50)
    print("NEWS HEADLINES EXTRACTION")
    print("=" * 50)

    articles = extract_news(SEARCH_QUERIES, LOOKBACK_DAYS)
    if articles:
        save_to_json(articles, OUTPUT_DIR)
    else:
        print("No articles extracted!")