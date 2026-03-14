"""
STEP 1B: News Headlines Extraction
------------------------------------
Pulls top business/finance headlines using NewsAPI.
"""

import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv() 

# --- CONFIG ---
NEWSAPI_KEY = os.getenv("NEWS_API_KEY")
BASE_URL = "https://newsapi.org/v2"

# Keywords to search
SEARCH_QUERIES = [
    "stock market",
    "federal reserve interest rates",
    "tech earnings",
]
LOOKBACK_DAYS = 7  
OUTPUT_DIR = "data/raw/news"


def extract_news(queries: list[str], days_back: int = LOOKBACK_DAYS) -> list[dict]:
    """
    Pull news articles for each search query.
    Free tier limits: 100 requests/day, 100 results per request.
    """
    if not NEWSAPI_KEY:
        raise ValueError(
            "NEWSAPI_KEY not found!"
        )

    from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    all_articles = []
    seen_urls = set() 

    for query in queries:
        print(f'  Searching: "{query}"...')
        try:
            response = requests.get(
                f"{BASE_URL}/everything",
                params={
                    "q": query,
                    "from": from_date,
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 50,  # Max per request on free tier
                    "apiKey": NEWSAPI_KEY,
                },
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            if data["status"] != "ok":
                print(f"  WARNING: API returned status={data['status']}")
                continue

            articles = data.get("articles", [])
            new_count = 0

            for article in articles:
                # Deduplicate by URL
                url = article.get("url", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                # Structure the record with metadata
                record = {
                    # --- Business data ---
                    "title": article.get("title"),
                    "description": article.get("description"),
                    "source_name": article.get("source", {}).get("name"),
                    "author": article.get("author"),
                    "url": url,
                    "published_at": article.get("publishedAt"),
                    "content_snippet": article.get("content"),  # truncated on free tier
                    # --- Search context ---
                    "search_query": query,
                    # --- Metadata ---
                    "source": "newsapi",
                    "ingested_at": datetime.utcnow().isoformat(),
                }
                all_articles.append(record)
                new_count += 1

            print(f"  ✓ Found {new_count} new articles for '{query}'")

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print("  ERROR: Invalid API key. Check your .env file.")
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