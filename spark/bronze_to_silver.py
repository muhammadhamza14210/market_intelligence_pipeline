"""
STEP 3A: Bronze → Silver Transformations
------------------------------------------
Reads raw JSON from data/raw/ (Bronze), cleans and enriches each source,
writes clean Parquet to data/silver/.

What this does:
  - Stocks:  Parse dates, cast types, drop nulls, deduplicate
  - News:    Parse dates, deduplicate by URL, add FinBERT sentiment scores
  - FRED:    Parse dates, cast types, pivot to wide format (one column per indicator)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType, FloatType
)
from pyspark.sql import Window
from transformers import pipeline
import os


# --- SPARK SESSION ---
spark = (
    SparkSession.builder
    .master("local[*]")        # Use all CPU cores
    .appName("MarketIntelligence-BronzeToSilver")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # Reduce noise

# --- PATHS ---
BRONZE_DIR = "data/raw"
SILVER_DIR = "data/silver"


# ============================================================
# 1. STOCKS: Bronze → Silver
# ============================================================
def transform_stocks():
    """
    Clean stock data:
    - Parse date strings to proper date type
    - Cast numeric columns
    - Drop rows with null prices
    - Deduplicate on (ticker, date)
    - Add daily return calculation
    """
    print("=" * 50)
    print("[STOCKS] Bronze → Silver")
    print("=" * 50)

    # Read all JSON files from bronze stocks folder
    df = spark.read.option("multiLine", True).json(f"{BRONZE_DIR}/stocks/")
    print(f"  Bronze records: {df.count()}")

    # Clean and transform
    silver = (
        df
        # Cast types
        .withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        .withColumn("open", F.col("open").cast(DoubleType()))
        .withColumn("high", F.col("high").cast(DoubleType()))
        .withColumn("low", F.col("low").cast(DoubleType()))
        .withColumn("close", F.col("close").cast(DoubleType()))
        .withColumn("volume", F.col("volume").cast(LongType()))
        # Parse ingestion timestamp
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        # Drop rows where critical fields are null
        .filter(F.col("close").isNotNull())
        .filter(F.col("date").isNotNull())
        .filter(F.col("ticker").isNotNull())
        # Deduplicate: keep the latest ingestion per ticker+date
        .withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("ticker", "date")
                .orderBy(F.col("ingested_at").desc())
            )
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        # Add daily return: (close - open) / open
        .withColumn(
            "daily_return",
            F.round((F.col("close") - F.col("open")) / F.col("open") * 100, 4)
        )
        # Add price range (high - low)
        .withColumn(
            "price_range",
            F.round(F.col("high") - F.col("low"), 4)
        )
        # Select final columns in order
        .select(
            "ticker", "date", "open", "high", "low", "close",
            "volume", "daily_return", "price_range", "source", "ingested_at"
        )
        .orderBy("ticker", "date")
    )

    # Write to Silver as Parquet (partitioned by ticker for efficient queries)
    output_path = f"{SILVER_DIR}/stocks"
    silver.write.mode("overwrite").partitionBy("ticker").parquet(output_path)

    print(f"  Silver records: {silver.count()}")
    print(f"  Tickers: {[row.ticker for row in silver.select('ticker').distinct().collect()]}")
    print(f"  Date range: {silver.agg(F.min('date'), F.max('date')).collect()[0]}")
    print(f"  ✓ Written to {output_path}")
    print()

    return silver


# ============================================================
# 2. NEWS: Bronze → Silver (with FinBERT Sentiment)
# ============================================================
def load_finbert():
    """
    Load FinBERT sentiment model from Hugging Face.
    This runs locally — no API key needed.
    First run downloads ~400MB model, then it's cached.
    """
    print("  Loading FinBERT model (first time downloads ~400MB)...")
    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model="ProsusAI/finbert",
        tokenizer="ProsusAI/finbert",
    )
    print("  ✓ FinBERT loaded")
    return sentiment_pipeline


def analyze_sentiment(texts: list, sentiment_pipeline) -> list:
    """
    Run FinBERT on a batch of texts.
    Returns list of (label, score) tuples.
    """
    results = []
    batch_size = 32
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        batch = [t[:512] if t else "" for t in batch]
        preds = sentiment_pipeline(batch)
        for pred in preds:
            results.append((pred["label"], round(pred["score"], 4)))
    return results


def transform_news():
    """
    Clean news data and add sentiment:
    - Parse published_at to timestamp
    - Deduplicate by URL
    - Run FinBERT on headlines
    - Add sentiment_label and sentiment_score columns
    """
    print("=" * 50)
    print("[NEWS] Bronze → Silver (+ FinBERT Sentiment)")
    print("=" * 50)

    df = spark.read.option("multiLine", True).json(f"{BRONZE_DIR}/news/")
    print(f"  Bronze records: {df.count()}")

    # Basic cleaning first
    cleaned = (
        df
        .withColumn("published_at", F.to_timestamp("published_at"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        # Extract just the date for joining with stocks later
        .withColumn("date", F.to_date("published_at"))
        # Drop nulls on title (can't do sentiment without it)
        .filter(F.col("title").isNotNull())
        # Deduplicate by URL
        .dropDuplicates(["url"])
    )

    # --- FinBERT Sentiment Analysis ---
    # Collect titles to run through the model
    print("  Running sentiment analysis...")
    pandas_df = cleaned.toPandas()

    sentiment_pipeline = load_finbert()
    titles = pandas_df["title"].fillna("").tolist()
    sentiments = analyze_sentiment(titles, sentiment_pipeline)

    # Add sentiment columns back
    pandas_df["sentiment_label"] = [s[0] for s in sentiments]
    pandas_df["sentiment_score"] = [s[1] for s in sentiments]

    # Convert back to Spark DataFrame
    silver = spark.createDataFrame(pandas_df)

    # Select final columns
    silver = silver.select(
        "title", "description", "source_name", "url",
        "published_at", "date", "search_query",
        "sentiment_label", "sentiment_score",
        "source", "ingested_at"
    ).orderBy("published_at")

    # Write to Silver
    output_path = f"{SILVER_DIR}/news"
    silver.write.mode("overwrite").parquet(output_path)

    # Print sentiment summary
    print(f"  Silver records: {silver.count()}")
    print(f"  Sentiment breakdown:")
    silver.groupBy("sentiment_label").count().show()
    print(f"  ✓ Written to {output_path}")
    print()

    return silver


# ============================================================
# 3. FRED: Bronze → Silver
# ============================================================
def transform_fred():
    """
    Clean FRED data:
    - Parse dates, cast values
    - Pivot from long format to wide format
      (one row per date, one column per indicator)
    - Forward-fill missing values (macro data has gaps on weekends)
    """
    print("=" * 50)
    print("[FRED] Bronze → Silver")
    print("=" * 50)

    df = spark.read.option("multiLine", True).json(f"{BRONZE_DIR}/fred/")
    print(f"  Bronze records: {df.count()}")

    # Clean and cast
    cleaned = (
        df
        .withColumn("date", F.to_date("date", "yyyy-MM-dd"))
        .withColumn("value", F.col("value").cast(DoubleType()))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .filter(F.col("value").isNotNull())
        .filter(F.col("date").isNotNull())
        # Deduplicate: keep latest ingestion per series+date
        .dropDuplicates(["series_name", "date"])
    )

    # Pivot: one row per date, one column per indicator
    pivoted = (
        cleaned
        .groupBy("date")
        .pivot("series_name")
        .agg(F.first("value"))
        .orderBy("date")
    )

    # Forward fill: for days where monthly/quarterly data is missing,
    # carry the last known value forward
    # (This is important because stocks trade daily but GDP is quarterly)
    window = Window.orderBy("date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    silver = pivoted
    for col_name in pivoted.columns:
        if col_name != "date":
            silver = silver.withColumn(
                col_name,
                F.last(F.col(col_name), ignorenulls=True).over(window)
            )

    # Write to Silver
    output_path = f"{SILVER_DIR}/fred"
    silver.write.mode("overwrite").parquet(output_path)

    print(f"  Silver records: {silver.count()}")
    print(f"  Columns: {silver.columns}")
    silver.show(5)
    print(f"  ✓ Written to {output_path}")
    print()

    return silver


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  STEP 3A: BRONZE → SILVER TRANSFORMATIONS")
    print("=" * 60 + "\n")

    os.makedirs(SILVER_DIR, exist_ok=True)

    stocks_silver = transform_stocks()
    news_silver = transform_news()
    fred_silver = transform_fred()

    print("=" * 60)
    print("  ALL SILVER LAYERS COMPLETE")
    print("=" * 60)
    print(f"\n  Output: {SILVER_DIR}/")
    print(f"    ├── stocks/   (partitioned by ticker, Parquet)")
    print(f"    ├── news/     (with sentiment scores, Parquet)")
    print(f"    └── fred/     (pivoted wide format, Parquet)")

    spark.stop()