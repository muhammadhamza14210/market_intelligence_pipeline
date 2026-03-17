"""
STEP 3B: Silver → Gold Transformations
----------------------------------------
Joins stocks + news sentiment + FRED macro data into a unified
analytics-ready table. This is the Gold layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
import os


# --- SPARK SESSION ---
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("MarketIntelligence-SilverToGold")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- PATHS ---
SILVER_DIR = "data/silver"
GOLD_DIR = "data/gold"


def build_daily_market_summary():
    """
    Build the main Gold table: fct_daily_market_summary
    """
    print("=" * 60)
    print("  SILVER → GOLD: fct_daily_market_summary")
    print("=" * 60)

    # --- 1. Read Silver tables ---
    print("\n[1/4] Reading Silver tables...")

    stocks = spark.read.parquet(f"{SILVER_DIR}/stocks")
    print(f"  Stocks: {stocks.count()} rows")

    news = spark.read.parquet(f"{SILVER_DIR}/news")
    print(f"  News:   {news.count()} rows")

    fred = spark.read.parquet(f"{SILVER_DIR}/fred")
    print(f"  FRED:   {fred.count()} rows")

    # --- 2. Aggregate news by date ---
    print("\n[2/4] Aggregating daily news sentiment...")

    daily_news = (
        news
        .groupBy("date")
        .agg(
            # Count of articles
            F.count("*").alias("news_article_count"),

            # === THE KEY SENTIMENT METRICS ===

            # Sentiment ratio: (positive - negative) / total
            # Ranges from -1.0 (all negative) to +1.0 (all positive)
            # THIS is the metric that varies meaningfully per day
            F.round(
                (F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0))
                 - F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)))
                / F.count("*"),
                4
            ).alias("sentiment_ratio"),

            # Average positive headline strength (how confident the good news is)
            F.round(
                F.avg(
                    F.when(F.col("sentiment_label") == "positive", F.col("sentiment_score"))
                ), 4
            ).alias("avg_positive_strength"),

            # Average negative headline strength
            F.round(
                F.avg(
                    F.when(F.col("sentiment_label") == "negative", F.col("sentiment_score"))
                ), 4
            ).alias("avg_negative_strength"),

            # Counts by label
            F.sum(
                F.when(F.col("sentiment_label") == "positive", 1).otherwise(0)
            ).alias("positive_news_count"),
            F.sum(
                F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)
            ).alias("negative_news_count"),
            F.sum(
                F.when(F.col("sentiment_label") == "neutral", 1).otherwise(0)
            ).alias("neutral_news_count"),

            # Collect top headlines (for LLM summary later)
            F.collect_list("title").alias("headlines"),
        )
    )

    print("  Daily sentiment summary:")
    daily_news.select(
        "date", "news_article_count", "sentiment_ratio",
        "positive_news_count", "negative_news_count", "neutral_news_count"
    ).orderBy("date").show(10, truncate=False)

    # --- 3. Join everything on date ---
    print("[3/4] Joining stocks + news + FRED on date...")

    gold = (
        stocks
        .join(daily_news, on="date", how="left")
        .join(fred, on="date", how="left")
    )

    # --- 4. Add derived features ---
    print("[4/4] Adding derived features...")

    ticker_window = (
        Window
        .partitionBy("ticker")
        .orderBy("date")
    )

    rolling_7d = (
        Window
        .partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-6, 0)
    )

    gold = (
        gold
        .withColumn(
            "prev_close",
            F.lag("close", 1).over(ticker_window)
        )
        .withColumn(
            "overnight_gap_pct",
            F.round(
                (F.col("open") - F.col("prev_close")) / F.col("prev_close") * 100,
                4
            )
        )
        .withColumn(
            "rolling_7d_avg_close",
            F.round(F.avg("close").over(rolling_7d), 4)
        )
        .withColumn(
            "rolling_7d_avg_volume",
            F.round(F.avg("volume").over(rolling_7d), 0).cast("long")
        )
        .withColumn(
            "price_vs_7d_avg_pct",
            F.round(
                (F.col("close") - F.col("rolling_7d_avg_close"))
                / F.col("rolling_7d_avg_close") * 100,
                4
            )
        )
        .withColumn(
            "volume_spike_ratio",
            F.round(
                F.col("volume") / F.col("rolling_7d_avg_volume"),
                4
            )
        )
        .drop("prev_close", "headlines")
        .orderBy("date", "ticker")
    )

    # --- Write Gold table ---
    output_path = f"{GOLD_DIR}/fct_daily_market_summary"
    gold.write.mode("overwrite").partitionBy("ticker").parquet(output_path)

    # --- Summary ---
    print(f"\n{'=' * 60}")
    print(f"  GOLD TABLE: fct_daily_market_summary")
    print(f"{'=' * 60}")
    print(f"  Total rows:    {gold.count()}")
    print(f"  Columns:       {len(gold.columns)}")
    print(f"  Date range:    {gold.agg(F.min('date'), F.max('date')).collect()[0]}")

    print(f"\n  Sample data (days with sentiment):")
    gold.filter(F.col("sentiment_ratio").isNotNull()).select(
        "date", "ticker", "close", "daily_return",
        "news_article_count", "sentiment_ratio",
        "positive_news_count", "negative_news_count"
    ).filter(F.col("ticker") == "AAPL").orderBy("date").show(10, truncate=False)

    print(f"\n  ✓ Written to {output_path}")
    return gold


def build_dim_tickers(stocks_gold):
    """
    Build a dimension table for tickers.
    """
    print(f"\n{'=' * 60}")
    print(f"  GOLD TABLE: dim_tickers")
    print(f"{'=' * 60}")

    dim = (
        stocks_gold
        .groupBy("ticker")
        .agg(
            F.round(F.avg("close"), 2).alias("avg_close"),
            F.round(F.avg("daily_return"), 4).alias("avg_daily_return"),
            F.round(F.stddev("daily_return"), 4).alias("volatility"),
            F.round(F.avg("volume"), 0).cast("long").alias("avg_volume"),
            F.min("date").alias("first_date"),
            F.max("date").alias("last_date"),
            F.count("*").alias("trading_days"),
            F.sum(
                F.when(F.col("sentiment_ratio").isNotNull(), 1).otherwise(0)
            ).alias("days_with_sentiment"),
        )
        .orderBy("ticker")
    )

    output_path = f"{GOLD_DIR}/dim_tickers"
    dim.write.mode("overwrite").parquet(output_path)

    dim.show(truncate=False)
    print(f"  ✓ Written to {output_path}")
    return dim


def build_sentiment_sensitivity(stocks_gold):
    """
    Build a sentiment sensitivity table.
    Uses sentiment_ratio (-1 to +1) to bucket days as
    positive, negative, or neutral news days.
    Then compares how each ticker performed on each type of day.
    """
    print(f"\n{'=' * 60}")
    print(f"  GOLD TABLE: fct_sentiment_sensitivity")
    print(f"{'=' * 60}")

    # Filter to only days with sentiment data
    with_sentiment = stocks_gold.filter(
        F.col("sentiment_ratio").isNotNull()
    )

    # Classify days using sentiment_ratio
    # ratio > 0 = more positive than negative headlines
    # ratio < 0 = more negative than positive headlines
    classified = with_sentiment.withColumn(
        "sentiment_day_type",
        F.when(F.col("sentiment_ratio") > 0, "bullish_news_day")
        .when(F.col("sentiment_ratio") < 0, "bearish_news_day")
        .otherwise("mixed_news_day")
    )

    # Show the day classification
    print("  Day classification:")
    classified.select("date", "sentiment_ratio", "sentiment_day_type").distinct().orderBy("date").show(10)

    # Per ticker: compare returns on bullish vs bearish news days
    sensitivity = (
        classified
        .groupBy("ticker")
        .agg(
            # Average return on bullish news days
            F.round(
                F.avg(
                    F.when(F.col("sentiment_day_type") == "bullish_news_day", F.col("daily_return"))
                ), 4
            ).alias("avg_return_bullish_news"),
            # Average return on bearish news days
            F.round(
                F.avg(
                    F.when(F.col("sentiment_day_type") == "bearish_news_day", F.col("daily_return"))
                ), 4
            ).alias("avg_return_bearish_news"),
            # Average return on mixed days
            F.round(
                F.avg(
                    F.when(F.col("sentiment_day_type") == "mixed_news_day", F.col("daily_return"))
                ), 4
            ).alias("avg_return_mixed_news"),
            # Total sentiment days
            F.count("*").alias("sentiment_days"),
            # Count by day type
            F.sum(
                F.when(F.col("sentiment_day_type") == "bullish_news_day", 1).otherwise(0)
            ).alias("bullish_day_count"),
            F.sum(
                F.when(F.col("sentiment_day_type") == "bearish_news_day", 1).otherwise(0)
            ).alias("bearish_day_count"),
            # Reactivity: average absolute return (higher = more volatile)
            F.round(F.avg(F.abs(F.col("daily_return"))), 4).alias("avg_abs_return"),
            # Extremes
            F.round(F.min("daily_return"), 4).alias("worst_day"),
            F.round(F.max("daily_return"), 4).alias("best_day"),
        )
        # Sensitivity: how much more does it return on bullish vs bearish days
        .withColumn(
            "sentiment_sensitivity",
            F.round(
                F.coalesce(F.col("avg_return_bullish_news"), F.lit(0))
                - F.coalesce(F.col("avg_return_bearish_news"), F.lit(0)),
                4
            )
        )
        .orderBy(F.col("sentiment_sensitivity").desc())
    )

    output_path = f"{GOLD_DIR}/fct_sentiment_sensitivity"
    sensitivity.write.mode("overwrite").parquet(output_path)

    print(f"  Records: {sensitivity.count()}")
    print(f"\n  Ticker reaction to news sentiment:")
    sensitivity.select(
        "ticker", "avg_return_bullish_news", "avg_return_bearish_news",
        "sentiment_sensitivity", "avg_abs_return", "worst_day", "best_day"
    ).show(20, truncate=False)
    print(f"  ✓ Written to {output_path}")
    return sensitivity


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  STEP 3B: SILVER → GOLD TRANSFORMATIONS")
    print("=" * 60 + "\n")

    os.makedirs(GOLD_DIR, exist_ok=True)

    gold = build_daily_market_summary()
    dim = build_dim_tickers(gold)
    sensitivity = build_sentiment_sensitivity(gold)

    print(f"\n{'=' * 60}")
    print(f"  ALL GOLD LAYERS COMPLETE")
    print(f"{'=' * 60}")
    print(f"\n  Output: {GOLD_DIR}/")
    print(f"    ├── fct_daily_market_summary/     (one row per ticker per day)")
    print(f"    ├── dim_tickers/                  (summary stats per ticker)")
    print(f"    └── fct_sentiment_sensitivity/    (how tickers react to sentiment)")
    spark.stop()