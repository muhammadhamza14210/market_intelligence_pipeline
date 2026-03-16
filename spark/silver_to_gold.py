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

    One row per ticker per date with:
    - Stock price data (from silver/stocks)
    - News sentiment aggregates (from silver/news)
    - Macro economic indicators (from silver/fred)
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
    # Multiple articles per day → we need daily aggregates
    print("\n[2/4] Aggregating daily news sentiment...")

    daily_news = (
        news
        .groupBy("date")
        .agg(
            # Count of articles
            F.count("*").alias("news_article_count"),
            # Average sentiment score
            F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment_score"),
            # Count by sentiment type
            F.sum(
                F.when(F.col("sentiment_label") == "positive", 1).otherwise(0)
            ).alias("positive_news_count"),
            F.sum(
                F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)
            ).alias("negative_news_count"),
            F.sum(
                F.when(F.col("sentiment_label") == "neutral", 1).otherwise(0)
            ).alias("neutral_news_count"),
            # Sentiment ratio: positive / total (higher = more bullish news)
            F.round(
                F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0))
                / F.count("*"),
                4
            ).alias("positive_sentiment_ratio"),
            # Collect top headlines (for LLM summary later)
            F.collect_list("title").alias("headlines"),
        )
    )

    daily_news.show(5, truncate=False)

    # --- 3. Join everything on date ---
    print("[3/4] Joining stocks + news + FRED on date...")

    # Start with stocks (our fact grain: one row per ticker per day)
    gold = (
        stocks
        # Left join daily news aggregates
        .join(daily_news, on="date", how="left")
        # Left join FRED macro indicators
        .join(fred, on="date", how="left")
    )

    # --- 4. Add derived features ---
    print("[4/4] Adding derived features...")

    # Window for rolling calculations per ticker
    ticker_window = (
        Window
        .partitionBy("ticker")
        .orderBy("date")
    )

    # Rolling 7-day window
    rolling_7d = (
        Window
        .partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-6, 0)
    )

    gold = (
        gold
        # Previous day close (for calculating overnight gaps)
        .withColumn(
            "prev_close",
            F.lag("close", 1).over(ticker_window)
        )
        # Overnight gap: (today's open - yesterday's close) / yesterday's close
        .withColumn(
            "overnight_gap_pct",
            F.round(
                (F.col("open") - F.col("prev_close")) / F.col("prev_close") * 100,
                4
            )
        )
        # 7-day rolling average close
        .withColumn(
            "rolling_7d_avg_close",
            F.round(F.avg("close").over(rolling_7d), 4)
        )
        # 7-day rolling average volume
        .withColumn(
            "rolling_7d_avg_volume",
            F.round(F.avg("volume").over(rolling_7d), 0).cast("long")
        )
        # 7-day rolling average sentiment
        .withColumn(
            "rolling_7d_avg_sentiment",
            F.round(F.avg("avg_sentiment_score").over(rolling_7d), 4)
        )
        # Price vs rolling average (is it trading above or below trend?)
        .withColumn(
            "price_vs_7d_avg_pct",
            F.round(
                (F.col("close") - F.col("rolling_7d_avg_close"))
                / F.col("rolling_7d_avg_close") * 100,
                4
            )
        )
        # Volume spike: today's volume vs 7-day average
        .withColumn(
            "volume_spike_ratio",
            F.round(
                F.col("volume") / F.col("rolling_7d_avg_volume"),
                4
            )
        )
        # Drop helper column
        .drop("prev_close", "headlines")
        # Final ordering
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
    print(f"  Tickers:       {[r.ticker for r in gold.select('ticker').distinct().collect()]}")
    print(f"\n  Column list:")
    for col in gold.columns:
        print(f"    - {col}")

    print(f"\n  Sample data:")
    gold.select(
        "date", "ticker", "close", "daily_return",
        "news_article_count", "avg_sentiment_score",
        "rolling_7d_avg_close", "volume_spike_ratio"
    ).show(10, truncate=False)

    print(f"\n  ✓ Written to {output_path}")
    return gold


def build_dim_tickers(stocks_gold):
    """
    Build a dimension table for tickers.
    Summary stats per ticker — no avg_sentiment since it's market-wide
    and would be identical for every ticker.
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
            # Count days with sentiment data available
            F.sum(
                F.when(F.col("avg_sentiment_score").isNotNull(), 1).otherwise(0)
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
    Shows how each ticker reacts to market-wide sentiment.

    Same sentiment day, different stock reactions = the insight.
    A ticker with high sensitivity swings more on sentiment shifts.

    Only uses days where sentiment data is available.
    """
    print(f"\n{'=' * 60}")
    print(f"  GOLD TABLE: fct_sentiment_sensitivity")
    print(f"{'=' * 60}")

    # Filter to only days with sentiment data
    with_sentiment = stocks_gold.filter(
        F.col("avg_sentiment_score").isNotNull()
    )

    # Classify sentiment days
    classified = with_sentiment.withColumn(
        "sentiment_bucket",
        F.when(F.col("avg_sentiment_score") >= 0.85, "high_positive")
        .when(F.col("avg_sentiment_score") >= 0.70, "moderate_positive")
        .when(F.col("avg_sentiment_score") >= 0.50, "neutral")
        .otherwise("negative")
    )

    # Per ticker: avg return on positive vs negative sentiment days
    sensitivity = (
        classified
        .groupBy("ticker")
        .agg(
            # Average return on high positive sentiment days
            F.round(
                F.avg(
                    F.when(F.col("sentiment_bucket") == "high_positive", F.col("daily_return"))
                ), 4
            ).alias("avg_return_high_sentiment"),
            # Average return on moderate/low sentiment days
            F.round(
                F.avg(
                    F.when(F.col("sentiment_bucket") != "high_positive", F.col("daily_return"))
                ), 4
            ).alias("avg_return_low_sentiment"),
            # How many sentiment days we have
            F.count("*").alias("sentiment_days"),
            # Average absolute daily return (measures overall reactivity)
            F.round(F.avg(F.abs(F.col("daily_return"))), 4).alias("avg_abs_return"),
            # Correlation hint: avg return * avg sentiment (positive = moves with sentiment)
            F.round(F.avg(F.col("daily_return")), 4).alias("avg_return_on_sentiment_days"),
            # Max drop and max gain on sentiment days
            F.round(F.min("daily_return"), 4).alias("worst_day"),
            F.round(F.max("daily_return"), 4).alias("best_day"),
        )
        # Sensitivity score: difference between high and low sentiment day returns
        .withColumn(
            "sentiment_sensitivity",
            F.round(
                F.col("avg_return_high_sentiment") - F.col("avg_return_low_sentiment"),
                4
            )
        )
        .orderBy(F.col("avg_abs_return").desc())
    )

    output_path = f"{GOLD_DIR}/fct_sentiment_sensitivity"
    sensitivity.write.mode("overwrite").parquet(output_path)

    print(f"  Records: {sensitivity.count()}")
    print(f"\n  Most reactive tickers to sentiment:")
    sensitivity.select(
        "ticker", "avg_return_high_sentiment", "avg_return_low_sentiment",
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

    # Build fact table
    gold = build_daily_market_summary()

    # Build dimension table
    dim = build_dim_tickers(gold)

    # Build sentiment sensitivity analysis
    sensitivity = build_sentiment_sensitivity(gold)

    print(f"\n{'=' * 60}")
    print(f"  ALL GOLD LAYERS COMPLETE")
    print(f"{'=' * 60}")
    print(f"\n  Output: {GOLD_DIR}/")
    print(f"    ├── fct_daily_market_summary/     (one row per ticker per day)")
    print(f"    ├── dim_tickers/                  (summary stats per ticker)")
    print(f"    └── fct_sentiment_sensitivity/    (how tickers react to sentiment)")
    print(f"\n  Your data journey:")
    print(f"    APIs → Bronze (raw JSON) → Silver (clean Parquet) → Gold (joined + features)")
    print(f"\n  Next: Step 4 - dbt models on the Gold layer")

    spark.stop()