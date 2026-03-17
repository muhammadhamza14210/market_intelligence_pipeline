"""
STEP 4A: Load Gold Parquet into DuckDB
----------------------------------------
This script loads Gold Parquet files into a local DuckDB database.
"""

import duckdb
import os

GOLD_DIR = "data/gold"
DB_PATH = "data/market_intelligence.duckdb"


def load_gold_to_duckdb():
    print("=" * 60)
    print("  LOADING GOLD LAYER INTO DUCKDB")
    print("=" * 60)

    # Create/connect to DuckDB file
    con = duckdb.connect(DB_PATH)

    # Create a schema for gold tables
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    # --- 1. fct_daily_market_summary ---
    path = f"{GOLD_DIR}/fct_daily_market_summary"
    if os.path.exists(path):
        con.execute(f"""
            CREATE OR REPLACE TABLE gold.fct_daily_market_summary AS
            SELECT * FROM read_parquet('{path}/**/*.parquet', hive_partitioning=true)
        """)
        count = con.execute("SELECT COUNT(*) FROM gold.fct_daily_market_summary").fetchone()[0]
        print(f"\n  ✓ fct_daily_market_summary: {count} rows")
        print(con.execute("SELECT * FROM gold.fct_daily_market_summary LIMIT 3").df())
    else:
        print(f"  SKIP: {path} not found")

    # --- 2. dim_tickers ---
    path = f"{GOLD_DIR}/dim_tickers"
    if os.path.exists(path):
        con.execute(f"""
            CREATE OR REPLACE TABLE gold.dim_tickers AS
            SELECT * FROM read_parquet('{path}/*.parquet')
        """)
        count = con.execute("SELECT COUNT(*) FROM gold.dim_tickers").fetchone()[0]
        print(f"\n  ✓ dim_tickers: {count} rows")
        print(con.execute("SELECT * FROM gold.dim_tickers LIMIT 3").df())
    else:
        print(f"  SKIP: {path} not found")

    # --- 3. fct_sentiment_sensitivity ---
    path = f"{GOLD_DIR}/fct_sentiment_sensitivity"
    if os.path.exists(path):
        con.execute(f"""
            CREATE OR REPLACE TABLE gold.fct_sentiment_sensitivity AS
            SELECT * FROM read_parquet('{path}/*.parquet')
        """)
        count = con.execute("SELECT COUNT(*) FROM gold.fct_sentiment_sensitivity").fetchone()[0]
        print(f"\n  ✓ fct_sentiment_sensitivity: {count} rows")
        print(con.execute("SELECT * FROM gold.fct_sentiment_sensitivity LIMIT 3").df())
    else:
        print(f"  SKIP: {path} not found")

    # --- Summary ---
    tables = con.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'gold'
    """).fetchall()

    print(f"\n{'=' * 60}")
    print(f"  DUCKDB READY")
    print(f"{'=' * 60}")
    print(f"  Database: {DB_PATH}")
    print(f"  Tables:")
    for schema, table in tables:
        print(f"    {schema}.{table}")
    print(f"\n  Next: Run dbt models on top of this")

    con.close()


if __name__ == "__main__":
    load_gold_to_duckdb()