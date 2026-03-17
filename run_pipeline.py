"""
MASTER PIPELINE RUNNER
-----------------------
Runs the entire pipeline end to end:
  Extract → Upload Bronze → Transform Silver → Transform Gold → Load DuckDB → dbt
Usage:
  python run_pipeline.py              # run everything
  python run_pipeline.py --from silver  # start from silver step
  python run_pipeline.py --from gold    # start from gold step
  python run_pipeline.py --from dbt     # just re-run dbt
"""

import subprocess
import sys
import time
from datetime import datetime


STEPS = {
    "extract": [
        ("Extract Stocks", "python -m ingestion.extract_stocks"),
        ("Extract News", "python -m ingestion.extract_news"),
        ("Extract FRED", "python -m ingestion.extract_fred"),
    ],
    "upload": [
        ("Upload to Azure Data Lake", "python -m azure.upload_to_lake"),
    ],
    "silver": [
        ("Bronze → Silver (+ FinBERT)", "python -m spark.bronze_to_silver"),
    ],
    "gold": [
        ("Silver → Gold", "python -m spark.silver_to_gold"),
    ],
    "duckdb": [
        ("Load Gold → DuckDB", "python -m spark.load_to_duckdb"),
    ],
    "dbt": [
        ("dbt deps", "cd dbt && dbt deps"),
        ("dbt run", "cd dbt && dbt run"),
        ("dbt test", "cd dbt && dbt test"),
    ],
}

STEP_ORDER = ["extract", "upload", "silver", "gold", "duckdb", "dbt"]


def run_command(name: str, command: str) -> bool:
    """Run a shell command and return success/failure."""
    print(f"\n  ▶ {name}")
    print(f"    Command: {command}")
    start = time.time()

    result = subprocess.run(
        command,
        shell=True,
        capture_output=False,
    )

    elapsed = time.time() - start
    if result.returncode == 0:
        print(f"    ✓ Done ({elapsed:.1f}s)")
        return True
    else:
        print(f"    ✗ FAILED (exit code {result.returncode})")
        return False


def run_pipeline(start_from: str = "extract"):
    """Run the pipeline from a given step."""
    print("=" * 60)
    print(f"  MARKET INTELLIGENCE PIPELINE")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Find starting index
    try:
        start_idx = STEP_ORDER.index(start_from)
    except ValueError:
        print(f"  ERROR: Unknown step '{start_from}'")
        print(f"  Available: {', '.join(STEP_ORDER)}")
        sys.exit(1)

    steps_to_run = STEP_ORDER[start_idx:]
    print(f"  Running steps: {' → '.join(steps_to_run)}")

    total_start = time.time()
    results = {}

    for step_name in steps_to_run:
        print(f"\n{'─' * 60}")
        print(f"  STEP: {step_name.upper()}")
        print(f"{'─' * 60}")

        step_success = True
        for task_name, command in STEPS[step_name]:
            success = run_command(task_name, command)
            if not success:
                step_success = False
                print(f"\n  ⚠ Step '{step_name}' failed at '{task_name}'")
                print(f"  Pipeline stopped. Fix the error and re-run with:")
                print(f"    python run_pipeline.py --from {step_name}")
                sys.exit(1)

        results[step_name] = step_success

    # Summary
    total_elapsed = time.time() - total_start
    print(f"\n{'=' * 60}")
    print(f"  PIPELINE COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Total time: {total_elapsed:.1f}s")
    print(f"  Steps run:")
    for step, success in results.items():
        status = "✓" if success else "✗"
        print(f"    {status} {step}")
    print()


if __name__ == "__main__":
    start_from = "extract"

    if "--from" in sys.argv:
        idx = sys.argv.index("--from")
        if idx + 1 < len(sys.argv):
            start_from = sys.argv[idx + 1]

    run_pipeline(start_from)