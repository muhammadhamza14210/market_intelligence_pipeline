"""
STEP 2B: Upload All Layers to Azure Data Lake
------------------------------------------------
Uploads Bronze (raw JSON), Silver (clean Parquet), and Gold (analytics Parquet)
to their respective containers in ADLS Gen2.
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

# Map local directories to (container, lake_folder) pairs
UPLOAD_MAP = {
    # --- BRONZE: raw JSON ---
    "data/raw/stocks": ("bronze", "stocks"),
    "data/raw/news": ("bronze", "news"),
    "data/raw/fred": ("bronze", "fred"),
    # --- SILVER: clean Parquet ---
    "data/silver/stocks": ("silver", "stocks"),
    "data/silver/news": ("silver", "news"),
    "data/silver/fred": ("silver", "fred"),
    # --- GOLD: analytics Parquet ---
    "data/gold/fct_daily_market_summary": ("gold", "fct_daily_market_summary"),
    "data/gold/dim_tickers": ("gold", "dim_tickers"),
    "data/gold/fct_sentiment_sensitivity": ("gold", "fct_sentiment_sensitivity"),
}


def get_datalake_client():
    """Create a Data Lake service client."""
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        print("ERROR: Run -> pip install azure-storage-file-datalake")
        sys.exit(1)

    if not STORAGE_ACCOUNT or not STORAGE_KEY:
        print("ERROR: Azure credentials not found in .env")
        print("Add to .env:")
        print("  AZURE_STORAGE_ACCOUNT=youraccountname")
        print("  AZURE_STORAGE_KEY=yourkey")
        sys.exit(1)

    account_url = f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=STORAGE_KEY)


def upload_file(file_system_client, local_path: str, lake_path: str) -> int:
    """Upload a single file to the Data Lake."""
    directory_name = os.path.dirname(lake_path)
    if directory_name:
        directory_client = file_system_client.get_directory_client(directory_name)
        try:
            directory_client.create_directory()
        except Exception:
            pass

    file_name = os.path.basename(lake_path)
    if directory_name:
        file_client = directory_client.get_file_client(file_name)
    else:
        file_client = file_system_client.get_file_client(file_name)

    with open(local_path, "rb") as f:
        file_data = f.read()
        file_client.upload_data(file_data, overwrite=True)

    return len(file_data)


def collect_files(local_dir: str) -> list:
    """
    Recursively collect all files in a directory.
    Handles both flat folders (JSON) and partitioned Parquet folders.
    Returns list of (local_path, relative_path) tuples.
    """
    files = []
    if not os.path.exists(local_dir):
        return files

    for root, dirs, filenames in os.walk(local_dir):
        # Skip hidden files and Spark metadata
        dirs[:] = [d for d in dirs if not d.startswith(".") and d != "_temporary"]
        for filename in filenames:
            if filename.startswith(".") or filename.startswith("_"):
                continue
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_dir)
            files.append((local_path, relative_path))

    return files


def upload_layer(client, container_name: str, local_dir: str, lake_folder: str) -> tuple:
    """Upload all files from a local directory to a lake folder."""
    file_system_client = client.get_file_system_client(container_name)
    files = collect_files(local_dir)

    if not files:
        return 0, 0

    layer_files = 0
    layer_bytes = 0

    for local_path, relative_path in files:
        lake_path = f"{lake_folder}/{relative_path}"
        size = upload_file(file_system_client, local_path, lake_path)
        layer_files += 1
        layer_bytes += size
        print(f"    ✓ {container_name}/{lake_path} ({size:,} bytes)")

    return layer_files, layer_bytes


def upload_all():
    """Upload all layers: Bronze, Silver, Gold."""
    print("=" * 60)
    print("  UPLOAD TO AZURE DATA LAKE — ALL LAYERS")
    print("=" * 60)
    print(f"  Account: {STORAGE_ACCOUNT}")
    print()

    client = get_datalake_client()

    total_files = 0
    total_bytes = 0
    layer_stats = {}

    current_container = None
    for local_dir, (container, lake_folder) in UPLOAD_MAP.items():
        if container != current_container:
            current_container = container
            print(f"\n  {'─' * 50}")
            print(f"  {container.upper()} LAYER")
            print(f"  {'─' * 50}")

        if not os.path.exists(local_dir):
            print(f"  SKIP: {local_dir}/ not found")
            continue

        print(f"\n  [{lake_folder}]")
        files, bytes_ = upload_layer(client, container, local_dir, lake_folder)

        if files == 0:
            print(f"    No files found in {local_dir}/")

        total_files += files
        total_bytes += bytes_

        # Track stats per layer
        if container not in layer_stats:
            layer_stats[container] = {"files": 0, "bytes": 0}
        layer_stats[container]["files"] += files
        layer_stats[container]["bytes"] += bytes_

    # --- Summary ---
    print(f"\n{'=' * 60}")
    print(f"  UPLOAD COMPLETE")
    print(f"{'=' * 60}")
    print(f"\n  Total: {total_files} files, {total_bytes / 1024:.1f} KB\n")
    for layer, stats in layer_stats.items():
        print(f"  {layer.upper():8s}: {stats['files']:3d} files, {stats['bytes'] / 1024:.1f} KB")
    print(f"\n  View in Azure Portal:")
    print(f"    Storage accounts > {STORAGE_ACCOUNT} > Containers")


def verify_upload():
    """List files in all containers to confirm upload."""
    print(f"\n{'=' * 60}")
    print(f"  VERIFYING UPLOAD")
    print(f"{'=' * 60}")

    client = get_datalake_client()

    for container in ["bronze", "silver", "gold"]:
        print(f"\n  [{container.upper()}]")
        try:
            fs = client.get_file_system_client(container)
            count = 0
            for path in fs.get_paths():
                if not path.name.endswith("/"):
                    print(f"    {path.name} ({path.content_length:,} bytes)")
                    count += 1
            if count == 0:
                print(f"    (empty)")
        except Exception as e:
            print(f"    ERROR: {e}")


if __name__ == "__main__":
    upload_all()
    verify_upload()