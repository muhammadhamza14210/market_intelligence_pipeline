"""
STEP 2B: Upload Raw Data to Azure Data Lake (Bronze)
------------------------------------------------------
Uploads local JSON files to the Bronze container in ADLS Gen2.
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
CONTAINER_NAME = "bronze"
LOCAL_RAW_DIR = "data/raw"

SOURCE_FOLDERS = {
    "stocks": "stocks",
    "news": "news",
    "fred": "fred",
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
        print("Run setup_azure_lake.sh first, then add to .env:")
        print("  AZURE_STORAGE_ACCOUNT=youraccountname")
        print("  AZURE_STORAGE_KEY=yourkey")
        sys.exit(1)

    account_url = f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=STORAGE_KEY)


def upload_file(file_system_client, local_path: str, lake_path: str) -> int:
    """Upload a single file to the Data Lake."""
    directory_name = os.path.dirname(lake_path)
    directory_client = file_system_client.get_directory_client(directory_name)

    try:
        directory_client.create_directory()
    except Exception:
        pass  # already exists

    file_name = os.path.basename(lake_path)
    file_client = directory_client.get_file_client(file_name)

    with open(local_path, "rb") as f:
        file_data = f.read()
        file_client.upload_data(file_data, overwrite=True)

    return len(file_data)


def upload_all():
    """Upload all raw JSON files to Bronze."""
    print("=" * 60)
    print("  UPLOAD TO AZURE DATA LAKE — BRONZE")
    print("=" * 60)
    print(f"  Account:   {STORAGE_ACCOUNT}")
    print(f"  Container: {CONTAINER_NAME}")
    print()

    client = get_datalake_client()
    file_system_client = client.get_file_system_client(CONTAINER_NAME)

    total_files = 0
    total_bytes = 0

    for source_folder, lake_folder in SOURCE_FOLDERS.items():
        local_dir = os.path.join(LOCAL_RAW_DIR, source_folder)

        if not os.path.exists(local_dir):
            print(f"  SKIP: {local_dir}/ not found")
            continue

        files = [f for f in os.listdir(local_dir) if f.endswith(".json")]
        if not files:
            print(f"  SKIP: No JSON files in {local_dir}/")
            continue

        print(f"  [{lake_folder}]")
        for filename in sorted(files):
            local_path = os.path.join(local_dir, filename)
            lake_path = f"{lake_folder}/{filename}"

            size = upload_file(file_system_client, local_path, lake_path)
            total_files += 1
            total_bytes += size
            print(f"    ✓ {lake_path} ({size:,} bytes)")

    print()
    print("=" * 60)
    print("  ✅ UPLOAD COMPLETE")
    print("=" * 60)
    print(f"  Files: {total_files}")
    print(f"  Size:  {total_bytes:,} bytes ({total_bytes/1024:.1f} KB)")
    print()
    print("  View in Azure Portal:")
    print(f"    Storage accounts > {STORAGE_ACCOUNT} > Containers > bronze")


def verify_upload():
    """List files in Bronze to confirm upload."""
    print("\nVerifying...")
    client = get_datalake_client()
    fs = client.get_file_system_client(CONTAINER_NAME)
    for path in fs.get_paths():
        print(f"  {path.name} ({path.content_length:,} bytes)")


if __name__ == "__main__":
    upload_all()
    print()
    verify_upload()