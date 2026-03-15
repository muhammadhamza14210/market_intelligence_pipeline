#!/bin/bash
# =============================================================
# STEP 2A: Create Azure Data Lake Gen2
# =============================================================
# This script creates:
#   1. A Resource Group (logical container for Azure resources)
#   2. A Storage Account with Data Lake Gen2 (hierarchical namespace)
#   3. Three containers: bronze, silver, gold
# Run: bash setup_azure_storage.sh
# =============================================================

set -e  # Exit on any error

# --- CONFIG ---
RESOURCE_GROUP="rg-market-intelligence"
STORAGE_ACCOUNT="marketintelligencedl"   # must be globally unique, lowercase, no dashes
LOCATION="eastus"                         # cheapest region, change if you want

echo "=============================================="
echo "  AZURE DATA LAKE GEN2 SETUP"
echo "=============================================="

# --- 1. Create Resource Group ---
echo ""
echo "[1/4] Creating Resource Group: $RESOURCE_GROUP"
az group create \
    --name $RESOURCE_GROUP \
    --location $LOCATION \
    --output table

# --- 2. Create Storage Account with Data Lake Gen2 ---
echo ""
echo "[2/4] Creating Storage Account: $STORAGE_ACCOUNT"
echo "  (This enables hierarchical namespace = Data Lake Gen2)"
az storage account create \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hns true \
    --output table
# --hns true  = hierarchical namespace = what makes it a Data Lake Gen2
# --sku Standard_LRS = cheapest tier (locally redundant)

# --- 3. Create Bronze / Silver / Gold containers ---
echo ""
echo "[3/4] Creating containers: bronze, silver, gold"

# Get the storage account key
ACCOUNT_KEY=$(az storage account keys list \
    --account-name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query "[0].value" \
    --output tsv)

for CONTAINER in bronze silver gold; do
    echo "  Creating: $CONTAINER"
    az storage container create \
        --name $CONTAINER \
        --account-name $STORAGE_ACCOUNT \
        --account-key $ACCOUNT_KEY \
        --output none
done

# --- 4. Create folder structure in bronze ---
echo ""
echo "[4/4] Creating folder structure in bronze container"
# In ADLS Gen2, "folders" are created by uploading with a path prefix
# We'll create placeholder files to establish the structure
for FOLDER in stocks news fred; do
    echo "  Creating: bronze/$FOLDER/"
    echo "placeholder" | az storage blob upload \
        --container-name bronze \
        --name "$FOLDER/.keep" \
        --account-name $STORAGE_ACCOUNT \
        --account-key $ACCOUNT_KEY \
        --data "@-" \
        --overwrite \
        --output none 2>/dev/null || true
done

# --- Print summary ---
echo ""
echo "=============================================="
echo "  SETUP COMPLETE!"
echo "=============================================="
echo ""
echo "  Resource Group:   $RESOURCE_GROUP"
echo "  Storage Account:  $STORAGE_ACCOUNT"
echo "  Location:         $LOCATION"
echo "  Containers:       bronze, silver, gold"
echo ""
echo "  Bronze structure:"
echo "    bronze/"
echo "    ├── stocks/"
echo "    ├── news/"
echo "    └── fred/"
echo ""
echo "  Connection string (save this for Python upload script):"
az storage account show-connection-string \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query "connectionString" \
    --output tsv
echo ""