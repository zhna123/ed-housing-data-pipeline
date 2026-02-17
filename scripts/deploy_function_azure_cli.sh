#!/usr/bin/env bash
set -euo pipefail

APP_NAME="eh-data-process-app"
RESOURCE_GROUP="na-adf-test"

if ! command -v az >/dev/null 2>&1; then
  echo "Azure CLI (az) is required."
  exit 1
fi

if ! command -v zip >/dev/null 2>&1; then
  echo "zip is required."
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$(mktemp -d)"
ZIP_PATH="${BUILD_DIR}/function_package.zip"

cleanup() {
  rm -rf "${BUILD_DIR}"
}
trap cleanup EXIT

cd "${ROOT_DIR}"

zip -r "${ZIP_PATH}" . \
  -x ".git/*" \
  -x ".venv/*" \
  -x "venv/*" \
  -x "__pycache__/*" \
  -x "*/__pycache__/*" \
  -x "*/*/__pycache__/*" \
  -x "*.pyc" \
  -x "*/*.pyc" \
  -x "*/*/*.pyc" \
  -x "data/*" \
  -x "logs/*" \
  -x "local.settings.json" \
  -x "pipelines/*" \
  -x "infra/*" \
  -x "docs/*" \
  -x "duckdb_viewer/*"

echo "Deploying function package..."

az functionapp deployment source config-zip \
  --name "${APP_NAME}" \
  --resource-group "${RESOURCE_GROUP}" \
  --src "${ZIP_PATH}" \
  --output none

echo "Package uploaded. Syncing function triggers..."

SUBSCRIPTION_ID=$(az account show --query id -o tsv)
SYNC_URI="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Web/sites/${APP_NAME}/syncfunctiontriggers?api-version=2022-03-01"

if az rest --method post --uri "${SYNC_URI}" --output none; then
  echo "Function triggers synced successfully."
else
  echo "Warning: Trigger sync may have failed. You can manually sync with:"
  echo "  az rest --method post --uri \"${SYNC_URI}\""
fi

echo ""
echo "Deployment complete for ${APP_NAME}."
echo ""
echo "Deployed functions:"
az functionapp function list \
  --name "${APP_NAME}" \
  --resource-group "${RESOURCE_GROUP}" \
  --query "[].{Name:name, Enabled:(!isDisabled), URL:invokeUrlTemplate}" \
  --output table
