#!/usr/bin/env bash
# Optional Bundle helper for the Databricks portfolio showcase.
# The primary and authoritative setup path is the private UI runbook in
# docs/databricks/. This script never creates databases, applies SQL, or runs
# Workflows implicitly.

set -euo pipefail

TARGET="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_DIR="$(cd "${SCRIPT_DIR}/../dab" && pwd)"

if ! command -v databricks >/dev/null 2>&1; then
  echo "error: the current Databricks CLI is required" >&2
  echo "install it from https://docs.databricks.com/en/dev-tools/cli/install.html" >&2
  exit 127
fi

case "${TARGET}" in
  '' | *[!A-Za-z0-9_-]*)
    echo "error: target must contain only letters, numbers, underscore, or hyphen" >&2
    exit 2
    ;;
esac

cat <<EOF
Optional Bundle deployment
  target:  ${TARGET}
  bundle:  ${BUNDLE_DIR}

This does not bootstrap Unity Catalog, publish pipelines, or run Jobs.
Complete those steps manually first and retain the UI-created resources as
the source of truth until the Bundle has passed a non-production rehearsal.
EOF

echo "==> Validating Bundle"
databricks bundle validate --target "${TARGET}" --dir "${BUNDLE_DIR}"

echo "==> Deploying Bundle"
databricks bundle deploy --target "${TARGET}" --dir "${BUNDLE_DIR}"

echo "==> Bundle deployment complete"
echo "Open Jobs & Pipelines and Apps in the workspace to review resources and runs."
echo "Private UI runbook: docs/databricks/00-index.md"
