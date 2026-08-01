#!/usr/bin/env bash
# Deploy the EuroStream cookbook to Cloudflare Pages.
#
# Prerequisites:
#   npm install          (in this directory)
#   npx wrangler login   (once)
#
# Usage:
#   ./deploy.sh                     # production
#   ./deploy.sh --branch staging    # preview branch
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "Building the site..."
npm run build

PROJECT_NAME=$(grep -oP '^name\s*=\s*"\K[^"]+' wrangler.toml)

if ! command -v wrangler &>/dev/null; then
    echo "Installing wrangler..."
    npm install --no-save wrangler
fi

if [[ "${1:-}" == "--branch" && -n "${2:-}" ]]; then
    echo "Deploying to preview branch: $2"
    npx wrangler pages deploy dist --project-name "$PROJECT_NAME" --branch "$2"
else
    echo "Deploying to production..."
    npx wrangler pages deploy dist --project-name "$PROJECT_NAME"
fi

echo ""
echo "Done! Check your Cloudflare Pages dashboard for the live URL."
