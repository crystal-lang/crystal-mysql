#!/usr/bin/env bash
#
# Run all manual auth tests against docker-compose.auth-test.yml containers.
#
# Usage:
#   spec/manual/run_auth_tests.sh
#
# Prerequisites:
#   docker compose -f docker-compose.auth-test.yml up -d
#   Wait ~15s for MySQL containers to initialize.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Check containers are running
if ! docker compose -f "$PROJECT_DIR/docker-compose.auth-test.yml" ps --format '{{.Service}}' 2>/dev/null | grep -q mysql80; then
  echo "ERROR: Docker containers not running. Start them with:"
  echo "  docker compose -f docker-compose.auth-test.yml up -d"
  echo "  # wait ~15s for init"
  exit 1
fi

cd "$PROJECT_DIR"
crystal spec spec/manual/*_test.cr
