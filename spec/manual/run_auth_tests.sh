#!/usr/bin/env bash
#
# Run all manual auth tests against docker-compose.auth-test.yml containers.
#
# Usage:
#   spec/manual/run_auth_tests.sh          # build and run
#   spec/manual/run_auth_tests.sh --skip-build  # run only (binaries must exist)
#
# Prerequisites:
#   docker compose -f docker-compose.auth-test.yml up -d
#   Wait ~15s for MySQL containers to initialize.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="${TMPDIR:-/tmp}/crystal-mysql-auth-tests"

TESTS=(
  auth_native_test
  auth_caching_sha2_test
  auth_switch_test
  auth_sha256_test
  auth_clear_password_test
)

skip_build=false
if [[ "${1:-}" == "--skip-build" ]]; then
  skip_build=true
fi

# Check containers are running
if ! docker compose -f "$PROJECT_DIR/docker-compose.auth-test.yml" ps --format '{{.Service}}' 2>/dev/null | grep -q mysql80; then
  echo "ERROR: Docker containers not running. Start them with:"
  echo "  docker compose -f docker-compose.auth-test.yml up -d"
  echo "  # wait ~15s for init"
  exit 1
fi

mkdir -p "$BUILD_DIR"

# Build
if [[ "$skip_build" == false ]]; then
  echo "Building test binaries..."
  for test in "${TESTS[@]}"; do
    echo "  $test"
    crystal build "$SCRIPT_DIR/$test.cr" -o "$BUILD_DIR/$test" 2>&1
  done
  echo ""
fi

# Run
pass=0
fail=0
total=0

for test in "${TESTS[@]}"; do
  binary="$BUILD_DIR/$test"
  if [[ ! -x "$binary" ]]; then
    echo "SKIP: $test (not built)"
    continue
  fi

  echo "=== $test ==="
  while IFS= read -r line; do
    total=$((total + 1))
    if [[ "$line" == PASS:* ]]; then
      pass=$((pass + 1))
    elif [[ "$line" == FAIL:* ]]; then
      fail=$((fail + 1))
    fi
    echo "  $line"
  done < <("$binary" 2>&1)
  echo ""
done

# Summary
echo "==============================="
echo "Results: $pass passed, $fail failed, $total total"
echo "==============================="

if [[ $fail -gt 0 ]]; then
  exit 1
fi
