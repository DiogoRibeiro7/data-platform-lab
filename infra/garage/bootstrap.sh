#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

compose() {
  docker compose -f "$REPO_ROOT/compose.yaml" "$@"
}

compose up -d garage

attempt=0
while ! compose exec -T garage /garage stats -a >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then
    echo "Garage did not become ready" >&2
    compose logs garage >&2 || true
    exit 1
  fi
  sleep 1
done

echo "Garage local S3 is ready at http://localhost:3900"
echo "Bucket: data-platform-lab"
echo "Load client credentials with: source infra/garage/dev.env"
