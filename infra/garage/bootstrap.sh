#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

compose() {
  docker compose -f "$REPO_ROOT/compose.yaml" "$@"
}

s3_ready() {
  python3 - <<'PY'
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

try:
    with urlopen("http://127.0.0.1:3900/", timeout=2) as response:
        status = response.status
except HTTPError as exc:
    status = exc.code
except (URLError, OSError):
    raise SystemExit(1)

raise SystemExit(0 if 200 <= status < 500 else 1)
PY
}

compose up -d garage

attempt=0
while ! s3_ready; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 120 ]; then
    echo "Garage S3 API did not become ready" >&2
    compose logs garage >&2 || true
    exit 1
  fi
  sleep 1
done

echo "Garage local S3 is ready at http://localhost:3900"
echo "Bucket: data-platform-lab"
echo "Load client credentials with: . \"$REPO_ROOT/infra/garage/dev.env\""
