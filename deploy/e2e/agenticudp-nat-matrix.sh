#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${ENTROPYOPS_URL:-http://localhost:8000}"
TENANT="${ENTROPYOPS_TENANT:-default}"
EXPECTED_MIN="${EXPECTED_ESTABLISHMENT_PCT:-95}"

echo "=== AgenticUDP NAT topology matrix check ==="
echo "Target: $BASE_URL (tenant=$TENANT)"

payload="$(curl -s --max-time 10 "$BASE_URL/api/agenticudp/traversal?tenant=$TENANT" || true)"
if [ -z "$payload" ]; then
  echo "ERROR: unable to query traversal diagnostics endpoint"
  exit 1
fi

python3 - "$payload" "$EXPECTED_MIN" <<'PY'
import json
import sys

data = json.loads(sys.argv[1])
target = float(sys.argv[2])
if not data.get("enabled"):
    print("SKIP: AgenticUDP disabled in this deployment")
    sys.exit(0)

diag = data.get("diagnostics", {})
stats = diag.get("stats", {})
accepted = float(stats.get("datagrams_accepted", 0))
invalid = float(stats.get("packets_invalid", 0))
checksum = float(stats.get("checksum_fails", 0))
den = accepted + invalid + checksum
est = 100.0 if den <= 0 else (accepted / den) * 100.0

print(f"Traversal mode: {stats.get('traversal_mode', 'unknown')}")
print(f"NAT distribution: {diag.get('nat_type_distribution', {})}")
print(f"Traversal distribution: {diag.get('traversal_distribution', {})}")
print(f"Relay endpoint: {diag.get('relay_endpoint', '')}")
print(f"Estimated session establishment: {est:.2f}%")

if est < target:
    print(f"FAIL: establishment {est:.2f}% < target {target:.2f}%")
    sys.exit(1)
print("PASS: establishment target met")
PY
