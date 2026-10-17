#!/usr/bin/env bash
# The packaged command, end to end, on a fresh store (mvn package first). CI runs this, and so does the release
# check; the replication demo (scripts/replication-demo.sh) does the same for serve, follow and client.
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
keystore="$here/scripts/keystore"
store="$here/target/smoke-store"
rm -rf "$store"

"$keystore" version
"$keystore" put "$store" city Boston
"$keystore" flush "$store"
test "$("$keystore" get "$store" city)" = Boston
"$keystore" delete "$store" city
"$keystore" flush "$store"
"$keystore" compact "$store"
# A deleted key is "not found" (exit 1), which an error would also exit with: check which. The last line, since a
# JVM can print a notice first ("Picked up JAVA_TOOL_OPTIONS").
if out="$("$keystore" get "$store" city 2>&1)"; then echo "city should be deleted" >&2; exit 1; fi
[[ "$out" == *"keystore: city: not found" ]]
"$keystore" dump "$store"
"$keystore" verify "$store"
rm -rf "$store-snapshot"
"$keystore" put "$store" river Charles
"$keystore" snapshot "$store" "$store-snapshot"
test "$("$keystore" get "$store-snapshot" river)" = Charles
echo "smoke: ok"
