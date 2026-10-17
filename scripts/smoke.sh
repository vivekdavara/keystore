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
if "$keystore" get "$store" city; then echo "city should be deleted" >&2; exit 1; fi
"$keystore" dump "$store"
"$keystore" verify "$store"
rm -rf "$store-snapshot"
"$keystore" put "$store" river Charles
"$keystore" snapshot "$store" "$store-snapshot"
test "$("$keystore" get "$store-snapshot" river)" = Charles
echo "smoke: ok"
