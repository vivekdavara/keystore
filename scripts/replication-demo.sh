#!/usr/bin/env bash
# A leader on 8104 and a follower on 8105, run from the packaged jar (mvn package first): a write
# to the leader is read back from the follower, both stop cleanly on SIGTERM, and the follower's
# directory has the write. CI runs this; so can you.
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
keystore="$here/scripts/keystore"
work="$here/target/replication-demo"
rm -rf "$work"
leader_pid=""
follower_pid=""
cleanup() {
  for pid in $follower_pid $leader_pid; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

"$keystore" serve "$work/leader" 8104 &
leader_pid=$!
"$keystore" follow "$work/follower" 8105 127.0.0.1:8104 &
follower_pid=$!

ready() { "$keystore" client "$1" info > /dev/null 2>&1; }
for _ in $(seq 1 100); do ready 127.0.0.1:8104 && ready 127.0.0.1:8105 && break; sleep 0.1; done

"$keystore" client 127.0.0.1:8104 put city Boston
"$keystore" client 127.0.0.1:8104 put river Charles
"$keystore" client 127.0.0.1:8104 delete river
for _ in $(seq 1 100); do
  if "$keystore" client 127.0.0.1:8105 info | grep -q ' seq=3 '; then break; fi
  sleep 0.1
done
test "$("$keystore" client 127.0.0.1:8105 get city)" = Boston
if "$keystore" client 127.0.0.1:8105 get river; then echo "river should be deleted" >&2; exit 1; fi
if "$keystore" client 127.0.0.1:8105 put city Paris; then echo "a follower took a write" >&2; exit 1; fi
"$keystore" client 127.0.0.1:8105 info
"$keystore" client 127.0.0.1:8104 info

# A JVM stopped by SIGTERM exits 143 once its shutdown hook (which closes the stores) is done.
kill -TERM "$follower_pid" "$leader_pid"
status=0; wait "$follower_pid" || status=$?; test "$status" -eq 143
status=0; wait "$leader_pid" || status=$?; test "$status" -eq 143
follower_pid=""
leader_pid=""
test "$("$keystore" get "$work/follower" city)" = Boston
echo "replication demo: ok"
