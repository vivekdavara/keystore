#!/usr/bin/env bash
# The measurements in the README. Usage: scripts/bench.sh [runs] [day]   (default 5 runs of each, every day)
# Each line is one run (after a warm-up run inside the JVM); the README reports medians.
set -euo pipefail
cd "$(dirname "$0")/.."
runs="${1:-5}"
: "${JAVA_HOME:=/opt/homebrew/opt/openjdk@21}"
export JAVA_HOME
mvn -B -q test-compile
cp="target/classes:target/test-classes"
bench() { "$JAVA_HOME/bin/java" -Xmx4g -cp "$cp" io.github.vivekdavara.keystore.bench.Bench "$@"; }

# The raw cost of a durable append, without Java: fsync against F_FULLFSYNC (macOS) or fdatasync (Linux).
cc -O2 -o target/fsync_cost scripts/fsync_cost.c
for _ in $(seq "$runs"); do target/fsync_cost target 1000; done

day="${2:-all}"
if [ "$day" = all ] || [ "$day" = 3 ]; then
    # Day 3: the server over loopback TCP, a follower catching up, and one write's replication lag.
    for connections in 1 4 16; do
        for _ in $(seq "$runs"); do bench server "$connections" 100000 100; done
    done
    for sync in NEVER EVERY_WRITE; do
        for _ in $(seq "$runs"); do bench catchup 1000000 100 "$sync"; done
    done
    for _ in $(seq "$runs"); do bench lag 10000 100 NEVER; done
    for _ in $(seq "$runs"); do bench lag 1000 100 EVERY_WRITE; done
    for depth in 1 16 128; do
        for _ in $(seq "$runs"); do bench pipeline "$depth" 200000 100; done
    done
    [ "$day" = 3 ] && exit 0
fi
if [ "$day" = all ] || [ "$day" = 2 ]; then
    # Day 2: flushes, merges and reads from tables.
    for _ in $(seq "$runs"); do bench fill 1000000 100; done
    for bits in 1 5 10 16; do
        for _ in $(seq "$runs"); do bench reads 1000000 100 "$bits"; done
    done
    for _ in $(seq "$runs"); do bench writes NEVER 200000 100; done
    [ "$day" = 2 ] && exit 0
fi

for _ in $(seq "$runs"); do bench writes EVERY_WRITE 1000 100; done
for _ in $(seq "$runs"); do bench writes NEVER 200000 100; done
for _ in $(seq "$runs"); do bench recover 1000000 100; done
for _ in $(seq "$runs"); do bench scan 67108864 zeros; done
for _ in $(seq "$runs"); do bench scan 67108864 random; done
for _ in $(seq "$runs"); do bench memory 1000000 100; done
for _ in $(seq "$runs"); do bench skiplist 1000000; done
