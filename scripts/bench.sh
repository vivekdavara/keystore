#!/usr/bin/env bash
# The day-1 measurements in the README. Usage: scripts/bench.sh [runs]   (default 5 runs of each)
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

for _ in $(seq "$runs"); do bench writes EVERY_WRITE 1000 100; done
for _ in $(seq "$runs"); do bench writes NEVER 200000 100; done
for _ in $(seq "$runs"); do bench recover 1000000 100; done
for _ in $(seq "$runs"); do bench scan 67108864 zeros; done
for _ in $(seq "$runs"); do bench scan 67108864 random; done
for _ in $(seq "$runs"); do bench memory 1000000 100; done
for _ in $(seq "$runs"); do bench skiplist 1000000; done
