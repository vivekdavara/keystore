#!/usr/bin/env bash
# The JMH benchmarks (src/test/java/io/github/vivekdavara/keystore/jmh). Usage:
#   scripts/jmh.sh                                  every benchmark, with the settings the README's numbers used
#   scripts/jmh.sh ReadBenchmark -p bloomBitsPerKey=10   any JMH options: a regex of benchmarks, -p, -f, -i ...
#   scripts/jmh.sh -f 1 -wi 0 -i 1 -r 100ms         a smoke run, as CI does
set -euo pipefail
cd "$(dirname "$0")/.."
: "${JAVA_HOME:=/opt/homebrew/opt/openjdk@21}"
export JAVA_HOME
mvn -B -q test-compile dependency:build-classpath -Dmdep.outputFile=target/jmh-classpath.txt -Dmdep.includeScope=test
cp="target/test-classes:target/classes:$(cat target/jmh-classpath.txt)"
if [ $# -eq 0 ]; then
    set -- -f 2 -wi 3 -w 1s -i 5 -r 1s -rf json -rff target/jmh-result.json
fi
exec "$JAVA_HOME/bin/java" -cp "$cp" org.openjdk.jmh.Main "$@"
