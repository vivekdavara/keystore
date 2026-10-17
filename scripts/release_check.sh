#!/usr/bin/env bash
# The release check (README, "Release check"): what CI runs, the same again on a clean export of the commit (so it
# needs nothing that isn't committed), the packaged command and the replication demo, then every measurement in the
# README's "Results" again at this commit (scripts/bench.sh 5 and scripts/jmh.sh), and the kill tests with their
# counts. Each step's output goes to target/release/<time>/<step>.txt; the check stops at the first step that fails,
# and ends with the medians of the bench runs (bench-medians.txt).
#
#   scripts/release_check.sh          all of it (about 25 minutes on an M3)
#   scripts/release_check.sh quick    the two test runs, the packaged command and the demo (about 6 minutes)
#
# The measurements need a quiet machine (nothing else using the CPU or the disk), and ports 8104 and 8105 free.
# conditions-start.txt and conditions-end.txt record the machine: the commit, the build, the CPU, the OS, the power
# and the load average.
set -euo pipefail

cd "$(dirname "$0")/.."
: "${JAVA_HOME:=/opt/homebrew/opt/openjdk@21}"
export JAVA_HOME
dir="target/release/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$dir"

step() {
  local name="$1"
  shift
  local start=$SECONDS
  echo "== $name: $*"
  if ! "$@" >"$dir/$name.txt" 2>&1; then
    echo "   FAILED after $((SECONDS - start)) s; see $dir/$name.txt"
    exit 1
  fi
  echo "   ok in $((SECONDS - start)) s"
}

conditions() {
  date
  echo "commit $(git rev-parse HEAD) ($(git describe --tags --always --dirty))"
  if [ -f target/keystore.jar ]; then scripts/keystore version; fi
  if command -v sysctl >/dev/null && sysctl -n machdep.cpu.brand_string >/dev/null 2>&1; then
    sysctl -n machdep.cpu.brand_string
    sw_vers -productVersion
    pmset -g batt | head -2
    pmset -g | grep -i lowpowermode || true
  else
    grep -m1 'model name' /proc/cpuinfo || true
    uname -sr
  fi
  uptime
}

echo "release check of $(git rev-parse --short=12 HEAD) ($(git describe --tags --always --dirty)), $(date)"
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "warning: tracked files have changes: the clean export checks the commit, every other step the working tree"
fi
conditions >"$dir/conditions-start.txt" 2>&1

export_dir="$(mktemp -d)"
trap 'rm -rf "$export_dir"' EXIT
git archive HEAD | tar -x -C "$export_dir"

step check mvn -B verify
step clean-export mvn -B -f "$export_dir/pom.xml" verify
step smoke scripts/smoke.sh
step replication-demo scripts/replication-demo.sh
if [ "${1:-}" = quick ]; then
  conditions >"$dir/conditions-end.txt" 2>&1
  echo "quick check passed; the outputs are in $dir"
  exit 0
fi

step bench scripts/bench.sh 5
step bench-medians "$JAVA_HOME/bin/java" -cp target/classes:target/test-classes \
  io.github.vivekdavara.keystore.bench.Medians "$dir/bench.txt"
step jmh scripts/jmh.sh -f 2 -wi 3 -w 1s -i 5 -r 1s -rf json -rff "$dir/jmh.json"
step kill-tests mvn -B test -Dtest='KillTest,FollowerKillTest,LeaderKillTest'
conditions >"$dir/conditions-end.txt" 2>&1
echo "all steps passed; the outputs are in $dir"
