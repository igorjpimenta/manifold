#!/bin/sh
# Wall-clock guard around the test suites: `with-guard.sh <cmd> [args...]`.
#
# Locally (macOS) the suites run under `taskpolicy -b` so a runaway test can't
# pin every core on the machine that also hosts the live gateway, plus a perl
# `alarm(N)` hard stop so a hung suite doesn't wedge the terminal forever.
#
# Neither taskpolicy nor a guaranteed perl exists on the Linux CI runner, so
# degrade: coreutils `timeout` keeps the hard stop (losing only the priority
# nice-ing, which is meaningless on a single-use runner), and a bare exec is
# the last resort. Exit status is the child's in every branch.
#
# Override the limit with TEST_GUARD_SECONDS.
set -e

LIMIT="${TEST_GUARD_SECONDS:-180}"

if command -v taskpolicy >/dev/null 2>&1 && command -v perl >/dev/null 2>&1; then
  exec taskpolicy -b perl -e "alarm($LIMIT); exec @ARGV" -- "$@"
elif command -v timeout >/dev/null 2>&1; then
  exec timeout "$LIMIT" "$@"
else
  exec "$@"
fi
