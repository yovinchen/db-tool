#!/usr/bin/env sh
set -eu

if [ "$#" -eq 0 ]; then
  set -- redis-server
fi

if [ "$1" != "redis-server" ]; then
  exec "$@"
fi

"$@" &
redis_pid="$!"

cleanup() {
  kill "$redis_pid" 2>/dev/null || true
  wait "$redis_pid" 2>/dev/null || true
}

trap cleanup INT TERM

until redis-cli ping >/dev/null 2>&1; do
  sleep 1
done

while IFS= read -r command || [ -n "$command" ]; do
  [ -z "$command" ] && continue
  case "$command" in
    \#*) continue ;;
  esac
  # Fixture commands are intentionally simple whitespace-separated Redis calls.
  # shellcheck disable=SC2086
  redis-cli $command >/dev/null
done </dbtool-fixtures/base-redis-seed.commands

wait "$redis_pid"
