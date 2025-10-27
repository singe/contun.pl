#!/usr/bin/env bash
set -euo pipefail

command -v nc >/dev/null 2>&1 || {
  echo "nc command not found; install netcat to run this test" >&2
  exit 1
}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

CLIENT_PORT=${CLIENT_PORT:-6100}
POOL_PORT=${POOL_PORT:-6200}
TARGET_PORT=${TARGET_PORT:-6300}
MESSAGE=${MESSAGE:-"hello-through-contun"}

TMPDIR=$(mktemp -d)
TARGET_OUTPUT="${TMPDIR}/target-output.txt"
HUB_LOG="${TMPDIR}/hub.log"
POOL_LOG="${TMPDIR}/pool.log"

cleanup() {
  local exit_code=$?
  [[ -n "${CLIENT_PID:-}" ]] && kill "${CLIENT_PID}" 2>/dev/null || true
  [[ -n "${TARGET_PID:-}" ]] && kill "${TARGET_PID}" 2>/dev/null || true
  [[ -n "${HUB_PID:-}" ]] && kill "${HUB_PID}" 2>/dev/null || true
  [[ -n "${POOL_PID:-}" ]] && kill "${POOL_PID}" 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "${TMPDIR}"
  exit "${exit_code}"
}
trap cleanup EXIT

start_nc_listener() {
  local port=$1
  local outfile=$2
  nc -l 127.0.0.1 "${port}" >"${outfile}" &
  local pid=$!
  sleep 0.2
  if kill -0 "${pid}" 2>/dev/null; then
    echo "${pid}"
    return
  fi
  nc -l -p "${port}" 127.0.0.1 >"${outfile}" &
  pid=$!
  sleep 0.2
  if kill -0 "${pid}" 2>/dev/null; then
    echo "${pid}"
    return
  fi
  echo "Failed to start nc listener on port ${port}" >&2
  exit 1
}

TARGET_PID=$(start_nc_listener "${TARGET_PORT}" "${TARGET_OUTPUT}")

perl "${REPO_ROOT}/hub.pl" \
  --client-bind 127.0.0.1 \
  --client-port "${CLIENT_PORT}" \
  --pool-bind 127.0.0.1 \
  --pool-port "${POOL_PORT}" \
  >"${HUB_LOG}" 2>&1 &
HUB_PID=$!

perl "${REPO_ROOT}/pool.pl" \
  --hub-host 127.0.0.1 \
  --hub-port "${POOL_PORT}" \
  --target-host 127.0.0.1 \
  --target-port "${TARGET_PORT}" \
  --workers 1 \
  >"${POOL_LOG}" 2>&1 &
POOL_PID=$!

sleep 1

printf "%s" "${MESSAGE}" | nc 127.0.0.1 "${CLIENT_PORT}"

sleep 1

if grep -q "${MESSAGE}" "${TARGET_OUTPUT}"; then
  echo "simple_connect: success"
else
  echo "simple_connect: FAILED"
  echo "--- hub log ---"
  cat "${HUB_LOG}" || true
  echo "--- pool log ---"
  cat "${POOL_LOG}" || true
  exit 1
fi
