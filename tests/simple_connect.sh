#!/usr/bin/env bash
set -euo pipefail

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
  [[ -n "${TARGET_PID:-}" ]] && kill "${TARGET_PID}" 2>/dev/null || true
  [[ -n "${HUB_PID:-}" ]] && kill "${HUB_PID}" 2>/dev/null || true
  [[ -n "${POOL_PID:-}" ]] && kill "${POOL_PID}" 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "${TMPDIR}"
  exit "${exit_code}"
}
trap cleanup EXIT

perl -MIO::Socket::INET -e '
  use strict;
  use warnings;
  my ($port, $outfile) = @ARGV;
  open my $fh, ">", $outfile or die "open $outfile: $!";
  my $server = IO::Socket::INET->new(
    LocalAddr => "127.0.0.1",
    LocalPort => $port,
    Listen    => 1,
    Proto     => "tcp",
    Reuse     => 1,
  ) or die "listen $port: $!";
  my $client = $server->accept() or die "accept failed: $!";
  $client->autoflush(1);
  while (1) {
    my $buf = "";
    my $bytes = sysread($client, $buf, 16 * 1024);
    last unless defined $bytes && $bytes > 0;
    print {$fh} $buf;
  }
' "${TARGET_PORT}" "${TARGET_OUTPUT}" &
TARGET_PID=$!

perl "${REPO_ROOT}/hub.pl" \
  --client-bind 127.0.0.1 \
  --client-port "${CLIENT_PORT}" \
  --pool-bind 127.0.0.1 \
  --pool-port "${POOL_PORT}" \
  --mode direct \
  >"${HUB_LOG}" 2>&1 &
HUB_PID=$!

# shellcheck disable=SC2206
if [[ -n "${POOL_BIN:-}" ]]; then
  read -r -a POOL_CMD <<<"${POOL_BIN}"
else
  POOL_CMD=(perl "${REPO_ROOT}/pool.pl")
fi

"${POOL_CMD[@]}" \
  --hub-host 127.0.0.1 \
  --hub-port "${POOL_PORT}" \
  --target-host 127.0.0.1 \
  --target-port "${TARGET_PORT}" \
  --mode direct \
  --workers 1 \
  >"${POOL_LOG}" 2>&1 &
POOL_PID=$!

sleep 1

perl -MIO::Socket::INET -e '
  my ($host, $port, $payload) = @ARGV;
  my $sock = IO::Socket::INET->new(
    PeerAddr => $host,
    PeerPort => $port,
    Proto    => "tcp",
  ) or die "client connect failed: $!";
  $sock->autoflush(1);
  print $sock $payload;
  select undef, undef, undef, 1.0;
' 127.0.0.1 "${CLIENT_PORT}" "${MESSAGE}"

sleep 1

if grep -q "${MESSAGE}" "${TARGET_OUTPUT}"; then
  echo "simple_connect: success"
else
  echo "simple_connect: FAILED"
  echo "--- hub log ---"
  cat "${HUB_LOG}" || true
  echo "--- pool log ---"
  cat "${POOL_LOG}" || true
  echo "--- target output ---"
  cat "${TARGET_OUTPUT}" || true
  exit 1
fi
