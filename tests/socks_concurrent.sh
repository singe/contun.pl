#!/usr/bin/env bash
set -euo pipefail

command -v python3 >/dev/null 2>&1 || {
  echo "python3 is required for this test" >&2
  exit 1
}
command -v curl >/dev/null 2>&1 || {
  echo "curl is required for this test" >&2
  exit 1
}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

CLIENT_PORT=${CLIENT_PORT:-9100}
POOL_PORT=${POOL_PORT:-9200}
TARGET_PORT=${TARGET_PORT:-9300}
WORKERS=${WORKERS:-4}
REQUESTS=${REQUESTS:-4}
TARGET_HOSTNAME=${TARGET_HOSTNAME:-localhost}

TMPDIR=$(mktemp -d)
SERVER_LOG="${TMPDIR}/server.log"
HUB_LOG="${TMPDIR}/hub.log"
POOL_LOG="${TMPDIR}/pool.log"

cleanup() {
  local exit_code=$?
  [[ -n "${SERVER_PID:-}" ]] && kill "${SERVER_PID}" 2>/dev/null || true
  [[ -n "${HUB_PID:-}" ]] && kill "${HUB_PID}" 2>/dev/null || true
  [[ -n "${POOL_PID:-}" ]] && kill "${POOL_PID}" 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "${TMPDIR}"
  exit "${exit_code}"
}
trap cleanup EXIT

python3 - <<'PY' "${TARGET_PORT}" "${SERVER_LOG}" &
import http.server
import socketserver
import sys
import threading

PORT = int(sys.argv[1])
LOG_PATH = sys.argv[2]

HTML_BODY = """\
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Concurrent SOCKS Test</title>
  </head>
  <body>
    <p>SOCKS served at {path}</p>
    <p>Thread {thread}</p>
  </body>
</html>
"""

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = HTML_BODY.format(path=self.path, thread=threading.get_ident()).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        with open(LOG_PATH, "a", encoding="utf-8") as log:
            log.write(f"{threading.get_ident()} {self.path}\n")

    def log_message(self, format, *args):
        return

with socketserver.ThreadingTCPServer(("127.0.0.1", PORT), Handler) as httpd:
    httpd.serve_forever()
PY
SERVER_PID=$!

perl "${REPO_ROOT}/hub.pl" \
  --client-bind 127.0.0.1 \
  --client-port "${CLIENT_PORT}" \
  --pool-bind 127.0.0.1 \
  --pool-port "${POOL_PORT}" \
  --mode socks \
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
  --mode socks \
  --workers "${WORKERS}" \
  >"${POOL_LOG}" 2>&1 &
POOL_PID=$!

sleep 2

seq 1 "${REQUESTS}" | xargs -I{} -P "${REQUESTS}" bash -c '
  set -euo pipefail
  curl -sS --max-time 5 \
    --socks5-hostname 127.0.0.1:'"${CLIENT_PORT}"' \
    "http://'"${TARGET_HOSTNAME}"':'"${TARGET_PORT}"'/req{}" >"'"${TMPDIR}"'/resp{}"
'

for idx in $(seq 1 "${REQUESTS}"); do
  if ! grep -q "/req${idx}" "${TMPDIR}/resp${idx}"; then
    echo "socks_concurrent: FAILED (response ${idx} missing)" >&2
    echo "--- hub log ---"
    cat "${HUB_LOG}" || true
    echo "--- pool log ---"
    cat "${POOL_LOG}" || true
    exit 1
  fi
done

if [[ $(wc -l < "${SERVER_LOG}") -lt ${REQUESTS} ]]; then
  echo "socks_concurrent: FAILED (server saw fewer requests than expected)" >&2
  exit 1
fi

echo "socks_concurrent: success"
