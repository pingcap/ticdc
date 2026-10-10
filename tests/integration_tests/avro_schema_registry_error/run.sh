#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR="$OUT_DIR/$TEST_NAME"
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=20
MOCK_SCHEMA_REGISTRY_PORT=${MOCK_SCHEMA_REGISTRY_PORT:-18088}
MOCK_SCHEMA_REGISTRY_PID=""

function start_mock_schema_registry() {
	python3 -u - "$MOCK_SCHEMA_REGISTRY_PORT" "$WORK_DIR" >"$WORK_DIR/mock_schema_registry.log" 2>&1 <<'PY' &
import http.server
import pathlib
import socketserver
import sys
import time

port = int(sys.argv[1])
workdir = pathlib.Path(sys.argv[2])

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/unauthorized", "/unexpected", "/delayed"):
            status = 200
            body = b"{}"
            if self.path == "/unauthorized":
                status = 401
            elif self.path == "/unexpected":
                body = b"registry-credential-sentinel"
            elif self.path == "/delayed":
                (workdir / "registry-entered").touch()
                deadline = time.monotonic() + 20
                while not (workdir / "registry-release").exists():
                    if time.monotonic() > deadline:
                        self.send_error(504)
                        return
                    time.sleep(0.05)
                status = int((workdir / "registry-release").read_text())
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def do_POST(self):
        if self.path.startswith("/subjects/") and self.path.endswith("/versions"):
            body = b"Internal Server Error"
            self.send_response(500)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def log_message(self, format, *args):
        sys.stderr.write("%s - - [%s] %s\n" %
                         (self.address_string(), self.log_date_time_string(), format % args))

class TCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

with TCPServer(("127.0.0.1", port), Handler) as httpd:
    httpd.serve_forever()
PY
	MOCK_SCHEMA_REGISTRY_PID=$!

	local i=0
	while ! curl -o /dev/null -fsS "http://127.0.0.1:${MOCK_SCHEMA_REGISTRY_PORT}"; do
		i=$((i + 1))
		if [ "$i" -gt 30 ]; then
			echo "failed to start mock schema registry"
			exit 1
		fi
		sleep 1
	done
}

function stop_mock_schema_registry() {
	if [ -n "$MOCK_SCHEMA_REGISTRY_PID" ]; then
		kill "$MOCK_SCHEMA_REGISTRY_PID" 2>/dev/null || true
		wait "$MOCK_SCHEMA_REGISTRY_PID" 2>/dev/null || true
	fi
}

function cleanup() {
	stop_mock_schema_registry
	stop_test "$WORK_DIR"
}

function create_changefeed() {
	local protocol=$1
	local changefeed_id=$2
	local topic_name=$3
	local start_ts=$4
	local sink_uri

	case "$protocol" in
	avro)
		sink_uri="kafka://127.0.0.1:9092/${topic_name}?protocol=avro&enable-tidb-extension=true&avro-enable-watermark=true&partition-num=1&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760&avro-decimal-handling-mode=string&avro-bigint-unsigned-handling-mode=string"
		;;
	*)
		echo "unsupported protocol: $protocol"
		exit 1
		;;
	esac

	cdc_cli_changefeed create \
		--start-ts="$start_ts" \
		--sink-uri="$sink_uri" \
		-c "$changefeed_id" \
		--schema-registry="http://127.0.0.1:${MOCK_SCHEMA_REGISTRY_PORT}"
	ensure "$MAX_RETRIES" "check_changefeed_status '127.0.0.1:8300' '$changefeed_id' 'normal'"
}

function check_schema_registry_validation() {
	local cf=$avro_changefeed_id
	local api="http://${CDC_HOST}:${CDC_PORT}/api/v2/changefeeds/$cf?keyspace=$KEYSPACE_NAME"
	local outputs="$WORK_DIR/credential_outputs"
	mkdir -p "$outputs"
	cdc_cli_changefeed pause -c "$cf"
	curl -fsS "$api" -o "$outputs/original.json"
	# A 401 response with the expected {} body must still reject credentials.
	# A successful HTTP response with a credential-bearing error body is rejected too.
	for path in unauthorized unexpected; do
		jq -n --arg uri "http://user:registry-credential-sentinel@127.0.0.1:$MOCK_SCHEMA_REGISTRY_PORT/$path" \
			'{replica_config:{memory_quota:7340032,sink:{schema_registry:$uri}}}' >"$WORK_DIR/registry-request.json"
		local status
		status=$(curl -sS -X PUT -H 'Content-Type: application/json' --data-binary "@$WORK_DIR/registry-request.json" \
			"$api" -o "$outputs/$path-error.json" -w '%{http_code}')
		[ "$status" = 400 ]
		curl -fsS "$api" -o "$outputs/$path-after.json"
		jq -e --slurp '.[0].config == .[1].config' "$outputs/original.json" "$outputs/$path-after.json" >/dev/null
	done

	# Hold validation open and check the observable config before it completes.
	for result in 200 401; do
		local quota=8388608
		if [ "$result" = 401 ]; then
			quota=9437184
		fi
		rm -f "$WORK_DIR/registry-entered" "$WORK_DIR/registry-release"
		curl -fsS "$api" -o "$outputs/delayed-before.json"
		jq -n --arg uri "http://user:registry-credential-sentinel@127.0.0.1:$MOCK_SCHEMA_REGISTRY_PORT/delayed" --argjson quota "$quota" \
			'{replica_config:{memory_quota:$quota,sink:{schema_registry:$uri}}}' >"$WORK_DIR/registry-request.json"
		curl -sS -X PUT -H 'Content-Type: application/json' --data-binary "@$WORK_DIR/registry-request.json" \
			"$api" -o "$outputs/delayed-response-$result.json" -w '%{http_code}' >"$outputs/delayed-status-$result.txt" &
		local update_pid=$!
		for ((i = 0; i < 30; i++)); do
			[ -f "$WORK_DIR/registry-entered" ] && break
			sleep 0.2
		done
		[ -f "$WORK_DIR/registry-entered" ]
		kill -0 "$update_pid"
		curl -fsS "$api" -o "$outputs/delayed-pending.json"
		jq -e --slurp '.[0].config == .[1].config' "$outputs/delayed-before.json" "$outputs/delayed-pending.json" >/dev/null
		echo "$result" >"$WORK_DIR/registry-release"
		wait "$update_pid"
		curl -fsS "$api" -o "$outputs/delayed-after.json"
		if [ "$result" = 200 ]; then
			[ "$(cat "$outputs/delayed-status-$result.txt")" = 200 ]
			jq -e '.config.memory_quota == 8388608 and (.config.sink | has("schema_registry") | not)' \
				"$outputs/delayed-after.json" >/dev/null
		else
			[ "$(cat "$outputs/delayed-status-$result.txt")" = 400 ]
			jq -e --slurp '.[0].config == .[1].config' "$outputs/delayed-before.json" "$outputs/delayed-after.json" >/dev/null
		fi
	done
	# Restore the original registry so the existing runtime-error assertions run.
	jq '{replica_config:.config}' "$outputs/original.json" >"$WORK_DIR/registry-request.json"
	curl -fsS -X PUT -H 'Content-Type: application/json' --data-binary "@$WORK_DIR/registry-request.json" \
		"$api" -o "$outputs/restore.json"
	cdc_cli_changefeed resume -c "$cf" >"$outputs/resume.txt" 2>&1
	if grep -Eq 'credential-sentinel' "$outputs"/* "$WORK_DIR"/cdc*.log "$WORK_DIR"/stdout*.log; then
		echo "Schema Registry credentials leaked in product output"
		exit 1
	fi
}

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	start_mock_schema_registry
	start_tidb_cluster --workdir "$WORK_DIR"

	run_sql "CREATE DATABASE avro_schema_registry_error;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "CREATE TABLE avro_schema_registry_error.t1(id INT PRIMARY KEY, v VARCHAR(32));" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	avro_changefeed_id="avro-schema-registry-error-$RANDOM"
	create_changefeed "avro" "$avro_changefeed_id" "ticdc-avro-schema-registry-error-$RANDOM" "$start_ts"
	check_schema_registry_validation

	run_sql "INSERT INTO avro_schema_registry_error.t1 VALUES (1, 'trigger schema register');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	ensure "$MAX_RETRIES" "check_changefeed_status '127.0.0.1:8300' '$avro_changefeed_id' 'warning' 'last_warning' 'register schema failed with status 500'"

	cleanup_process "$CDC_BINARY"
}

trap cleanup EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
