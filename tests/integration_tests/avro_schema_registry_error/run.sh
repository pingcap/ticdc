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
	python3 -u - "$MOCK_SCHEMA_REGISTRY_PORT" >"$WORK_DIR/mock_schema_registry.log" 2>&1 <<'PY' &
import http.server
import socketserver
import sys

port = int(sys.argv[1])

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            body = b"{}"
            self.send_response(200)
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

class TCPServer(socketserver.TCPServer):
    allow_reuse_address = True

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
	debezium-avro)
		sink_uri="kafka://127.0.0.1:9092/${topic_name}?protocol=debezium-avro&enable-tidb-extension=true&avro-enable-watermark=true&partition-num=1&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760&avro-decimal-handling-mode=precise&avro-bigint-unsigned-handling-mode=string"
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
	debezium_changefeed_id="debezium-avro-schema-registry-error-$RANDOM"
	create_changefeed "avro" "$avro_changefeed_id" "ticdc-avro-schema-registry-error-$RANDOM" "$start_ts"
	create_changefeed "debezium-avro" "$debezium_changefeed_id" "ticdc-debezium-avro-schema-registry-error-$RANDOM" "$start_ts"

	run_sql "INSERT INTO avro_schema_registry_error.t1 VALUES (1, 'trigger schema register');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	ensure "$MAX_RETRIES" "check_changefeed_status '127.0.0.1:8300' '$avro_changefeed_id' 'warning' 'last_warning' 'register schema failed with status 500'"
	ensure "$MAX_RETRIES" "check_changefeed_status '127.0.0.1:8300' '$debezium_changefeed_id' 'warning' 'last_warning' 'register schema failed with status 500'"

	cleanup_process "$CDC_BINARY"
}

trap cleanup EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
