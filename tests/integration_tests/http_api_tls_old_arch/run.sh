#!/bin/bash
#
# Scenario: Run TiCDC server in old architecture mode with TLS enabled.
# Steps:
# 1) Start a non-TLS TiDB cluster (downstream) and a TLS-enabled TiDB cluster (upstream).
# 2) Override TICDC_NEWARCH for the server process so `cdc server` chooses
#    the old-arch (tiflow) path without mutating the current shell env.
# 3) Start TiCDC server with TLS flags (CA/cert/key) and mTLS client CN allowlist,
#    and connect it to a TLS PD endpoint.
# 4) Verify the HTTPS API is reachable and the server did not start in new-arch mode.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TLS_DIR=$(cd $CUR/../_certificates && pwd)

function run() {
	# This case is only meaningful for the classic architecture tests.
	if [ "$NEXT_GEN" = 1 ]; then
		return
	fi

	# storage and kafka are the same as mysql.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR
	start_tls_tidb_cluster --workdir $WORK_DIR --tlsdir $TLS_DIR

	logsuffix="_${TEST_NAME}_old_arch_tls"

	# Force TiCDC to run in old-arch mode, even though integration tests default to new-arch.
	TICDC_NEWARCH=false run_cdc_server \
		--workdir $WORK_DIR \
		--binary $CDC_BINARY \
		--logsuffix "$logsuffix" \
		--pd "https://${TLS_PD_HOST}:${TLS_PD_PORT}" \
		--addr "127.0.0.1:8300" \
		--tlsdir "$TLS_DIR" \
		--cert-allowed-cn "client" # The common name of client.pem

	# Verify the server is actually serving HTTPS.
	health_code=$(curl -o /dev/null -s -w "%{http_code}" \
		--cacert "${TLS_DIR}/ca.pem" \
		--cert "${TLS_DIR}/client.pem" \
		--key "${TLS_DIR}/client-key.pem" \
		--user ticdc:ticdc_secret \
		--max-time 20 \
		"https://127.0.0.1:8300/api/v2/health")
	if [ "$health_code" != "200" ]; then
		echo "unexpected /api/v2/health http code: ${health_code}"
		exit 1
	fi

	# Guardrail: ensure we are not accidentally testing the new-arch server path.
	cdc_log="${WORK_DIR}/cdc${logsuffix}.log"
	if [ ! -f "$cdc_log" ]; then
		echo "cdc log file not found: ${cdc_log}"
		exit 1
	fi
	if grep -Fq "TiCDC(new arch) server created" "$cdc_log"; then
		echo "unexpected new-arch server detected in logs: ${cdc_log}"
		exit 1
	fi

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run "$@"
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
