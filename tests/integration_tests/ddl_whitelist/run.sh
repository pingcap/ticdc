#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# The assertions inspect a TiDB downstream through the MySQL sink.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	# RECOVER TABLE is not supported by next-gen TiDB.
	if [ "$NEXT_GEN" = 1 ]; then
		echo "skip ddl_whitelist: RECOVER TABLE requires classic TiDB"
		return
	fi
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR" --tidb-config "$CUR/conf/tidb_config.toml"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	local mode force_replicate start_ts
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	for mode in default forced ignored; do
		force_replicate=false
		if [ "$mode" = forced ]; then
			force_replicate=true
		fi
		cat >"$WORK_DIR/$mode.toml" <<EOF
force-replicate = $force_replicate
[filter]
rules = ["ddl_whitelist_${mode}*.*"]
EOF
		cdc_cli_changefeed create -c "ddl-whitelist-$mode" --start-ts="$start_ts" \
			--sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/" --config="$WORK_DIR/$mode.toml"
	done
	python3 "$CUR/test.py"
	cleanup_process "$CDC_BINARY"
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
