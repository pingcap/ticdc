#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

if [ "$SINK_TYPE" != "iceberg" ]; then
	echo "skip iceberg integration test, sink type is $SINK_TYPE"
	exit 0
fi

if [ -z "${ICEBERG_SPARK_READBACK:-}" ]; then
	if command -v spark-sql >/dev/null 2>&1; then
		export ICEBERG_SPARK_READBACK=1
	else
		export ICEBERG_SPARK_READBACK=0
	fi
fi
if [ "${ICEBERG_SPARK_READBACK}" = "1" ]; then
	export ICEBERG_SPARK_PACKAGES="${ICEBERG_SPARK_PACKAGES:-org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1}"
fi

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_sql "CREATE TABLE test.iceberg_append_basic(id INT PRIMARY KEY, val INT);"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	WAREHOUSE_DIR="$WORK_DIR/iceberg_warehouse"
	SINK_URI="iceberg://?warehouse=file://$WAREHOUSE_DIR&catalog=hadoop&namespace=ns&mode=append&commit-interval=1s&enable-checkpoint-table=true&enable-global-checkpoint-table=true&partitioning=days(_tidb_commit_time)"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
}

function wait_file_exists() {
	file_pattern=$1
	check_time=${2:-60}
	i=0
	while [ $i -lt $check_time ]; do
		if ls $file_pattern >/dev/null 2>&1; then
			return 0
		fi
		((i++))
		sleep 1
	done
	echo "file not found after ${check_time}s: ${file_pattern}"
	return 1
}

function iceberg_check_append_basic() {
	run_sql "INSERT INTO test.iceberg_append_basic(id, val) VALUES (1, 1);"
	run_sql "INSERT INTO test.iceberg_append_basic(id, val) VALUES (2, 2);"
	run_sql "UPDATE test.iceberg_append_basic SET val = 22 WHERE id = 2;"
	run_sql "DELETE FROM test.iceberg_append_basic WHERE id = 1;"

	WAREHOUSE_DIR="$WORK_DIR/iceberg_warehouse"
	TABLE_ROOT="$WAREHOUSE_DIR/ns/test/iceberg_append_basic"
	METADATA_DIR="$TABLE_ROOT/metadata"
	DATA_DIR="$TABLE_ROOT/data"

	# Wait for iceberg commit output files.
	wait_file_exists "$METADATA_DIR/v*.metadata.json" 120
	wait_file_exists "$DATA_DIR/snap-*.parquet" 120

	# Hint: Spark readback is disabled by default.
	# Enable it via:
	#   ICEBERG_SPARK_READBACK=1
	#   ICEBERG_SPARK_PACKAGES=...  (or ICEBERG_SPARK_JARS=...)
	#   spark-sql in PATH (or set SPARK_HOME)
	# See docs/design/2026-01-30-ticdc-iceberg-sink-user-guide.md for examples.
	if [ "${ICEBERG_SPARK_READBACK:-0}" != "1" ]; then
		echo "[info] Spark readback disabled; set ICEBERG_SPARK_READBACK=1 with ICEBERG_SPARK_PACKAGES/ICEBERG_SPARK_JARS and spark-sql to enable."
	fi

	# Verify we did not write delete files in append mode.
	if ls "$DATA_DIR/delete-"*.parquet >/dev/null 2>&1; then
		echo "unexpected delete parquet files found in append mode"
		ls -la "$DATA_DIR"
		exit 1
	fi

	# Verify commit watermark exists in latest metadata file.
	latest_meta=$(ls -1 "$METADATA_DIR"/v*.metadata.json | sort -V | tail -n 1)
	committed_ts=$(jq -r '.snapshots[-1].summary["tidb.committed_resolved_ts"] // ""' "$latest_meta")
	if [ -z "$committed_ts" ] || [ "$committed_ts" == "null" ]; then
		echo "missing tidb.committed_resolved_ts in iceberg metadata"
		cat "$latest_meta"
		exit 1
	fi

	# Verify checkpoint tables are created.
	CHECKPOINT_DIR="$WAREHOUSE_DIR/ns/__ticdc/__tidb_checkpoints/data"
	GLOBAL_CHECKPOINT_DIR="$WAREHOUSE_DIR/ns/__ticdc/__tidb_global_checkpoints/data"
	CHECKPOINT_METADATA_DIR="$WAREHOUSE_DIR/ns/__ticdc/__tidb_checkpoints/metadata"
	GLOBAL_CHECKPOINT_METADATA_DIR="$WAREHOUSE_DIR/ns/__ticdc/__tidb_global_checkpoints/metadata"
	wait_file_exists "$CHECKPOINT_DIR/snap-*.parquet" 120
	wait_file_exists "$GLOBAL_CHECKPOINT_DIR/snap-*.parquet" 120
	wait_file_exists "$CHECKPOINT_METADATA_DIR/v*.metadata.json" 120
	wait_file_exists "$GLOBAL_CHECKPOINT_METADATA_DIR/v*.metadata.json" 120

	# Optional: Spark readback verification (requires Spark + Iceberg Spark runtime).
	if [ "${ICEBERG_SPARK_READBACK:-0}" = "1" ]; then
		warehouse_uri="file://$WAREHOUSE_DIR"
		table_name="iceberg_test.ns.test.iceberg_append_basic"
		readback=$(iceberg_spark_sql_scalar \
			--warehouse "$warehouse_uri" \
			--sql "SELECT concat_ws(',', CAST(count(*) AS STRING), CAST(sum(CASE WHEN _tidb_op = 'I' THEN 1 ELSE 0 END) AS STRING), CAST(sum(CASE WHEN _tidb_op = 'U' THEN 1 ELSE 0 END) AS STRING), CAST(sum(CASE WHEN _tidb_op = 'D' THEN 1 ELSE 0 END) AS STRING)) AS v FROM $table_name")
		if [ "$readback" != "4,2,1,1" ]; then
			echo "spark readback mismatch, expected 4,2,1,1, got: $readback"
			exit 1
		fi

		spark_watermark=$(iceberg_spark_sql_scalar \
			--warehouse "$warehouse_uri" \
			--sql "SELECT coalesce(summary['tidb.committed_resolved_ts'], 'MISSING') AS v FROM ${table_name}.snapshots ORDER BY snapshot_id DESC LIMIT 1")
		if [ "$spark_watermark" = "MISSING" ]; then
			echo "spark readback missing tidb.committed_resolved_ts in snapshots summary"
			exit 1
		fi

		checkpoint_rows=$(iceberg_spark_sql_scalar \
			--warehouse "$warehouse_uri" \
			--sql "SELECT CAST(count(*) AS STRING) AS v FROM iceberg_test.ns.__ticdc.__tidb_checkpoints")
		if [ "$checkpoint_rows" -le 0 ]; then
			echo "spark readback expected checkpoint table rows, got: $checkpoint_rows"
			exit 1
		fi

		global_checkpoint_rows=$(iceberg_spark_sql_scalar \
			--warehouse "$warehouse_uri" \
			--sql "SELECT CAST(count(*) AS STRING) AS v FROM iceberg_test.ns.__ticdc.__tidb_global_checkpoints")
		if [ "$global_checkpoint_rows" -le 0 ]; then
			echo "spark readback expected global checkpoint table rows, got: $global_checkpoint_rows"
			exit 1
		fi
	fi

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
prepare "$@"
iceberg_check_append_basic "$@"
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
