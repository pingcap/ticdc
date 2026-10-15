#!/bin/bash

# Capture write-lease integration test.
#
# Runs three captures with rate-limited INSERT/UPDATE traffic. For the MySQL
# sink, it delays and drops coordinator-to-capture write-lease grants, then
# verifies write admission closes, Redo publication stops, both recover, stale
# grants are rejected, and the data remains consistent. The drop simulates
# one-way P2P control-plane loss only.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4
CDC_BASE_PORT=${CDC_PORT}
LEASE_DB=capture_write_lease
REDO_DB=capture_write_lease_redo
LEASE_PROBE_TABLE_PREFIX=lease_probe
LEASE_PROBE_TABLE_COUNT=3
YCSB_TABLE=usertable
YCSB_RECORD_COUNT=10000
YCSB_OPERATION_COUNT=285000
YCSB_TARGET=1000
YCSB_THREADS=4
LEASE_DELAY_MS=7000
LEASE_DELAY_SECONDS=9
LEASE_DROP_SECONDS=15
LEASE_DELAY_FAILPOINT=github.com/pingcap/ticdc/coordinator/DelayCaptureWriteLeaseResponse
LEASE_DROP_FAILPOINT=github.com/pingcap/ticdc/coordinator/DropCaptureWriteLeaseResponse
LEASE_DUPLICATE_FAILPOINT=github.com/pingcap/ticdc/coordinator/DuplicateCaptureWriteLeaseResponse
MYSQL_HANG_FAILPOINT=github.com/pingcap/ticdc/pkg/sink/mysql/MySQLSinkHangLongTime
MAIN_CHANGEFEED_ID=capture-write-lease-main-test
REDO_CHANGEFEED_ID=capture-write-lease-redo-test
REDO_STORAGE_PATH="file://$WORK_DIR/redo"
REDO_DOWNLOAD_PATH="$WORK_DIR/cdc_data/redo/$REDO_CHANGEFEED_ID"

function ycsb_load() {
	go-ycsb load mysql -P "$CUR/conf/write_lease_workload" \
		--threads="$YCSB_THREADS" \
		-p mysql.host=${UP_TIDB_HOST} \
		-p mysql.port=${UP_TIDB_PORT} \
		-p mysql.user=root \
		-p mysql.db=${LEASE_DB} \
		-p table=${YCSB_TABLE} \
		-p recordcount=${YCSB_RECORD_COUNT} \
		-p operationcount=0
}

function ycsb_run() {
	go-ycsb run mysql -P "$CUR/conf/write_lease_workload" \
		--target="$YCSB_TARGET" \
		--threads="$YCSB_THREADS" \
		-p mysql.host=${UP_TIDB_HOST} \
		-p mysql.port=${UP_TIDB_PORT} \
		-p mysql.user=root \
		-p mysql.db=${LEASE_DB} \
		-p table=${YCSB_TABLE} \
		-p recordcount=${YCSB_RECORD_COUNT} \
		-p operationcount=${YCSB_OPERATION_COUNT}
}

function probe_count() {
	local expression=""
	local index

	for index in $(seq "$LEASE_PROBE_TABLE_COUNT"); do
		if [ -n "$expression" ]; then
			expression+=" + "
		fi
		expression+="(SELECT COUNT(*) FROM ${REDO_DB}.${LEASE_PROBE_TABLE_PREFIX}_${index})"
	done
	mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -N -s \
		-e "SELECT ${expression};"
}

function wait_for_probe_count() {
	local expected=$1
	local count

	for ((i = 0; i < 60; i++)); do
		count=$(probe_count 2>/dev/null || true)
		if [ "$count" = "$expected" ]; then
			return
		fi
		sleep 1
	done

	echo "downstream probe row count is not ${expected}" >&2
	return 1
}

function capture_has_gate_state() {
	local port=$1
	local state=$2

	curl -fsS --max-time 5 "http://127.0.0.1:${port}/metrics" |
		grep -E "^ticdc_server_capture_write_gate_state\\{state=\\\"${state}\\\"\\}[[:space:]]+1(\\.0+)?$" >/dev/null
}

function wait_for_gate_state() {
	local state=$1
	local target_port=${2:-}
	local port

	for ((i = 0; i < 30; i++)); do
		if [ -n "$target_port" ]; then
			if capture_has_gate_state "$target_port" "$state"; then
				return
			fi
		else
			for port in $(seq $((CDC_BASE_PORT + 1)) $((CDC_BASE_PORT + CDC_COUNT))); do
				if capture_has_gate_state "$port" "$state"; then
					lease_gate_state_port=$port
					return
				fi
			done
		fi
		sleep 1
	done

	echo "write gates did not reach ${state}" >&2
	return 1
}

function wait_for_all_gate_state() {
	local state=$1
	local port
	local ready

	for ((i = 0; i < 30; i++)); do
		ready=true
		for port in $(seq $((CDC_BASE_PORT + 1)) $((CDC_BASE_PORT + CDC_COUNT))); do
			if ! capture_has_gate_state "$port" "$state"; then
				ready=false
				break
			fi
		done
		if [ "$ready" = true ]; then
			return
		fi
		sleep 1
	done

	echo "not all write gates reached ${state}" >&2
	return 1
}

function capture_has_active_p2p_lease() {
	local port=$1

	curl -fsS --max-time 5 "http://127.0.0.1:${port}/metrics" |
		awk '$1 == "ticdc_server_capture_p2p_lease_remaining_seconds" && $2 > 0 { found = 1 } END { exit !found }'
}

function wait_for_active_p2p_leases() {
	local port
	local ready

	for ((i = 0; i < 30; i++)); do
		ready=true
		for port in $(seq $((CDC_BASE_PORT + 1)) $((CDC_BASE_PORT + CDC_COUNT))); do
			if ! capture_has_active_p2p_lease "$port"; then
				ready=false
				break
			fi
		done
		if [ "$ready" = true ]; then
			return
		fi
		sleep 1
	done

	echo "captures did not obtain active P2P write leases" >&2
	return 1
}

function redo_resolved_ts() {
	cdc redo meta --storage="$REDO_STORAGE_PATH" --tmp-dir="$REDO_DOWNLOAD_PATH/meta" |
		grep -oE "resolved-ts:[0-9]+" | awk -F: '{print $2}'
}

function assert_redo_resolved_before() {
	local upper_bound=$1
	local duration=$2
	local resolved_ts

	for ((i = 0; i < duration; i++)); do
		resolved_ts=$(redo_resolved_ts)
		if ! [[ "$resolved_ts" =~ ^[0-9]+$ ]]; then
			echo "invalid redo resolved ts: ${resolved_ts}" >&2
			return 1
		fi
		if [ "$resolved_ts" -ge "$upper_bound" ]; then
			echo "redo resolved ts ${resolved_ts} advanced to ${upper_bound} while write gates were closed" >&2
			return 1
		fi
		sleep 1
	done
}

function enable_lease_failpoint() {
	local name=$1
	local expr=$2
	local port

	for port in $(seq $((CDC_BASE_PORT + 1)) $((CDC_BASE_PORT + CDC_COUNT))); do
		enable_failpoint --addr "127.0.0.1:${port}" --name "$name" --expr "$expr"
	done
}

function disable_lease_failpoint() {
	local name=$1
	local port

	for port in $(seq $((CDC_BASE_PORT + 1)) $((CDC_BASE_PORT + CDC_COUNT))); do
		disable_failpoint --addr "127.0.0.1:${port}" --name "$name"
	done
}

function disable_lease_failpoint_best_effort() {
	local name=$1
	local port

	for port in $(seq $((CDC_BASE_PORT + 1)) $((CDC_BASE_PORT + CDC_COUNT))); do
		disable_failpoint --addr "127.0.0.1:${port}" --name "$name" >/dev/null 2>&1 || true
	done
}

function rejected_lease_response_count() {
	local reason=$1
	local port

	for port in $(seq $((CDC_BASE_PORT + 1)) $((CDC_BASE_PORT + CDC_COUNT))); do
		curl -fsS --max-time 5 "http://127.0.0.1:${port}/metrics" 2>/dev/null || true
	done | awk -v reason="$reason" '
		$0 ~ "^ticdc_server_capture_lease_response_rejected_total\\{reason=\"" reason "\"\\}" {
			total += $NF
		}
		END {
			printf "%.0f\n", total
		}
	'
}

function wait_for_rejected_lease_response() {
	local reason=$1
	local previous_count=$2
	local count

	for ((i = 0; i < 30; i++)); do
		count=$(rejected_lease_response_count "$reason")
		if [ "$count" -gt "$previous_count" ]; then
			return
		fi
		sleep 1
	done

	echo "no new ${reason} lease response rejection observed" >&2
	return 1
}

function stale_lease_response_rejected_count() {
	local unknown_count
	local replayed_count

	unknown_count=$(rejected_lease_response_count unknown_sequence)
	replayed_count=$(rejected_lease_response_count replayed_sequence)
	echo $((unknown_count + replayed_count))
}

function wait_for_stale_lease_response_rejection() {
	local previous_count=$1
	local count

	for ((i = 0; i < 30; i++)); do
		count=$(stale_lease_response_rejected_count)
		if [ "$count" -gt "$previous_count" ]; then
			return
		fi
		sleep 1
	done

	echo "no new stale lease response rejection observed" >&2
	return 1
}

function assert_cdc_processes_alive() {
	local port
	local pid

	for port in $(seq $((CDC_BASE_PORT + 1)) $((CDC_BASE_PORT + CDC_COUNT))); do
		pid=$(get_cdc_pid 127.0.0.1 "$port")
		if ! kill -0 "$pid" >/dev/null 2>&1; then
			echo "cdc on port ${port} exited during P2P lease expiry" >&2
			return 1
		fi
	done
}

function start_lease_response_delay() {
	enable_lease_failpoint "$LEASE_DELAY_FAILPOINT" "return(${LEASE_DELAY_MS})"
	(
		sleep "$LEASE_DELAY_SECONDS"
		disable_lease_failpoint "$LEASE_DELAY_FAILPOINT"
	) &
	lease_fault_pid=$!
}

function start_lease_response_drop() {
	enable_lease_failpoint "$LEASE_DROP_FAILPOINT" "return(true)"
	(
		sleep "$LEASE_DROP_SECONDS"
		disable_lease_failpoint "$LEASE_DROP_FAILPOINT"
	) &
	lease_fault_pid=$!
}

function insert_probe_rows() {
	local start=$1
	local index
	local values
	local id

	for index in $(seq "$LEASE_PROBE_TABLE_COUNT"); do
		values=""
		for id in $(seq "$start" $((start + 99))); do
			if [ -n "$values" ]; then
				values+=","
			fi
			values+="(${id}, ${id})"
		done
		run_sql "INSERT INTO ${REDO_DB}.${LEASE_PROBE_TABLE_PREFIX}_${index} VALUES ${values};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
}

function run_lease_expiry_round() {
	local round=$1
	local fault=$2
	local expected_after=$((round * 100 * LEASE_PROBE_TABLE_COUNT))
	local blocked_cdc_port
	local count
	local rejected_count
	local redo_target_tso

	case "$fault" in
	delay)
		rejected_count=$(stale_lease_response_rejected_count)
		start_lease_response_delay
		;;
	drop) start_lease_response_drop ;;
	*)
		echo "unknown lease response fault ${fault}" >&2
		return 1
		;;
	esac
	wait_for_gate_state p2p_expired
	blocked_cdc_port=$lease_gate_state_port
	assert_cdc_processes_alive
	if [ "$fault" = drop ]; then
		# Close every capture gate so neither Redo writers nor RedoMeta can publish
		# progress for events produced below.
		wait_for_all_gate_state p2p_expired
		sleep 1
	fi
	insert_probe_rows $(((round - 1) * 100 + 1))
	if [ "$fault" = drop ]; then
		redo_target_tso=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
		assert_redo_resolved_before "$redo_target_tso" 3
	fi
	sleep 2
	count=$(probe_count)
	if [ "$count" -ge "$expected_after" ]; then
		echo "all probe rows replicated while cdc on port ${blocked_cdc_port} was blocked" >&2
		return 1
	fi
	wait "$lease_fault_pid"
	if [ "$fault" = delay ]; then
		# Depending on whether a newer grant was applied first, the delayed grant is
		# rejected as an unknown or replayed sequence; neither may reopen admission.
		wait_for_stale_lease_response_rejection "$rejected_count"
	fi
	wait_for_gate_state writable "$blocked_cdc_port"
	wait_for_probe_count "$expected_after"
	if [ "$fault" = drop ]; then
		ensure 60 check_redo_resolved_ts "$REDO_CHANGEFEED_ID" "$redo_target_tso" \
			"$REDO_STORAGE_PATH" "$REDO_DOWNLOAD_PATH/meta"
	fi
}

function run_write_lease_test() {
	local duplicate_rejected_count
	local redo_start_tso

	if [ "$SINK_TYPE" != mysql ]; then
		return
	fi

	run_sql "CREATE DATABASE ${LEASE_DB};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE ${REDO_DB};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# Dynamic table scheduling assigns each small probe table to a capture. Three
	# tables cover the three captures while keeping probe traffic negligible.
	for i in $(seq "$LEASE_PROBE_TABLE_COUNT"); do
		run_sql \
			"CREATE TABLE ${REDO_DB}.${LEASE_PROBE_TABLE_PREFIX}_${i} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL);" \
			${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
	cdc_cli_changefeed create --start-ts="$start_ts" --sink-uri="$SINK_URI" \
		--changefeed-id="$REDO_CHANGEFEED_ID" --config="$CUR/conf/changefeed-redo.toml" \
		--server="127.0.0.1:$((CDC_BASE_PORT + 1))"
	ycsb_load
	for i in $(seq "$LEASE_PROBE_TABLE_COUNT"); do
		check_table_exists "${REDO_DB}.${LEASE_PROBE_TABLE_PREFIX}_${i}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done
	check_table_exists "${LEASE_DB}.${YCSB_TABLE}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff "$WORK_DIR" "$CUR/conf/write_lease_diff_config.toml" 120
	wait_for_active_p2p_leases
	redo_start_tso=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	ensure 60 check_redo_resolved_ts "$REDO_CHANGEFEED_ID" "$redo_start_tso" \
		"$REDO_STORAGE_PATH" "$REDO_DOWNLOAD_PATH/meta"

	ycsb_run >"$WORK_DIR/ycsb.log" 2>&1 &
	ycsb_pid=$!
	sleep 30
	for round in $(seq 1 3); do
		fault=delay
		if [ "$round" = 3 ]; then
			# Drop grants while still receiving heartbeats: a deterministic one-way
			# P2P control-plane network-loss fault.
			fault=drop
		fi
		assert_cdc_processes_alive
		run_lease_expiry_round "$round" "$fault"
		sleep 30
	done

	duplicate_rejected_count=$(rejected_lease_response_count replayed_sequence)
	enable_lease_failpoint "$LEASE_DUPLICATE_FAILPOINT" "return(true)"
	wait_for_rejected_lease_response replayed_sequence "$duplicate_rejected_count"
	disable_lease_failpoint "$LEASE_DUPLICATE_FAILPOINT"
	wait "$ycsb_pid"
	grep -Eq '^INSERT - .*Count: [1-9][0-9]*,' "$WORK_DIR/ycsb.log"
	grep -Eq '^UPDATE - .*Count: [1-9][0-9]*,' "$WORK_DIR/ycsb.log"
	check_sync_diff "$WORK_DIR" "$CUR/conf/write_lease_diff_config.toml" 120
}

function run_redo_apply_test() {
	local count
	local expected_after=$((4 * 100 * LEASE_PROBE_TABLE_COUNT))
	local redo_apply_tso

	if [ "$SINK_TYPE" != mysql ]; then
		return
	fi

	# Hold the normal MySQL sink while Redo continues, then recover the missing
	# downstream rows from Redo after the captures stop.
	enable_lease_failpoint "$MYSQL_HANG_FAILPOINT" "return(true)"
	insert_probe_rows 1001
	redo_apply_tso=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	ensure 60 check_redo_resolved_ts "$REDO_CHANGEFEED_ID" "$redo_apply_tso" \
		"$REDO_STORAGE_PATH" "$REDO_DOWNLOAD_PATH/meta"
	count=$(probe_count)
	if [ "$count" -ge "$expected_after" ]; then
		echo "MySQL sink was not blocked before Redo recovery" >&2
		return 1
	fi

	cleanup_process "$CDC_BINARY"
	cdc redo apply --log-level debug --tmp-dir="$REDO_DOWNLOAD_PATH/apply" \
		--storage="$REDO_STORAGE_PATH" \
		--sink-uri="mysql://normal:123456@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/" >"$WORK_DIR/cdc_redo.log"
	check_sync_diff "$WORK_DIR" "$CUR/conf/write_lease_diff_config.toml" 120
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	# create $DB_COUNT databases and import initial workload
	for i in $(seq $DB_COUNT); do
		db="capture_write_lease_$i"
		run_sql "CREATE DATABASE $db;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		go-ycsb load mysql -P $CUR/conf/workload1 -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=$db
	done

	export GO_FAILPOINTS='github.com/pingcap/ticdc/utils/dynstream/InjectDropEvent=10%return(true)'
	# start $CDC_COUNT cdc servers, and create a changefeed
	for i in $(seq $CDC_COUNT); do
		run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "$i" --addr "127.0.0.1:$((CDC_BASE_PORT + i))" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
	done

	TOPIC_NAME="ticdc-capture-write-lease-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/" ;;
	esac
	if [ "$SINK_TYPE" = mysql ]; then
		cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" \
			--changefeed-id="$MAIN_CHANGEFEED_ID" --config="$CUR/conf/changefeed-main.toml" \
			--server="127.0.0.1:$((CDC_BASE_PORT + 1))"
	else
		cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --server="127.0.0.1:$((CDC_BASE_PORT + 1))"
	fi
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	# check tables are created and data is synchronized
	for i in $(seq $DB_COUNT); do
		check_table_exists "capture_write_lease_$i.usertable" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	# add more data in upstream and check again
	for i in $(seq $DB_COUNT); do
		db="capture_write_lease_$i"
		go-ycsb load mysql -P $CUR/conf/workload2 -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=$db
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	run_write_lease_test
	run_redo_apply_test

	cleanup_process $CDC_BINARY
}

function cleanup() {
	disable_lease_failpoint_best_effort "$LEASE_DELAY_FAILPOINT"
	disable_lease_failpoint_best_effort "$LEASE_DROP_FAILPOINT"
	disable_lease_failpoint_best_effort "$LEASE_DUPLICATE_FAILPOINT"
	disable_lease_failpoint_best_effort "$MYSQL_HANG_FAILPOINT"
	stop_test "$WORK_DIR"
}

trap 'cleanup' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
