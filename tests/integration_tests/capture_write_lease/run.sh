#!/bin/bash

# Capture write-lease integration test.
#
# Runs three captures with rate-limited INSERT/UPDATE traffic. For the MySQL
# sink, it delays and drops coordinator-to-capture write-lease grants, then
# verifies write admission closes, recovers, rejects stale grants, and remains
# data-consistent. The drop simulates one-way P2P control-plane loss only.

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
LEASE_PROBE_TABLE_PREFIX=lease_probe
LEASE_PROBE_TABLE_COUNT=3
YCSB_TABLE=usertable
YCSB_RECORD_COUNT=10000
YCSB_OPERATION_COUNT=570000
YCSB_TARGET=1000
YCSB_THREADS=4
LEASE_DELAY_MS=7000
LEASE_DELAY_SECONDS=9
LEASE_DELAY_MARKER=/tmp/ticdc-delay-write-lease-response
LEASE_DROP_MARKER=/tmp/ticdc-drop-write-lease-response
LEASE_DUPLICATE_MARKER=/tmp/ticdc-duplicate-write-lease-response

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
		expression+="(SELECT COUNT(*) FROM ${LEASE_DB}.${LEASE_PROBE_TABLE_PREFIX}_${index})"
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
	(
		local deadline=$((SECONDS + LEASE_DELAY_SECONDS))
		local temp_marker="${LEASE_DELAY_MARKER}.$$"
		while ((SECONDS < deadline)); do
			printf '%s' "$LEASE_DELAY_MS" >"$temp_marker"
			mv "$temp_marker" "$LEASE_DELAY_MARKER"
			sleep 0.05
		done
		rm -f "$temp_marker"
	) &
	lease_fault_pid=$!
}

function start_lease_response_drop() {
	(
		local deadline=$((SECONDS + LEASE_DELAY_SECONDS))
		local temp_marker="${LEASE_DROP_MARKER}.$$"
		while ((SECONDS < deadline)); do
			printf '1\n' >"$temp_marker"
			mv "$temp_marker" "$LEASE_DROP_MARKER"
			sleep 0.05
		done
		rm -f "$temp_marker"
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
		run_sql "INSERT INTO ${LEASE_DB}.${LEASE_PROBE_TABLE_PREFIX}_${index} VALUES ${values};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
}

function run_lease_expiry_round() {
	local round=$1
	local fault=$2
	local expected_after=$((round * 100 * LEASE_PROBE_TABLE_COUNT))
	local blocked_cdc_port
	local count
	local rejected_count

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
	insert_probe_rows $(((round - 1) * 100 + 1))
	sleep 2
	count=$(probe_count)
	if [ "$count" -ge "$expected_after" ]; then
		echo "all probe rows replicated while cdc on port ${blocked_cdc_port} was blocked" >&2
		return 1
	fi
	wait "$lease_fault_pid"
	if [ "$fault" = delay ]; then
		rm -f "$LEASE_DELAY_MARKER"
		# Depending on whether a newer grant was applied first, the delayed grant is
		# rejected as an unknown or replayed sequence; neither may reopen admission.
		wait_for_stale_lease_response_rejection "$rejected_count"
	else
		rm -f "$LEASE_DROP_MARKER"
	fi
	wait_for_gate_state writable "$blocked_cdc_port"
	wait_for_probe_count "$expected_after"
}

function run_write_lease_test() {
	local duplicate_rejected_count

	if [ "$SINK_TYPE" != mysql ]; then
		return
	fi

	run_sql "CREATE DATABASE ${LEASE_DB};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# Dynamic table scheduling assigns each small probe table to a capture. Three
	# tables cover the three captures while keeping probe traffic negligible.
	for i in $(seq "$LEASE_PROBE_TABLE_COUNT"); do
		run_sql "CREATE TABLE ${LEASE_DB}.${LEASE_PROBE_TABLE_PREFIX}_${i} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
	ycsb_load
	for i in $(seq "$LEASE_PROBE_TABLE_COUNT"); do
		check_table_exists "${LEASE_DB}.${LEASE_PROBE_TABLE_PREFIX}_${i}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done
	check_table_exists "${LEASE_DB}.${YCSB_TABLE}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff "$WORK_DIR" "$CUR/conf/write_lease_diff_config.toml" 120

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
	printf '1\n' >"$LEASE_DUPLICATE_MARKER"
	wait_for_rejected_lease_response replayed_sequence "$duplicate_rejected_count"
	wait "$ycsb_pid"
	grep -Eq '^INSERT - .*Count: [1-9][0-9]*,' "$WORK_DIR/ycsb.log"
	grep -Eq '^UPDATE - .*Count: [1-9][0-9]*,' "$WORK_DIR/ycsb.log"
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

	export GO_FAILPOINTS='github.com/pingcap/ticdc/utils/dynstream/InjectDropEvent=10%return(true);github.com/pingcap/ticdc/coordinator/DelayCaptureWriteLeaseResponse=return(true);github.com/pingcap/ticdc/coordinator/DropCaptureWriteLeaseResponse=return(true);github.com/pingcap/ticdc/coordinator/DuplicateCaptureWriteLeaseResponse=return(true)'
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
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --server="127.0.0.1:$((CDC_BASE_PORT + 1))"
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

	cleanup_process $CDC_BINARY
}

function cleanup() {
	rm -f "$LEASE_DELAY_MARKER" "$LEASE_DROP_MARKER" "$LEASE_DUPLICATE_MARKER"
	stop_test "$WORK_DIR"
}

trap 'cleanup' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
