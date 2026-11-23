#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"

SINK_TYPE=$1
if [ "$SINK_TYPE" != "mysql" ]; then
	exit 0
fi

WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=${CDC_BINARY:-cdc.test}
NEW_PD_HOST=127.0.0.1
NEW_PD_PORT=2879
NEW_PD_PEER_PORT=2880
NEW_PD_ADDR="http://$NEW_PD_HOST:$NEW_PD_PORT"

wait_for_log() {
	local pattern=$1
	local retries=${2:-90}

	for ((i = 0; i < retries; i++)); do
		if grep -Fq "$pattern" "$WORK_DIR/cdc.log"; then
			return 0
		fi
		sleep 1
	done

	return 1
}

wait_for_new_pd_tso() {
	for ((i = 0; i < 30; i++)); do
		if run_cdc_cli_tso_query "$NEW_PD_HOST" "$NEW_PD_PORT" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done

	return 1
}

wait_for_new_pd_metadata_write() {
	local probe_service_id="ticdc-pd-scale-in-readiness-probe"

	for ((i = 0; i < 60; i++)); do
		# Deleting an absent service safepoint is idempotent, but still commits a
		# PD GC state transaction. This verifies metadata writes without leaving
		# probe data behind.
		if curl -fsS -X DELETE \
			"$NEW_PD_ADDR/pd/api/v1/gc/safepoint/$probe_service_id" \
			>/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done

	return 1
}

wait_for_pd4_leader() {
	for ((i = 0; i < 60; i++)); do
		if curl -fsS "$NEW_PD_ADDR/pd/api/v1/leader" |
			tr -d '[:space:]' |
			grep -Fq '"name":"pd4"'; then
			return 0
		fi
		sleep 1
	done

	return 1
}

remove_pd_member() {
	local pd_name=$1

	for ((i = 0; i < 30; i++)); do
		if ! curl -fsS "$NEW_PD_ADDR/pd/api/v1/members" |
			tr -d '[:space:]' |
			grep -Fq "\"name\":\"$pd_name\""; then
			return 0
		fi
		curl -fsS -X DELETE "$NEW_PD_ADDR/pd/api/v1/members/name/$pd_name" >/dev/null 2>&1 || true
		sleep 1
	done

	return 1
}

prepare() {
	rm -rf "$WORK_DIR"
	mkdir -p "$WORK_DIR"

	start_tidb_cluster --workdir "$WORK_DIR" --multiple-upstream-pd true

	local initial_pd_addrs
	initial_pd_addrs="http://${UP_PD_HOST_1}:${UP_PD_PORT_1},http://${UP_PD_HOST_2}:${UP_PD_PORT_2},http://${UP_PD_HOST_3}:${UP_PD_PORT_3}"
	run_cdc_server \
		--workdir "$WORK_DIR" \
		--binary "$CDC_BINARY" \
		--pd "$initial_pd_addrs"

	pd-server \
		--advertise-client-urls "$NEW_PD_ADDR" \
		--client-urls "http://0.0.0.0:$NEW_PD_PORT" \
		--advertise-peer-urls "http://$NEW_PD_HOST:$NEW_PD_PEER_PORT" \
		--peer-urls "http://0.0.0.0:$NEW_PD_PEER_PORT" \
		--config "$WORK_DIR/pd-config.toml" \
		--log-file "$WORK_DIR/pd4.log" \
		--data-dir "$WORK_DIR/pd4" \
		--name pd4 \
		--join "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" &

	check_pd_health "$NEW_PD_HOST" "$NEW_PD_PORT"
	wait_for_log "$NEW_PD_ADDR" || {
		echo "TiCDC did not discover the newly added PD endpoint"
		return 1
	}

	curl -fsS -X POST "$NEW_PD_ADDR/pd/api/v1/leader/transfer/pd4"
	wait_for_pd4_leader || {
		echo "PD leader was not transferred to pd4"
		return 1
	}
	for pd_name in pd1 pd2 pd3; do
		remove_pd_member "$pd_name" || {
			echo "failed to remove $pd_name"
			return 1
		}
	done

	check_pd_health "$NEW_PD_HOST" "$NEW_PD_PORT"
	wait_for_new_pd_tso || {
		echo "the new PD endpoint did not serve TSO after the original PD nodes were removed"
		return 1
	}
	wait_for_new_pd_metadata_write || {
		echo "the new PD endpoint did not accept metadata writes after the original PD nodes were removed"
		return 1
	}
}

trap 'stop_test "$WORK_DIR"' EXIT

prepare

# The create request does not specify pd_addrs, so TiCDC must reuse the default
# upstream clients after every PD endpoint supplied at startup is gone.
create_response=$(curl -fsS \
	--user ticdc:ticdc_secret \
	--header 'Content-Type: application/json' \
	--data '{"changefeed_id":"test","sink_uri":"blackhole://"}' \
	"http://${CDC_HOST}:${CDC_PORT}/api/v2/changefeeds")

echo "$create_response" | tr -d '[:space:]' | grep -Fq '"id":"test"'

echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
