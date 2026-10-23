#!/bin/bash

# Sourced by run.sh: reuse its TiDB cluster, CDC servers and split_region table.
CREDENTIAL_CF=credential-update
CREDENTIAL_API="http://${CDC_HOST}:${CDC_PORT}/api/v2/changefeeds"
CREDENTIAL_OUTPUTS="$WORK_DIR/credential_outputs"
KAFKA_AUTH_PID=""

function stop_kafka_auth_server() {
	if [ -n "$KAFKA_AUTH_PID" ]; then
		kill "$KAFKA_AUTH_PID" 2>/dev/null || true
		wait "$KAFKA_AUTH_PID" 2>/dev/null || true
		KAFKA_AUTH_PID=""
	fi
}

function start_kafka_auth_server() {
	stop_kafka_auth_server
	kafka_auth_server --password="$1" >"$WORK_DIR/kafka_auth_server.log" 2>&1 &
	KAFKA_AUTH_PID=$!
	for ((i = 0; i < 30; i++)); do
		if curl -fsS "http://127.0.0.1:18089/ready" >/dev/null 2>&1; then
			return
		fi
		sleep 1
	done
	echo "authenticated Kafka fixture did not become ready"
	exit 1
}

function assert_credential_outputs() {
	# Input config files and SQL setup logs contain test credentials by design.
	# Check only product responses and CDC logs; never print a leaking body.
	if grep -Eq 'credential-sentinel|b2F1dGgtY3JlZGVudGlhbC1zZW50aW5lbA==' "$CREDENTIAL_OUTPUTS"/* "$WORK_DIR"/cdc*.log "$WORK_DIR"/stdout*.log; then
		echo "changefeed credentials leaked in product output"
		exit 1
	fi
}

function credential_request() {
	local method=$1 path=$2 output=$3 expected=$4
	shift 4
	local status
	status=$(curl -sS --max-time 30 -o "$CREDENTIAL_OUTPUTS/$output" -w '%{http_code}' \
		-X "$method" -H 'Content-Type: application/json' \
		"$CREDENTIAL_API/$CREDENTIAL_CF$path?keyspace=$KEYSPACE_NAME" "$@")
	if [ "$status" != "$expected" ]; then
		echo "credential request $method $path: expected HTTP $expected, got $status"
		exit 1
	fi
}

function credential_resume_and_check() {
	local marker=$1
	credential_request GET "" "resume-before-$marker.json" 200
	cdc_cli_changefeed resume -c "$CREDENTIAL_CF" >"$CREDENTIAL_OUTPUTS/resume-$marker.txt" 2>&1
	run_sql "INSERT INTO split_region.test1(id, val) VALUES ($marker, $marker);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	local target_ts checkpoint seen
	target_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	for ((i = 0; i < 60; i++)); do
		credential_request GET "" "query-$marker.json" 200
		checkpoint=$(jq -r '.checkpoint_ts' "$CREDENTIAL_OUTPUTS/query-$marker.json")
		seen=false
		if [ "$SINK_TYPE" = kafka ]; then
			if curl -fsS "http://127.0.0.1:18089/seen/$marker" >/dev/null 2>&1; then
				seen=true
			fi
		else
			if [ "$(mysql -uroot -h"$DOWN_TIDB_HOST" -P"$DOWN_TIDB_PORT" -Nse \
				"SELECT COUNT(*) FROM split_region.test1 WHERE id=$marker AND val=$marker")" = 1 ]; then
				seen=true
			fi
		fi
		if [ "$checkpoint" != null ] && [ "$checkpoint" -gt "$target_ts" ] && [ "$seen" = true ]; then
			# Resume removes fields unused by this sink. Check the settings exercised
			# here; fresh downstream replication above verifies the stored password.
			if ! jq -e --slurp 'map({sink_uri, memory_quota: .config.memory_quota,
				kafka_config: .config.sink.kafka_config}) | .[0] == .[1]' \
				"$CREDENTIAL_OUTPUTS/resume-before-$marker.json" "$CREDENTIAL_OUTPUTS/query-$marker.json" >/dev/null; then
				echo "resumed changefeed changed sink URI, memory quota or Kafka configuration"
				exit 1
			fi
			assert_credential_outputs
			return
		fi
		sleep 2
	done
	echo "resumed changefeed failed to replicate new row $marker and advance checkpoint"
	exit 1
}

function check_invalid_credential_create() {
	local sink_uri=$1 start_ts=$2 status
	if [ "$SINK_TYPE" = kafka ]; then
		jq -n --arg uri "$sink_uri" --arg keyspace "$KEYSPACE_NAME" --argjson start "$start_ts" \
			'{changefeed_id:"credential-update",keyspace:$keyspace,start_ts:$start,sink_uri:$uri,
			replica_config:{sink:{kafka_config:{sasl_user:"alice",sasl_mechanism:"PLAIN",sasl_password:"credential-sentinel-wrong"}}}}' >"$WORK_DIR/create-rejected.json"
	else
		jq -n --arg uri "mysql://credential_update:credential-sentinel-wrong@$DOWN_TIDB_HOST:$DOWN_TIDB_PORT/" \
			--arg keyspace "$KEYSPACE_NAME" --argjson start "$start_ts" \
			'{changefeed_id:"credential-update",keyspace:$keyspace,start_ts:$start,sink_uri:$uri}' >"$WORK_DIR/create-rejected.json"
	fi
	status=$(curl -sS -X POST -H 'Content-Type: application/json' --data-binary "@$WORK_DIR/create-rejected.json" \
		"$CREDENTIAL_API" -o "$CREDENTIAL_OUTPUTS/create-rejected.json" -w '%{http_code}')
	[ "$status" = 400 ]
	credential_request GET "" create-not-found.json 400
	jq -e '.error_code == "CDC:ErrChangeFeedNotExists"' "$CREDENTIAL_OUTPUTS/create-not-found.json" >/dev/null
}

function check_credential_updates() {
	if [ "$SINK_TYPE" != mysql ] && [ "$SINK_TYPE" != kafka ]; then
		return
	fi
	if [ "$SINK_TYPE" = kafka ]; then
		# CI restores prebuilt CDC binaries without this case's fixture.
		make -C "$CUR/../../.." kafka_auth_server
	fi
	mkdir -p "$CREDENTIAL_OUTPUTS"
	cdc_cli_changefeed remove -c test >"$CREDENTIAL_OUTPUTS/remove-original.txt" 2>&1
	local password=credential-sentinel-p1 sink_uri start_ts
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	if [ "$SINK_TYPE" = kafka ]; then
		start_kafka_auth_server "$password"
		sink_uri='kafka://127.0.0.1:19092/credentials?protocol=open-protocol&partition-num=1&required-acks=1&kafka-client=franz'
		cat >"$WORK_DIR/credential-create.toml" <<EOF
[sink.kafka-config]
sasl-user = "alice"
sasl-password = "$password"
sasl-mechanism = "PLAIN"
EOF
	else
		mysql -uroot -h"$DOWN_TIDB_HOST" -P"$DOWN_TIDB_PORT" <<EOF
CREATE USER 'credential_update'@'%' IDENTIFIED BY '$password';
GRANT ALL PRIVILEGES ON *.* TO 'credential_update'@'%';
EOF
		sink_uri="mysql://credential_update:$password@$DOWN_TIDB_HOST:$DOWN_TIDB_PORT/"
		: >"$WORK_DIR/credential-create.toml"
	fi
	check_invalid_credential_create "$sink_uri" "$start_ts"
	cdc_cli_changefeed create -c "$CREDENTIAL_CF" --start-ts="$start_ts" \
		--sink-uri="$sink_uri" --config="$WORK_DIR/credential-create.toml" >"$CREDENTIAL_OUTPUTS/create.txt" 2>&1
	cdc_cli_changefeed pause -c "$CREDENTIAL_CF"
	credential_resume_and_check 1073747000

	# The CLI reads a redacted API model; omitted credentials must survive TOML defaults.
	cdc_cli_changefeed pause -c "$CREDENTIAL_CF"
	echo 'memory-quota = 2097152' >"$WORK_DIR/credential-update.toml"
	cdc_cli_changefeed update -c "$CREDENTIAL_CF" --no-confirm --config="$WORK_DIR/credential-update.toml" \
		>"$CREDENTIAL_OUTPUTS/cli-update.txt" 2>&1
	credential_request GET "" cli-update.json 200
	jq -e '.config.memory_quota == 2097152' "$CREDENTIAL_OUTPUTS/cli-update.json" >/dev/null
	credential_resume_and_check 1073747001

	# HTTP partial updates and GET -> edit -> PUT both preserve hidden credentials.
	cdc_cli_changefeed pause -c "$CREDENTIAL_CF"
	credential_request PUT "" partial-update.json 200 -d '{"replica_config":{"memory_quota":3145728}}'
	jq -e '.config.memory_quota == 3145728' "$CREDENTIAL_OUTPUTS/partial-update.json" >/dev/null
	credential_resume_and_check 1073747002
	cdc_cli_changefeed pause -c "$CREDENTIAL_CF"
	credential_request GET "" roundtrip-before.json 200
	jq '{replica_config: (.config | .memory_quota = 4194304)}' "$CREDENTIAL_OUTPUTS/roundtrip-before.json" >"$WORK_DIR/roundtrip-request.json"
	credential_request PUT "" roundtrip-update.json 200 --data-binary "@$WORK_DIR/roundtrip-request.json"
	credential_request GET "" roundtrip-after.json 200
	jq -e '.config.memory_quota == 4194304' "$CREDENTIAL_OUTPUTS/roundtrip-after.json" >/dev/null
	credential_resume_and_check 1073747003

	# Revoke P1 in the downstream before storing P2; success now requires P2.
	cdc_cli_changefeed pause -c "$CREDENTIAL_CF"
	password=credential-sentinel-p2
	if [ "$SINK_TYPE" = kafka ]; then
		start_kafka_auth_server "$password"
		cat >>"$WORK_DIR/credential-update.toml" <<EOF
[sink.kafka-config]
sasl-password = "$password"
EOF
		cdc_cli_changefeed update -c "$CREDENTIAL_CF" --no-confirm --config="$WORK_DIR/credential-update.toml" \
			>"$CREDENTIAL_OUTPUTS/rotate.txt" 2>&1
	else
		mysql -uroot -h"$DOWN_TIDB_HOST" -P"$DOWN_TIDB_PORT" -e "ALTER USER 'credential_update'@'%' IDENTIFIED BY '$password';"
		sink_uri="mysql://credential_update:$password@$DOWN_TIDB_HOST:$DOWN_TIDB_PORT/"
		cdc_cli_changefeed update -c "$CREDENTIAL_CF" --no-confirm --sink-uri="$sink_uri" \
			>"$CREDENTIAL_OUTPUTS/rotate.txt" 2>&1
	fi
	credential_resume_and_check 1073747004

	# Wrong and explicitly empty passwords must reject the whole update.
	cdc_cli_changefeed pause -c "$CREDENTIAL_CF"
	credential_request GET "" rejected-before.json 200
	# Check the CLI error path as well as HTTP's atomic rejection below.
	if [ "$SINK_TYPE" = kafka ]; then
		cat >"$WORK_DIR/credential-rejected.toml" <<EOF
memory-quota = 7340032
[sink.kafka-config]
sasl-password = "credential-sentinel-wrong"
EOF
		if cdc_cli_changefeed update -c "$CREDENTIAL_CF" --no-confirm --config="$WORK_DIR/credential-rejected.toml" \
			>"$CREDENTIAL_OUTPUTS/cli-rejected.txt" 2>&1; then
			echo "CLI accepted an invalid password"
			exit 1
		fi
	fi
	for candidate in credential-sentinel-wrong ''; do
		if [ "$SINK_TYPE" = kafka ]; then
			jq -n --arg password "$candidate" '{replica_config:{memory_quota:7340032,sink:{kafka_config:{sasl_password:$password}}}}' >"$WORK_DIR/rejected-request.json"
		else
			jq -n --arg uri "mysql://credential_update:$candidate@$DOWN_TIDB_HOST:$DOWN_TIDB_PORT/" \
				'{sink_uri:$uri,replica_config:{memory_quota:7340032}}' >"$WORK_DIR/rejected-request.json"
		fi
		credential_request PUT "" rejected.json 400 --data-binary "@$WORK_DIR/rejected-request.json"
		credential_request GET "" rejected-after.json 200
		jq -e --slurp '.[0].config == .[1].config and .[0].sink_uri == .[1].sink_uri' \
			"$CREDENTIAL_OUTPUTS/rejected-before.json" "$CREDENTIAL_OUTPUTS/rejected-after.json" >/dev/null
	done
	credential_resume_and_check 1073747005

	if [ "$SINK_TYPE" = kafka ]; then
		# Display markers remain literal valid passwords when explicitly supplied.
		local marker=1073747006
		for password in '******' xxxxx; do
			cdc_cli_changefeed pause -c "$CREDENTIAL_CF"
			start_kafka_auth_server "$password"
			jq -n --arg password "$password" '{replica_config:{sink:{kafka_config:{sasl_password:$password}}}}' >"$WORK_DIR/literal-request.json"
			credential_request PUT "" literal.json 200 --data-binary "@$WORK_DIR/literal-request.json"
			credential_resume_and_check "$marker"
			marker=$((marker + 1))
		done
		check_oauth_error_redaction
	else
		# An explicit URI can equal the masked GET value. It still replaces the
		# real password: revoke P2 and set the downstream's literal password to xxxxx.
		cdc_cli_changefeed pause -c "$CREDENTIAL_CF"
		credential_request GET "" literal-uri-before.json 200
		sink_uri=$(jq -r '.sink_uri' "$CREDENTIAL_OUTPUTS/literal-uri-before.json")
		mysql -uroot -h"$DOWN_TIDB_HOST" -P"$DOWN_TIDB_PORT" -e "ALTER USER 'credential_update'@'%' IDENTIFIED BY 'xxxxx';"
		cdc_cli_changefeed update -c "$CREDENTIAL_CF" --no-confirm --sink-uri="$sink_uri" \
			>"$CREDENTIAL_OUTPUTS/literal-uri.txt" 2>&1
		credential_resume_and_check 1073747006
	fi
	cdc_cli_changefeed query -c "$CREDENTIAL_CF" >"$CREDENTIAL_OUTPUTS/cli-query.txt" 2>&1
	cdc_cli_changefeed remove -c "$CREDENTIAL_CF" >"$CREDENTIAL_OUTPUTS/remove.txt" 2>&1
	assert_credential_outputs
	stop_kafka_auth_server
}

function check_oauth_error_redaction() {
	cdc_cli_changefeed pause -c "$CREDENTIAL_CF"
	credential_request GET "" oauth-before.json 200
	for driver in franz sarama; do
		local token_calls
		token_calls=$(curl -fsS http://127.0.0.1:18089/token-count)
		jq -n --arg uri "kafka://127.0.0.1:19092/credentials?protocol=open-protocol&partition-num=1&required-acks=1&kafka-client=$driver" \
			'{sink_uri:$uri,replica_config:{memory_quota:8388608,sink:{kafka_config:{sasl_mechanism:"OAUTHBEARER",sasl_oauth_client_id:"alice",sasl_oauth_client_secret:"b2F1dGgtY3JlZGVudGlhbC1zZW50aW5lbA==",sasl_oauth_token_url:"http://127.0.0.1:18089/token?client_secret=oauth-credential-sentinel"}}}}' >"$WORK_DIR/oauth-request.json"
		credential_request PUT "" "oauth-$driver.json" 400 --data-binary "@$WORK_DIR/oauth-request.json"
		[ "$(curl -fsS http://127.0.0.1:18089/token-count)" -gt "$token_calls" ]
		credential_request GET "" oauth-after.json 200
		jq -e --slurp '.[0].config == .[1].config and .[0].sink_uri == .[1].sink_uri' \
			"$CREDENTIAL_OUTPUTS/oauth-before.json" "$CREDENTIAL_OUTPUTS/oauth-after.json" >/dev/null
	done
	credential_resume_and_check 1073747008
}
