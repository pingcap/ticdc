#!/bin/bash

# Verify credential omission, CLI masking and updates on release-8.5's JSON API.
set -euo pipefail
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
if [ "$SINK_TYPE" != "mysql" ]; then
	exit 0
fi
API="http://${CDC_HOST}:${CDC_PORT}/api/v2/changefeeds"
query_json() {
	curl -sf -X GET "$API/$1?keyspace=$KEYSPACE_NAME" -o "$2"
}

check_credential_redaction() {
	local outputs="$WORK_DIR/credential_outputs"
	mkdir -p "$outputs"
	cdc_cli_changefeed create -c cf-credentials \
		--sink-uri='blackhole://user:uri-credential-sentinel@localhost/?password=query-credential-sentinel&kafka-client=sarama' \
		--config="$CUR/conf/credentials.toml" >"$outputs/create.txt" 2>&1
	query_json cf-credentials "$outputs/query.json"
	cdc_cli_changefeed query -c cf-credentials >"$outputs/cli-query.txt" 2>&1
	curl -fsS "$API?keyspace=$KEYSPACE_NAME" -o "$outputs/list.json"
	for endpoint in debug/info api/v2/unsafe/metadata; do
		curl -fsS "http://${CDC_HOST}:${CDC_PORT}/$endpoint?keyspace=$KEYSPACE_NAME" \
			-o "$outputs/${endpoint//\//-}.txt"
	done

	# Secrets are absent, while public identifiers and the Kafka driver survive.
	jq -e '.config.sink.kafka_config.sasl_user == "visible-user" and
		.config.sink.kafka_config.sasl_oauth_client_id == "visible-client" and
		.config.sink.kafka_config.glue_schema_registry_config.registry_name == "visible-registry" and
		.config.sink.pulsar_config["tls-certificate-path"] == "visible-pulsar-key" and
		.config.sink.pulsar_config["tls-private-key-path"] == "visible-pulsar-cert" and
		(.sink_uri | contains("kafka-client=sarama"))' "$outputs/query.json" >/dev/null
	python3 - "$outputs/query.json" <<'PY_CHECK'
import json, sys
with open(sys.argv[1]) as f:
    cfg = json.load(f)["config"]
paths = [
    ("consistent", "storage"),
    ("sink", "kafka_config", "sasl_password"),
    ("sink", "kafka_config", "sasl_gssapi_password"),
    ("sink", "kafka_config", "sasl_oauth_client_secret"),
    ("sink", "kafka_config", "sasl_oauth_token_url"),
    ("sink", "kafka_config", "key"),
    ("sink", "kafka_config", "large_message_handle", "claim_check_storage_uri"),
    ("sink", "kafka_config", "glue_schema_registry_config", "access_key"),
    ("sink", "kafka_config", "glue_schema_registry_config", "secret_access_key"),
    ("sink", "kafka_config", "glue_schema_registry_config", "token"),
    ("sink", "pulsar_config", "authentication-token"),
    ("sink", "pulsar_config", "basic-password"),
    ("sink", "pulsar_config", "oauth2", "oauth2-private-key"),
    ("sink", "pulsar_config", "oauth2", "oauth2-issuer-url"),
]
for path in paths:
    node = cfg
    for key in path[:-1]:
        node = node[key]
    if path[-1] in node:
        raise SystemExit("credential field was not omitted: " + ".".join(path))
PY_CHECK
	cdc_cli_changefeed pause -c cf-credentials
	echo 'memory-quota = 2097152' >"$WORK_DIR/credential-update.toml"
	cdc_cli_changefeed update -c cf-credentials --config="$WORK_DIR/credential-update.toml" --no-confirm \
		>"$outputs/update.txt" 2>&1
	query_json cf-credentials "$outputs/updated.json"
	jq -e '.config.memory_quota == 2097152 and
		.config.sink.pulsar_config["tls-certificate-path"] == "visible-pulsar-key" and
		.config.sink.pulsar_config["tls-private-key-path"] == "visible-pulsar-cert"' "$outputs/updated.json" >/dev/null
	# Invalid redo and sink storage URIs must reject updates and redact errors.
	for kind in redo sink; do
		local expected_status expected_code
		if [ "$kind" = redo ]; then
			# Replica config validation errors use the existing HTTP 500 mapping.
			expected_status=500
			expected_code=CDC:ErrInvalidReplicaConfig
			jq -n '{replica_config:{consistent:{level:"eventual",storage:"s3:///missing-bucket?secret-access-key=redo-credential-sentinel"}}}' >"$WORK_DIR/storage-error.json"
		else
			expected_status=400
			expected_code=CDC:ErrSinkURIInvalid
			jq -n '{sink_uri:"s3:///missing-bucket?protocol=canal-json&secret-access-key=storage-credential-sentinel"}' >"$WORK_DIR/storage-error.json"
		fi
		local status
		status=$(curl -sS -X PUT "$API/cf-credentials?keyspace=$KEYSPACE_NAME" \
			-H 'Content-Type: application/json' --data-binary "@$WORK_DIR/storage-error.json" \
			-o "$outputs/$kind-error.json" -w '%{http_code}')
		if [ "$status" != "$expected_status" ]; then
			echo "FAIL: invalid $kind storage update: expected HTTP $expected_status, got $status"
			exit 1
		fi
		jq -e --arg code "$expected_code" '.error_code == $code' "$outputs/$kind-error.json" >/dev/null
		query_json cf-credentials "$outputs/$kind-after.json"
		jq -e --slurp '.[0].config == .[1].config and .[0].sink_uri == .[1].sink_uri' \
			"$outputs/updated.json" "$outputs/$kind-after.json" >/dev/null
	done
	cdc_cli_changefeed resume -c cf-credentials >"$outputs/resume.txt" 2>&1
	# The diagnostic API must also redact the retained credentials after an update.
	curl -fsS "http://${CDC_HOST}:${CDC_PORT}/api/v2/unsafe/metadata?keyspace=$KEYSPACE_NAME" -o "$outputs/metadata-after.json"
	jq -e 'any(.[]; (.value | fromjson? | .config.sink."kafka-config"."sasl-password") == "******")' \
		"$outputs/metadata-after.json" >/dev/null
	# Confluent and Glue are mutually exclusive, so cover Confluent separately.
	cat >"$WORK_DIR/schema-credential.toml" <<EOF
[sink]
schema-registry = "https://user:schema-credential-sentinel@registry.example"
EOF
	cdc_cli_changefeed create -c cf-schema-credential --sink-uri=blackhole:// \
		--config="$WORK_DIR/schema-credential.toml" >"$outputs/schema-create.txt" 2>&1
	query_json cf-schema-credential "$outputs/schema.json"
	jq -e '.config.sink | has("schema_registry") | not' "$outputs/schema.json" >/dev/null
	cdc_cli_changefeed remove -c cf-schema-credential >"$outputs/schema-remove.txt" 2>&1
	cdc_cli_changefeed remove -c cf-credentials >"$outputs/remove.txt" 2>&1
	if grep -Eq 'credential-sentinel' "$outputs"/* "$WORK_DIR"/cdc*.log "$WORK_DIR"/stdout*.log; then
		echo "FAIL: credential leaked in API, CLI or CDC log output"
		exit 1
	fi
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	check_credential_redaction
	cleanup_process $CDC_BINARY
}
trap 'stop_test $WORK_DIR' EXIT
run "$@"
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
