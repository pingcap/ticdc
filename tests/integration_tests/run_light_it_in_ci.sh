#!/bin/bash

set -eo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

sink_type=$1
group=$2
group_num=${group#G}

# This file is used for running light integration tests in CI pipelines.
# If we implement a new test case, which is light, we should add it to this file.
# If the new test case is heavy, please add it to run_heavy_it_in_ci.sh.
#
# Here are four groups of tests defined below, corresponding to four sink types: mysql, kafka, pulsar, and storage.
# Please add the new test case to each group according to the sink type.
# For example, the case "batch_add_table" should be added to all four groups, because it should be tested in all sink types.
# The case "kafka_big_messages" should be added to the kafka group only, because it is a kafka-specific test case.
# The case will not be executed on a sink type if it is not added to the corresponding group.
#
# For each sink type, we define 16 groups of tests.
# When we add a case, we should keep the cost of each group as close as possible to reduce the waiting time of CI pipelines.
# The number of groups should not be changed, which is 16.
# But if we have to add a new group, the new group number should be updated in the CI pipeline configuration file:
# For mysql: https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pull_cdc_mysql_integration_light.groovy
# For kafka: https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pull_cdc_kafka_integration_light.groovy
# For pulsar: https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pull_cdc_pulsar_integration_light.groovy
# For storage: https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pull_cdc_storage_integration_light.groovy

# Resource allocation for mysql light integration tests in CI pipelines:
# https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pod-pull_cdc_mysql_integration_light.yaml
# 4 CPU, 16 Gi memory.
mysql_groups=(
	# G00
	'charset_gbk changefeed_finish sql_mode changefeed_reconstruct fail_over_ddl_A'
	# G01
	'common_1 foreign_key changefeed_pause_resume fail_over_ddl_B'
	# G02
	'new_ci_collation safe_mode savepoint fail_over_ddl_C'
	# G03
	'capture_suicide_while_balance_table kv_client_stream_reconnect fail_over_ddl_D'
	# G04
	'ci_collation_compatibility multi_capture resourcecontrol fail_over_ddl_E'
	# G05
	'vector simple partition_table fail_over_ddl_F'
	# G06
	'http_api http_api_tls fail_over_ddl_G'
	# G07
	'http_api_tls_with_user_auth fail_over_ddl_H'
	# G08
	'capture_session_done_during_task changefeed_dup_error_restart mysql_sink_retry fail_over_ddl_I'
	# G09
	'cdc_server_tips ddl_sequence server_config_compatibility fail_over_ddl_J'
	# G10
	'changefeed_error bdr_mode fail_over_ddl_K'
	# G11
	'multi_tables_ddl ddl_attributes multi_cdc_cluster fail_over_ddl_L'
	# G12
	'row_format tiflash multi_rocks fail_over_ddl_M'
	# G13
	'cli_tls_with_auth cli_with_auth fail_over_ddl_N'
	# G14
	'batch_add_table batch_update_to_no_batch fail_over_ddl_O'
	# G15
	'split_region changefeed_resume_with_checkpoint_ts autorandom gc_safepoint'
)

# Resource allocation for kafka light integration tests in CI pipelines:
# https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pod-pull_cdc_kafka_integration_light.yaml
# 6 CPU, 16 Gi memory.
kafka_groups=(
	# G00
	'canal_json_basic'
	# G01
	'canal_json_claim_check'
	# G02
	'canal_json_content_compatible'
	# G03
	'canal_json_handle_key_only'
	# G04
	'canal_json_storage_basic'
	# G05
	'open_protocol_claim_check'
	# G06
	'open_protocol_handle_key_only'
	# G07
	'kafka_big_messages'
	# G08
	'kafka_compression'
	# G19
	'kafka_messages'
	# G10
	'mq_sink_dispatcher'
	# G11
	'multi_topics'
	# G12
	''
	# G13
	'avro_basic'
)

# Resource allocation for pulsar light integration tests in CI pipelines:
# https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pod-pull_cdc_pulsar_integration_light.yaml
# 6 CPU, 32 Gi memory.
pulsar_groups=(
	# G00
	'canal_json_basic'
	# G01
	'canal_json_claim_check'
	# G02
	'canal_json_content_compatible'
	# G03
	'canal_json_handle_key_only'
	# G04
	'canal_json_storage_basic'
	# G05
	''
	# G06
	''
	# G07
	''
	# G08
	''
	# G09
	''
	# G10
	''
	# G11
	''
	# G12
	''
	# G13
	''
	# G14
	''
	# G15
	''
)

# Resource allocation for storage light integration tests in CI pipelines:
# https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pod-pull_cdc_storage_integration_light.yaml
# 6 CPU, 16 Gi memory.
storage_groups=(
	# G00
	'lossy_ddl'
	# G01
	'storage_cleanup'
	# G02
	'csv_storage_basic'
	# G03
	'csv_storage_multi_tables_ddl'
	# G04
	''
	# G05
	''
	# G06
	''
	# G07
	''
	# G08
	''
	# G09
	''
	# G10
	''
	# G11
	''
	# G12
	''
	# G13
	''
	# G14
	''
	# G15
	''
)

# Source shared functions and check test coverage
source "$CUR/_utils/check_coverage.sh"
check_test_coverage "$CUR"

case "$sink_type" in
mysql) groups=("${mysql_groups[@]}") ;;
kafka) groups=("${kafka_groups[@]}") ;;
pulsar) groups=("${pulsar_groups[@]}") ;;
storage) groups=("${storage_groups[@]}") ;;
*)
	echo "Error: unknown sink type: ${sink_type}"
	exit 1
	;;
esac

# Print debug information
echo "Sink Type: ${sink_type}"
echo "Group Name: ${group}"
echo "Group Number (parsed): ${group_num}"

if [[ $group_num =~ ^[0-9]+$ ]] && [[ -n ${groups[10#${group_num}]} ]]; then
	# force use decimal index
	test_names="${groups[10#${group_num}]}"
	# Run test cases
	echo "Run cases: ${test_names}"
	export TICDC_NEWARCH=true
	"${CUR}"/run.sh "${sink_type}" "${test_names}"
else
	echo "Warnning: invalid group name: ${group}, or this group is empty."
	# For now, the CI pipeline will fail if the group is empty.
	# So we comment out the exit command here.
	# But if the groups are full of test cases, we should uncomment the exit command.
	# exit 1
fi
