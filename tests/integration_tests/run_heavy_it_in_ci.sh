#!/bin/bash

set -eo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

sink_type=$1
group=$2
group_num=${group#G}

# This file is used for running heavy integration tests in CI pipelines.
# If we implement a new test case, which is heavy, we should add it to this file.
# If the new test case is light, please add it to run_light_it_in_ci.sh.
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
# For mysql: https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pull_cdc_mysql_integration_heavy.groovy
# For kafka: https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pull_cdc_kafka_integration_heavy.groovy
# For pulsar: https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pull_cdc_pulsar_integration_heavy.groovy
# For storage: https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pull_cdc_storage_integration_heavy.groovy

# 12 CPU cores will be allocated to run each mysql heavy group in CI pipelines.
mysql_groups=(
	# G00
	'generate_column many_pk_or_uk multi_source'
	# G01
	'api_v2 ddl_for_split_tables_with_random_move_table'
	# G02
	'availability ddl_for_split_tables_with_failover'
	# G03
	''
	# G04
	'syncpoint syncpoint_check_ts'
	# G05
	'move_table ddl_for_split_tables_with_merge_and_split'
	# G06
	'cdc ddl_for_split_tables_with_random_merge_and_split'
	# G07
	'resolve_lock merge_table'
	# G08
	'bank'
	# G09
	'drop_many_tables'
	# G10
	'default_value http_proxies'
	# G11
	'ddl_reentrant force_replicate_table'
	# G12
	'tidb_mysql_test ddl_with_random_move_table'
	# G13
	'fail_over region_merge'
	# G14
	'fail_over_ddl_mix'
	# G15
	'fail_over_ddl_mix_with_syncpoint'
)

# 12 CPU cores will be allocated to run each kafka heavy group in CI pipelines.
kafka_groups=(
	# G00
	'generate_column many_pk_or_uk multi_source'
	# G01
	# ddl_for_split_tables_with_random_move_table
	'canal_json_basic canal_json_claim_check canal_json_content_compatible'
	# G02
	'canal_json_handle_key_only'
	# G03
	'canal_json_adapter_compatibility'
	# G04
	'open_protocol_claim_check open_protocol_handle_key_only'
	# G05
	# move_table ddl_for_split_tables_with_merge_and_split
	'drop_many_tables'
	# G06
	# ddl_for_split_tables_with_random_merge_and_split
	'cdc default_value'
	# G07
	# merge_table
	'resolve_lock force_replicate_table'
	# G08
	'tidb_mysql_test'
	# G09
	'mq_sink_error_resume'
	# G10
	'kafka_column_selector kafka_column_selector_avro'
	# fail_over_ddl_mix_with_syncpoint
	# ddl_with_random_move_table
	# fail_over_ddl_mix
	# G11
	'fail_over region_merge'
	# G12
	''
	# G13
	'debezium01'
	# G14
	'debezium02'
	# G15
	'debezium03'
)

# 12 CPU cores will be allocated to run each pulsar heavy group in CI pipelines.
pulsar_groups=(
	# G00
	'generate_column many_pk_or_uk multi_source'
	# G01
	'canal_json_basic canal_json_claim_check canal_json_content_compatible ddl_for_split_tables_with_random_move_table'
	# G02
	'canal_json_handle_key_only canal_json_storage_basic canal_json_storage_partition_table ddl_for_split_tables_with_failover'
	# G03
	'canal_json_adapter_compatibility'
	# G04
	'open_protocol_claim_check open_protocol_handle_key_only'
	# G05
	# move_table
	'drop_many_tables ddl_for_split_tables_with_merge_and_split'
	# G06
	'cdc default_value'
	# G07
	# merge_table
	'resolve_lock force_replicate_table'
	# G08
	'tidb_mysql_test'
	# G09
	'mq_sink_error_resume'
	# G10
	fail_over_ddl_mix_with_syncpoint
	# G11
	'ddl_with_random_move_table'
	# G12
	'fail_over region_merge'
	# G13
	# fail_over_ddl_mix
	'debezium01'
	# G14
	'debezium02'
	# G15
	'debezium03'
)

storage_groups=(
	# G00
	'many_pk_or_uk generate_column multi_source'
	# G01
	''
	# G02
	'canal_json_storage_basic canal_json_storage_partition_table'
	# G03
	''
	# G04
	' '
	# G05
	# 'move_table drop_many_tables'
	'drop_many_tables'
	# G06
	'cdc default_value'
	# G07
	'resolve_lock force_replicate_table'
	# G08
	'tidb_mysql_test'
	# G09
	''
	# G10
	''
	# G11
	''
	# G12
	''
	# G13
	'fail_over region_merge'
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
	echo "Error: invalid group name: ${group}"
	# For now, the CI pipeline will fail if the group is empty.
	# So we comment out the exit command here.
	# But if the groups are full of test cases, we should uncomment the exit command.
	# exit 1
fi
