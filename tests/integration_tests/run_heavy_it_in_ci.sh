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
	'random_drop_message random_drop_message random_drop_message'
	# G01
	'random_drop_message random_drop_message random_drop_message'
	# G02
	'random_drop_message random_drop_message random_drop_message'
	# G03
	'random_drop_message random_drop_message random_drop_message'
	# G04
	'random_drop_message random_drop_message random_drop_message'
	# G05
	'random_drop_message random_drop_message random_drop_message'
	# G06
	'random_drop_message random_drop_message random_drop_message'
	# G07
	'random_drop_message random_drop_message random_drop_message'
	# G01
	'random_drop_message random_drop_message random_drop_message'
	# G02
	'random_drop_message random_drop_message random_drop_message'
	# G03
	'random_drop_message random_drop_message random_drop_message'
	# G04
	'random_drop_message random_drop_message random_drop_message'
	# G05
	'random_drop_message random_drop_message random_drop_message'
	# G06
	'random_drop_message random_drop_message random_drop_message'
	# G04
	'random_drop_message random_drop_message random_drop_message'
	# G05
	'random_drop_message random_drop_message random_drop_message'
)

# 12 CPU cores will be allocated to run each kafka heavy group in CI pipelines.
kafka_groups=(
	# G00
	'random_drop_message random_drop_message random_drop_message'
	# G01
	'random_drop_message random_drop_message random_drop_message'
	# G02
	'random_drop_message random_drop_message random_drop_message'
	# G03
	'random_drop_message random_drop_message random_drop_message'
	# G04
	'random_drop_message random_drop_message random_drop_message'
	# G05
	'random_drop_message random_drop_message random_drop_message'
	# G06
	'random_drop_message random_drop_message random_drop_message'
	# G07
	'random_drop_message random_drop_message random_drop_message'
	# G01
	'random_drop_message random_drop_message random_drop_message'
	# G02
	'random_drop_message random_drop_message random_drop_message'
	# G03
	'random_drop_message random_drop_message random_drop_message'
	# G04
	'random_drop_message random_drop_message random_drop_message'
	# G05
	'random_drop_message random_drop_message random_drop_message'
	# G06
	'random_drop_message random_drop_message random_drop_message'
	# G04
	'random_drop_message random_drop_message random_drop_message'
	# G05
	'random_drop_message random_drop_message random_drop_message'
)

# 12 CPU cores will be allocated to run each pulsar heavy group in CI pipelines.
pulsar_groups=(
	# G00
	'generate_column many_pk_or_uk multi_source'
	# G01
	'canal_json_basic canal_json_claim_check canal_json_content_compatible ddl_for_split_tables_with_random_move_table'
	# G02
	'canal_json_handle_key_only ddl_for_split_tables_with_failover'
	# G03
	'canal_json_adapter_compatibility ddl_for_split_tables_with_merge_and_split'
	# G04
	'open_protocol_claim_check open_protocol_handle_key_only'
	# G05
	'move_table drop_many_tables'
	# G06
	'cdc default_value ddl_for_split_tables_with_random_merge_and_split'
	# G07
	'merge_table resolve_lock force_replicate_table'
	# G08
	'tidb_mysql_test'
	# G09
	'mq_sink_error_resume'
	# G10
	'ddl_for_split_tables_random_schedule'
	# G11
	'ddl_with_random_move_table'
	# G12
	'fail_over region_merge multi_changefeeds'
	# G13
	'debezium01 fail_over_ddl_mix'
	# G14
	'debezium02'
	# G15
	'debezium03'
)

storage_groups=(
	# G00
	'random_drop_message random_drop_message random_drop_message'
	# G01
	'random_drop_message random_drop_message random_drop_message'
	# G02
	'random_drop_message random_drop_message random_drop_message'
	# G03
	'random_drop_message random_drop_message random_drop_message'
	# G04
	'random_drop_message random_drop_message random_drop_message'
	# G05
	'random_drop_message random_drop_message random_drop_message'
	# G06
	'random_drop_message random_drop_message random_drop_message'
	# G07
	'random_drop_message random_drop_message random_drop_message'
	# G01
	'random_drop_message random_drop_message random_drop_message'
	# G02
	'random_drop_message random_drop_message random_drop_message'
	# G03
	'random_drop_message random_drop_message random_drop_message'
	# G04
	'random_drop_message random_drop_message random_drop_message'
	# G05
	'random_drop_message random_drop_message random_drop_message'
	# G06
	'random_drop_message random_drop_message random_drop_message'
	# G04
	'random_drop_message random_drop_message random_drop_message'
	# G05
	'random_drop_message random_drop_message random_drop_message'
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
