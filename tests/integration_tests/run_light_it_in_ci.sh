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
	'savepoint savepoint savepoint savepoint'
	# G01
	'savepoint savepoint savepoint savepoint'
	# G02
	'savepoint savepoint savepoint savepoint'
	# G03
	'savepoint savepoint savepoint savepoint'
	# G04
	'savepoint savepoint savepoint savepoint'
	# G05
	'savepoint savepoint savepoint savepoint'
	# G06
	'savepoint savepoint savepoint savepoint'
	# G07
	'savepoint savepoint savepoint savepoint'
	# G08
	'savepoint savepoint savepoint savepoint'
	# G09
	'savepoint savepoint savepoint savepoint'
	# G10
	'savepoint savepoint savepoint savepoint'
	# G11
	'savepoint savepoint savepoint savepoint'
	# G12
	'savepoint savepoint savepoint savepoint'
	# G13
	'savepoint savepoint savepoint savepoint'
	# G14
	'savepoint savepoint savepoint savepoint'
	# G15
	'savepoint savepoint savepoint savepoint'
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
	'debezium_basic'
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
