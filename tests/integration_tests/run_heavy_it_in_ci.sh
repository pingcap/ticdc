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
# For each sink type, we define 16 groups of tests. And 12 CPU cores will be allocated to run each group in CI pipelines.
# When we add a case, we should keep the cost of each group as close as possible to reduce the waiting time of CI pipelines.
# The number of groups should not be changed, which is 16.
# But if we have to add a new group, the new group number should be updated in the CI pipeline configuration file:
# For mysql: https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pull_cdc_integration_test.groovy
# For kafka: https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pull_cdc_integration_kafka_test.groovy
# For pulsar: https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pull_cdc_integration_pulsar_test.groovy
# For storage: https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/ticdc/latest/pull_cdc_integration_storage_test.groovy

mysql_groups=(
	# G00
	'changefeed_pause_resume region_merge'
	# G01
	'api_v2 availability bank'
	# G02
	'multi_cdc_cluster multi_rocks'
	# G03
	'multi_source fail_over'
	# G04
	'syncpoint tidb_mysql_test resolve_lock'
	# G05
	'move_table cdc'
	# G06
	'fail_over_ddl_A fail_over_ddl_B'
	# G07
	'fail_over_ddl_C fail_over_ddl_D'
	# G08
	'fail_over_ddl_E fail_over_ddl_F'
	# G09
	'fail_over_ddl_G fail_over_ddl_H'
	# G10
	'fail_over_ddl_I fail_over_ddl_J'
	# G11
	'fail_over_ddl_K fail_over_ddl_L'
	# G12
	'fail_over_ddl_M fail_over_ddl_N'
	# G13
	'fail_over_ddl_O'
	# G14
	'fail_over_ddl_mix'
	# G15
	'fail_over_ddl_mix_with_syncpoint'
)

kafka_groups=(
	# G00
	''
	# G01
	''
	# G02
	''
	# G03
	''
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

pulsar_groups=(
	# G00
	''
	# G01
	''
	# G02
	''
	# G03
	''
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

storage_groups=(
	# G00
	''
	# G01
	''
	# G02
	''
	# G03
	''
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
	"${CUR}"/run.sh "${sink_type}" "${test_names}"
else
	echo "Error: invalid group name: ${group}"
	exit 1
fi
