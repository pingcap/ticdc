#!/bin/bash

set -eo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

sink_type=${1:-mysql}

# This script is a standalone CI entry for the random DDL+DML suite.
# It intentionally stays outside run_heavy_it_in_ci.sh so the expensive weekly
# workload can be triggered independently from the regular heavy matrix.
case "${sink_type}" in
mysql)
	test_names="weekly_rand_single weekly_rand_multi weekly_rand_multi_failover weekly_rand_slow_lossy_ddl"
	;;
kafka | storage | pulsar)
	test_names="weekly_rand_single weekly_rand_multi weekly_rand_multi_failover"
	;;
*)
	echo "Error: unknown sink type: ${sink_type}"
	exit 1
	;;
esac

export TICDC_NEWARCH=true
export RUN_PROFILE=${RUN_PROFILE:-weekly}
export RUN_DURATION=${RUN_DURATION:-30m}
export RUN_SEED=${RUN_SEED:-$(date -u +%Y%m%d%H)}

echo "Sink Type: ${sink_type}"
echo "Run cases: ${test_names}"
echo "RUN_PROFILE=${RUN_PROFILE}"
echo "RUN_DURATION=${RUN_DURATION}"
echo "RUN_SEED=${RUN_SEED}"

"${CUR}"/run.sh "${sink_type}" "${test_names}"
