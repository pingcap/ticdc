#!/bin/bash

error_handler() {
	local line_no=$1
	local error_code=$2
	local last_command="${BASH_COMMAND}"
	echo -e "\033[31mError occurred in script $0 at line $line_no"
	echo -e "Error code: $error_code"
	echo -e "Failed command: $last_command\033[0m"
}

# Set error handler
trap 'error_handler ${LINENO} $?' ERR

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
source $CUR/owner.sh
source $CUR/capture.sh
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

export DOWN_TIDB_HOST
export DOWN_TIDB_PORT

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR
}

trap stop_tidb_cluster EXIT
if [ "$SINK_TYPE" == "mysql" ]; then
	prepare $*
	test_owner_ha $*
	test_capture_ha $*
fi
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
