#!/bin/bash

# error_handler() {
#     local line_no=$1
#     local error_code=$2
#     local last_command="${BASH_COMMAND}"
#     echo -e "\033[31mError occurred in script $0 at line $line_no"
#     echo -e "Error code: $error_code"
#     echo -e "Failed command: $last_command\033[0m"
# }

# Set error handler
trap error_handler ${LINENO} $? ERR

set -eu
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
MAX_RETRIES=10
function test_owner_ha() {
	# test_kill_owner
	# test_hang_up_owner
	# test_expire_owner
	test_owner_retryable_error
	# test_delete_owner_key
}
# test_kill_owner starts two captures and kill the owner
# we expect the live capture will be elected as the new
# owner
function test_kill_owner() {
	echo "run test case test_kill_owner"
	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql "CREATE table test.availability1(id int primary key, val int);"
	run_sql "CREATE table test.availability2(id int primary key, val int);"
	run_sql "CREATE table test.availability3(id int primary key, val int);"
	# start a capture server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix test_kill_owner.server1
	# create changefeed after cdc is started
	run_cdc_cli changefeed create --start-ts=$start_ts \
		--sink-uri="mysql://normal:123456@127.0.0.1:3306/"
	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep '\"is_coordinator\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/\"id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id
	# run another server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8301" --logsuffix test_kill_owner.server2
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep -v \"$owner_id\" | grep -v cluster_id | grep id"
	capture_id=$($CDC_BINARY cli capture list --server 'http://127.0.0.1:8301' 2>&1 | awk -F '"' '/\"id/{print $4}' | grep -v "$owner_id")
	echo "capture_id:" $capture_id
	# kill the server
	kill_cdc_pid $owner_pid
	# check that the new owner is elected
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --server 'http://127.0.0.1:8301' 2>&1 |grep $capture_id -A1 | grep '\"is_coordinator\": true'"
	echo "test_kill_owner: pass"
	cleanup_process $CDC_BINARY
}
# test_hang_up_owner starts two captures and stops the owner
# by sending a SIGSTOP signal.
# We expect another capture will be elected as the new owner
function test_hang_up_owner() {
	echo "run test case test_hang_up_owner"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix test_hang_up_owner.server1
	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep '\"is_coordinator\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/\"id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id
	# run another server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8301" --logsuffix test_hang_up_owner.server2
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list --server 'http://127.0.0.1:8301'  2>&1 | grep -v \"$owner_id\" | grep id"
	capture_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/\"id/{print $4}' | grep -v "$owner_id")
	echo "capture_id:" $capture_id
	# stop the owner
	kill -SIGSTOP $owner_pid
	# check that the new owner is elected
	ensure $MAX_RETRIES "ETCDCTL_API=3 etcdctl get /tidb/cdc/default/__cdc_meta__/owner --prefix | grep '$capture_id'"
	# resume the original process
	kill -SIGCONT $owner_pid
	echo "test_hang_up_owner: pass"
	cleanup_process $CDC_BINARY
}
# test_expire_owner stops the owner by sending
# the SIGSTOP signal and wait unitl its session
# expires.
# We expect when the owner process resumes, it suicides
# itself and recovers from the death.
function test_expire_owner() {
	echo "run test case test_expire_owner"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix test_expire_owner.server1
	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep '\"is_coordinator\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/\"id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id
	# stop the owner
	kill -SIGSTOP $owner_pid
	echo "process status:" $(ps -h -p $owner_pid -o "s")
	# ensure the session has expired
	ensure $MAX_RETRIES "ETCDCTL_API=3 etcdctl get /tidb/cdc/default/__cdc_meta__/owner --prefix | grep -v '$owner_id'"
	# resume the owner
	kill -SIGCONT $owner_pid
	echo "process status:" $(ps -h -p $owner_pid -o "s")
	# ensure the owner has recovered
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep '\"is_coordinator\": true'"
	echo "test_expire_owner pass"
	cleanup_process $CDC_BINARY
}

# test some retryable error meeting in the campaign owner loop
function test_owner_retryable_error() {
	echo "run test case test_owner_retryable_error"
	export GO_FAILPOINTS='github.com/pingcap/ticdc/server/campaign-compacted-error=1*return(true)'

	# start a capture server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix test_owner_retryable_error.server1
	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep '\"is_coordinator\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/\"id/{print $4}')
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id
	export GO_FAILPOINTS='github.com/pingcap/ticdc/coordinator/coordinator-run-with-error=1*return(true);github.com/pingcap/ticdc/server/resign-failed=1*return(true)'
	# run another server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix test_owner_retryable_error.server2 --addr "127.0.0.1:8301"
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep -v \"$owner_id\" | grep id"
	capture_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}' | grep -v "$owner_pid")
	capture_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/\"id/{print $4}' | grep -v "$owner_id")
	echo "capture_id:" $capture_id
	# resign the first capture, the second capture campaigns to be owner.
	# However we have injected two failpoints, the second capture owner runs
	# with error and before it exits resign owner also failed, so the second
	# capture will restart and the first capture campaigns to be owner again.
	curl -X POST http://127.0.0.1:8300/api/v2/owner/resign
	ensure $MAX_RETRIES "ETCDCTL_API=3 etcdctl get /tidb/cdc/default/__cdc_meta__/owner --prefix | grep  '$owner_id'"
	# The second capture will restart but not exit, so there are two capture servers.
	# So the wc -l will be 2.
	ensure $MAX_RETRIES "ps -C $CDC_BINARY -o pid= | awk '{print \$1}' | wc -l | grep 2"
	echo "test_owner_retryable_error pass"
	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}


# make sure when owner key in etcd is deleted, the owner will resign,
# and only one owner exists in the cluster at the same time.
function test_delete_owner_key() {
	echo "run test case delete_owner_key"

	# start a capture server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix test_gap_between_watch_capture.server1
	# ensure the server become the owner
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep '\"is_coordinator\": true'"
	owner_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	owner_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/id/{print $4}')
	owner_key=$(etcdctl get /tidb/cdc/default/__cdc_meta__/owner --prefix | grep -B 1 "$owner_id" | head -n 1)
	echo "owner pid:" $owner_pid
	echo "owner id" $owner_id
	echo "owner key" $owner_key

	# run another server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8301" --logsuffix test_gap_between_watch_capture.server2
	ensure $MAX_RETRIES "$CDC_BINARY cli capture list 2>&1 | grep -v \"$owner_id\" | grep id"
	capture_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}' | grep -v "$owner_pid")
	capture_id=$($CDC_BINARY cli capture list 2>&1 | awk -F '"' '/id/{print $4}' | grep -v "$owner_id")
	echo "capture_id:" $capture_id

	etcdctl del $owner_key
	ensure $MAX_RETRIES "ETCDCTL_API=3 etcdctl get /tidb/cdc/default/__cdc_meta__/owner --prefix | grep  '$capture_id'"
	# ensure the first capture has resign owner
	ensure $MAX_RETRIES "curl -X GET http://127.0.0.1:8300/status | grep '\"is_owner\": false'"

	sleep 3

	for i in $(seq 1 3); do
		run_sql "INSERT INTO test.availability$i(id, val) VALUES (1, 1);"
		ensure $MAX_RETRIES nonempty "select id, val from test.availability$i where id=1 and val=1"
		run_sql "UPDATE test.availability$i set val = 22 where id = 1;"
		ensure $MAX_RETRIES nonempty "select id, val from test.availability$i where id=1 and val=22"
		run_sql "DELETE from test.availability$i where id=1;"
		ensure $MAX_RETRIES empty "select id, val from test.availability$i where id=1"
	done

	export GO_FAILPOINTS=''
	echo "delete_owner_key pass"
	cleanup_process $CDC_BINARY
}