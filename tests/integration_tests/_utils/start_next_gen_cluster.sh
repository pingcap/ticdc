#!/usr/bin/env bash
# dependencies: docker, mc(minio client)
# You should compile pd-cse, tidb-cse, replication-worker first

set -x
source "$CUR/../_utils/test_prepare"

check_bin() {
	if [ ! -f "$1" ]; then
		echo "Error: $1 is not a file" >&2
		exit 1
	fi
	if [ ! -x "$1" ]; then
		echo "Error: $1 is not executable" >&2
		exit 1
	fi
}

show_help() {
	cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --db.binpath PATH             Path to tidb-server binary (default: ./bin/tidb-server)
  --kv.binpath PATH             Path to tikv-server binary (default: ./bin/tikv-server)
  --pd.binpath PATH             Path to pd-server binary (default: ./bin/pd-server)
  --cse-ctl.binpath PATH        Path to cse-ctl binary (default: ./bin/cse-ctl)
  --tikv-worker.binpath PATH    Path to tikv-worker binary (default: ./bin/tikv-worker)
  --keyspace-name NAME          Keyspace name (default: tenant-1)
  --upstream-port-offset NUM    Upstream port offset (default: 0)
  --cdc-pd-port NUM             CDC PD port (default: 22379)
  -h, --help                    Show this help message and exit

Environment Variables:
  WORK_DIR                      Required working directory
  MINIO_CONTAINER_NAME          MinIO container name (default: minio)
  MINIO_ROOT_USER               MinIO root user (default: minioadmin)
  MINIO_ROOT_PASSWORD           MinIO root password (default: minioadmin)
  MINIO_MC_ALIAS                MinIO mc alias (default: localminio)
  MINIO_API_PORT                MinIO API port (default: 9000)
  MINIO_CONSOLE_PORT            MinIO console port (default: 9001)
  TIDB_VERSION                  TiDB version (default: v8.5.1)
  TIDB_PLAYGROUND_TAG           TiDB playground tag (default: serverless-cdc)
  TIDB_PLAYGROUND_TAG_CDC_PD    TiDB playground CDC PD tag (default: serverless-cdc-pd)
EOF
}

dump_variables() {
	cat >"$OUT_DIR/next_gen.env" <<EOF
DB_BINPATH=$DB_BINPATH
KV_BINPATH=$KV_BINPATH
PD_BINPATH=$PD_BINPATH
CSE_CTL_BINPATH=$CSE_CTL_BINPATH
TIKV_WORKER_BINPATH=$TIKV_WORKER_BINPATH
UPSTREAM_PORT_OFFSET=$UPSTREAM_PORT_OFFSET
GLOBAL_PD_PORT=$GLOBAL_PD_PORT
CDC_PD_PORT=$CDC_PD_PORT
MINIO_CONTAINER_NAME=$MINIO_CONTAINER_NAME
MINIO_ROOT_USER=$MINIO_ROOT_USER
MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD
MINIO_MC_ALIAS=$MINIO_MC_ALIAS
MINIO_API_PORT=$MINIO_API_PORT
MINIO_CONSOLE_PORT=$MINIO_CONSOLE_PORT
TIDB_VERSION=$TIDB_VERSION
TIDB_PLAYGROUND_TAG=$TIDB_PLAYGROUND_TAG
TIDB_PLAYGROUND_TAG_CDC_PD=$TIDB_PLAYGROUND_TAG_CDC_PD
TIDB_PLAYGROUND_TAG_DOWNSTREAM=$TIDB_PLAYGROUND_TAG_DOWNSTREAM
TIDB_PLAYGROUND_TAG_OTHER=$TIDB_PLAYGROUND_TAG_OTHER
TIDB_KEYSPACE_1_PID=$TIDB_KEYSPACE_1_PID
KEYSPACE_NAME=$KEYSPACE_NAME
WORK_DIR=$WORK_DIR
UPSTREAM_TIUP_PID=$UPSTREAM_TIUP_PID
CDC_PD_TIUP_PID=$CDC_PD_TIUP_PID
DOWNSTREAM_TIUP_PID=$DOWNSTREAM_TIUP_PID
OTHER_TIUP_PID=$OTHER_TIUP_PID
TIKV_WORKER_PID=$TIKV_WORKER_PID
EOF
	echo "Variables dumped to $WORK_DIR/next_gen.env"
}

check_port_available() {
	local host=${1:-127.0.0.1}
	local port=$2
	local prompt=$3
	while ! nc -z "$host" "$port"; do
		echo "$prompt"
		sleep 1
	done
}

# Parse command line arguments manually
while [[ $# -gt 0 ]]; do
	case "$1" in
	-h | --help)
		show_help
		exit 0
		;;
	--keyspace-name)
		KEYSPACE_NAME="$2"
		shift 2
		;;
	--db.binpath)
		DB_BINPATH="$2"
		shift 2
		;;
	--kv.binpath)
		KV_BINPATH="$2"
		shift 2
		;;
	--pd.binpath)
		PD_BINPATH="$2"
		shift 2
		;;
	--cse-ctl.binpath)
		CSE_CTL_BINPATH="$2"
		shift 2
		;;
	--tikv-worker.binpath)
		TIKV_WORKER_BINPATH="$2"
		shift 2
		;;
	--upstream-port-offset)
		UPSTREAM_PORT_OFFSET="$2"
		shift 2
		;;
	--cdc-pd-port)
		# this port is for pd which is used by cdc only
		CDC_PD_PORT=$2
		shift 2
		;;
	*)
		echo "Unknown option: $1"
		exit 1
		;;
	esac
done

if [ -z "$WORK_DIR" ]; then
	echo "Error: environment variable WORK_DIR is empty" >&2
	exit 1
fi

check_bin "$DB_BINPATH" || exit 1
check_bin "$KV_BINPATH" || exit 1
check_bin "$PD_BINPATH" || exit 1
check_bin "$CSE_CTL_BINPATH" || exit 1
check_bin "$TIKV_WORKER_BINPATH" || exit 1

mkdir -p "$WORK_DIR"

CLEANUP_SCRIPT="$(dirname "$0")/cleanup_next_gen_cluster.sh"
[ -x "$CLEANUP_SCRIPT" ] && "$CLEANUP_SCRIPT"

echo "Check minio container"
if ! docker ps -a --filter "name=$MINIO_CONTAINER_NAME" | grep -q "$MINIO_CONTAINER_NAME"; then
	echo "Deploy minio"
	docker run -d \
		--name "$MINIO_CONTAINER_NAME" \
		-p "$MINIO_API_PORT:9000" \
		-p "$MINIO_CONSOLE_PORT:9001" \
		-e MINIO_ROOT_USER="$MINIO_ROOT_USER" \
		-e MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
		--restart unless-stopped \
		minio/minio:RELEASE.2025-05-24T17-08-30Z \
		server /data --console-address ":9001"
else
	echo "MinIO container already exists, skipping creation"
	# Ensure container is running
	docker start "$MINIO_CONTAINER_NAME" || true
fi

check_port_available "" "$MINIO_API_PORT" "Wait for minio to be available"
# sleep 1 second while minio becomes available
sleep 1

echo "Create bucket"
mc alias set "$MINIO_MC_ALIAS" "http://localhost:$MINIO_API_PORT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>&1
if ! mc ls "$MINIO_MC_ALIAS"/cse &>/dev/null; then
	mc mb "$MINIO_MC_ALIAS"/cse
else
	echo "Bucket cse already exists, skipping creation"
fi

cat >"$WORK_DIR/pd.toml" <<EOF
[keyspace]
pre-alloc = ["$KEYSPACE_NAME"]
EOF

cat >"$WORK_DIR/tikv.toml" <<EOF
[storage]
api-version = 2
enable-ttl = true

[dfs]
prefix = "serverless"
s3-endpoint = "http://127.0.0.1:$MINIO_API_PORT"
s3-key-id = "$MINIO_ROOT_USER"
s3-secret-key = "$MINIO_ROOT_PASSWORD"
s3-bucket = "cse"
s3-region = "local"

[rfengine]
wal-sync-dir = "$WORK_DIR/tiup-cluster/playground-serverless/tikv-22160/raft-wal"
lightweight-backup = true
target-file-size = "512MB"
wal-chunk-target-file-size = "128MB"
EOF

cat >"$WORK_DIR/tidb-system.toml" <<EOF
keyspace-name = "SYSTEM"
tikv-worker-url = "http://127.0.0.1:19000"

[instance]
tidb_service_scope = 'dxf_service'
EOF

cat >"$WORK_DIR/tidb.toml" <<EOF
keyspace-name = "$KEYSPACE_NAME"
EOF

# We should start a SYSTEM tidb first on next gen
echo "Start upstream cluster and wait for it to be ready"
nohup tiup playground "$TIDB_VERSION" --tag "$TIDB_PLAYGROUND_TAG" \
	--db.config "$WORK_DIR/tidb-system.toml" --db.binpath "$DB_BINPATH" --db.host "$UP_TIDB_HOST" --db.port "$UP_SYSTEM_TIDB_PORT" \
	--kv.config "$WORK_DIR/tikv.toml" --kv.binpath "$KV_BINPATH" --kv.host "$UP_TIKV_HOST_1" --kv.port "$UP_TIKV_PORT_1" \
	--pd.config "$WORK_DIR/pd.toml" --pd.binpath "$PD_BINPATH" --pd.host "$UP_PD_HOST_1" --pd.port "$GLOBAL_PD_PORT" \
	--tiflash 1 &
UPSTREAM_TIUP_PID=$!
echo "upstream tiup pid: $UPSTREAM_TIUP_PID"
check_port_available "$UP_TIDB_HOST" "$UP_SYSTEM_TIDB_PORT" "Wait for system TiDB to be available"

echo "Start the $KEYSPACE_NAME tidb"
nohup "$DB_BINPATH" --config "$WORK_DIR/tidb.toml" -P "$UP_TIDB_PORT" --store=tikv --path="$UP_PD_HOST_1:$GLOBAL_PD_PORT" -status 15000 -log-file "$WORK_DIR/tidb-$KEYSPACE_NAME.log" -log-slow-query "$WORK_DIR/tidb-slow.log" &
TIDB_KEYSPACE_1_PID=$!
check_port_available "$UP_TIDB_HOST" "$UP_TIDB_PORT" "Wait for $KEYSPACE_NAME TiDB to be available"

echo "run backup"
cat >"$WORK_DIR/tikv_worker.toml" <<EOF
data-dir = "$WORK_DIR/tiup-cluster/playground-serverless/br"
addr = "127.0.0.1:5998"

[pd]
endpoints = ["$UP_PD_HOST_1:$GLOBAL_PD_PORT"]

[security]

[dfs]
prefix = "serverless"
s3-endpoint = "http://127.0.0.1:$MINIO_API_PORT"
s3-key-id = "$MINIO_ROOT_USER"
s3-secret-key = "$MINIO_ROOT_PASSWORD"
s3-bucket = "cse"
s3-region = "local"
EOF
"$CSE_CTL_BINPATH" backup --pd "$UP_PD_HOST_1:$GLOBAL_PD_PORT" --config "$WORK_DIR/tikv_worker.toml" --lightweight --interval 0

echo "Start CDC PD cluster and wait for it to be ready"
nohup tiup playground "$TIDB_VERSION" --tag "$TIDB_PLAYGROUND_TAG_CDC_PD" \
	--pd.host "$UP_PD_HOST_1" --pd.port "$UP_PD_PORT_1" --pd 1 \
	--kv 0 \
	--db 0 \
	--tiflash 0 &
CDC_PD_TIUP_PID=$!
echo "cdc pd tiup pid: $CDC_PD_TIUP_PID"
sleep 10
check_port_available "$UP_PD_HOST_1" "$UP_PD_PORT_1" "Wait for CDC PD to be available"

echo "deploy replication-worker"
cat >"$WORK_DIR/replication_config.toml" <<EOF
data-dir = "$WORK_DIR/tiup-cluster/playground-serverless/br"

[replication-worker]
enabled = true
grpc-addr = "0.0.0.0:19160"
advertise-addr = "127.0.0.1:19160"

[replication-worker.merged-engine]
merged-store-id = 1024

[dfs]
prefix = "serverless"
s3-endpoint = "http://127.0.0.1:$MINIO_API_PORT"
s3-key-id = "$MINIO_ROOT_USER"
s3-secret-key = "$MINIO_ROOT_PASSWORD"
s3-bucket = "cse"
s3-region = "local"
EOF
nohup "$TIKV_WORKER_BINPATH" --config "$WORK_DIR/replication_config.toml" --pd-endpoints "$UP_PD_HOST_1:$GLOBAL_PD_PORT" --log-file "$WORK_DIR/tikv_worker.log" &
TIKV_WORKER_PID=$!

# Start other TiDB
nohup tiup playground "$TIDB_VERSION" --tag "$TIDB_PLAYGROUND_TAG_OTHER" \
	--pd.host "$UP_PD_HOST_1" \
	--kv.host "$UP_TIKV_HOST_1" \
	--db.host "$UP_TIDB_HOST" --db.port "$UP_TIDB_OTHER_PORT" \
	--tiflash 0 &
OTHER_TIUP_PID=$!
echo "other tiup pid: $OTHER_TIUP_PID"
check_port_available "$UP_TIDB_HOST" "$UP_TIDB_OTHER_PORT" "Wait for other tidb cluster to be available"

# Start a downstream TiDB
nohup tiup playground "$TIDB_VERSION" --tag "$TIDB_PLAYGROUND_TAG_DOWNSTREAM" \
	--pd.host "$DOWN_PD_HOST" --pd.port "$DOWN_PD_PORT" \
	--kv.host "$DOWN_TIKV_HOST" --kv.port "$DOWN_TIKV_PORT" \
	--db.host "$DOWN_TIDB_HOST" --db.port "$DOWN_TIDB_PORT" &
DOWNSTREAM_TIUP_PID=$!
echo "downstream tiup pid: $CDC_PD_TIUP_PID"
check_port_available "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" "Wait for downstream to be available"

run_sql "update mysql.tidb set variable_value='60m' where variable_name='tikv_gc_life_time';" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
run_sql "update mysql.tidb set variable_value='60m' where variable_name='tikv_gc_life_time';" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
run_sql "CREATE user 'normal'@'%' identified by '123456';" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
run_sql "GRANT select,insert,update,delete,index,create,drop,alter,create view,references ON *.* TO 'normal'@'%';" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
run_sql "FLUSH privileges" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

dump_variables
