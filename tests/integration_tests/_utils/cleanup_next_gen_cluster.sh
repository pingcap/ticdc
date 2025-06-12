#!/usr/bin/env bash

echo "cleaning cluster..."

if [ -z "$WORK_DIR" ]; then
    echo "Error: environment variable WORK_DIR is empty" >&2
    exit 1
fi

if [ ! -f "$WORK_DIR/next_gen.env" ]; then
    echo "next_gen.env not found in $WORK_DIR"
    exit 0
fi

source "$WORK_DIR/next_gen.env"

rm -rf "$WORK_DIR/tiup-cluster" "$WORK_DIR/cdc-data" "$WORK_DIR/cdc-log"

[ -n "$TIDB_PLAYGROUND_TAG" ] && tiup clean "$TIDB_PLAYGROUND_TAG" 2>/dev/null
[ -n "$TIDB_PLAYGROUND_TAG_CDC_PD" ] && tiup clean "$TIDB_PLAYGROUND_TAG_CDC_PD" 2>/dev/null
[ -n "$MINIO_CONTAINER_NAME" ] && docker rm -f "$MINIO_CONTAINER_NAME"
[ -n "$UPSTREAM_TIUP_PID" ] && kill "$UPSTREAM_TIUP_PID" 2>/dev/null
[ -n "$CDC_PD_TIUP_PID" ] && kill "$CDC_PD_TIUP_PID" 2>/dev/null

cwd=$(pwd)
cd "$WORK_DIR" || exit
rm -f next_gen.env pd.toml replication_config.toml tidb.toml tikv.toml tikv_worker.toml
cd "$cwd" || exit
