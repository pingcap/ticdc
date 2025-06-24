#!/usr/bin/env bash

set +u

echo "cleaning cluster..."

function kill_process_tree() {
    pkill -9 -P "$1" 2>/dev/null
    kill -9 "$1" 2>/dev/null
}

if [ -z "$OUT_DIR" ]; then
    echo "Error: environment variable OUT_DIR is empty" >&2
    exit 1
fi

if [ -f "$OUT_DIR/next_gen.env" ]; then
    # echo "next_gen.env not found in $OUT_DIR"
    # exit 0
    source "$OUT_DIR/next_gen.env"

    [ -n "$TIDB_PLAYGROUND_TAG" ] && tiup clean "$TIDB_PLAYGROUND_TAG" 2>/dev/null || true
    [ -n "$TIDB_PLAYGROUND_TAG_CDC_PD" ] && tiup clean "$TIDB_PLAYGROUND_TAG_CDC_PD" 2>/dev/null || true
    [ -n "$TIDB_PLAYGROUND_TAG_DOWNSTREAM" ] && tiup clean "$TIDB_PLAYGROUND_TAG_DOWNSTREAM" 2>/dev/null || true
    [ -n "$MINIO_CONTAINER_NAME" ] && docker rm -f "$MINIO_CONTAINER_NAME" 2>/dev/null || true
    [ -n "$UPSTREAM_TIUP_PID" ] && kill_process_tree "$UPSTREAM_TIUP_PID"
    [ -n "$CDC_PD_TIUP_PID" ] && kill_process_tree "$CDC_PD_TIUP_PID"
    [ -n "$DOWNSTREAM_TIUP_PID" ] && kill_process_tree "$DOWNSTREAM_TIUP_PID"
fi

ps -ef | grep tiup | awk '{print $2}' | xargs -I{} kill -9 {} 2>/dev/null || true

rm -f $OUT_DIR/next_gen.env || true
