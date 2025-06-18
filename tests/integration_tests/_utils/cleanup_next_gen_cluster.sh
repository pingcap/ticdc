#!/usr/bin/env bash

echo "cleaning cluster..."

if [ -z "$OUT_DIR" ]; then
    echo "Error: environment variable OUT_DIR is empty" >&2
    exit 1
fi

if [ -f "$OUT_DIR/next_gen.env" ]; then
    # echo "next_gen.env not found in $OUT_DIR"
    # exit 0
    source "$OUT_DIR/next_gen.env"

    [ -n "$TIDB_PLAYGROUND_TAG" ] && tiup clean "$TIDB_PLAYGROUND_TAG" 2>/dev/null
    [ -n "$TIDB_PLAYGROUND_TAG_CDC_PD" ] && tiup clean "$TIDB_PLAYGROUND_TAG_CDC_PD" 2>/dev/null
    [ -n "$TIDB_PLAYGROUND_TAG_CDC_PD" ] && tiup clean "$TIDB_PLAYGROUND_TAG_DOWNSTREAM" 2>/dev/null
    [ -n "$MINIO_CONTAINER_NAME" ] && docker rm -f "$MINIO_CONTAINER_NAME"
    [ -n "$UPSTREAM_TIUP_PID" ] && kill "$UPSTREAM_TIUP_PID" 2>/dev/null
    [ -n "$CDC_PD_TIUP_PID" ] && kill "$CDC_PD_TIUP_PID" 2>/dev/null
    [ -n "$DOWNSTREAM_TIUP_PID" ] && kill "$DOWNSTREAM_TIUP_PID" 2>/dev/null
fi

rm -f $OUT_DIR/next_gen.env
