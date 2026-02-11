#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# This case is only for old architecture.
if [ "${NEXT_GEN:-0}" = 1 ]; then
	exit 0
fi

export TICDC_NEWARCH=false
bash "$CUR/../http_api/run.sh" "$@"
export TICDC_NEWARCH=true
