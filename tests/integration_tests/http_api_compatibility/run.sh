#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# This case is only for old architecture.
if [ "${NEXT_GEN:-0}" = 1 ]; then
	exit 0
fi

unset TICDC_NEWARCH

bash "$CUR/../http_api/run.sh" "$@"
