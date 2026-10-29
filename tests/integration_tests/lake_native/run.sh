#!/bin/bash
# The generic integration runner also discovers this case for other sink types.
set -euo pipefail
if [ "${1:-lake}" != lake ]; then
	echo "lake_native: requires the lake sink suite; skipping"
	exit 0
fi
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$CUR/../../.."
exec python3 "$CUR/local.py"
