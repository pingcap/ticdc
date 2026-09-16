# Copyright 2022 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

dashboard_file="metrics/grafana/ticdc_new_arch.json"
python_checker="scripts/check-ticdc-dashboard.py"
has_error=0

if command -v python3 &>/dev/null; then
	check_output=""
	if ! check_output=$(python3 "$python_checker" "$dashboard_file"); then
		echo "Find dashboard issues in $dashboard_file"
		echo "$check_output"
		has_error=1
	fi
elif command -v jq &>/dev/null; then
	# Fallback for environments without python3. This keeps the previous
	# duplicate ID check so CI still catches obvious dashboard regressions.
	dup=$(jq '[.panels[] | .panels[]?] | group_by(.id) | .[] | select(length > 1) | .[] | { id: .id, title: .title }' "$dashboard_file")
	if [[ -n $dup ]]; then
		echo "Find panels with duplicated ID in $dashboard_file"
		echo "$dup"
		echo "Please choose a new ID that is larger than the max ID:"
		jq '[.panels[] | .panels[]? | .id] | max' "$dashboard_file"
		has_error=1
	fi
fi

exit "$has_error"
