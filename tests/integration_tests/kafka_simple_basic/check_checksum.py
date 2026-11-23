# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import sys


historical_ids = {1, 3, 5, 7, 9}
new_ids = {2, 4, 8}
deleted_ids = {1, 3, 7, 9}
expected = (
    {("INSERT", row_id) for row_id in historical_ids | new_ids}
    | {("DELETE", row_id) for row_id in deleted_ids}
    | {("UPDATE", 5)}
)
seen = set()
mixed_commit_ts = set()
with open(sys.argv[1], encoding="utf-8") as source:
    for line in source:
        message = json.loads(line)
        operation = message.get("type")
        if message.get("table") != "checksum_sparse" or operation not in {
            "INSERT", "UPDATE", "DELETE"
        }:
            continue
        row = message["old"] if operation == "DELETE" else message["data"]
        row_id = int(row["id"])
        key = (operation, row_id)
        assert key in expected, message
        seen.add(key)
        assert int(row["v"]) == (51 if operation == "UPDATE" else row_id * 10), message
        checksum = message.get("checksum")
        if operation == "DELETE" or row_id in historical_ids and operation == "INSERT":
            assert checksum is None, message
        else:
            assert checksum is not None, message
            assert checksum["current"] != 0, message
            assert checksum["previous"] == 0, message
            assert checksum["version"] > 0 and not checksum["corrupted"], message
        if operation != "INSERT" or row_id in new_ids:
            mixed_commit_ts.add(message["commitTs"])
        if operation == "UPDATE":
            assert int(message["old"]["v"]) == 50, message

assert seen == expected, f"missing DML rows: {expected - seen}"
assert len(mixed_commit_ts) == 1, mixed_commit_ts
print("Verified all 13 DML rows and sparse checksums, including 8 rows in one transaction")
