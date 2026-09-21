#!/usr/bin/env python3
"""Compare every replayed demo row against SQL, including the post-B column."""
import argparse
import json
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("result")
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--port", default="4400")
parser.add_argument("--user", default="root")
args = parser.parse_args()
result = json.load(open(args.result))
sql = "SELECT JSON_OBJECT('id',id,'balance',balance,'note',note,'revision',revision) FROM snapshot_demo.accounts ORDER BY id"
data = subprocess.check_output(["mysql", "--batch", "--raw", "--skip-column-names", "-h"+args.host, "-P"+args.port, "-u"+args.user, "-e", sql], text=True)
expected = {str(row["id"]): row for row in map(json.loads, data.splitlines())}
assert result["final_rows"] == expected, "Kafka replay differs from SQL rows"
assert result["incremental_rows"] >= 4 and result["ddl_events"] >= 1
print(json.dumps({"sql_rows": len(expected), "all_columns_equal": True, **{k: v for k, v in result.items() if k != "final_rows"}}, indent=2))
