#!/usr/bin/env python3
"""Real-source acceptance for conflict-key and explicitly skipped source DDL.

Uses the same private environment and isolated ownership rules as run.py.
"""

import argparse
import json
import time
import urllib.parse

from run import Run, quote


class OptionsRun(Run):
    def __init__(self, manual=False):
        self.manual = manual
        super().__init__()
        if manual:
            q = urllib.parse.parse_qs(self.parsed.query)
            q["conflict-key"] = ["ID"]  # Match authoritative source casing.
            self.parsed = self.parsed._replace(
                query=urllib.parse.urlencode(q, doseq=True)
            )

    def api(self, path, data=None, method=None):
        if self.manual and path == "/api/v2/changefeeds" and data:
            data["replica_config"]["force_replicate"] = True
        return super().api(path, data, method)

    def drained_marker(self):
        marker = int(
            self.local(
                "BEGIN;SELECT COUNT(*) FROM mysql.tidb;SELECT @@tidb_current_ts;COMMIT"
            ).splitlines()[-1]
        )
        assert marker > 0
        deadline = time.monotonic() + 75
        while self.check_feed()["checkpoint_ts"] < marker:
            if time.monotonic() > deadline:
                raise AssertionError("checkpoint did not pass skipped DDL")
            time.sleep(0.3)
        self.results.setdefault("drained_markers", []).append(marker)
        self.save()

    def test_manual(self):
        db = "`" + self.database + "`"
        self.local(
            f"CREATE TABLE {db}.keyless(id BIGINT,v VARCHAR(64));INSERT INTO {db}.keyless VALUES(1,'one'),(2,'two')"
        )
        self.simple("keyless", ["id", "v"])
        self.local(
            f"UPDATE {db}.keyless SET v='updated' WHERE id=1;DELETE FROM {db}.keyless WHERE id=2;INSERT INTO {db}.keyless VALUES(3,'three')"
        )
        self.simple("keyless", ["id", "v"])
        self.local(
            f"ALTER TABLE {db}.keyless ADD COLUMN extra BIGINT NULL;INSERT INTO {db}.keyless VALUES(4,'four',42)"
        )
        self.simple("keyless", ["id", "v", "extra"])
        journal = self.lake(
            "SELECT plan FROM " + quote(self.database) + '."keyless_cdc_schema_epoch"'
        )

        plan = json.loads(journal[0][0])
        assert plan["schema"]["keys"] == ["id"], plan
        self.results["manual_key"] = {
            "source_primary_key": False,
            "source_unique_key": False,
            "effective_key": plan["schema"]["keys"],
            "force_replicate": self.feed_status()["config"].get("force_replicate"),
        }
        # bend-cdc also permits replacing a single native PK, not a composite.
        self.local(
            f"CREATE TABLE {db}.override_key(other_id INT PRIMARY KEY,id BIGINT,v VARCHAR(64));INSERT INTO {db}.override_key VALUES(10,1,'one'),(20,2,'two')"
        )
        self.simple("override_key", ["other_id", "id", "v"])
        self.local(
            f"UPDATE {db}.override_key SET v='changed' WHERE other_id=10;DELETE FROM {db}.override_key WHERE other_id=20"
        )
        self.simple("override_key", ["other_id", "id", "v"])
        self.drained_marker()
        self.save()

    def test_rename(self):
        db = "`" + self.database + "`"
        target = quote(self.database)
        self.local(
            f"CREATE TABLE {db}.original(id INT PRIMARY KEY,v VARCHAR(64));INSERT INTO {db}.original VALUES(1,'old')"
        )
        self.simple("original", ["id", "v"])
        self.local(f"RENAME TABLE {db}.original TO {db}.renamed")
        self.drained_marker()
        assert self.lake("SELECT id::VARCHAR,v FROM " + target + '."original"') == [
            ["1", "old"]
        ]
        assert self.lake(
            "SELECT COUNT(*)::VARCHAR FROM system.tables WHERE database=? AND name='renamed'",
            [self.database],
        ) == [["0"]]
        # No historical rows are moved. Subsequent DML gets a new projection.
        self.local(
            f"UPDATE {db}.renamed SET v='new' WHERE id=1;INSERT INTO {db}.renamed VALUES(2,'inserted')"
        )
        self.simple("renamed", ["id", "v"])
        assert self.lake("SELECT id::VARCHAR,v FROM " + target + '."original"') == [
            ["1", "old"]
        ]
        self.results["rename"] = {
            "source_renamed": True,
            "old_target_preserved": True,
            "new_target_populated_by_subsequent_dml": True,
        }
        self.save()

    def test_drop_database(self):
        db = "`" + self.database + "`"
        target = quote(self.database)
        before = self.lake(
            "SELECT id::VARCHAR,v FROM " + target + '."renamed" ORDER BY id'
        )
        self.local(f"DROP DATABASE {db}")
        self.drained_marker()
        assert (
            self.lake("SELECT id::VARCHAR,v FROM " + target + '."renamed" ORDER BY id')
            == before
        )
        assert self.lake("SELECT id::VARCHAR,v FROM " + target + '."original"') == [
            ["1", "old"]
        ]
        self.local(
            f"CREATE DATABASE {db};CREATE TABLE {db}.after_drop(id INT PRIMARY KEY,v INT);INSERT INTO {db}.after_drop VALUES(1,42)"
        )
        self.simple("after_drop", ["id", "v"])
        self.drained_marker()
        self.results["drop_database"] = {
            "target_database_preserved": True,
            "existing_target_data_preserved": True,
            "subsequent_source_database_recreation_and_new_table_dml": True,
        }
        self.save()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manual-key", action="store_true")
    args = parser.parse_args()
    run = OptionsRun(manual=args.manual_key)
    try:
        run.setup()
        if args.manual_key:
            run.case(
                "force replicated keyless DML/DDL and single native key override",
                run.test_manual,
            )
        else:
            run.case(
                "RENAME skipped, old target preserved and subsequent DML continues",
                run.test_rename,
            )
            run.case(
                "DROP DATABASE skipped, checkpoint and subsequent DML continue",
                run.test_drop_database,
            )
        run.cleanup()
        run.results["status"] = "PASS"
        run.save()
    except Exception as error:
        run.results["status"] = "FAIL"
        run.results["error"] = run.sanitize(error)
        run.save()
        print("ERROR", run.sanitize(error), flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
