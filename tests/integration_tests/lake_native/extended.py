#!/usr/bin/env python3
"""Opt-in real TiUP/Lake DDL matrix and explicit unsupported-scope probes.

Run after run.py's documented local setup. Each invocation owns a fresh random
source/target database/feed. A rejected probe PASS means rejection was verified,
not that the corresponding feature is supported.
"""

import argparse
import json
import time

from run import Run, quote


class ExtendedRun(Run):
    def __init__(self, force_replicate=False):
        self.force_replicate = force_replicate
        super().__init__()

    def api(self, path, data=None, method=None):
        if path == "/api/v2/changefeeds" and data and self.force_replicate:
            data["replica_config"]["force_replicate"] = True
        return super().api(path, data, method)

    def schema(self, table, column, nullable, default=None):
        self.check_feed()
        rows = self.lake("DESCRIBE " + quote(self.database) + "." + quote(table))
        row = next(r for r in rows if r[0] == column)
        assert row[2] == ("YES" if nullable else "NO"), row
        if default is not None:
            actual = row[3].strip("()' ")
            assert actual.upper() == default.upper(), (column, actual, default)
        self.results.setdefault("schema_observations", []).append(row)
        self.save()

    def unique_without_pk(self):
        db = "`" + self.database + "`"
        self.local(
            f"CREATE TABLE {db}.implicit_key(id INT NOT NULL UNIQUE,v INT);"
            f"INSERT INTO {db}.implicit_key VALUES(1,5),(3,8)"
        )
        self.simple("implicit_key", ["id", "v"])
        self.local(f"UPDATE {db}.implicit_key SET id=2,v=6 WHERE id=1")
        self.simple("implicit_key", ["id", "v"])
        self.local(f"DELETE FROM {db}.implicit_key WHERE id=2")
        self.simple("implicit_key", ["id", "v"])
        self.results["implicit_key_config"] = {
            "force_replicate": self.feed_status()
            .get("config", {})
            .get("force_replicate", False),
            "declared_primary_key": False,
            "not_null_unique_key": "id",
        }
        self.save()

    def int_to_varchar(self):
        db = "`" + self.database + "`"
        self.local(
            f"CREATE TABLE {db}.varchar_widen(id INT PRIMARY KEY,v INT NOT NULL DEFAULT 7);INSERT INTO {db}.varchar_widen VALUES(1,-2147483648)"
        )
        self.simple("varchar_widen", ["id", "v"])
        self.local(
            f"ALTER TABLE {db}.varchar_widen MODIFY COLUMN v VARCHAR(200) NOT NULL DEFAULT '7';INSERT INTO {db}.varchar_widen VALUES(2,'0012')"
        )
        self.simple("varchar_widen", ["id", "v"])
        self.schema("varchar_widen", "v", False, "7")

    def tests(self):
        self.case(
            "NOT NULL UNIQUE without declared PK, including key change",
            self.unique_without_pk,
        )
        self.case(
            "INT to sufficiently wide VARCHAR(200) with literal default",
            self.int_to_varchar,
        )
        db = "`" + self.database + "`"
        table = db + ".matrix"
        self.local(
            f"CREATE TABLE {table}(id INT PRIMARY KEY,n INT NOT NULL DEFAULT 1,s VARCHAR(32),tiny TINYINT);"
            f"INSERT INTO {table}(id,s,tiny) VALUES(1,'seed',12)"
        )
        self.simple("matrix", ["id", "n", "s", "tiny"])

        def execute(sql, names):
            self.local(sql)
            self.simple("matrix", names)

        names = ["id", "n", "s", "tiny"]

        def default_change():
            execute(
                f"ALTER TABLE {table} MODIFY COLUMN n INT NOT NULL DEFAULT 7;"
                f"INSERT INTO {table}(id,s) VALUES(2,'default7')",
                names,
            )
            self.schema("matrix", "n", False, "7")

        self.case(
            "same-type MODIFY literal default preserves old values", default_change
        )

        def nullable():
            execute(
                f"ALTER TABLE {table} MODIFY COLUMN n INT NULL DEFAULT NULL;"
                f"INSERT INTO {table}(id,s) VALUES(3,'null')",
                names,
            )
            self.schema("matrix", "n", True, "NULL")

        self.case("NOT NULL to NULL with explicit DEFAULT NULL", nullable)

        def not_null():
            execute(f"UPDATE {table} SET n=8 WHERE n IS NULL", names)
            execute(
                f"ALTER TABLE {table} MODIFY COLUMN n INT NOT NULL DEFAULT 9;"
                f"INSERT INTO {table}(id) VALUES(4)",
                names,
            )
            self.schema("matrix", "n", False, "9")

        self.case("NULL to NOT NULL after removing source nulls", not_null)

        def implicit_nullable():
            execute(
                f"ALTER TABLE {table} MODIFY COLUMN n BIGINT DEFAULT 11;"
                f"INSERT INTO {table}(id,n) VALUES(5,NULL)",
                names,
            )
            self.schema("matrix", "n", True, "11")

        self.case(
            "MODIFY complete definition implicitly removes NOT NULL", implicit_nullable
        )

        def default_removed():
            execute(
                f"ALTER TABLE {table} MODIFY COLUMN n BIGINT NULL;"
                f"INSERT INTO {table}(id) VALUES(6)",
                names,
            )
            self.schema("matrix", "n", True, "NULL")

        self.case(
            "MODIFY omitted DEFAULT removes previous source default", default_removed
        )

        def widening():
            execute(f"ALTER TABLE {table} MODIFY COLUMN tiny INT", names)
            execute(
                f"ALTER TABLE {table} MODIFY COLUMN tiny BIGINT;"
                f"UPDATE {table} SET tiny=9223372036854775806 WHERE id=1",
                names,
            )
            execute(f"ALTER TABLE {table} MODIFY COLUMN s TEXT", names)

        self.case("TINYINT to INT to BIGINT and VARCHAR to TEXT", widening)

        def add_defaults():
            execute(
                f"ALTER TABLE {table} ADD COLUMN amount DECIMAL(20,6) NOT NULL DEFAULT 1.234567",
                names + ["amount"],
            )
            execute(
                f"ALTER TABLE {table} ADD COLUMN optional INT NULL DEFAULT NULL",
                names + ["amount", "optional"],
            )
            self.schema("matrix", "optional", True, "NULL")
            execute(
                f"ALTER TABLE {table} ADD COLUMN note VARCHAR(64) NOT NULL DEFAULT 'O\\'Reilly\\\\path'",
                names + ["amount", "optional", "note"],
            )

        self.case("ADD numeric/text/NULL literal defaults and backfill", add_defaults)

        def change():
            execute(
                f"ALTER TABLE {table} CHANGE COLUMN tiny counter BIGINT NULL DEFAULT 17;"
                f"INSERT INTO {table}(id) VALUES(7)",
                ["id", "n", "s", "counter", "amount", "optional", "note"],
            )
            self.schema("matrix", "counter", True, "17")

        self.case("CHANGE rename plus full default/nullability definition", change)

        def multi():
            execute(
                f"ALTER TABLE {table} ADD COLUMN extra INT NOT NULL DEFAULT 42,"
                " DROP COLUMN optional, RENAME COLUMN note TO message;"
                f"UPDATE {table} SET extra=43 WHERE id=1",
                ["id", "n", "s", "counter", "amount", "extra", "message"],
            )

        self.case("multi-operation ADD/DROP/RENAME final projection", multi)

    def reject(self, name):
        db = "`" + self.database + "`"
        target = quote(self.database) + '."probe"'
        before = None
        if name not in ("partition", "no-pk"):
            self.local(
                f"CREATE TABLE {db}.probe(id INT PRIMARY KEY,v INT DEFAULT 1);"
                f"INSERT INTO {db}.probe VALUES(1,7)"
            )
            self.simple("probe", ["id", "v"])
            before = self.lake("SELECT * FROM " + target)
        sql = {
            "partition": f"CREATE TABLE {db}.probe(id INT PRIMARY KEY,v INT) PARTITION BY HASH(id) PARTITIONS 2",
            "no-pk": f"CREATE TABLE {db}.probe(id INT,v INT)",
            "narrow": f"ALTER TABLE {db}.probe MODIFY COLUMN v TINYINT",
            "expression-default": f"ALTER TABLE {db}.probe ADD COLUMN ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }[name]
        self.local(sql)
        if name in ("partition", "no-pk"):
            self.local(f"INSERT INTO {db}.probe VALUES(1,5)")
        deadline = time.monotonic() + 65
        while time.monotonic() < deadline:
            status = self.feed_status()
            if status.get("state") == "failed":
                break
            time.sleep(0.5)
        else:
            raise AssertionError(
                "probe did not fail closed: "
                + self.sanitize(
                    json.dumps(
                        {k: status.get(k) for k in ("state", "error", "checkpoint_ts")}
                    )
                )
            )
        # Verify a stable failed lifecycle, not only a transient reported error.
        checkpoint = status["checkpoint_ts"]
        time.sleep(2)
        after = self.feed_status()
        assert after.get("state") == "failed", self.sanitize(after)
        assert after["checkpoint_ts"] == checkpoint
        self.results["rejection"] = {
            "case": name,
            "state": after["state"],
            "checkpoint_ts": checkpoint,
            "error": self.sanitize(after.get("error")),
        }
        if before is not None:
            assert self.lake("SELECT * FROM " + target) == before
        else:
            assert self.lake(
                "SELECT COUNT(*)::VARCHAR FROM system.tables WHERE database=? AND name='probe'",
                [self.database],
            ) == [["0"]]
        self.save()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--reject",
        choices=[
            "partition",
            "no-pk",
            "narrow",
            "expression-default",
        ],
    )
    parser.add_argument("--unique-only", action="store_true")
    parser.add_argument("--varchar-only", action="store_true")
    args = parser.parse_args()
    run = ExtendedRun(force_replicate=args.reject == "no-pk")
    try:
        run.setup()
        if args.reject:
            run.case(
                "expected rejection: " + args.reject, lambda: run.reject(args.reject)
            )
        elif args.varchar_only:
            run.case(
                "INT to sufficiently wide VARCHAR(200) with literal default",
                run.int_to_varchar,
            )
        elif args.unique_only:
            run.case(
                "NOT NULL UNIQUE without declared PK, including key change",
                run.unique_without_pk,
            )
        else:
            run.tests()
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
