# Copyright 2026 PingCAP, Inc.
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

"""Exercise the documented DDL whitelist against a real MySQL sink.

Only the Python standard library and the integration harness's mysql client
are required. Tables contain a few rows, allowing exact schema/data comparison
after every DDL, including intermediate states that a final diff would miss.
"""

import difflib
import json
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request


def mysql(upstream, sql):
    prefix = "UP" if upstream else "DOWN"
    result = subprocess.run(
        [
            "mysql", "-uroot", f"-h{os.environ[prefix + '_TIDB_HOST']}",
            f"-P{os.environ[prefix + '_TIDB_PORT']}",
            "--default-character-set=utf8mb4", "--batch", "--skip-column-names",
            "--raw", "-e", sql,
        ],
        capture_output=True, text=True, timeout=90, check=True,
    )
    return result.stdout.strip()


def normalize_schema(ddl, upstream):
    # Allocator reservations differ between clusters. REBASE AUTO ID also has
    # an explicit assertion below, before any replicated INSERT can advance it.
    ddl = re.sub(r" AUTO_INCREMENT=\d+", "", ddl)
    # TiCDC deliberately disables TTL on the downstream to avoid double deletion.
    if upstream:
        ddl = ddl.replace("TTL_ENABLE='ON'", "TTL_ENABLE='OFF'")
    return ddl


class DDLTest:
    def __init__(self, mode, suffix=""):
        self.mode = mode
        self.schema = f"ddl_whitelist_{mode}{suffix}"
        self.replicate = mode != "ignored"
        self.steps = 0
        keyspace = urllib.parse.quote(os.environ["KEYSPACE_NAME"])
        self.status_url = (
            f"http://{os.environ['CDC_HOST']}:{os.environ['CDC_PORT']}"
            f"/api/v2/changefeeds/ddl-whitelist-{mode}?keyspace={keyspace}"
        )

    def sql(self, statement, upstream=True):
        return mysql(upstream, f"USE `{self.schema}`; {statement}")

    def status(self):
        with urllib.request.urlopen(self.status_url, timeout=10) as response:
            return json.load(response)

    def checkpoint(self):
        # Acquire a fresh TSO after the SQL has completed. The explicit
        # transaction makes @@tidb_current_ts valid independently of autocommit.
        target = int(mysql(True, "BEGIN; SELECT @@tidb_current_ts; COMMIT;"))
        assert target > 0, "failed to acquire a post-DDL TSO"
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            status = self.status()
            if status.get("error"):
                raise AssertionError(f"{self.schema}: changefeed error: {status['error']}")
            if int(status.get("checkpoint_ts", 0)) > target:
                return
            time.sleep(0.5)
        raise AssertionError(f"{self.schema}: checkpoint did not pass {target}: {status}")

    def expect_table_dispatchers(self, table, expected):
        table_id = int(self.sql(
            "SELECT TIDB_TABLE_ID FROM information_schema.tables "
            f"WHERE TABLE_SCHEMA='{self.schema}' AND TABLE_NAME='{table}'"))
        tables_url = self.status_url.replace("?", "/tables?", 1)
        deadline = time.monotonic() + 60
        actual = None
        while time.monotonic() < deadline:
            with urllib.request.urlopen(tables_url, timeout=10) as response:
                items = json.load(response)["items"]
            # Do not deduplicate: these small, unsplit tables must have exactly
            # one task when eligible, and none after losing the last key.
            actual = sum(item["table_ids"].count(table_id) for item in items)
            if actual == expected:
                return
            time.sleep(0.5)
        raise AssertionError(
            f"{self.schema}.{table}: expected {expected} dispatchers, got {actual}")

    def snapshot(self, upstream):
        schema_info = mysql(upstream,
            "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME "
            f"FROM information_schema.schemata WHERE SCHEMA_NAME='{self.schema}'")
        if not schema_info:
            return None
        tables = mysql(upstream,
            "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.tables "
            f"WHERE TABLE_SCHEMA='{self.schema}' ORDER BY TABLE_NAME")
        result = {"database": schema_info, "tables": {}}
        for line in tables.splitlines():
            table, table_type = line.split("\t")
            ddl = self.sql(f"SHOW CREATE TABLE `{table}`", upstream).split("\t")[1]
            rows = self.sql(f"SELECT * FROM `{table}`", upstream).splitlines()
            result["tables"][table] = {
                "type": table_type,
                "ddl": normalize_schema(ddl, upstream),
                "rows": sorted(rows),
            }
        return result

    def expect_snapshot(self, expected):
        deadline = time.monotonic() + 60
        actual = None
        while time.monotonic() < deadline:
            try:
                actual = self.snapshot(False)
                if actual == expected:
                    return
            except subprocess.CalledProcessError as error:
                actual = {"mysql_error": error.stderr}
            time.sleep(0.5)
        diff = "\n".join(difflib.unified_diff(
            json.dumps(expected, indent=2, ensure_ascii=False).splitlines(),
            json.dumps(actual, indent=2, ensure_ascii=False).splitlines(),
            fromfile="expected", tofile="downstream", lineterm="",
        ))
        raise AssertionError(f"{self.schema}: schema/data mismatch\n{diff}")

    def step(self, label, statement, replicate=None, database=False):
        if replicate is None:
            replicate = self.replicate
        before = self.snapshot(False) if not replicate else None
        print(f"[{self.schema}] {label} ({'replicate' if replicate else 'ignore'}): {statement}", flush=True)
        if database:
            mysql(True, statement)
        else:
            self.sql(statement)
        self.checkpoint()
        # Index DDL can be asynchronous downstream. Poll actual metadata even
        # after the checkpoint advances, before issuing dependent DDL or DML.
        self.expect_snapshot(self.snapshot(True) if replicate else before)
        self.steps += 1

    def create_table(self, name, definition, seed):
        statement = f"CREATE TABLE {name} {definition}"
        self.step("CREATE TABLE", statement)
        if not self.replicate:
            # The missing downstream table is already asserted above. Seed a
            # downstream sentinel so ignored ALTER/TRUNCATE/DROP operations
            # have an observable effect if they are accidentally forwarded.
            self.sql(statement, False)
            self.sql(seed, False)
        self.step("DML before DDL", seed)


def run_matrix(mode):
    test = DDLTest(mode)
    db = test.schema
    key = ", UNIQUE KEY uk_id (id)" if mode == "default" else ""
    test.step("CREATE DATABASE", f"CREATE DATABASE {db} CHARACTER SET utf8 COLLATE utf8_bin", True, True)
    test.step("ALTER DATABASE CHARACTER SET",
              f"ALTER DATABASE {db} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", True, True)

    test.create_table("t", f"(id BIGINT NOT NULL, v VARCHAR(20), n INT DEFAULT 0{key}) CHARSET=utf8",
                      "INSERT INTO t VALUES (1, 'one', 10), (2, 'two', 20)")
    # Views are eligible independently of the base table's replication key.
    # The ignored-mode sentinel provides the otherwise missing downstream base
    # table. Run before ignored DML makes the base tables' contents diverge.
    test.step("CREATE VIEW", "CREATE VIEW v AS SELECT id, v FROM t", True)
    test.step("DROP VIEW", "DROP VIEW v", True)
    # These standalone ADD COLUMN variants are rejected by TiDB, so they cannot
    # exercise a CDC eligibility transition. Supported ADD COLUMN + ADD INDEX
    # combinations are covered by run_replication_key_transitions instead.
    for constraint in ("UNIQUE", "PRIMARY KEY"):
        before_upstream, before_downstream = test.snapshot(True), test.snapshot(False)
        try:
            test.sql(f"ALTER TABLE t ADD COLUMN inline_key BIGINT NOT NULL {constraint}")
        except subprocess.CalledProcessError as error:
            assert "8200" in error.stderr and "unsupported add column" in error.stderr, error.stderr
            assert constraint in error.stderr, error.stderr
        else:
            raise AssertionError(f"ADD COLUMN {constraint} is now supported; add a replication assertion")
        test.checkpoint()
        assert test.snapshot(True) == before_upstream, "rejected ADD COLUMN changed upstream schema/data"
        test.expect_snapshot(before_downstream)
        print(f"[{mode}] ADD COLUMN {constraint}: rejected upstream as expected", flush=True)
    # Earlier ignored DDLs did not create their objects downstream. Provision
    # them before testing a later RENAME/MODIFY/DROP, so it cannot pass merely
    # because the sink tolerates an object-not-found error.
    ignored_fixtures = {
        "RENAME INDEX": "CREATE INDEX idx_v ON t(v)",
        "ALTER TABLE INDEX VISIBILITY": "CREATE INDEX idx_n ON t(n)",
        "DROP INDEX": "ALTER TABLE t RENAME INDEX idx_v TO idx_v2",
        "DROP nullable UNIQUE INDEX": "ALTER TABLE t ADD UNIQUE INDEX nullable_uk(v)",
        "MODIFY COLUMN": "ALTER TABLE t ADD COLUMN added INT DEFAULT 7",
    }
    for label, sql in [
        ("CREATE INDEX", "CREATE INDEX idx_v ON t(v)"),
        ("ADD INDEX", "ALTER TABLE t ADD INDEX idx_n(n)"),
        ("ADD nullable UNIQUE INDEX is not an effective key", "ALTER TABLE t ADD UNIQUE INDEX nullable_uk(v)"),
        ("DROP nullable UNIQUE INDEX", "ALTER TABLE t DROP INDEX nullable_uk"),
        ("RENAME INDEX", "ALTER TABLE t RENAME INDEX idx_v TO idx_v2"),
        ("ALTER TABLE INDEX VISIBILITY", "ALTER TABLE t ALTER INDEX idx_n INVISIBLE"),
        ("DROP INDEX", "ALTER TABLE t DROP INDEX idx_v2"),
        ("ADD COLUMN", "ALTER TABLE t ADD COLUMN added INT DEFAULT 7"),
        ("MODIFY COLUMN", "ALTER TABLE t MODIFY COLUMN added BIGINT NOT NULL DEFAULT 8"),
        ("ALTER COLUMN DEFAULT VALUE", "ALTER TABLE t ALTER COLUMN added SET DEFAULT 9"),
        ("DML after column changes", "INSERT INTO t(id, v, n) VALUES (3, 'three', 30)"),
        ("DROP COLUMN", "ALTER TABLE t DROP COLUMN added"),
        ("ALTER TABLE COMMENT", "ALTER TABLE t COMMENT='DDL whitelist'"),
        ("ALTER TABLE CHARACTER SET", "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"),
        ("TRUNCATE TABLE", "TRUNCATE TABLE t"),
        ("DML after TRUNCATE", "INSERT INTO t VALUES (4, 'four', 40)"),
        ("RENAME TABLE", "RENAME TABLE t TO renamed"),
        ("DML after RENAME", "INSERT INTO renamed VALUES (5, 'five', 50)"),
    ]:
        if not test.replicate and label in ignored_fixtures:
            test.sql(ignored_fixtures[label], False)
        test.step(label, sql)

    if test.replicate:
        test.create_table("keys_t", f"(id BIGINT NOT NULL, v INT{key})",
                          "INSERT INTO keys_t VALUES (1, 10)")
        test.step("ADD PRIMARY KEY", "ALTER TABLE keys_t ADD PRIMARY KEY(id) NONCLUSTERED")
        # Default mode keeps uk_id; forced mode forwards dropping its last key.
        test.step("DROP PRIMARY KEY", "ALTER TABLE keys_t DROP PRIMARY KEY")
        test.step("DML after DROP PRIMARY KEY", "INSERT INTO keys_t VALUES (2, 20)")

    partition_definition = (
        f"(id BIGINT NOT NULL, v INT{key}) PARTITION BY RANGE(id) "
        "(PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))"
    )
    test.create_table("pt", partition_definition, "INSERT INTO pt VALUES (1, 10), (11, 110)")
    for label, sql in [
        ("ADD PARTITION", "ALTER TABLE pt ADD PARTITION (PARTITION p2 VALUES LESS THAN (30))"),
        ("DML after ADD PARTITION", "INSERT INTO pt VALUES (21, 210)"),
        ("TRUNCATE PARTITION", "ALTER TABLE pt TRUNCATE PARTITION p1"),
        ("DROP PARTITION", "ALTER TABLE pt DROP PARTITION p2"),
        ("REORGANIZE PARTITION", "ALTER TABLE pt REORGANIZE PARTITION p0 INTO "
         "(PARTITION p00 VALUES LESS THAN (5), PARTITION p01 VALUES LESS THAN (10))"),
    ]:
        if not test.replicate and label == "DROP PARTITION":
            test.sql("ALTER TABLE pt ADD PARTITION (PARTITION p2 VALUES LESS THAN (30))", False)
        test.step(label, sql)
    test.create_table("exchange_t", f"(id BIGINT NOT NULL, v INT{key})", "INSERT INTO exchange_t VALUES (2, 20)")
    if not test.replicate:
        test.sql("ALTER TABLE pt REORGANIZE PARTITION p0 INTO "
                 "(PARTITION p00 VALUES LESS THAN (5), PARTITION p01 VALUES LESS THAN (10))", False)
    test.step("EXCHANGE PARTITION", "ALTER TABLE pt EXCHANGE PARTITION p00 WITH TABLE exchange_t WITHOUT VALIDATION")
    test.step("DML after EXCHANGE PARTITION", "INSERT INTO pt VALUES (3, 30); INSERT INTO exchange_t VALUES (4, 40)")

    test.create_table("ttl_t", f"(id BIGINT NOT NULL, created_at DATETIME{key})",
                      "INSERT INTO ttl_t VALUES (1, '2035-01-01')")
    test.step("ALTER TABLE TTL", "ALTER TABLE ttl_t TTL=created_at + INTERVAL 1 DAY TTL_ENABLE='ON'")
    if not test.replicate:
        test.sql("ALTER TABLE ttl_t TTL=created_at + INTERVAL 1 DAY TTL_ENABLE='OFF'", False)
    test.step("ALTER TABLE TTL interval", "ALTER TABLE ttl_t TTL_JOB_INTERVAL='2h'")
    test.step("ALTER TABLE REMOVE TTL", "ALTER TABLE ttl_t REMOVE TTL")

    auto_key = "UNIQUE KEY" if mode == "default" else "KEY"
    test.create_table("auto_t", f"(id BIGINT NOT NULL AUTO_INCREMENT, v INT, {auto_key} idx_id(id))",
                      "INSERT INTO auto_t(v) VALUES (10)")
    test.step("REBASE AUTO ID", "ALTER TABLE auto_t AUTO_INCREMENT=1000000000")
    # information_schema.tables.AUTO_INCREMENT is a local allocator cache and
    # can be zero after DDL. Inspect the persisted global allocator instead.
    actual = test.sql("SHOW CREATE TABLE auto_t", False)
    auto_id = re.search(r" AUTO_INCREMENT=(\d+)", actual)
    assert auto_id, f"missing AUTO_INCREMENT allocator: {actual}"
    assert (int(auto_id[1]) >= 1000000000) == test.replicate, f"REBASE AUTO ID was not respected: {actual}"
    test.step("DML after REBASE AUTO ID", "INSERT INTO auto_t(v) VALUES (20)")

    test.create_table("recover_t", f"(id BIGINT NOT NULL, v INT{key})", "INSERT INTO recover_t VALUES (1, 10)")
    test.step("DROP TABLE", "DROP TABLE recover_t")
    if not test.replicate:
        # A wrongly forwarded RECOVER must be visible, not hidden by an
        # already-existing downstream sentinel table.
        test.sql("DROP TABLE recover_t", False)
    test.step("RECOVER TABLE", "RECOVER TABLE recover_t")
    test.step("DML after RECOVER", "INSERT INTO recover_t VALUES (2, 20)")

    # Sequence DDL is outside TiCDC's whitelist even with force-replicate=true.
    for label, sql in [
        ("CREATE SEQUENCE outside whitelist", "CREATE SEQUENCE seq START WITH 1"),
        ("ALTER SEQUENCE outside whitelist", "ALTER SEQUENCE seq RESTART WITH 100"),
        ("DROP SEQUENCE outside whitelist", "DROP SEQUENCE seq"),
    ]:
        test.step(label, sql, False)
    test.step("DROP DATABASE", f"DROP DATABASE {db}", True, True)
    print(f"[{mode}] passed {test.steps} DDL/DML checks", flush=True)


def run_key_transitions():
    test = DDLTest("default", "_keys")
    test.step("CREATE DATABASE", f"CREATE DATABASE {test.schema}", True, True)
    for name, ddl in [
        ("create_uk", "CREATE UNIQUE INDEX uk ON create_uk(id)"),
        ("add_uk", "ALTER TABLE add_uk ADD UNIQUE INDEX uk(id)"),
        ("add_pk", "ALTER TABLE add_pk ADD PRIMARY KEY(id) NONCLUSTERED"),
    ]:
        statement = f"CREATE TABLE {name}(id BIGINT NOT NULL, v INT)"
        test.step("ineligible CREATE TABLE", statement, False)
        # Provision the missing table to avoid the schema mismatch warned about
        # in the documentation when a previously skipped table becomes eligible.
        test.sql(statement, False)
        test.step("add first effective key", ddl)
        test.step("CREATE TABLE LIKE after eligibility change", f"CREATE TABLE {name}_like LIKE {name}")
        test.step("DML after eligibility change",
                  f"INSERT INTO {name} VALUES (1, 10); INSERT INTO {name}_like VALUES (2, 20)")
    test.step("DROP DATABASE", f"DROP DATABASE {test.schema}", True, True)

    # Run last: unlike the successful transitions above, do not provision the
    # skipped table downstream. The index DDL does not backfill CREATE TABLE;
    # the missing table must surface as an error, not silent data loss.
    test = DDLTest("default", "_missing_table")
    test.step("CREATE DATABASE", f"CREATE DATABASE {test.schema}", True, True)
    test.step("ineligible CREATE TABLE without downstream provisioning",
              "CREATE TABLE missing_t(id BIGINT NOT NULL, v INT)", False)
    test.sql("ALTER TABLE missing_t ADD UNIQUE INDEX uk(id)")
    test.sql("INSERT INTO missing_t VALUES (1, 10)")
    target = int(mysql(True, "BEGIN; SELECT @@tidb_current_ts; COMMIT;"))
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        status = test.status()
        error = status.get("error")
        if error:
            message = json.dumps(error)
            assert "1146" in message and "missing_t" in message, message
            assert int(status["checkpoint_ts"]) < target, status
            assert not test.snapshot(False)["tables"], "missing table was unexpectedly backfilled"
            print("[default] missing downstream table reported error 1146 as expected", flush=True)
            break
        assert int(status["checkpoint_ts"]) < target, "checkpoint passed unsynchronized data"
        time.sleep(0.5)
    else:
        raise AssertionError(f"missing downstream table did not report an error: {status}")


def run_replication_key_transitions(mode):
    test = DDLTest(mode, "_key_transitions")
    test.step("CREATE DATABASE", f"CREATE DATABASE {test.schema}", True, True)
    # Keep these small index changes off the distributed/fast-reorg path.
    mysql(True, "SET GLOBAL tidb_enable_dist_task=OFF; SET GLOBAL tidb_ddl_enable_fast_reorg=OFF")
    for name, definition, lose_key, acquire_key, retains_key in [
        ("drop_column", "id BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL",
         "DROP COLUMN id", "ADD PRIMARY KEY(v) NONCLUSTERED", False),
        ("drop_pk", "id BIGINT PRIMARY KEY NONCLUSTERED, v BIGINT NOT NULL",
         "DROP PRIMARY KEY", "ADD PRIMARY KEY(id) NONCLUSTERED", False),
        # ADD COLUMN + ADD INDEX on an existing column is a multi-schema DDL;
        # TiDB does not support adding a column with an inline UNIQUE constraint.
        ("drop_uk", "id BIGINT NOT NULL, v BIGINT NOT NULL, UNIQUE KEY uk(id)",
         "DROP INDEX uk", "ADD COLUMN extra INT, ADD UNIQUE INDEX uk(id)", False),
        ("modify_nullable", "id BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL",
         "MODIFY COLUMN id BIGINT", "MODIFY COLUMN id BIGINT NOT NULL", False),
        ("modify_composite_nullable", "id BIGINT NOT NULL, v BIGINT NOT NULL, UNIQUE KEY uk(id, v)",
         "MODIFY COLUMN id BIGINT", "MODIFY COLUMN id BIGINT NOT NULL", False),
        ("multi_schema", "id BIGINT NOT NULL, v BIGINT NOT NULL, UNIQUE KEY uk(id)",
         "DROP INDEX uk, ADD COLUMN extra INT", "ADD UNIQUE INDEX uk(id), DROP COLUMN extra", False),
        ("multi_drop_column", "id BIGINT NOT NULL UNIQUE, v BIGINT NOT NULL",
         "DROP COLUMN id, ADD COLUMN extra INT", "ADD UNIQUE INDEX uk_v(v), DROP COLUMN extra", False),
        ("retain_other_key", "id BIGINT NOT NULL UNIQUE, v BIGINT PRIMARY KEY NONCLUSTERED",
         "DROP COLUMN id", "ADD UNIQUE INDEX uk_v(v)", True),
    ]:
        test.create_table(name, f"({definition})", f"INSERT INTO {name}(id, v) VALUES (1, 1), (2, 2)")
        test.expect_table_dispatchers(name, 1)
        if name == "drop_column":
            # FIRST changes the indexed column's offset, not eligibility.
            test.step("ADD COLUMN before indexed column", f"ALTER TABLE {name} ADD COLUMN pad INT FIRST")
            test.expect_table_dispatchers(name, 1)
            test.step("DML after index column offset changed", f"UPDATE {name} SET v=v+1")
            test.step("DROP non-key column", f"ALTER TABLE {name} DROP COLUMN pad")
            test.expect_table_dispatchers(name, 1)

        # The key-loss DDL must execute downstream before default-mode dispatchers
        # are removed. step() waits for a post-DDL checkpoint and compares
        # intermediate schema and rows.
        test.step("remove a replication key", f"ALTER TABLE {name} {lose_key}")
        remains_replicated = mode == "forced" or retains_key
        test.expect_table_dispatchers(name, 1 if remains_replicated else 0)
        if remains_replicated:
            test.step("DML while key is removed", f"UPDATE {name} SET v=v+10")
            test.expect_table_dispatchers(name, 1)
            test.step("restore a replication key", f"ALTER TABLE {name} {acquire_key}")
        else:
            columns, values = ("v", "9000") if name in ("drop_column", "multi_drop_column") else ("id, v", "9000, 9000")
            test.step("DML without a replication key is ignored",
                      f"INSERT INTO {name}({columns}) VALUES ({values})", False)
            test.expect_table_dispatchers(name, 0)
            # Schema must catch up, but re-eligibility must not backfill the
            # row written while this table had no dispatcher.
            test.sql(f"ALTER TABLE {name} {acquire_key}")
            test.checkpoint()
            expected = test.snapshot(True)
            expected["tables"][name]["rows"] = sorted(
                test.sql(f"SELECT * FROM {name} WHERE v <> 9000").splitlines())
            test.expect_snapshot(expected)
            test.steps += 1
            print(f"[{mode}] {name}: restored key without backfilling skipped row", flush=True)
            # DELETE of the skipped row is harmless downstream and restores
            # equal data sets for the subsequent ordinary DML assertions.
            test.step("DELETE skipped row after restoring key", f"DELETE FROM {name} WHERE v=9000")
        test.expect_table_dispatchers(name, 1)
        test.step("DML after restoring key",
                  f"UPDATE {name} SET v=v+100; DELETE FROM {name} WHERE MOD(v, 2)=0")
        test.step("DROP TABLE after key transitions", f"DROP TABLE {name}")
    test.step("DROP DATABASE", f"DROP DATABASE {test.schema}", True, True)
    print(f"[{mode}] passed {test.steps} replication-key DDL/DML checks", flush=True)


if __name__ == "__main__":
    # matrix_mode selects the initial table/configuration for the DDL whitelist:
    # default = usable key, force=false; forced = no usable key, force=true;
    # ignored = no usable key, force=false (not excluded by a name filter).
    for matrix_mode in ("default", "forced", "ignored"):
        run_matrix(matrix_mode)
    # transition_mode selects the policy while initially eligible tables lose
    # and regain a usable key: default removes/recreates dispatchers, whereas
    # forced keeps them and continues replicating. No separate ignored run is
    # needed: default already covers that state while the last key is absent.
    # Cases retaining another usable key verify dispatchers stay in both modes.
    for transition_mode in ("default", "forced"):
        run_replication_key_transitions(transition_mode)
    run_key_transitions()
    print("DDL whitelist schema, data, filtering, and checkpoint checks passed", flush=True)
