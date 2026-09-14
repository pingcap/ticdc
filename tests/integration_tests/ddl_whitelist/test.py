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
    # Earlier ignored DDLs did not create their objects downstream. Provision
    # them before testing a later RENAME/MODIFY/DROP, so it cannot pass merely
    # because the sink tolerates an object-not-found error.
    ignored_fixtures = {
        "RENAME INDEX": "CREATE INDEX idx_v ON t(v)",
        "ALTER TABLE INDEX VISIBILITY": "CREATE INDEX idx_n ON t(n)",
        "DROP INDEX": "ALTER TABLE t RENAME INDEX idx_v TO idx_v2",
        "MODIFY COLUMN": "ALTER TABLE t ADD COLUMN added INT DEFAULT 7",
    }
    for label, sql in [
        ("CREATE INDEX", "CREATE INDEX idx_v ON t(v)"),
        ("ADD INDEX", "ALTER TABLE t ADD INDEX idx_n(n)"),
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
        # Views have no indexes of their own; exercise them over a replicated
        # base table in both the default and force-replicate configurations.
        test.step("CREATE VIEW", "CREATE VIEW v AS SELECT id, v FROM renamed")
        test.step("DROP VIEW", "DROP VIEW v")
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


def run_drop_last_key():
    test = DDLTest("default", "_drop")
    test.step("CREATE DATABASE", f"CREATE DATABASE {test.schema}", True, True)
    # Run these at the end: the documentation excludes further replication
    # after removing the last effective key with force-replicate=false.
    for name, key, ddl in [
        ("drop_pk", "PRIMARY KEY(id) NONCLUSTERED", "ALTER TABLE drop_pk DROP PRIMARY KEY"),
        ("drop_uk", "UNIQUE KEY uk(id)", "ALTER TABLE drop_uk DROP INDEX uk"),
    ]:
        test.sql(f"CREATE TABLE {name}(id BIGINT NOT NULL, v INT, {key})")
        test.checkpoint()
        assert test.sql(f"SHOW CREATE TABLE {name}", False)
        test.step("drop last effective key must be ignored", ddl, False)


if __name__ == "__main__":
    for matrix_mode in ("default", "forced", "ignored"):
        run_matrix(matrix_mode)
    run_key_transitions()
    run_drop_last_key()
    print("DDL whitelist schema, data, filtering, and checkpoint checks passed", flush=True)
