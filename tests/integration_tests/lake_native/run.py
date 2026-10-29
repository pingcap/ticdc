#!/usr/bin/env python3
"""Opt-in TiUP -> native TiCDC -> isolated Lake database acceptance test.

Requires an already-running local TiUP and current-branch CDC server. Credentials
are read from LAKE_URI_FILE (mode 0600), never argv or repository config. Only the
random native_e2e_* database and this run's changefeed are created/dropped.
"""

import base64
import datetime
import decimal
import json
import os
import pathlib
import re
import signal
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def quote(name):
    return '"' + name.replace('"', '""') + '"'


class Run:
    def __init__(self, existing=False):
        credential_file = pathlib.Path(os.environ["LAKE_URI_FILE"])
        if credential_file.stat().st_mode & 0o077:
            raise RuntimeError("LAKE_URI_FILE must have private 0600 permissions")
        self.uri = credential_file.read_text().strip()
        self.parsed = urllib.parse.urlparse(self.uri)
        self.root = pathlib.Path(os.environ["E2E_RUN_DIR"])
        self.root.mkdir(parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        self.cdc = os.environ.get("CDC_SERVER", "http://127.0.0.1:18300")
        self.port = os.environ.get("MYSQL_PORT", "14000")
        self.database = (
            "native_e2e_"
            + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S_")
            + uuid.uuid4().hex[:8]
        )
        self.feed = self.database.replace("_", "-")
        # Plain HTTP is only enabled by the sink's explicit test scheme.
        scheme = "http" if self.parsed.scheme == "lake+http" else "https"
        self.base = scheme + "://" + self.parsed.netloc.rsplit("@", 1)[-1]
        self.results = {
            "database": self.database,
            "changefeed": self.feed,
            "cases": [],
            "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        self.feed_created = False
        self.remote_created = False
        if existing:
            saved = json.loads((self.root / "result.json").read_text())
            database = saved["database"]
            if not re.fullmatch(
                r"native_e2e_[0-9]{8}_[0-9]{6}_[0-9a-f]{8}", database
            ) or saved["changefeed"] != database.replace("_", "-"):
                raise RuntimeError(
                    "refusing cleanup outside this runner's isolated namespace"
                )
            self.results = saved
            self.database = database
            self.feed = saved["changefeed"]
            self.remote_created = True
            self.feed_created = True
        else:
            if (self.root / "result.json").exists():
                raise RuntimeError(
                    "E2E_RUN_DIR already contains a result; use a new directory or --cleanup"
                )
            self.save()

    def sanitize(self, text):
        return (
            str(text)
            .replace(self.uri, "[REDACTED_URI]")
            .replace(self.parsed.password or "\0", "[REDACTED]")
        )

    def save(self):
        path = self.root / "result.json"
        path.write_text(json.dumps(self.results, indent=2, ensure_ascii=False))
        os.chmod(path, 0o600)

    def local(self, sql):
        proc = subprocess.run(
            [
                "mysql",
                "-h",
                "127.0.0.1",
                "-P",
                self.port,
                "-u",
                "root",
                "--default-character-set=utf8mb4",
                "-N",
                "-B",
                "--raw",
                "-e",
                "SET time_zone='+00:00';" + sql,
            ],
            text=True,
            capture_output=True,
            timeout=40,
        )
        if proc.returncode:
            raise RuntimeError("local SQL failed: " + self.sanitize(proc.stderr))
        return proc.stdout.strip()

    def api(self, path, data=None, method=None):
        req = urllib.request.Request(
            self.cdc + path,
            data=None if data is None else json.dumps(data).encode(),
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                raw = response.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as error:
            raise RuntimeError(
                "CDC HTTP %d: %s" % (error.code, self.sanitize(error.read().decode()))
            ) from None

    def lake(self, sql, params=None):
        """No mutation retries: an ambiguous response stops the test for inspection."""
        query_id = str(uuid.uuid4())
        auth = (
            urllib.parse.unquote(self.parsed.username)
            + ":"
            + urllib.parse.unquote(self.parsed.password)
        )
        headers = {
            "Authorization": "Basic " + base64.b64encode(auth.encode()).decode(),
            "Content-Type": "application/json",
            "X-DATABEND-WAREHOUSE": urllib.parse.parse_qs(self.parsed.query).get(
                "warehouse", ["default"]
            )[0],
            "X-DATABEND-QUERY-ID": query_id,
        }
        body = {
            "sql": sql,
            "session": {"database": "default", "settings": {"timezone": "UTC"}},
            "pagination": {"wait_time_secs": 1, "max_rows_per_page": 1024},
        }
        if params is not None:
            body["params"] = params
        req = urllib.request.Request(
            self.base + "/v1/query", data=json.dumps(body).encode(), headers=headers
        )
        rows = []
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(req, timeout=40) as response:
                    result = json.load(response)
                    route = response.headers.get("X-DATABEND-ROUTE-HINT")
            except urllib.error.HTTPError as error:
                raise RuntimeError(
                    "Lake HTTP %d request %s: %s"
                    % (error.code, query_id, self.sanitize(error.read(1024).decode()))
                ) from None
            if result.get("state") == "Failed" or result.get("error"):
                raise RuntimeError(
                    "Lake SQL request %s failed: %s"
                    % (query_id, self.sanitize(result.get("error")))
                )
            if result.get("id") != query_id:
                raise RuntimeError("Lake query identity mismatch: " + query_id)
            rows.extend(result.get("data") or [])
            path = result.get("next_uri")
            if result.get("state") == "Succeeded" and (
                not path or path.endswith("/final")
            ):
                final = result.get("final_uri")
                if final and final.startswith("/v1/query/"):
                    try:
                        with urllib.request.urlopen(
                            urllib.request.Request(self.base + final, headers=headers),
                            timeout=5,
                        ) as response:
                            response.read()
                    except (OSError, urllib.error.HTTPError):
                        pass  # Result cleanup does not revoke confirmed query success.
                return rows
            if not path or not path.startswith("/v1/query/") or path.startswith("//"):
                raise RuntimeError("Missing terminal evidence for " + query_id)
            headers["X-DATABEND-STICKY-NODE"] = result.get("node_id", "")
            headers["X-DATABEND-NODE-ID"] = result.get("node_id", "")
            if route:
                headers["X-DATABEND-ROUTE-HINT"] = route
            req = urllib.request.Request(self.base + path, headers=headers)
            time.sleep(0.1)
        raise RuntimeError("Lake query terminal state timeout: " + query_id)

    def feed_status(self):
        return self.api("/api/v2/changefeeds/" + self.feed)

    def check_feed(self):
        status = self.feed_status()
        if status.get("state") in ("failed", "error", "warning") or status.get("error"):
            raise RuntimeError(
                "changefeed unhealthy: "
                + self.sanitize(
                    json.dumps(
                        {
                            "state": status.get("state"),
                            "error": status.get("error"),
                            "checkpoint_ts": status.get("checkpoint_ts"),
                        }
                    )
                )
            )
        return status

    def case(self, name, action):
        started = time.monotonic()
        record = {"name": name}
        try:
            action()
            record.update(status="PASS", seconds=round(time.monotonic() - started, 3))
            print("PASS", name, record["seconds"], flush=True)
        except Exception as error:
            record.update(
                status="FAIL",
                seconds=round(time.monotonic() - started, 3),
                error=self.sanitize(error),
            )
            print("FAIL", name, record["error"], flush=True)
            raise
        finally:
            self.results["cases"].append(record)
            self.save()

    def compare(self, table, columns, numeric=(), json_columns=(), timeout=75):
        # JSON_OBJECT keeps control characters/newlines and SQL NULL unambiguous.
        source = (
            "SELECT JSON_OBJECT("
            + ",".join("'%s',%s" % (name, expr) for name, expr, _ in columns)
            + ") FROM `%s`.`%s` ORDER BY id" % (self.database, table)
        )
        expected = [
            json.loads(line) for line in self.local(source).splitlines() if line
        ]
        target = (
            "SELECT "
            + ",".join(expr for _, _, expr in columns)
            + " FROM "
            + quote(self.database)
            + "."
            + quote(table)
            + " ORDER BY id"
        )
        deadline = time.monotonic() + timeout
        last = ""
        while time.monotonic() < deadline:
            self.check_feed()
            try:
                data = self.lake(target)
                actual = [dict(zip([c[0] for c in columns], row)) for row in data]

                # Lake may bind ORDER BY id to the projected VARCHAR alias.
                # Compare the key-indexed set, never numeric-vs-text sort order.
                def key(row):
                    return str(row["id"]), str(row.get("tenant", ""))

                expected.sort(key=key)
                actual.sort(key=key)
                if len(expected) != len(actual):
                    raise AssertionError(
                        "row count expected %d actual %d" % (len(expected), len(actual))
                    )
                for left, right in zip(expected, actual):
                    for name in left:
                        a, b = left[name], right[name]
                        if a is None or b is None:
                            assert a is b, (name, a, b)
                        elif name in json_columns:
                            assert json.loads(
                                a, parse_float=decimal.Decimal
                            ) == json.loads(b, parse_float=decimal.Decimal), (
                                name,
                                a,
                                b,
                            )
                        elif name in numeric:
                            da, db = decimal.Decimal(str(a)), decimal.Decimal(str(b))
                            assert abs(da - db) <= max(
                                abs(da), decimal.Decimal(1)
                            ) * decimal.Decimal("0.000001"), (name, a, b)
                        else:
                            assert str(a) == str(b), (name, a, b)
                return
            except (RuntimeError, AssertionError) as error:
                last = self.sanitize(error)
                time.sleep(0.5)
        raise AssertionError("target did not converge: " + last)

    def simple(self, table, names):
        self.compare(
            table,
            [
                (name, "CAST(`%s` AS CHAR)" % name, quote(name) + "::VARCHAR")
                for name in names
            ],
        )

    def setup(self):
        self.results["source_version"] = self.local("SELECT version()")
        self.results["cdc_status"] = self.api("/api/v2/status")
        self.results["lake_version"] = self.lake("SELECT version(), ?::BIGINT", [1])[0]
        self.local("CREATE DATABASE `%s`" % self.database)
        self.lake("CREATE DATABASE " + quote(self.database))
        self.remote_created = True
        q = urllib.parse.parse_qs(self.parsed.query)
        q.update(
            {
                "exclusive": ["true"],
                "workers": ["1"],
                "batch-rows": ["64"],
                "batch-bytes": ["32768"],
                "flush-interval": ["200ms"],
                "spool-bytes": ["65536"],
                "memory-ratio": ["0.001"],
                "source-bytes": ["268435456"],
                "source-events": ["1"],
                "spool-dir": [str(self.root / "spool")],
            }
        )
        sink = self.parsed._replace(
            path="/" + self.database, query=urllib.parse.urlencode(q, doseq=True)
        ).geturl()
        self.api(
            "/api/v2/changefeeds",
            {
                "changefeed_id": self.feed,
                "sink_uri": sink,
                "replica_config": {
                    "filter": {"rules": [self.database + ".*"]},
                    "sink": {"transaction_atomicity": "none"},
                    "scheduler": {"enable_table_across_nodes": False},
                },
            },
        )
        self.feed_created = True
        self.save()
        time.sleep(2)
        self.check_feed()

    def tests(self):
        db = "`" + self.database + "`"
        self.case(
            "DML insert/update/delete and nonclustered PK/UK changes",
            lambda: self.test_keys(db),
        )
        self.case(
            "Composite primary key and escaped identifiers",
            lambda: self.test_composite(db),
        )
        self.case(
            "Quoted identifiers and operation discriminator collision",
            lambda: self.test_identifiers(db),
        )
        self.case(
            "Column scalar/binary/decimal/JSON/ENUM/SET/temporal fidelity",
            lambda: self.test_types(db),
        )
        self.case("DDL add/default/rename/widen/drop", lambda: self.test_ddl(db))
        self.case(
            "TRUNCATE new generation and subsequent DML", lambda: self.test_truncate(db)
        )
        self.case(
            "Large transaction and low traffic batching", lambda: self.test_batch(db)
        )
        if os.environ.get("E2E_CDC_BINARY"):
            self.case(
                "Graceful CDC process restart with paused DML/DDL backlog",
                lambda: self.test_restart(db),
            )
        self.case("DROP TABLE cleanup", lambda: self.test_drop(db))
        self.results["last_status"] = self.feed_status()
        # Never persist the full sink URI returned by a future API version.
        self.results["last_status"] = json.loads(
            self.sanitize(json.dumps(self.results["last_status"]))
        )
        self.save()

    def test_keys(self, db):
        self.local(
            f"CREATE TABLE {db}.keys_test (id BIGINT PRIMARY KEY NONCLUSTERED,email VARCHAR(128) UNIQUE, value VARCHAR(255)); INSERT INTO {db}.keys_test VALUES (1,'a','first'),(2,'b','second')"
        )
        self.simple("keys_test", ["id", "email", "value"])
        self.local(f"UPDATE {db}.keys_test SET email='c',value='changed' WHERE id=1")
        self.simple("keys_test", ["id", "email", "value"])
        self.local(f"UPDATE {db}.keys_test SET id=3 WHERE id=1")
        self.simple("keys_test", ["id", "email", "value"])
        self.local(
            f"BEGIN; DELETE FROM {db}.keys_test WHERE id=2; INSERT INTO {db}.keys_test VALUES (2,'new','reused'); COMMIT;"
        )
        self.simple("keys_test", ["id", "email", "value"])
        self.local(f"DELETE FROM {db}.keys_test WHERE id=3")
        self.simple("keys_test", ["id", "email", "value"])

    def test_composite(self, db):
        self.local(
            f"CREATE TABLE {db}.composite_test(id INT, tenant VARCHAR(64), `action` VARCHAR(64), PRIMARY KEY(tenant,id)); INSERT INTO {db}.composite_test VALUES (1,'a|b','insert'),(2,'a|b','update')"
        )
        self.simple("composite_test", ["id", "tenant", "action"])
        self.local(
            f"UPDATE {db}.composite_test SET tenant='b|c',action='delete' WHERE id=1; DELETE FROM {db}.composite_test WHERE id=2"
        )
        self.simple("composite_test", ["id", "tenant", "action"])

    def test_identifiers(self, db):
        self.local(
            f"""CREATE TABLE {db}.`quoted"table` (id INT PRIMARY KEY,`a"b` VARCHAR(64),`__ticdc_operation` VARCHAR(64),`action` VARCHAR(64)); INSERT INTO {db}.`quoted"table` VALUES(1,'quoted-column','source-value','insert')"""
        )
        self.simple('quoted"table', ["id", 'a"b', "__ticdc_operation", "action"])
        self.local(
            f"""UPDATE {db}.`quoted"table` SET `action`='delete',`a"b`='changed' WHERE id=1"""
        )
        self.simple('quoted"table', ["id", 'a"b', "__ticdc_operation", "action"])

    def test_types(self, db):
        sets = ",".join("'s%d'" % i for i in range(64))
        definitions = (
            """id INT PRIMARY KEY, s_tiny TINYINT, u_tiny TINYINT UNSIGNED,
        s_small SMALLINT, u_small SMALLINT UNSIGNED,s_medium MEDIUMINT,u_medium MEDIUMINT UNSIGNED,
        s_int INT,u_int INT UNSIGNED,s_big BIGINT,u_big BIGINT UNSIGNED,
        d DECIMAL(65,30),f FLOAT,g DOUBLE, label ENUM('2','x'),labels SET(%s),
        bytes BLOB,txt TEXT,doc JSON,sql_null JSON,json_null JSON,
        dt DATETIME(6),ts TIMESTAMP(6) NULL,day DATE,duration TIME(6),yr YEAR,bits BIT(64),fixed_bytes BINARY(8),var_bytes VARBINARY(32),char_value CHAR(8),bool_value BOOLEAN,
        generated_value BIGINT GENERATED ALWAYS AS (s_big + 1) STORED,
        virtual_value BIGINT GENERATED ALWAYS AS (id * 2) VIRTUAL"""
            % sets
        )
        self.local(f"CREATE TABLE {db}.types_test({definitions})")
        hexbytes = bytes(range(256)).hex()
        self.local(f"""INSERT INTO {db}.types_test(id,s_tiny,u_tiny,s_small,u_small,s_medium,u_medium,s_int,u_int,s_big,u_big,d,f,g,label,labels,bytes,txt,doc,sql_null,json_null,dt,ts,day,duration,yr,bits,fixed_bytes,var_bytes,char_value,bool_value)
        VALUES (1,-128,255,-32768,65535,-8388608,16777215,-2147483648,4294967295,9223372036854775806,18446744073709551615,
        12345678901234567890123456789012345.123456789012345678901234567890,1.234567,1.234567890123,'2','s63',UNHEX('{hexbytes}'),
        '中文😀 quote\\\' slash\\\\ newline\\n',JSON_OBJECT('uint',18446744073709551615,'arr',JSON_ARRAY(NULL,TRUE),'label','2'),NULL,CAST('null' AS JSON),
        '2026-10-25 01:02:03.123456','2026-10-25 01:02:03.654321','2026-10-25','-123:45:56.123456',2026,b'1111111111111111111111111111111111111111111111111111111111111111',UNHEX('000102ff'),UNHEX('00feff'),'char',TRUE);
        INSERT INTO {db}.types_test(id) VALUES (2);
        INSERT INTO {db}.types_test(id,s_tiny,u_tiny,s_small,u_small,s_medium,u_medium,s_int,u_int,s_big,u_big,d)
        VALUES(3,127,0,32767,0,8388607,0,2147483647,0,-9223372036854775808,0,-99999999999999999999999999999999999.999999999999999999999999999999)""")
        names = [
            "id",
            "s_tiny",
            "u_tiny",
            "s_small",
            "u_small",
            "s_medium",
            "u_medium",
            "s_int",
            "u_int",
            "s_big",
            "u_big",
            "d",
            "f",
            "g",
            "label",
            "labels",
            "txt",
            "dt",
            "ts",
            "day",
            "duration",
            "yr",
            "generated_value",
            "char_value",
            "bool_value",
        ]
        cols = [(n, "CAST(`%s` AS CHAR)" % n, quote(n) + "::VARCHAR") for n in names]
        cols = [
            (name, "CAST(CAST(`%s` AS DOUBLE) AS CHAR)" % name, target)
            if name in ("f", "g")
            else (name, source, target)
            for name, source, target in cols
        ]
        cols += [
            ("bytes", "HEX(bytes)", "upper(hex(" + quote("bytes") + "))"),
            ("bits", "CAST(bits+0 AS CHAR)", quote("bits") + "::VARCHAR"),
            (
                "fixed_bytes",
                "HEX(fixed_bytes)",
                "upper(hex(" + quote("fixed_bytes") + "))",
            ),
            ("var_bytes", "HEX(var_bytes)", "upper(hex(" + quote("var_bytes") + "))"),
        ]
        cols += [
            (
                n,
                "CAST(`%s` AS CHAR)" % n,
                "CASE WHEN is_null_value("
                + quote(n)
                + ") THEN 'null' ELSE "
                + quote(n)
                + "::VARCHAR END",
            )
            for n in ["doc", "sql_null", "json_null"]
        ]
        self.compare(
            "types_test",
            cols,
            numeric=("f", "g"),
            json_columns=("doc", "sql_null", "json_null"),
        )
        described = self.lake("DESCRIBE " + quote(self.database) + '."types_test"')
        self.results["types_describe"] = described
        assert "virtual_value" not in [r[0] for r in described]
        self.local(
            f"UPDATE {db}.types_test SET label='x',labels='s0,s63',d=-1.000000000000000000000000000001,doc=JSON_OBJECT('changed',true),bytes=UNHEX('00ff'),txt=NULL WHERE id=1"
        )
        self.compare(
            "types_test",
            cols,
            numeric=("f", "g"),
            json_columns=("doc", "sql_null", "json_null"),
        )

    def test_ddl(self, db):
        self.local(
            f"CREATE TABLE {db}.ddl_test(id INT PRIMARY KEY,value INT); INSERT INTO {db}.ddl_test VALUES(1,42)"
        )
        self.simple("ddl_test", ["id", "value"])
        self.local(
            f"ALTER TABLE {db}.ddl_test ADD COLUMN extra VARCHAR(64) NOT NULL DEFAULT 'hello'"
        )
        self.simple("ddl_test", ["id", "value", "extra"])
        self.local(f"ALTER TABLE {db}.ddl_test RENAME COLUMN value TO amount")
        self.simple("ddl_test", ["id", "amount", "extra"])
        self.local(
            f"ALTER TABLE {db}.ddl_test MODIFY COLUMN amount BIGINT; UPDATE {db}.ddl_test SET amount=9223372036854775806 WHERE id=1"
        )
        self.simple("ddl_test", ["id", "amount", "extra"])
        self.local(
            f"ALTER TABLE {db}.ddl_test DROP COLUMN extra; INSERT INTO {db}.ddl_test VALUES(2,-12)"
        )
        self.simple("ddl_test", ["id", "amount"])
        self.local(
            f"CREATE INDEX amount_index ON {db}.ddl_test(amount);ALTER TABLE {db}.ddl_test DROP INDEX amount_index"
        )
        self.local(f"ALTER TABLE {db}.ddl_test ADD COLUMN text_value TEXT NOT NULL")
        self.simple("ddl_test", ["id", "amount", "text_value"])

    def test_truncate(self, db):
        self.local(
            f"CREATE TABLE {db}.truncate_test(id INT PRIMARY KEY,value VARCHAR(64));INSERT INTO {db}.truncate_test VALUES(1,'old')"
        )
        self.simple("truncate_test", ["id", "value"])
        self.local(
            f"TRUNCATE TABLE {db}.truncate_test; INSERT INTO {db}.truncate_test VALUES(2,'new')"
        )
        self.simple("truncate_test", ["id", "value"])

    def test_batch(self, db):
        self.local(f"CREATE TABLE {db}.batch_test(id INT PRIMARY KEY,value TEXT)")
        values = ",".join("(%d,'%s')" % (i, "x" * 256) for i in range(1200))
        # Use stdin for large synthetic transactions, rather than shell/argv limits.
        sql = (
            "SET time_zone='+00:00';BEGIN;INSERT INTO %s.batch_test VALUES%s;COMMIT;"
            % (db, values)
        )
        proc = subprocess.run(
            ["mysql", "-h", "127.0.0.1", "-P", self.port, "-u", "root"],
            input=sql,
            text=True,
            capture_output=True,
            timeout=60,
        )
        assert proc.returncode == 0, proc.stderr
        self.simple("batch_test", ["id", "value"])
        self.local(
            f"UPDATE {db}.batch_test SET value='tail' WHERE id=1199;DELETE FROM {db}.batch_test WHERE id<100"
        )
        self.simple("batch_test", ["id", "value"])
        with urllib.request.urlopen(self.cdc + "/metrics", timeout=5) as response:
            text = response.read().decode()
        count = 0
        for line in text.splitlines():
            if (
                line.startswith("ticdc_lake_spool_load_bytes_count{")
                and ('changefeed="' + self.feed + '"') in line
            ):
                count += float(line.rsplit(" ", 1)[-1])
        self.results["spool_disk_load_count"] = count
        assert count > 0, "large transaction did not exercise disk spool"

    def wait_state(self, expected, timeout=60):
        deadline = time.monotonic() + timeout
        last = ""
        while time.monotonic() < deadline:
            try:
                status = self.feed_status()
                if status.get("state") == expected:
                    return status
                last = status.get("state", "missing state")
            except (OSError, RuntimeError) as error:
                # GET is safe to repeat while the restarted coordinator finishes
                # bootstrap. Never retry an ambiguous mutation POST here.
                last = self.sanitize(error)
            time.sleep(0.3)
        raise AssertionError("changefeed did not reach " + expected + ": " + last)

    def test_restart(self, db):
        self.local(
            f"CREATE TABLE {db}.restart_test(id INT PRIMARY KEY,value VARCHAR(64));INSERT INTO {db}.restart_test VALUES(1,'before'),(2,'keep')"
        )
        self.simple("restart_test", ["id", "value"])
        marker = int(
            self.local(
                f"BEGIN;SELECT COUNT(*) FROM {db}.restart_test;SELECT @@tidb_current_ts;COMMIT"
            ).splitlines()[-1]
        )
        assert marker > 0, (
            "source checkpoint marker must be a real transaction read TSO"
        )
        deadline = time.monotonic() + 60
        while self.check_feed().get("checkpoint_ts", 0) < marker:
            if time.monotonic() > deadline:
                raise AssertionError(
                    "checkpoint did not drain before controlled restart"
                )
            time.sleep(0.3)
        self.api("/api/v2/changefeeds/" + self.feed + "/pause", {}, "POST")
        self.wait_state("stopped")
        status = self.api("/api/v2/status")
        pid = status["pid"]
        binary = os.path.realpath(os.environ["E2E_CDC_BINARY"])
        command = subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "command="], text=True
        ).strip()
        argv = shlex.split(command)
        if len(argv) < 2 or os.path.realpath(argv[0]) != binary or argv[1] != "server":
            raise RuntimeError("refusing to stop an unowned CDC process")
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 45
        while True:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            if time.monotonic() > deadline:
                raise AssertionError("old local CDC process did not terminate")
            time.sleep(0.2)
        # This is a drained, exclusive restart, not ambiguous-query failover.
        self.local(
            f"UPDATE {db}.restart_test SET value='queued' WHERE id=1;INSERT INTO {db}.restart_test VALUES(3,'queued-insert');ALTER TABLE {db}.restart_test ADD COLUMN extra BIGINT NULL"
        )
        console = (self.root / "restart-console.log").open("w")
        # Keep the original owned capture's endpoint, including an ephemeral
        # port assigned by local.py; never bind another runner's fixed port.
        address = urllib.parse.urlparse(self.cdc).netloc
        process = subprocess.Popen(
            [
                binary,
                "server",
                "--newarch",
                "--pd",
                os.environ.get("E2E_PD", "http://127.0.0.1:12379"),
                "--addr",
                address,
                "--advertise-addr",
                address,
                "--data-dir",
                os.environ["E2E_CDC_DATA_DIR"],
                "--log-file",
                str(self.root / "restarted-cdc.log"),
                "--tz",
                "UTC",
            ],
            stdout=console,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        console.close()
        (self.root / "restart-pid").write_text(str(process.pid))
        deadline = time.monotonic() + 60
        while True:
            try:
                if self.api("/api/v2/status").get("is_owner"):
                    break
            except (OSError, RuntimeError):
                pass
            if time.monotonic() > deadline:
                raise AssertionError("new local CDC did not become ready")
            time.sleep(0.5)
        self.wait_state("stopped")
        self.api("/api/v2/changefeeds/" + self.feed + "/resume", {}, "POST")
        self.wait_state("normal")
        self.simple("restart_test", ["id", "value", "extra"])
        self.results["restart"] = {
            "old_pid": pid,
            "new_pid": process.pid,
            "before_checkpoint": marker,
            "after_checkpoint": self.feed_status().get("checkpoint_ts"),
        }
        self.save()

    def test_drop(self, db):
        self.local(
            f"CREATE TABLE {db}.drop_test(id INT PRIMARY KEY);INSERT INTO {db}.drop_test VALUES(1)"
        )
        self.simple("drop_test", ["id"])
        self.local(f"DROP TABLE {db}.drop_test")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            self.check_feed()
            rows = self.lake(
                "SELECT COUNT(*)::VARCHAR FROM system.tables WHERE database=? AND name IN (?,?)",
                [self.database, "drop_test", "drop_test_cdc_raw"],
            )
            if rows == [["0"]]:
                return
            time.sleep(0.5)
        raise AssertionError("DROP left target/raw tables")

    def cleanup(self):
        # Failures intentionally keep this isolated run for diagnosis. An operator
        # can remove just this feed and native_e2e_* database after inspecting.
        if self.feed_created:
            status = self.feed_status()
            if status.get("state") == "normal":
                self.api("/api/v2/changefeeds/" + self.feed + "/pause", {}, "POST")
                self.wait_state("stopped")
            self.api("/api/v2/changefeeds/" + self.feed, method="DELETE")
        if self.remote_created:
            assert self.database.startswith("native_e2e_")
            self.lake("DROP DATABASE IF EXISTS " + quote(self.database))
        self.local("DROP DATABASE IF EXISTS `%s`" % self.database)
        self.results["cleanup"] = "PASS"
        self.save()


if __name__ == "__main__":
    run = Run(existing="--cleanup" in sys.argv[1:])
    if "--cleanup" in sys.argv[1:]:
        run.cleanup()
        print("CLEANED", run.database, flush=True)
        sys.exit(0)
    try:
        run.setup()
        run.tests()
        run.cleanup()
        run.results["status"] = "PASS"
        run.save()
        print("RESULT", str(run.root / "result.json"), flush=True)
    except Exception as error:
        run.results["status"] = "FAIL"
        run.results["error"] = run.sanitize(error)
        run.save()
        print(
            "ERROR",
            run.sanitize(error),
            "RESULT",
            str(run.root / "result.json"),
            flush=True,
        )
        sys.exit(1)
