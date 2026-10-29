#!/usr/bin/env python3
"""Owned local TiUP + TiCDC + Docker Lake lifecycle for native sink E2E.

Unlike run.py's external-warehouse mode, this runner always creates its own
loopback-only Lake, credentials, source and capture. No cloud URI is read.
"""

import argparse
import hashlib
import json
import os
import pathlib
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[2]
# Fixed multi-architecture release, not a moving latest/nightly tag.
IMAGE = (
    "datafuselabs/databend:v1.2.925-patch-13@sha256:"
    "7f93f005aa0d626ebc7554d142b0a6b6ebb90e085b0d5bd0d97470084c42d41b"
)
SUITES = {
    "acceptance": ["run.py"],
    "ddl": ["extended.py"],
    "manual-key": ["options.py", "--manual-key"],
    "skipped-ddl": ["options.py"],
    **{
        f"reject-{name}": ["extended.py", "--reject", name]
        for name in ("partition", "no-pk", "narrow", "expression-default")
    },
    "fault": None,
}


def wait_ready(check, process, timeout, description):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(
                description + " exited before becoming ready; inspect logs"
            )
        try:
            if check():
                return
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
            pass
        time.sleep(0.5)
    raise RuntimeError(description + " readiness timeout; inspect logs")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def require_python():
    if sys.version_info < (3, 10):
        raise SystemExit("Lake E2E requires Python 3.10 or newer")


def check_source_port(port):
    # On BSD/macOS a specific-address SO_REUSEADDR bind can coexist with a
    # wildcard listener. Detect a live loopback/wildcard service before binding.
    with socket.socket() as probe:
        probe.settimeout(1)
        if probe.connect_ex(("127.0.0.1", port)) == 0:
            raise RuntimeError("source port %d is already in use" % port)
    with socket.socket() as sock:
        # TIME_WAIT is not a live listener; allow immediate reruns.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))


def file_sha256(path):
    # file_digest is only available in Python 3.11+.
    digest = hashlib.sha256()
    with path.open("rb") as binary:
        for chunk in iter(lambda: binary.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Local:
    def __init__(self, root, image, version):
        self.root = root
        self.image = image
        self.version = version
        self.processes = []
        self.container = "ticdc-lake-e2e-" + uuid.uuid4().hex[:12]
        self.container_created = False
        self.tag = "native-e2e-" + uuid.uuid4().hex[:12]
        self.env = os.environ.copy()
        # Never inherit a cloud target or another runner's capture/restart options.
        for key in (
            "LAKE_URI_FILE",
            "LAKE_NATIVE_TEST_URI",
            "E2E_CDC_BINARY",
            "E2E_CDC_DATA_DIR",
            "E2E_PD",
            "CDC_SERVER",
            "MYSQL_PORT",
        ):
            self.env.pop(key, None)
        self.summary = {"status": "RUNNING", "suites": []}
        self.binary = None
        self.offset = int(os.environ.get("E2E_PORT_OFFSET", "10000"))
        if not 0 <= self.offset <= 30000:
            raise RuntimeError("E2E_PORT_OFFSET must be in [0,30000]")

    def command(self, args, **kwargs):
        kwargs.setdefault("timeout", 900)
        return subprocess.run(args, check=True, env=self.env, **kwargs)

    def start(self, args, log):
        with (self.root / log).open("wb") as output:
            process = subprocess.Popen(
                args,
                env=self.env,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                cwd=REPO,
            )
        self.processes.append(process)
        return process

    def save(self):
        (self.root / "summary.json").write_text(
            json.dumps(self.summary, indent=2) + "\n"
        )

    def prepare(self):
        for program in ("docker", "tiup", "mysql", "go"):
            if not shutil.which(program):
                raise RuntimeError("missing local prerequisite: " + program)
        self.command(
            ["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        # Fixed source ports must not silently attach to an existing playground.
        for base in (4000, 10080, 2379, 2380, 20160, 20180):
            check_source_port(base + self.offset)
        self.summary["source_commit"] = self.command(
            ["git", "-C", str(REPO), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.summary["source_dirty"] = bool(
            self.command(
                ["git", "-C", str(REPO), "status", "--porcelain"],
                capture_output=True,
                text=True,
            ).stdout.strip()
        )
        supplied = os.environ.get("E2E_CDC_BINARY")
        if supplied:
            self.binary = pathlib.Path(supplied).resolve(strict=True)
        else:
            self.binary = self.root / "cdc"
            build = self.start(["make", "cdc"], "build.log")
            if build.wait(timeout=900):
                raise RuntimeError(
                    "current-branch TiCDC build failed; inspect build.log"
                )
            shutil.copy2(REPO / "bin/cdc", self.binary)
        self.summary["cdc_binary_supplied"] = bool(supplied)
        self.summary["cdc_version"] = self.command(
            [str(self.binary), "version"], capture_output=True, text=True
        ).stdout.strip()
        self.summary["cdc_sha256"] = file_sha256(self.binary)
        print("Starting owned local Lake background container", flush=True)
        with (self.root / "pull.log").open("w") as log:
            self.command(
                ["docker", "pull", self.image], stdout=log, stderr=subprocess.STDOUT
            )
        self.summary["lake_image"] = json.loads(
            self.command(
                ["docker", "image", "inspect", self.image],
                capture_output=True,
                text=True,
            ).stdout
        )[0]["Id"]
        self.summary["lake_image_tag"] = self.image
        # No host network, no cloud storage, no MinIO download, no exposed password.
        password = uuid.uuid4().hex
        self.env["QUERY_DEFAULT_PASSWORD"] = password
        self.command(
            [
                "docker",
                "create",
                "--name",
                self.container,
                "--label",
                "ticdc.e2e=" + self.tag,
                "--publish",
                "127.0.0.1::8000",
                "--env",
                "QUERY_STORAGE_TYPE=fs",
                "--env",
                "QUERY_DEFAULT_USER=ticdc_e2e",
                "--env",
                "QUERY_DEFAULT_PASSWORD",
                self.image,
            ],
            stdout=subprocess.DEVNULL,
        )
        self.container_created = True
        self.command(["docker", "start", self.container], stdout=subprocess.DEVNULL)
        address = self.command(
            ["docker", "port", self.container, "8000/tcp"],
            capture_output=True,
            text=True,
        ).stdout.strip()
        uri = f"lake+http://ticdc_e2e:{password}@{address}/default?exclusive=true"
        credentials = self.root / "local-uri"
        credentials.write_text(uri)
        credentials.chmod(0o600)
        self.env["LAKE_URI_FILE"] = str(credentials)
        self.env["LAKE_NATIVE_TEST_URI"] = uri
        self.env.pop("QUERY_DEFAULT_PASSWORD")
        self.env["MYSQL_PORT"] = str(4000 + self.offset)
        self.env["E2E_RUN_DIR"] = str(self.root / "readiness")
        # Reuse the exact HTTP/terminal-state adapter used by the assertions.
        from run import Run

        old_env = os.environ.copy()
        try:
            os.environ.update(self.env)
            probe = Run()
        finally:
            os.environ.clear()
            os.environ.update(old_env)
        wait_ready(lambda: probe.lake("SELECT 1") == [["1"]], None, 90, "Lake")
        self.summary["lake_version"] = probe.lake("SELECT version()")[0][0]
        if probe.lake("SELECT ?::BIGINT", [1]) != [["1"]]:
            raise RuntimeError("Lake must support Query API bound parameters")
        print("Starting owned local TiDB/PD/TiKV", flush=True)
        source = self.start(
            [
                "tiup",
                "playground",
                self.version,
                "--tag",
                self.tag,
                "--db",
                "1",
                "--pd",
                "1",
                "--kv",
                "1",
                "--tiflash",
                "0",
                "--ticdc",
                "0",
                "--without-monitor",
                "--host",
                "127.0.0.1",
                "--port-offset",
                str(self.offset),
            ],
            "tiup.log",
        )
        wait_ready(lambda: probe.local("SELECT 1") == "1", source, 180, "TiDB")
        # Owned source only: legacy index construction works on low-space laptops.
        probe.local(
            "SET GLOBAL tidb_ddl_enable_fast_reorg=OFF;SET GLOBAL tidb_enable_dist_task=OFF"
        )
        self.summary["source_version"] = probe.local("SELECT version()")
        self.summary["source_adjustments"] = [
            "tidb_ddl_enable_fast_reorg=OFF",
            "tidb_enable_dist_task=OFF",
        ]
        port = free_port()
        self.env.update(
            CDC_SERVER=f"http://127.0.0.1:{port}",
            E2E_CDC_BINARY=str(self.binary),
            E2E_CDC_DATA_DIR=str(self.root / "cdc-data"),
            E2E_PD=f"http://127.0.0.1:{2379 + self.offset}",
        )
        capture = self.start(
            [
                str(self.binary),
                "server",
                "--newarch",
                "--tz",
                "UTC",
                "--pd",
                self.env["E2E_PD"],
                "--addr",
                f"127.0.0.1:{port}",
                "--data-dir",
                self.env["E2E_CDC_DATA_DIR"],
                "--log-file",
                str(self.root / "cdc.log"),
            ],
            "cdc-console.log",
        )

        def capture_ready():
            with urllib.request.urlopen(
                self.env["CDC_SERVER"] + "/api/v2/status", timeout=5
            ) as response:
                if not json.load(response).get("is_owner", False):
                    return False
            # Owner election precedes coordinator bootstrap. Do not submit the
            # first create request while the read API still returns HTTP 500.
            with urllib.request.urlopen(
                self.env["CDC_SERVER"] + "/api/v2/changefeeds", timeout=5
            ) as response:
                return response.status == 200

        wait_ready(capture_ready, capture, 90, "TiCDC")
        self.save()

    def suites(self, names):
        for name in names:
            self.env["E2E_RUN_DIR"] = str(self.root / name)
            if name == "fault":
                args = [
                    "go",
                    "test",
                    "-p",
                    "2",
                    "-race",
                    "./downstreamadapter/sink/lake",
                    "-run",
                    "TestLakeNativeLive",
                    "-v",
                    "-count=1",
                    "-timeout=20m",
                ]
            else:
                args = [sys.executable, str(HERE / SUITES[name][0]), *SUITES[name][1:]]
            print("Running " + name, flush=True)
            started = time.monotonic()
            child = self.start(args, name + ".log")
            # Reap the old capture while acceptance's owned restart waits for exit.
            while child.poll() is None:
                for process in self.processes:
                    process.poll()
                time.sleep(0.2)
            record = {
                "name": name,
                "exit_code": child.returncode,
                "seconds": round(time.monotonic() - started, 2),
            }
            self.summary["suites"].append(record)
            self.save()
            if child.returncode:
                raise RuntimeError(
                    name + " failed; inspect " + str(self.root / (name + ".log"))
                )

    def cleanup(self):
        # A restarted capture is launched by run.py, not this process. Check both
        # its generated PID path and exact binary before signalling it.
        restarted = self.root / "acceptance/restart-pid"
        errors = []
        if restarted.exists():
            try:
                pid = int(restarted.read_text())
                command = subprocess.run(
                    ["ps", "-p", str(pid), "-o", "command="],
                    capture_output=True,
                    text=True,
                )
                argv = shlex.split(command.stdout.strip())
                if command.returncode == 0:
                    data_dir = (
                        argv[argv.index("--data-dir") + 1]
                        if "--data-dir" in argv
                        and argv.index("--data-dir") + 1 < len(argv)
                        else ""
                    )
                    if (
                        len(argv) < 2
                        or pathlib.Path(argv[0]).resolve() != self.binary.resolve()
                        or argv[1] != "server"
                        or pathlib.Path(data_dir).resolve()
                        != (self.root / "cdc-data").resolve()
                    ):
                        raise RuntimeError("refusing to stop unowned restarted capture")
                    os.kill(pid, signal.SIGTERM)
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        try:
                            os.kill(pid, 0)
                        except ProcessLookupError:
                            break
                        time.sleep(0.2)
                    else:
                        raise RuntimeError("restarted capture did not terminate")
            except Exception as error:
                errors.append(str(error))
        for process in reversed(self.processes):
            try:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=35)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=10)
            except ProcessLookupError:
                pass
            except Exception as error:
                errors.append("process cleanup: " + type(error).__name__)
        # TiUP may have already exited after a child startup failure. Its unique
        # tag confines cleanup to this run; never use broad pkill/tiup clean --all.
        if (self.root / "tiup.log").exists():
            try:
                result = subprocess.run(
                    ["tiup", "clean", self.tag],
                    env=self.env,
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                (self.root / "tiup-clean.log").write_text(result.stdout + result.stderr)
                if result.returncode:
                    errors.append("TiUP tag cleanup failed")
            except Exception as error:
                errors.append("TiUP cleanup: " + type(error).__name__)
        if self.container_created:
            try:
                with (self.root / "lake.log").open("wb") as log:
                    subprocess.run(
                        ["docker", "logs", self.container],
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        timeout=30,
                    )
            except Exception as error:
                errors.append("Lake logs: " + type(error).__name__)
            try:
                result = subprocess.run(
                    ["docker", "rm", "--force", "--volumes", self.container],
                    capture_output=True,
                    text=True,
                    timeout=45,
                )
                if result.returncode:
                    errors.append("Lake container cleanup failed")
            except Exception as error:
                errors.append("Lake cleanup: " + type(error).__name__)
        (self.root / "local-uri").unlink(missing_ok=True)
        self.summary["cleanup"] = "PASS" if not errors else errors
        if errors:
            self.summary["status"] = "FAIL"
        self.save()


def main():
    require_python()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--suite",
        choices=SUITES,
        action="append",
        help="repeat to select suites; default all",
    )
    args = parser.parse_args()
    os.umask(0o077)
    root = pathlib.Path(
        tempfile.mkdtemp(prefix="ticdc-lake-local-", dir=os.environ.get("OUT_DIR"))
    )
    print("Local E2E artifacts: " + str(root), flush=True)
    runner = Local(
        root,
        os.environ.get("LAKE_E2E_IMAGE", IMAGE),
        os.environ.get("TIDB_E2E_VERSION", "v8.5.1"),
    )

    def interrupted(signum, _frame):
        raise InterruptedError("local E2E interrupted by signal " + str(signum))

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        runner.prepare()
        runner.suites(args.suite or list(SUITES))
        runner.summary["status"] = "PASS"
    except Exception as error:
        # Do not include inherited env, query SQL or credentials in the summary.
        runner.summary["status"] = "FAIL"
        runner.summary["error"] = type(error).__name__ + ": " + str(error)
        print(runner.summary["error"], file=sys.stderr)
    finally:
        runner.cleanup()
    print("RESULT " + str(root / "summary.json"), flush=True)
    return 0 if runner.summary["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
