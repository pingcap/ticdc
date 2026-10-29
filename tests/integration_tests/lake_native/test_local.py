#!/usr/bin/env python3
"""Service-free checks for the local E2E lifecycle and target selection."""

import json
import hashlib
import os
import pathlib
import shlex
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from local import Local, check_source_port, file_sha256, require_python, wait_ready
from run import Run


class LocalRunnerTest(unittest.TestCase):
    def test_source_ports_reject_live_loopback_and_wildcard_listeners(self):
        for address in ("127.0.0.1", "0.0.0.0"):
            with self.subTest(address=address), socket.socket() as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind((address, 0))
                listener.listen()
                port = listener.getsockname()[1]
                with self.assertRaisesRegex(RuntimeError, "already in use"):
                    check_source_port(port)
            # A previous connection's TIME_WAIT does not forbid a new run.
            check_source_port(port)

    def test_python_requirement_and_chunked_digest(self):
        with patch("local.sys.version_info", (3, 9)):
            with self.assertRaisesRegex(SystemExit, "Python 3.10"):
                require_python()
        with patch("local.sys.version_info", (3, 10)):
            require_python()
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "binary"
            data = b"lake-test" * (1 << 19)
            path.write_bytes(data)
            self.assertEqual(file_sha256(path), hashlib.sha256(data).hexdigest())

    def test_owned_background_process_is_stopped(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = Local(pathlib.Path(directory), "test-image", "v8.5.1")
            child = runner.start(
                [sys.executable, "-c", "import time; time.sleep(60)"], "owned.log"
            )
            try:
                runner.cleanup()
                self.assertIsNotNone(child.poll())
                self.assertEqual(runner.summary["cleanup"], "PASS")
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()

    def test_never_inherits_cloud_or_other_capture(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.dict(
                os.environ,
                {
                    "LAKE_URI_FILE": "/not/read/cloud-secret",
                    "LAKE_NATIVE_TEST_URI": "must-not-use",
                    "CDC_SERVER": "https://not-local.invalid",
                    "E2E_CDC_BINARY": "/unowned/cdc",
                    "MYSQL_PORT": "1234",
                },
            ),
        ):
            runner = Local(pathlib.Path(directory), "test-image", "v8.5.1")
            for name in (
                "LAKE_URI_FILE",
                "LAKE_NATIVE_TEST_URI",
                "CDC_SERVER",
                "E2E_CDC_BINARY",
                "MYSQL_PORT",
            ):
                self.assertNotIn(name, runner.env)
            runner.summary["status"] = "PASS"
            runner.cleanup()
            self.assertEqual(
                json.loads((runner.root / "summary.json").read_text())["cleanup"],
                "PASS",
            )

    def test_explicit_http_scheme_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            credential = root / "uri"
            credential.write_text("lake+http://user:password@127.0.0.1:18000/default")
            credential.chmod(0o600)
            with patch.dict(
                os.environ,
                {"LAKE_URI_FILE": str(credential), "E2E_RUN_DIR": str(root / "http")},
            ):
                self.assertEqual(Run().base, "http://127.0.0.1:18000")
            credential.write_text("lake://user:password@lake.example/default")
            with patch.dict(
                os.environ,
                {"LAKE_URI_FILE": str(credential), "E2E_RUN_DIR": str(root / "https")},
            ):
                self.assertEqual(Run().base, "https://lake.example")

    def test_readiness_retries_service_errors_but_checks_exit(self):
        attempts = iter([RuntimeError("not ready"), True])

        def probe():
            value = next(attempts)
            if isinstance(value, Exception):
                raise value
            return value

        with patch("local.time.sleep"):
            wait_ready(probe, None, 1, "test-service")
        process = subprocess.Popen(["true"])
        process.wait()
        with self.assertRaisesRegex(RuntimeError, "exited before"):
            wait_ready(lambda: True, process, 1, "test-service")

    def test_refuses_unowned_restart_pid_and_still_removes_credential(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "acceptance").mkdir()
            (root / "acceptance/restart-pid").write_text(str(os.getpid()))
            (root / "local-uri").write_text("secret")
            runner = Local(root, "test-image", "v8.5.1")
            runner.binary = root / "unowned-cdc"
            with patch("local.os.kill") as kill:
                runner.cleanup()
                kill.assert_not_called()
            self.assertEqual(runner.summary["status"], "FAIL")
            self.assertIn("unowned restarted capture", runner.summary["cleanup"][0])
            self.assertFalse((root / "local-uri").exists())

    def test_restart_ownership_compares_canonical_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "acceptance").mkdir()
            (root / "acceptance/restart-pid").write_text("12345")
            (root / "sub").mkdir()
            runner = Local(root, "test-image", "v8.5.1")
            runner.binary = root / "sub/../cdc"
            command = shlex.join(
                [
                    str(runner.binary.resolve()),
                    "server",
                    "--data-dir",
                    str(root / "cdc-data"),
                ]
            )
            with (
                patch(
                    "local.subprocess.run",
                    return_value=subprocess.CompletedProcess([], 0, stdout=command),
                ),
                patch("local.os.kill", side_effect=[None, ProcessLookupError]) as kill,
            ):
                runner.cleanup()
                self.assertEqual(kill.call_args_list[0].args[0], 12345)
            self.assertEqual(runner.summary["cleanup"], "PASS")

    def test_log_failure_does_not_skip_container_removal(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = Local(pathlib.Path(directory), "test-image", "v8.5.1")
            runner.container_created = True

            def execute(args, **kwargs):
                if args[:2] == ["docker", "logs"]:
                    raise subprocess.TimeoutExpired(args, 30)
                return subprocess.CompletedProcess(args, 0)

            with patch("local.subprocess.run", side_effect=execute) as run:
                runner.cleanup()
                self.assertEqual(run.call_args_list[-1].args[0][:2], ["docker", "rm"])
            self.assertEqual(runner.summary["status"], "FAIL")
            self.assertIn("Lake logs", runner.summary["cleanup"][0])


if __name__ == "__main__":
    unittest.main()
