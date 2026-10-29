#!/usr/bin/env python3
"""Read-only Go overlay for real Lake parity probes of fixed bend-cdc b648162.

Uses the same private LAKE_URI_FILE/E2E_RUN_DIR as run.py; no baseline files are
changed. Requires the independent bend-cdc module at BEND_CDC_DIR.
"""

import json
import os
import pathlib
import subprocess
import urllib.parse


def main():
    credential = pathlib.Path(os.environ["LAKE_URI_FILE"])
    if credential.stat().st_mode & 0o077:
        raise RuntimeError("LAKE_URI_FILE must have private 0600 permissions")
    raw = credential.read_text().strip()
    uri = urllib.parse.urlparse(raw)
    root = pathlib.Path(os.environ["E2E_RUN_DIR"])
    root.mkdir(parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    log = root / "bend-live-parity.log"
    if log.exists():
        raise RuntimeError("refusing to overwrite previous baseline evidence")
    repo = pathlib.Path(os.environ["BEND_CDC_DIR"]).resolve()
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repo, text=True
    ).strip()
    if head != "b6481621021e82aee659ad88e8e6c00f6dd11906":
        raise RuntimeError(
            "baseline checkout must be fixed b648162, not HEAD of another version"
        )
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=repo, text=True
    )
    if dirty:
        raise RuntimeError("baseline must be clean; overlay does not modify checkout")
    fixture = pathlib.Path(__file__).with_name("baseline_parity_test.go.txt").resolve()
    overlay = root / "bend-overlay.json"
    overlay.write_text(
        json.dumps(
            {
                "Replace": {
                    str(repo / "pkg/source/tidb/zz_native_parity_overlay_test.go"): str(
                        fixture
                    )
                }
            }
        )
    )
    q = urllib.parse.parse_qs(uri.query)
    # This authorized endpoint exposes the Basic-auth Query API directly.
    q.update(login=["disable"], sslmode=["enable"])
    env = os.environ.copy()
    env["TIDB_DDL_INTEGRATION_DSN"] = uri._replace(
        scheme="databend", query=urllib.parse.urlencode(q, doseq=True)
    ).geturl()
    env.setdefault("GOMAXPROCS", "4")
    with log.open("w") as output:
        os.chmod(log, 0o600)
        result = subprocess.run(
            [
                "go",
                "test",
                "-p",
                "2",
                "-tags",
                "integration",
                "-overlay",
                str(overlay),
                "./pkg/source/tidb",
                "-run",
                "^(TestBendParityBaselineLive|TestDatabendSchemaIndexWarningAndTruncate)$",
                "-v",
                "-count=1",
                "-timeout=12m",
            ],
            cwd=repo,
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
        )
    # Driver errors are private, but scrub literal credentials from evidence too.
    text = log.read_text().replace(raw, "[REDACTED_URI]")
    for password in (uri.password, urllib.parse.unquote(uri.password or "")):
        if password:
            text = text.replace(password, "[REDACTED]")
    log.write_text(text)
    print("BASELINE", "PASS" if result.returncode == 0 else "FAIL", log)
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
