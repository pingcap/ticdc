#!/usr/bin/env python3
"""Native snapshot demo operations. Run on the Linux host sharing the artifact FS."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "start", "stop", "create", "wait", "verify"])
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--keyspace", default="snap_demo_0921")
    parser.add_argument("--pd", default="http://127.0.0.1:2379")
    parser.add_argument("--sql-port", default="4400")
    parser.add_argument("--broker", default="10.2.15.7:9092")
    parser.add_argument("--server", default="http://127.0.0.1:18300")
    parser.add_argument("--provider", default="http://127.0.0.1:18400")
    parser.add_argument("--worker", default="http://127.0.0.1:39000")
    parser.add_argument("--name", default="native-snapshot-demo")
    parser.add_argument("--topic", default="native-snapshot-demo-20260921")
    parser.add_argument("--cse-binary", default="/data/nvme0n1/cdc-snapshotkv-runner-20260909/target/debug/cse-ctl")
    parser.add_argument("--cse-config", default="/data/nvme0n1/cdc-snapshotkv-runner-20260909/cse-pack-backup.toml")
    parser.add_argument("--cluster-id", default="snapshot-demo-20260921")
    parser.add_argument("--advertise-addr", default="10.2.15.7:18300")
    parser.add_argument("--reset-fixture", action="store_true", help="drop only snapshot_demo before seeding")
    args = parser.parse_args()
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    (root / "logs").mkdir(exist_ok=True)

    def sql(query):
        return subprocess.check_output(["mysql", "--batch", "--raw", "--skip-column-names", "-h127.0.0.1", "-P"+args.sql_port, "-uroot", "-e", query], text=True)

    def http(url, data=None):
        request = urllib.request.Request(url, data=None if data is None else json.dumps(data).encode(), headers={"Content-Type": "application/json"})
        return json.load(urllib.request.urlopen(request, timeout=120))

    def write(name, value):
        (root / name).write_text(json.dumps(value, indent=2))

    def cse_config():
        text = Path(args.cse_config).read_text()
        # The lab uses simple top-level string values. Secrets stay in memory.
        def get(name):
            return re.search(r"^"+name+r'\s*=\s*"([^"]*)"', text, re.M).group(1)
        return get

    if args.action == "prepare":
        if (root / "manifest.json").exists():
            raise SystemExit("use a fresh run directory; existing manifest is immutable")
        if len(args.keyspace) > 20:
            raise SystemExit("TiCDC keyspace names must be at most 20 characters")
        if args.reset_fixture:
            sql("DROP DATABASE IF EXISTS snapshot_demo")
        sql('CREATE DATABASE snapshot_demo; CREATE TABLE snapshot_demo.accounts(id BIGINT PRIMARY KEY,balance BIGINT NOT NULL,note VARCHAR(100)); CREATE TABLE snapshot_demo.empty_table(id INT PRIMARY KEY); SET GLOBAL tidb_gc_life_time="24h";')
        for start in range(1, 3001, 500):
            values = ",".join("(%d,%d,'snapshot-%d')" % (i, i*10, i) for i in range(start, start+500))
            sql("INSERT INTO snapshot_demo.accounts VALUES " + values)
        backup = http(args.worker+"/api/v1/x/cluster/lightweight_backup", {})
        write("backup.json", backup)
        with (root / "logs/pack.log").open("w") as log:
            subprocess.run([args.cse_binary, "pack-backup", "--config", args.cse_config, "--backup-name", backup["backup_name"], "--keyspace-name", args.keyspace, "--data-dir", str(root/"pack-work")], stdout=log, stderr=subprocess.STDOUT, check=True)
        info = subprocess.check_output([args.cse_binary, "show", "backup", "--config", args.cse_config, "--name", backup["backup_name"]], stderr=subprocess.STDOUT, text=True)
        (root / "logs/backup-info.log").write_text(info)
        ts = int(re.search(r"backup_ts:\s*(\d+)", info).group(1))
        path = re.search(r"Copyable path: (\S+)", (root/"logs/pack.log").read_text()).group(1)
        bucket, key = path.split("/", 1)
        import boto3  # Only the backup preparation helper needs this dependency.
        config = cse_config()
        client = boto3.client("s3", endpoint_url=config("s3-endpoint"), aws_access_key_id=config("s3-key-id"), aws_secret_access_key=config("s3-secret-key"))
        data = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        keyspace = http(args.pd+"/pd/api/v2/keyspaces/"+args.keyspace)
        manifest = {"snapshot_ts": ts, "keyspace_id": keyspace["id"], "backup_uri": "s3://"+path+"?endpoint="+config("s3-endpoint")+"&region=local", "backup_digest": hashlib.sha256(data).hexdigest()}
        write("manifest.json", manifest)
        sql("USE snapshot_demo; UPDATE accounts SET balance=99999,note='updated-after-B' WHERE id=1; DELETE FROM accounts WHERE id=2; INSERT INTO accounts VALUES(3001,30010,'inserted-after-B'); ALTER TABLE accounts ADD COLUMN revision INT DEFAULT 0; INSERT INTO accounts VALUES(3002,30020,'after-DDL',1);")
        # An SQL snapshot TSO taken after all fixture commits bounds the wait.
        target = int(sql("BEGIN; SELECT @@tidb_current_ts; COMMIT;").strip())
        write("target.json", {"checkpoint_ts": target})
        config_text = '[filter]\nrules=["snapshot_demo.*"]\n[snapshot]\n'
        for k, v in {"backup-uri": manifest["backup_uri"], "backup-digest": manifest["backup_digest"], "provider-url": args.provider, "artifact-dir": str(root/"artifacts")}.items():
            config_text += k+"="+json.dumps(v)+"\n"
        (root / "changefeed.toml").write_text(config_text)
        print(json.dumps({"snapshot_ts": ts, "keyspace_id": keyspace["id"], "target_ts": target}))
    elif args.action == "start":
        (root / "cdc.toml").write_text("newarch = true\nenable-legacy-safepoint = true\n")
        env = os.environ.copy()
        config = cse_config()
        env.update(DFS_S3_KEY_ID=config("s3-key-id"), DFS_S3_SECRET_KEY=config("s3-secret-key"))
        from urllib.parse import urlsplit
        commands = {
            "provider": [str(root/"bin/snapshot-provider"), "-root", str(root/"artifacts"), "-cse-binary", args.cse_binary, "-listen", urlsplit(args.provider).netloc],
            "cdc": [str(root/"bin/cdc"), "server", "--pd="+args.pd, "--addr=0.0.0.0:"+str(urlsplit(args.server).port), "--advertise-addr="+args.advertise_addr, "--cluster-id="+args.cluster_id, "--data-dir="+str(root/"cdc-data"), "--log-file="+str(root/"logs/cdc.log"), "--config="+str(root/"cdc.toml")],
        }
        for name, command in commands.items():
            pidfile = root/(name+".pid")
            if pidfile.exists() and Path("/proc/"+pidfile.read_text()+"/cmdline").exists():
                raise SystemExit(name+" PID still exists; stop before starting")
            with (root/("logs/"+name+"-stdout.log")).open("a") as log:
                proc = subprocess.Popen(command, env=env if name=="provider" else None, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            pidfile.write_text(str(proc.pid))
            print(name, proc.pid)
        for _ in range(300):
            try:
                if http(args.server+"/api/v2/status").get("is_owner"):
                    print("capture API and coordinator ready")
                    break
            except (OSError, ValueError):
                pass
            time.sleep(.2)
        else:
            raise SystemExit("capture did not become ready; inspect logs/cdc.log")
    elif args.action == "stop":
        for name, binary in [("cdc", "cdc"), ("provider", "snapshot-provider")]:
            pidfile = root/(name+".pid")
            if not pidfile.exists():
                continue
            pid = int(pidfile.read_text())
            proc = Path("/proc/%d/cmdline" % pid)
            if proc.exists():
                if proc.read_bytes().split(b"\0")[0] != str(root/"bin"/binary).encode():
                    raise SystemExit("refusing to stop a reused PID")
                os.kill(pid, signal.SIGTERM)
                for _ in range(300):
                    if not proc.exists():
                        break
                    time.sleep(.1)
                else:
                    raise SystemExit(name+" has not drained")
            pidfile.unlink()
    elif args.action == "create":
        manifest = json.loads((root/"manifest.json").read_text())
        subprocess.run([str(root/"bin/cdc"), "cli", "changefeed", "create", "--server="+args.server, "--keyspace="+args.keyspace, "--changefeed-id="+args.name, "--sink-uri=kafka://"+args.broker+"/"+args.topic+"?protocol=open-protocol&partition-num=1&replication-factor=1&required-acks=-1", "--start-ts="+str(manifest["snapshot_ts"]), "--config="+str(root/"changefeed.toml"), "--no-confirm"], check=True)
    elif args.action == "wait":
        target = json.loads((root/"target.json").read_text())["checkpoint_ts"]
        state = {}
        for _ in range(300):
            try:
                state = http(args.server+"/api/v2/changefeeds/"+args.name+"?keyspace="+args.keyspace)
            except urllib.error.HTTPError as error:
                # The capture can own its election before keyspace controllers
                # are ready after restart. Retry transient server responses.
                if error.code not in (500, 502, 503, 504):
                    raise
                state = {"error": error.read(4096).decode(errors="replace")}
                time.sleep(1)
                continue
            except (urllib.error.URLError, TimeoutError) as error:
                state = {"error": str(error)}
                time.sleep(1)
                continue
            if state.get("bootstrap_complete") and state.get("checkpoint_ts", 0) >= target:
                write(args.name+"-status.json", state)
                print("snapshot committed and incremental checkpoint reached", target)
                break
            time.sleep(1)
        else:
            raise SystemExit("changefeed did not catch up: "+str(state.get("error")))
    elif args.action == "verify":
        output = root/(args.name+"-verification.json")
        subprocess.run([str(root/"bin/snapshot-verify"), "-broker", args.broker, "-topic", args.topic, "-output", str(output)], check=True)
        subprocess.run(["python3", str(Path(__file__).with_name("compare.py")), str(output), "--port", args.sql_port], check=True)


if __name__ == "__main__":
    main()
