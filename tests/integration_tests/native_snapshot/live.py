#!/usr/bin/env python3
"""Exercise live incremental delivery after demo.py wait, then compare all rows."""
import argparse
import json
from pathlib import Path
import subprocess
import threading
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--name", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--keyspace", default="snap_demo_0921")
    parser.add_argument("--server", default="http://127.0.0.1:18300")
    parser.add_argument("--broker", default="10.2.15.7:9092")
    parser.add_argument("--sql-port", default="4400")
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--interval", type=float, default=0.1)
    args = parser.parse_args()
    if args.duration < 15 or args.interval <= 0:
        parser.error("duration must be at least 15 seconds and interval positive")
    root = args.root.resolve()
    journal = root / (args.name + "-live-writes.jsonl")
    if journal.exists():
        raise SystemExit("use a fresh live run name; write journal already exists")
    stop = threading.Event()
    failures = []
    commits = {}
    samples = []

    def sql(query):
        return subprocess.check_output([
            "mysql", "--batch", "--raw", "--skip-column-names", "-h127.0.0.1",
            "-P" + args.sql_port, "-uroot", "-e", query,
        ], text=True, timeout=30)

    def state():
        url = args.server + "/api/v2/changefeeds/" + args.name + "?keyspace=" + args.keyspace
        with urllib.request.urlopen(url, timeout=10) as response:
            return json.load(response)

    if not state().get("bootstrap_complete"):
        raise SystemExit("run demo.py wait before the live workload")
    if int(sql("SELECT COUNT(*) FROM snapshot_demo.accounts WHERE id>=10000")):
        raise SystemExit("live workload ID range is already occupied")

    def writer():
        deadline = time.monotonic() + args.duration
        try:
            with journal.open("x") as log:
                seq = 0
                while time.monotonic() < deadline and not stop.is_set():
                    seq += 1
                    # The CSE lab supports optimistic transactions; its older
                    # server rejects TiDB's pessimistic lock conflict option.
                    # One transaction changes an existing row, inserts a row,
                    # and removes the previous live row. Write the journal entry
                    # before starting the next transaction.
                    sql("USE snapshot_demo; BEGIN OPTIMISTIC; "
                        f"UPDATE accounts SET balance=balance+1,note='live-{seq}',revision={seq} WHERE id=1; "
                        f"INSERT INTO accounts VALUES({10000+seq},{seq},'live-insert-{seq}',{seq}); "
                        f"DELETE FROM accounts WHERE id={10000+seq-1}; COMMIT;")
                    committed = time.time()
                    commits[seq] = committed
                    log.write(json.dumps({"seq": seq, "committed_at": committed}) + "\n")
                    log.flush()
                    stop.wait(args.interval)
        except Exception as error:
            failures.append(str(error))
            stop.set()

    started = time.time()
    thread = threading.Thread(target=writer)
    thread.start()
    try:
        while thread.is_alive():
            # Capture Kafka's current end offset while SQL writes continue.
            # This samples actual delivered values, not just CDC checkpoints.
            time.sleep(5)
            if not thread.is_alive():
                break
            output = root / (args.name + f"-live-sample-{len(samples):02}.json")
            subprocess.run([
                str(root / "bin/snapshot-verify"), "-broker", args.broker,
                "-topic", args.topic, "-output", str(output),
            ], check=True, timeout=90, stdout=subprocess.DEVNULL)
            result = json.loads(output.read_text())
            note = result["final_rows"]["1"]["note"]
            seq = int(note.removeprefix("live-")) if note.startswith("live-") else 0
            observed = time.time()
            status = state()
            sample = {
                "observed_at": observed, "writer_active": thread.is_alive(),
                "committed_seq": max(commits, default=0), "kafka_seq": seq,
                "kafka_end_offset": result["kafka_end_offset"],
                "incremental_rows": result["incremental_rows"],
                "checkpoint_ts": status.get("checkpoint_ts"),
                "state": status.get("state"),
            }
            if seq in commits:
                # This includes polling/verification time; it is a freshness
                # observation, not a per-event latency measurement.
                sample["observed_row_age_seconds"] = observed - commits[seq]
            samples.append(sample)
            print(json.dumps(sample), flush=True)
    finally:
        stop.set()
        thread.join(timeout=35)
    if thread.is_alive():
        raise RuntimeError("writer failed to stop")
    if failures:
        raise RuntimeError("SQL workload failed: " + failures[0])
    final_seq = max(commits, default=0)
    target = int(sql("BEGIN; SELECT @@tidb_current_ts; COMMIT;").strip())
    (root / "target.json").write_text(json.dumps({"checkpoint_ts": target}))
    helper = Path(__file__).with_name("demo.py")
    common = ["--root", str(root), "--name", args.name, "--topic", args.topic,
              "--keyspace", args.keyspace, "--server", args.server,
              "--broker", args.broker, "--sql-port", args.sql_port]
    subprocess.run(["python3", str(helper), "wait", *common], check=True, timeout=330)
    subprocess.run(["python3", str(helper), "verify", *common], check=True, timeout=120)
    final = json.loads((root / (args.name + "-verification.json")).read_text())
    assert final["final_rows"]["1"]["note"] == f"live-{final_seq}"
    # All transactions have update+insert; all except the first delete a row.
    assert final["incremental_rows"] == 4 + 3 * final_seq - 1
    live_sequences = {s["kafka_seq"] for s in samples if s["writer_active"] and s["kafka_seq"] > 0}
    assert len(live_sequences) >= 2, "Kafka did not demonstrate progress while SQL was writing"
    summary = {
        "started_at": started, "finished_at": time.time(),
        "requested_write_duration_seconds": args.duration,
        "transactions": final_seq, "live_dml_events": 3 * final_seq - 1,
        "live_progress_samples": len(live_sequences), "samples": samples,
        "target_ts": target, "all_columns_equal": True,
        "final_sql_rows": len(final["final_rows"]),
    }
    (root / (args.name + "-live-summary.json")).write_text(json.dumps(summary, indent=2))
    print(json.dumps({k: v for k, v in summary.items() if k != "samples"}, indent=2))


if __name__ == "__main__":
    main()
