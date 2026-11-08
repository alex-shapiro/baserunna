#!/usr/bin/env python3
"""Runs a Jepsen-style list-append test against a local Baserunna cluster on MinIO.

The script:
- starts MinIO and N `baserunna` processes behind TCP fault proxies
- runs concurrent list-append transactions against random nodes
- injects `SIGKILL` and `SIGSTOP` faults on nodes and MinIO
- partitions nodes from MinIO and from peers by dropping or rejecting TCP connections
- heals the cluster, reads the final list of every key, and writes `history.edn`
- checks `history.edn` with `elle_check` and exits with status 1 on any violation or client error

    cargo build --release -p baserunna --bin baserunna --example elle_check
    scripts/jepsen/run.py --minutes 10 --out runs/1
    java -jar elle-cli.jar -m list-append -f edn -c strict-serializable runs/1/history.edn
"""

import argparse
import http.client
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_NAME = "jep"
CLUSTER_SECRET = "jepsen-cluster-secret"


class TcpProxy:
    """TCP proxy from 127.0.0.1:listen_port to 127.0.0.1:target_port with pass, drop, and reject modes."""

    def __init__(self, listen_port, target_port):
        self.listen_port = listen_port
        self.target_port = target_port
        self.mode = "pass"
        self.conns = []
        self.lock = threading.Lock()
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", listen_port))
        self.sock.listen(256)
        threading.Thread(target=self.accept_loop, daemon=True).start()

    def set_mode(self, mode):
        self.mode = mode
        if mode != "pass":
            with self.lock:
                for conn in self.conns:
                    self.close_socket(conn)
                self.conns = []

    @staticmethod
    def close_socket(sock):
        try:
            sock.close()
        except OSError:
            pass

    def accept_loop(self):
        while True:
            client_sock, _ = self.sock.accept()
            if self.mode == "reject":
                client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
                self.close_socket(client_sock)
                continue
            if self.mode == "drop":
                threading.Thread(target=self.discard_bytes, args=(client_sock,), daemon=True).start()
                continue
            try:
                upstream_sock = socket.create_connection(("127.0.0.1", self.target_port), timeout=5)
                upstream_sock.settimeout(None)
            except OSError:
                self.close_socket(client_sock)
                continue
            with self.lock:
                self.conns += [client_sock, upstream_sock]
            for src, dst in ((client_sock, upstream_sock), (upstream_sock, client_sock)):
                threading.Thread(target=self.forward_bytes, args=(src, dst), daemon=True).start()

    def discard_bytes(self, conn):
        try:
            while conn.recv(65536):
                pass
        except OSError:
            pass
        self.close_socket(conn)

    def forward_bytes(self, src, dst):
        """Copies bytes from src to dst and forwards the TCP half-close on EOF."""
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    dst.shutdown(socket.SHUT_WR)
                    return
                if self.mode != "pass":
                    break
                dst.sendall(data)
        except OSError:
            pass
        self.close_socket(src)
        self.close_socket(dst)


class Cluster:
    def __init__(self, args):
        self.args = args
        self.out_dir = args.out
        self.nodes = [None] * args.nodes
        self.minio = None
        self.store_proxies = [TcpProxy(17080 + i, 9000) for i in range(args.nodes)]
        self.peer_proxies = [TcpProxy(19080 + i, 18080 + i) for i in range(args.nodes)]
        self.lock = threading.Lock()
        self.stopped_nodes = set()
        self.log = open(os.path.join(self.out_dir, "nemesis.log"), "a")
        threading.Thread(target=self.supervise_nodes, daemon=True).start()

    def supervise_nodes(self):
        """Restarts any node that exits unexpectedly."""
        while True:
            time.sleep(1)
            with self.lock:
                for i, proc in enumerate(self.nodes):
                    if proc is not None and proc.poll() is not None and i not in self.stopped_nodes:
                        self.log_event(f"n{i} exited with {proc.returncode}; restarting")
                        self.start_node_locked(i)

    def log_event(self, msg):
        line = f"{time.time():.3f} {msg}"
        print(line, flush=True)
        self.log.write(line + "\n")
        self.log.flush()

    def start_minio(self):
        data_dir = os.path.join(self.out_dir, "minio")
        os.makedirs(data_dir, exist_ok=True)
        env = dict(os.environ, MINIO_ROOT_USER="minioadmin", MINIO_ROOT_PASSWORD="minioadmin")
        log = open(os.path.join(self.out_dir, "minio.log"), "a")
        self.minio = subprocess.Popen(
            [self.args.minio, "server", "--address", "127.0.0.1:9000", "--console-address", "127.0.0.1:9001", data_dir],
            env=env, stdout=log, stderr=log)
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", 9000), timeout=1).close()
                break
            except OSError:
                time.sleep(0.1)
        subprocess.run([self.args.mc, "alias", "set", "jep", "http://127.0.0.1:9000", "minioadmin", "minioadmin"],
                       check=True, capture_output=True)
        subprocess.run([self.args.mc, "mb", "--ignore-existing", "jep/jepsen"], check=True, capture_output=True)

    def start_node(self, i):
        with self.lock:
            self.start_node_locked(i)

    def start_node_locked(self, i):
        self.stopped_nodes.discard(i)
        env = dict(os.environ,
                   AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin", AWS_REGION="us-east-1",
                   AWS_ENDPOINT=f"http://127.0.0.1:{17080 + i}", AWS_ALLOW_HTTP="true",
                   BASERUNNA_STORE="s3://jepsen/c", BASERUNNA_LISTEN=f"127.0.0.1:{18080 + i}",
                   BASERUNNA_ADVERTISE=f"http://127.0.0.1:{19080 + i}", BASERUNNA_CLUSTER_SECRET=CLUSTER_SECRET,
                   BASERUNNA_SIGNUP="open", BASERUNNA_PBKDF2_ITERATIONS="1000", BASERUNNA_USER_RPS="0",
                   BASERUNNA_DISK_CACHE_DIR=os.path.join(self.out_dir, f"cache{i}"), BASERUNNA_LOG="warn",
                   BASERUNNA_LEASE_MS=str(self.args.lease_ms))
        log = open(os.path.join(self.out_dir, f"n{i}.log"), "a")
        self.nodes[i] = subprocess.Popen([self.args.binary], env=env, stdout=log, stderr=log)
        self.log_event(f"start n{i} pid {self.nodes[i].pid}")

    def kill_node(self, i):
        with self.lock:
            self.stopped_nodes.add(i)
        proc = self.nodes[i]
        if proc and proc.poll() is None:
            proc.send_signal(signal.SIGCONT)
            proc.kill()
            proc.wait()
            self.log_event(f"kill n{i}")

    def signal_node(self, i, sig):
        proc = self.nodes[i]
        if proc and proc.poll() is None:
            proc.send_signal(sig)

    def heal(self):
        for proxy in self.store_proxies + self.peer_proxies:
            proxy.set_mode("pass")
        if self.minio:
            self.minio.send_signal(signal.SIGCONT)
        for i in range(len(self.nodes)):
            self.signal_node(i, signal.SIGCONT)
            if self.nodes[i] is None or self.nodes[i].poll() is not None:
                self.start_node(i)
        self.log_event("heal")

    def stop(self):
        for i in range(len(self.nodes)):
            self.kill_node(i)
        if self.minio:
            self.minio.send_signal(signal.SIGCONT)
            self.minio.terminate()
            self.minio.wait()


def run_nemesis(cluster, stop_event, rng):
    node_count = len(cluster.nodes)
    while not stop_event.is_set():
        time.sleep(rng.uniform(2, 8))
        if stop_event.is_set():
            break
        node_idx = rng.randrange(node_count)
        fault = rng.choice(["kill", "kill", "pause", "pause", "store-drop", "store-reject", "peer-drop", "minio-pause",
                            "kill-all", "none"])
        duration = rng.choice([0.5, 2, 4, 7, 12, 20])
        cluster.log_event(f"nemesis {fault} n{node_idx} for {duration}s")
        if fault == "kill":
            cluster.kill_node(node_idx)
            time.sleep(min(duration, 5))
            cluster.start_node(node_idx)
        elif fault == "kill-all":
            for j in range(node_count):
                cluster.kill_node(j)
            time.sleep(min(duration, 3))
            for j in range(node_count):
                cluster.start_node(j)
        elif fault == "pause":
            cluster.signal_node(node_idx, signal.SIGSTOP)
            time.sleep(duration)
            cluster.signal_node(node_idx, signal.SIGCONT)
        elif fault in ("store-drop", "store-reject"):
            cluster.store_proxies[node_idx].set_mode(fault.split("-")[1])
            time.sleep(duration)
            cluster.store_proxies[node_idx].set_mode("pass")
        elif fault == "peer-drop":
            cluster.peer_proxies[node_idx].set_mode("drop")
            time.sleep(duration)
            cluster.peer_proxies[node_idx].set_mode("pass")
        elif fault == "minio-pause":
            cluster.minio.send_signal(signal.SIGSTOP)
            time.sleep(min(duration, 7))
            cluster.minio.send_signal(signal.SIGCONT)
        cluster.log_event(f"nemesis {fault} n{node_idx} done")


class UnknownOutcomeError(Exception):
    """Exception raised if a request may have been applied."""


class DefiniteFailureError(Exception):
    """Exception raised if a request was not applied."""


def send_http_request(port, method, path, body=None, token=None, timeout=12.0):
    """Sends an HTTP request and returns (status, json_body)."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    headers = {"content-type": "application/json"}
    if token:
        headers["authorization"] = f"Bearer {token}"
    payload = json.dumps(body).encode() if body is not None else None
    try:
        conn.connect()
    except OSError as e:
        raise DefiniteFailureError(f"connect: {e}")
    try:
        conn.request(method, path, body=payload, headers=headers)
        resp = conn.getresponse()
        raw = resp.read()
    except (OSError, http.client.HTTPException) as e:
        raise UnknownOutcomeError(f"{method} {path}: {e}")
    finally:
        conn.close()
    try:
        parsed = json.loads(raw) if raw else None
    except ValueError:
        parsed = None
    return resp.status, parsed


def raise_for_error_response(status, body, step):
    err = (body or {}).get("error", {}) if isinstance(body, dict) else {}
    if status == 503 and (err.get("details") or {}).get("outcome") == "unknown":
        raise UnknownOutcomeError(f"{step}: 503 unknown")
    if status in (404, 409, 412, 429, 503):
        raise DefiniteFailureError(f"{step}: {status} {err.get('code')}")
    raise RuntimeError(f"{step}: unexpected {status} {body}")


def is_stream_key(key):
    return key % 2 == 1


def build_read_op(key):
    if is_stream_key(key):
        return {"op": "read_stream", "stream": f"s{key}", "from_seq": 0, "limit": 1000}
    return {"op": "get", "key": f"k{key}"}


def build_append_op(key, value):
    return {"op": "append", "stream": f"s{key}", "records": [{"value": str(value)}]}


def parse_list(key, result):
    if is_stream_key(key):
        if result.get("done") is not True:
            raise RuntimeError(f"incomplete stream read {result}")
        return [int(x["value"]) for x in result["records"]]
    if not result.get("found"):
        return None
    return [int(x) for x in result["value"].split(",")]


class WorkloadState:
    def __init__(self, args):
        self.args = args
        self.lock = threading.Lock()
        self.events = []
        self.active_keys = list(range(args.active_keys))
        self.append_counts = {}
        self.next_key = args.active_keys
        self.next_value = 1
        self.next_process = args.clients
        self.created_streams = set()

    def record_event(self, kind, process, ops):
        with self.lock:
            self.events.append((kind, process, time.monotonic_ns(), [list(op) for op in ops]))

    def generate_txn(self, rng, interactive):
        ops = []
        with self.lock:
            for _ in range(1 + rng.randrange(self.args.max_mops)):
                slot = rng.randrange(len(self.active_keys))
                key = self.active_keys[slot]
                if rng.random() < 0.5 and (interactive or is_stream_key(key)):
                    ops.append(["append", key, self.next_value])
                    self.next_value += 1
                    self.append_counts[key] = self.append_counts.get(key, 0) + 1
                    if self.append_counts[key] >= self.args.max_appends:
                        self.active_keys[slot] = self.next_key
                        self.next_key += 1
                else:
                    ops.append(["r", key, None])
        appended_keys = set()
        filtered_ops = []
        for op in ops:
            if op[0] == "append":
                appended_keys.add(op[1])
            elif is_stream_key(op[1]) and op[1] in appended_keys:
                continue
            filtered_ops.append(op)
        return filtered_ops


def attach_read_results(ops, results):
    if len(results) != len(ops):
        raise RuntimeError(f"{len(results)} results for {len(ops)} ops")
    return [[op[0], op[1], parse_list(op[1], r)] if op[0] == "r" else op for op, r in zip(ops, results)]


def run_one_shot_txn(port, token, ops):
    api_ops = [build_read_op(op[1]) if op[0] == "r" else build_append_op(op[1], op[2]) for op in ops]
    status, body = send_http_request(port, "POST", f"/v1/dbs/{DB_NAME}/txn", {"ops": api_ops}, token)
    if status == 200:
        return attach_read_results(ops, body["results"])
    raise_for_error_response(status, body, "one-shot")


def run_interactive_txn(port, token, ops):
    base = f"/v1/dbs/{DB_NAME}/transactions"
    kv_keys = sorted({op[1] for op in ops if op[0] == "append" and not is_stream_key(op[1])})
    try:
        status, body = send_http_request(port, "POST", base, {"ops": [build_read_op(k) for k in kv_keys]}, token)
    except UnknownOutcomeError as e:
        raise DefiniteFailureError(f"begin: {e}")
    if status != 201:
        try:
            raise_for_error_response(status, body, "begin")
        except UnknownOutcomeError as e:
            raise DefiniteFailureError(str(e))
    txn_id = body["txn_id"]
    current_lists = {k: parse_list(k, r) or [] for k, r in zip(kv_keys, body["results"])}
    api_ops = []
    for op in ops:
        if op[0] == "r":
            api_ops.append(build_read_op(op[1]))
        elif is_stream_key(op[1]):
            api_ops.append(build_append_op(op[1], op[2]))
        else:
            current_lists[op[1]].append(op[2])
            api_ops.append({"op": "put", "key": f"k{op[1]}", "value": ",".join(map(str, current_lists[op[1]]))})
    try:
        status, body = send_http_request(port, "POST", f"{base}/{txn_id}/ops", {"ops": api_ops}, token)
    except UnknownOutcomeError as e:
        raise DefiniteFailureError(f"ops: {e}")
    if status != 200:
        try:
            raise_for_error_response(status, body, "ops")
        except UnknownOutcomeError as e:
            raise DefiniteFailureError(str(e))
    filled = attach_read_results(ops, body["results"])
    status, body = send_http_request(port, "POST", f"{base}/{txn_id}/commit", {}, token)
    if status == 200:
        return filled
    raise_for_error_response(status, body, "commit")


def ensure_streams_exist(state, ports, token, ops, rng):
    for key in sorted({op[1] for op in ops if is_stream_key(op[1])}):
        if key in state.created_streams:
            continue
        deadline = time.time() + 120
        while True:
            try:
                status, body = send_http_request(
                    rng.choice(ports), "POST", f"/v1/dbs/{DB_NAME}/streams", {"name": f"s{key}"}, token)
                if status == 201 or (status == 409 and body["error"]["code"] == "already_exists"):
                    break
            except (DefiniteFailureError, UnknownOutcomeError):
                pass
            if time.time() > deadline:
                raise RuntimeError(f"cannot create stream s{key}")
            time.sleep(0.2)
        with state.lock:
            state.created_streams.add(key)


def run_client(state, client_idx, ports, token, stop_event, rng, errors):
    process = client_idx
    while not stop_event.is_set():
        invoked = False
        try:
            interactive = rng.random() < state.args.interactive
            ops = state.generate_txn(rng, interactive)
            port = rng.choice(ports)
            ensure_streams_exist(state, ports, token, ops, rng)
            state.record_event("invoke", process, ops)
            invoked = True
            try:
                filled = (run_interactive_txn if interactive else run_one_shot_txn)(port, token, ops)
                state.record_event("ok", process, filled)
            except DefiniteFailureError:
                state.record_event("fail", process, ops)
            except UnknownOutcomeError:
                state.record_event("info", process, ops)
                with state.lock:
                    process, state.next_process = state.next_process, state.next_process + 1
        except Exception as e:  # noqa: BLE001
            errors.append(f"client {client_idx}: {e!r}")
            if invoked:
                state.record_event("info", process, ops)
                with state.lock:
                    process, state.next_process = state.next_process, state.next_process + 1
            time.sleep(1)
        time.sleep(rng.random() * state.args.max_pause)


def create_user_and_db(ports):
    deadline = time.time() + 120
    while True:
        try:
            body = {"email": "jepsen@jepsen.test", "password": "password-123"}
            status, resp = send_http_request(ports[0], "POST", "/v1/auth/signup", body)
            if status == 201:
                token = resp["token"]
                break
            status, resp = send_http_request(ports[0], "POST", "/v1/auth/login", body)
            if status == 200:
                token = resp["token"]
                break
        except (DefiniteFailureError, UnknownOutcomeError):
            pass
        if time.time() > deadline:
            raise RuntimeError("cannot sign up")
        time.sleep(0.5)
    while True:
        try:
            status, resp = send_http_request(ports[0], "POST", "/v1/dbs", {"name": DB_NAME}, token)
            if status == 201 or (status == 409 and resp["error"]["code"] == "already_exists"):
                return token
        except (DefiniteFailureError, UnknownOutcomeError):
            pass
        time.sleep(0.5)


def read_final_lists(state, ports, token):
    keys = [k for k in range(state.next_key) if not is_stream_key(k) or k in state.created_streams]
    final_lists = {}
    for i in range(0, len(keys), 64):
        ops = [["r", k, None] for k in keys[i:i + 64]]
        process = 1_000_000 + i
        for attempt in range(200):
            state.record_event("invoke", process, ops)
            try:
                filled = run_one_shot_txn(ports[attempt % len(ports)], token, ops)
                state.record_event("ok", process, filled)
                final_lists.update({op[1]: op[2] or [] for op in filled})
                break
            except (DefiniteFailureError, UnknownOutcomeError):
                state.record_event("fail", process, ops)
                time.sleep(1)
        else:
            raise RuntimeError("final reads failed after 200 attempts")
    return final_lists


def format_edn_history(events):
    lines = []
    for i, (kind, process, timestamp, ops) in enumerate(events):
        def format_op(op):
            if op[0] == "append":
                return f"[:append {op[1]} {op[2]}]"
            val = "nil" if op[2] is None else "[" + " ".join(map(str, op[2])) + "]"
            return f"[:r {op[1]} {val}]"
        lines.append(f"{{:index {i}, :type :{kind}, :process {process}, :time {timestamp}, :f :txn, "
                     f":value [{' '.join(format_op(op) for op in ops)}]}}")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=5)
    parser.add_argument("--out", required=True)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--active-keys", type=int, default=8)
    parser.add_argument("--max-appends", type=int, default=32)
    parser.add_argument("--max-mops", type=int, default=4)
    parser.add_argument("--interactive", type=float, default=0.5)
    parser.add_argument("--max-pause", type=float, default=0.05)
    parser.add_argument("--lease-ms", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=int(time.time()))
    parser.add_argument("--no-nemesis", action="store_true")
    parser.add_argument("--binary", default=os.path.join(ROOT_DIR, "target/release/baserunna"))
    parser.add_argument("--elle-check", default=os.path.join(ROOT_DIR, "target/release/examples/elle_check"))
    parser.add_argument("--minio", default=shutil.which("minio") or "minio")
    parser.add_argument("--mc", default=shutil.which("mc") or "mc")
    args = parser.parse_args()
    for tool in (args.binary, args.elle_check):
        if not os.access(tool, os.X_OK):
            sys.exit(f"{tool} is missing: cargo build --release -p baserunna --bin baserunna --example elle_check")
    os.makedirs(args.out, exist_ok=True)
    rng = random.Random(args.seed)
    cluster = Cluster(args)
    cluster.log_event(f"seed {args.seed}")
    cluster.start_minio()
    for i in range(args.nodes):
        cluster.start_node(i)
    ports = [18080 + i for i in range(args.nodes)]
    try:
        token = create_user_and_db(ports)
        state = WorkloadState(args)
        stop_event, errors, threads = threading.Event(), [], []
        for client_idx in range(args.clients):
            thread = threading.Thread(
                target=run_client, args=(state, client_idx, ports, token, stop_event, random.Random(rng.random()), errors))
            thread.start()
            threads.append(thread)
        nemesis_thread = None
        if not args.no_nemesis:
            nemesis_thread = threading.Thread(
                target=run_nemesis, args=(cluster, stop_event, random.Random(rng.random())))
            nemesis_thread.start()
        time.sleep(args.minutes * 60)
        stop_event.set()
        for thread in threads:
            thread.join()
        if nemesis_thread:
            nemesis_thread.join()
        cluster.heal()
        time.sleep(3 * args.lease_ms / 1000 + 5)
        final_lists = read_final_lists(state, ports, token)
        history_path = os.path.join(args.out, "history.edn")
        with open(history_path, "w") as history_file:
            history_file.write(format_edn_history(state.events))
        counts = {}
        for event in state.events:
            counts[event[0]] = counts.get(event[0], 0) + 1
        verdict = subprocess.run([args.elle_check, history_path], capture_output=True, text=True)
        problems = [] if verdict.returncode == 0 else [(verdict.stdout + verdict.stderr).strip()]
        problems += errors
        summary = {
            "counts": counts,
            "keys": len(final_lists),
            "client_errors": errors[:20],
            "problems": problems[:50],
        }
        with open(os.path.join(args.out, "summary.json"), "w") as summary_file:
            json.dump(summary, summary_file, indent=1)
        cluster.log_event(json.dumps({
            "counts": counts,
            "problems": len(problems),
            "verdict": verdict.stdout.strip()[-300:],
        }))
        return 1 if problems else 0
    finally:
        cluster.stop()


if __name__ == "__main__":
    sys.exit(main())
