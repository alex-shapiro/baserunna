#!/usr/bin/env python3
"""Jepsen-style list-append test of a real Baserunna cluster on MinIO.

Starts MinIO and N baserunna processes, runs concurrent list-append
transactions against random nodes while a nemesis kills, pauses, and
partitions nodes and the object store, then heals the cluster, reads every
key, and writes the history in EDN for Elle.

Each node reaches MinIO through its own TCP proxy, and peers forward to a node
through another proxy, so the nemesis can cut a node off from storage or from
its peers. A proxy in "drop" mode accepts connections and discards their bytes,
so requests time out instead of failing fast.

    scripts/jepsen/run.py --minutes 10 --out runs/1
    cargo run --release --example elle_check -- runs/1/history.edn
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

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB = "jep"
SECRET = "jepsen-cluster-secret"


class Proxy:
    """TCP proxy from 127.0.0.1:listen to 127.0.0.1:target with pass, drop, and reject modes."""

    def __init__(self, listen, target):
        self.listen, self.target, self.mode = listen, target, "pass"
        self.conns, self.lock = [], threading.Lock()
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", listen))
        self.sock.listen(256)
        threading.Thread(target=self.accept, daemon=True).start()

    def set(self, mode):
        self.mode = mode
        if mode != "pass":
            with self.lock:
                for c in self.conns:
                    self.close(c)
                self.conns = []

    @staticmethod
    def close(s):
        try:
            s.close()
        except OSError:
            pass

    def accept(self):
        while True:
            c, _ = self.sock.accept()
            if self.mode == "reject":
                c.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
                self.close(c)
                continue
            if self.mode == "drop":
                threading.Thread(target=self.sink, args=(c,), daemon=True).start()
                continue
            try:
                u = socket.create_connection(("127.0.0.1", self.target), timeout=5)
                u.settimeout(None)
            except OSError:
                self.close(c)
                continue
            with self.lock:
                self.conns += [c, u]
            for a, b in ((c, u), (u, c)):
                threading.Thread(target=self.pipe, args=(a, b), daemon=True).start()

    def sink(self, c):
        try:
            while c.recv(65536):
                pass
        except OSError:
            pass
        self.close(c)

    def pipe(self, a, b):
        """Copies bytes from a to b, then passes the half-close on as a TCP stack would."""
        try:
            while True:
                d = a.recv(65536)
                if not d:
                    b.shutdown(socket.SHUT_WR)
                    return
                if self.mode != "pass":
                    break
                b.sendall(d)
        except OSError:
            pass
        self.close(a)
        self.close(b)


class Cluster:
    def __init__(self, args):
        self.args, self.out = args, args.out
        self.nodes = [None] * args.nodes
        self.minio = None
        self.store_proxies = [Proxy(17080 + i, 9000) for i in range(args.nodes)]
        self.peer_proxies = [Proxy(19080 + i, 18080 + i) for i in range(args.nodes)]
        self.lock = threading.Lock()
        self.down = set()
        self.log = open(os.path.join(self.out, "nemesis.log"), "a")
        threading.Thread(target=self.supervise, daemon=True).start()

    def supervise(self):
        """Restarts nodes that exit on their own, as a process supervisor would."""
        while True:
            time.sleep(1)
            with self.lock:
                for i, p in enumerate(self.nodes):
                    if p is not None and p.poll() is not None and i not in self.down:
                        self.note(f"n{i} exited with {p.returncode}; restarting")
                        self.start_locked(i)

    def note(self, msg):
        line = f"{time.time():.3f} {msg}"
        print(line, flush=True)
        self.log.write(line + "\n")
        self.log.flush()

    def start_minio(self):
        data = os.path.join(self.out, "minio")
        os.makedirs(data, exist_ok=True)
        env = dict(os.environ, MINIO_ROOT_USER="minioadmin", MINIO_ROOT_PASSWORD="minioadmin")
        log = open(os.path.join(self.out, "minio.log"), "a")
        self.minio = subprocess.Popen(
            [self.args.minio, "server", "--address", "127.0.0.1:9000", "--console-address", "127.0.0.1:9001", data],
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

    def start(self, i):
        with self.lock:
            self.start_locked(i)

    def start_locked(self, i):
        self.down.discard(i)
        env = dict(os.environ,
                   AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin", AWS_REGION="us-east-1",
                   AWS_ENDPOINT=f"http://127.0.0.1:{17080 + i}", AWS_ALLOW_HTTP="true",
                   BASERUNNA_STORE="s3://jepsen/c", BASERUNNA_LISTEN=f"127.0.0.1:{18080 + i}",
                   BASERUNNA_ADVERTISE=f"http://127.0.0.1:{19080 + i}", BASERUNNA_CLUSTER_SECRET=SECRET,
                   BASERUNNA_SIGNUP="open", BASERUNNA_PBKDF2_ITERATIONS="1000", BASERUNNA_USER_RPS="0",
                   BASERUNNA_DISK_CACHE_DIR=os.path.join(self.out, f"cache{i}"), BASERUNNA_LOG="warn",
                   BASERUNNA_LEASE_MS=str(self.args.lease_ms))
        log = open(os.path.join(self.out, f"n{i}.log"), "a")
        self.nodes[i] = subprocess.Popen([self.args.binary], env=env, stdout=log, stderr=log)
        self.note(f"start n{i} pid {self.nodes[i].pid}")

    def kill(self, i):
        with self.lock:
            self.down.add(i)
        p = self.nodes[i]
        if p and p.poll() is None:
            p.send_signal(signal.SIGCONT)
            p.kill()
            p.wait()
            self.note(f"kill n{i}")

    def signal(self, i, sig):
        p = self.nodes[i]
        if p and p.poll() is None:
            p.send_signal(sig)

    def heal(self):
        for p in self.store_proxies + self.peer_proxies:
            p.set("pass")
        if self.minio:
            self.minio.send_signal(signal.SIGCONT)
        for i in range(len(self.nodes)):
            self.signal(i, signal.SIGCONT)
            if self.nodes[i] is None or self.nodes[i].poll() is not None:
                self.start(i)
        self.note("heal")

    def stop(self):
        for i in range(len(self.nodes)):
            self.kill(i)
        if self.minio:
            self.minio.send_signal(signal.SIGCONT)
            self.minio.terminate()
            self.minio.wait()


def nemesis(cluster, stop, rng):
    n = len(cluster.nodes)
    while not stop.is_set():
        time.sleep(rng.uniform(2, 8))
        if stop.is_set():
            break
        i = rng.randrange(n)
        f = rng.choice(["kill", "kill", "pause", "pause", "store-drop", "store-reject", "peer-drop", "minio-pause",
                        "kill-all", "none"])
        d = rng.choice([0.5, 2, 4, 7, 12, 20])
        cluster.note(f"nemesis {f} n{i} for {d}s")
        if f == "kill":
            cluster.kill(i)
            time.sleep(min(d, 5))
            cluster.start(i)
        elif f == "kill-all":
            for j in range(n):
                cluster.kill(j)
            time.sleep(min(d, 3))
            for j in range(n):
                cluster.start(j)
        elif f == "pause":
            cluster.signal(i, signal.SIGSTOP)
            time.sleep(d)
            cluster.signal(i, signal.SIGCONT)
        elif f in ("store-drop", "store-reject"):
            cluster.store_proxies[i].set(f.split("-")[1])
            time.sleep(d)
            cluster.store_proxies[i].set("pass")
        elif f == "peer-drop":
            cluster.peer_proxies[i].set("drop")
            time.sleep(d)
            cluster.peer_proxies[i].set("pass")
        elif f == "minio-pause":
            cluster.minio.send_signal(signal.SIGSTOP)
            time.sleep(min(d, 7))
            cluster.minio.send_signal(signal.SIGCONT)
        cluster.note(f"nemesis {f} n{i} done")


class Unknown(Exception):
    """The request may have been applied."""


class Failed(Exception):
    """The request was certainly not applied."""


def call(port, method, path, body=None, token=None, timeout=12.0):
    """Returns (status, json). Raises Unknown on timeouts and broken connections after sending."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    headers = {"content-type": "application/json"}
    if token:
        headers["authorization"] = f"Bearer {token}"
    data = json.dumps(body).encode() if body is not None else None
    try:
        conn.connect()
    except OSError as e:
        raise Failed(f"connect: {e}")
    try:
        conn.request(method, path, body=data, headers=headers)
        r = conn.getresponse()
        raw = r.read()
    except (OSError, http.client.HTTPException) as e:
        raise Unknown(f"{method} {path}: {e}")
    finally:
        conn.close()
    try:
        j = json.loads(raw) if raw else None
    except ValueError:
        j = None
    return r.status, j


def classify(status, j, what):
    err = (j or {}).get("error", {}) if isinstance(j, dict) else {}
    if status == 503 and (err.get("details") or {}).get("outcome") == "unknown":
        raise Unknown(f"{what}: 503 unknown")
    if status in (404, 409, 412, 429, 503):
        raise Failed(f"{what}: {status} {err.get('code')}")
    raise RuntimeError(f"{what}: unexpected {status} {j}")


def is_stream(k):
    return k % 2 == 1


def read_op(k):
    if is_stream(k):
        return {"op": "read_stream", "stream": f"s{k}", "from_seq": 0, "limit": 1000}
    return {"op": "get", "key": f"k{k}"}


def append_op(k, v):
    return {"op": "append", "stream": f"s{k}", "records": [{"value": str(v)}]}


def parse_list(k, r):
    if is_stream(k):
        if r.get("done") is not True:
            raise RuntimeError(f"incomplete stream read {r}")
        return [int(x["value"]) for x in r["records"]]
    if not r.get("found"):
        return None
    return [int(x) for x in r["value"].split(",")]


class World:
    def __init__(self, args, rng):
        self.args, self.lock, self.events = args, threading.Lock(), []
        self.active = list(range(args.active_keys))
        self.appends, self.next_key, self.next_value = {}, args.active_keys, 1
        self.next_process, self.created = args.clients, set()

    def record(self, kind, process, mops):
        with self.lock:
            self.events.append((kind, process, time.monotonic_ns(), [list(m) for m in mops]))

    def generate(self, rng, interactive):
        mops = []
        with self.lock:
            for _ in range(1 + rng.randrange(self.args.max_mops)):
                slot = rng.randrange(len(self.active))
                k = self.active[slot]
                if rng.random() < 0.5 and (interactive or is_stream(k)):
                    mops.append(["append", k, self.next_value])
                    self.next_value += 1
                    self.appends[k] = self.appends.get(k, 0) + 1
                    if self.appends[k] >= self.args.max_appends:
                        self.active[slot] = self.next_key
                        self.next_key += 1
                else:
                    mops.append(["r", k, None])
        appended, out = set(), []
        for m in mops:
            if m[0] == "append":
                appended.add(m[1])
            elif is_stream(m[1]) and m[1] in appended:
                continue
            out.append(m)
        return out


def fill(mops, results):
    if len(results) != len(mops):
        raise RuntimeError(f"{len(results)} results for {len(mops)} ops")
    return [[m[0], m[1], parse_list(m[1], r)] if m[0] == "r" else m for m, r in zip(mops, results)]


def one_shot(port, token, mops):
    ops = [read_op(m[1]) if m[0] == "r" else append_op(m[1], m[2]) for m in mops]
    s, j = call(port, "POST", f"/v1/dbs/{DB}/txn", {"ops": ops}, token)
    if s == 200:
        return fill(mops, j["results"])
    classify(s, j, "one-shot")


def interactive(port, token, mops):
    base = f"/v1/dbs/{DB}/transactions"
    kv = sorted({m[1] for m in mops if m[0] == "append" and not is_stream(m[1])})
    try:
        s, j = call(port, "POST", base, {"ops": [read_op(k) for k in kv]}, token)
    except Unknown as e:
        raise Failed(f"begin: {e}")
    if s != 201:
        try:
            classify(s, j, "begin")
        except Unknown as e:
            raise Failed(str(e))
    tid = j["txn_id"]
    lists = {k: parse_list(k, r) or [] for k, r in zip(kv, j["results"])}
    ops = []
    for m in mops:
        if m[0] == "r":
            ops.append(read_op(m[1]))
        elif is_stream(m[1]):
            ops.append(append_op(m[1], m[2]))
        else:
            lists[m[1]].append(m[2])
            ops.append({"op": "put", "key": f"k{m[1]}", "value": ",".join(map(str, lists[m[1]]))})
    try:
        s, j = call(port, "POST", f"{base}/{tid}/ops", {"ops": ops}, token)
    except Unknown as e:
        raise Failed(f"ops: {e}")
    if s != 200:
        try:
            classify(s, j, "ops")
        except Unknown as e:
            raise Failed(str(e))
    filled = fill(mops, j["results"])
    s, j = call(port, "POST", f"{base}/{tid}/commit", {}, token)
    if s == 200:
        return filled
    classify(s, j, "commit")


def ensure_streams(w, ports, token, mops, rng):
    for k in sorted({m[1] for m in mops if is_stream(m[1])}):
        if k in w.created:
            continue
        deadline = time.time() + 120
        while True:
            try:
                s, j = call(rng.choice(ports), "POST", f"/v1/dbs/{DB}/streams", {"name": f"s{k}"}, token)
                if s == 201 or (s == 409 and j["error"]["code"] == "already_exists"):
                    break
            except (Failed, Unknown):
                pass
            if time.time() > deadline:
                raise RuntimeError(f"cannot create stream s{k}")
            time.sleep(0.2)
        with w.lock:
            w.created.add(k)


def client(w, ci, ports, token, stop, rng, errors):
    process = ci
    while not stop.is_set():
        invoked = False
        try:
            inter = rng.random() < w.args.interactive
            mops = w.generate(rng, inter)
            port = rng.choice(ports)
            ensure_streams(w, ports, token, mops, rng)
            w.record("invoke", process, mops)
            invoked = True
            try:
                filled = (interactive if inter else one_shot)(port, token, mops)
                w.record("ok", process, filled)
            except Failed:
                w.record("fail", process, mops)
            except Unknown:
                w.record("info", process, mops)
                with w.lock:
                    process, w.next_process = w.next_process, w.next_process + 1
        except Exception as e:  # noqa: BLE001
            errors.append(f"client {ci}: {e!r}")
            if invoked:
                w.record("info", process, mops)
                with w.lock:
                    process, w.next_process = w.next_process, w.next_process + 1
            time.sleep(1)
        time.sleep(rng.random() * w.args.max_pause)


def setup(ports):
    deadline = time.time() + 120
    while True:
        try:
            body = {"email": "jepsen@jepsen.test", "password": "password-123"}
            s, j = call(ports[0], "POST", "/v1/auth/signup", body)
            if s == 201:
                token = j["token"]
                break
            s, j = call(ports[0], "POST", "/v1/auth/login", body)
            if s == 200:
                token = j["token"]
                break
        except (Failed, Unknown):
            pass
        if time.time() > deadline:
            raise RuntimeError("cannot sign up")
        time.sleep(0.5)
    while True:
        try:
            s, j = call(ports[0], "POST", "/v1/dbs", {"name": DB}, token)
            if s == 201 or (s == 409 and j["error"]["code"] == "already_exists"):
                return token
        except (Failed, Unknown):
            pass
        time.sleep(0.5)


def final_reads(w, ports, token):
    keys = [k for k in range(w.next_key) if not is_stream(k) or k in w.created]
    finals = {}
    for i in range(0, len(keys), 64):
        mops = [["r", k, None] for k in keys[i:i + 64]]
        process = 1_000_000 + i
        for attempt in range(200):
            w.record("invoke", process, mops)
            try:
                filled = one_shot(ports[attempt % len(ports)], token, mops)
                w.record("ok", process, filled)
                finals.update({m[1]: m[2] or [] for m in filled})
                break
            except (Failed, Unknown):
                w.record("fail", process, mops)
                time.sleep(1)
        else:
            raise RuntimeError("final reads never succeeded")
    return finals


def check(events, finals):
    """Checks properties that need no dependency graph; Elle checks the rest."""
    appended, problems = {}, []
    for kind, _, _, mops in events:
        if kind == "invoke":
            for m in mops:
                if m[0] == "append":
                    appended[m[2]] = m[1]
    for k, l in finals.items():
        if len(set(l)) != len(l):
            problems.append(f"key {k} has duplicates: {l}")
        for v in l:
            if appended.get(v) != k:
                problems.append(f"key {k} holds {v}, never appended to it")
    for kind, p, _, mops in events:
        for m in mops:
            fin = finals.get(m[1], [])
            if kind == "ok" and m[0] == "r" and m[2] is not None and fin[:len(m[2])] != m[2]:
                problems.append(f"process {p} read key {m[1]} as {m[2]}, not a prefix of final {fin}")
            if kind == "ok" and m[0] == "append" and m[2] not in fin:
                problems.append(f"process {p} committed append {m[2]} to key {m[1]}, missing from final {fin}")
            if kind == "fail" and m[0] == "append" and m[2] in fin:
                problems.append(f"process {p} failed append {m[2]} to key {m[1]}, present in final {fin}")
    return problems


def edn(events):
    out = []
    for i, (kind, p, t, mops) in enumerate(events):
        def mop(m):
            if m[0] == "append":
                return f"[:append {m[1]} {m[2]}]"
            v = "nil" if m[2] is None else "[" + " ".join(map(str, m[2])) + "]"
            return f"[:r {m[1]} {v}]"
        out.append(f"{{:index {i}, :type :{kind}, :process {p}, :time {t}, :f :txn, "
                   f":value [{' '.join(mop(m) for m in mops)}]}}")
    return "\n".join(out) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=5)
    ap.add_argument("--out", required=True)
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--clients", type=int, default=10)
    ap.add_argument("--active-keys", type=int, default=8)
    ap.add_argument("--max-appends", type=int, default=32)
    ap.add_argument("--max-mops", type=int, default=4)
    ap.add_argument("--interactive", type=float, default=0.5)
    ap.add_argument("--max-pause", type=float, default=0.05)
    ap.add_argument("--lease-ms", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=int(time.time()))
    ap.add_argument("--no-nemesis", action="store_true")
    ap.add_argument("--binary", default=os.path.join(ROOT, "target/release/baserunna"))
    ap.add_argument("--minio", default=shutil.which("minio") or "minio")
    ap.add_argument("--mc", default=shutil.which("mc") or "mc")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    rng = random.Random(args.seed)
    cluster = Cluster(args)
    cluster.note(f"seed {args.seed}")
    cluster.start_minio()
    for i in range(args.nodes):
        cluster.start(i)
    ports = [18080 + i for i in range(args.nodes)]
    try:
        token = setup(ports)
        w = World(args, rng)
        stop, errors, threads = threading.Event(), [], []
        for ci in range(args.clients):
            t = threading.Thread(target=client, args=(w, ci, ports, token, stop, random.Random(rng.random()), errors))
            t.start()
            threads.append(t)
        nem = None
        if not args.no_nemesis:
            nem = threading.Thread(target=nemesis, args=(cluster, stop, random.Random(rng.random())))
            nem.start()
        time.sleep(args.minutes * 60)
        stop.set()
        for t in threads:
            t.join()
        if nem:
            nem.join()
        cluster.heal()
        time.sleep(3 * args.lease_ms / 1000 + 5)
        finals = final_reads(w, ports, token)
        with open(os.path.join(args.out, "history.edn"), "w") as f:
            f.write(edn(w.events))
        counts = {}
        for e in w.events:
            counts[e[0]] = counts.get(e[0], 0) + 1
        problems = check(w.events, finals)
        summary = {"counts": counts, "keys": len(finals), "client_errors": errors[:20], "problems": problems[:50]}
        with open(os.path.join(args.out, "summary.json"), "w") as f:
            json.dump(summary, f, indent=1)
        cluster.note(json.dumps({"counts": counts, "problems": len(problems), "client_errors": len(errors)}))
        return 1 if problems else 0
    finally:
        cluster.stop()


if __name__ == "__main__":
    sys.exit(main())
