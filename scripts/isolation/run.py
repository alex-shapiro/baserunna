"""Runs one tenant isolation workload against a node in a memory-limited cgroup
and writes the result JSON (ISOLATION_PLAN.md, milestone 0).

Usage: python3 -I run.py <workload> <small|attack|-> <out.json> [node binary] [client flags]

Run inside a Linux VM with cgroup v2 and sudo. The baselines come from a VM
with 4 vCPUs and 8 GiB of memory. Results from a VM of a different size are not
comparable with them. Each result records the VM's vCPUs, memory, and kernel.

The node binary defaults to $NODE_BIN, then to
$ISOLATION_TARGET_DIR/release/baserunna. The harness binaries (isolation and
fake_s3_server) come from $EXAMPLES_DIR, which defaults to
$ISOLATION_TARGET_DIR/release/examples. $ISOLATION_TARGET_DIR defaults to
$HOME/target-isolation, where build.py puts the binaries. Ports are free ports
unless $S3_PORT and $NODE_PORT are set. $ISO_CGROUP replaces
/sys/fs/cgroup/iso. Arguments that start with -- go to the isolation client,
such as --clients.
"""

import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

NODE_MEMORY_MAX = "1536M"
CONTROLLERS = "+memory +cpu +pids"
# The store delays every request by this many milliseconds, as a remote object store would.
DEFAULT_STORE_LATENCY_MS = "20"


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return str(s.getsockname()[1])


def sudo_write(path, text):
    """Writes `text` to a root-owned file, such as a cgroup control file."""
    subprocess.run(
        ["sudo", "tee", str(path)],
        input=text.encode(),
        stdout=subprocess.DEVNULL,
        check=True,
    )


def keyed_value(path, key):
    """Returns the first number after `key` in a keyed file such as memory.events or
    /proc/meminfo, or 0."""
    try:
        for line in Path(path).read_text().splitlines():
            fields = line.split()
            if len(fields) >= 2 and fields[0] == key:
                return int(fields[1])
    except OSError:
        pass
    return 0


def is_running(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def wait_until(condition, seconds):
    deadline = time.monotonic() + seconds
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.05)


def node_environment(workload, s3_env, node_port):
    env = dict(os.environ, **s3_env)
    env.update(
        BASERUNNA_LISTEN=f"127.0.0.1:{node_port}",
        BASERUNNA_SIGNUP="open",
        BASERUNNA_CACHE_MB="64",
    )
    if workload == "kv_throughput":
        # The throughput load is 10 owners from one client address; 0 turns each limit off.
        env.update(
            BASERUNNA_USER_RPS="0",
            BASERUNNA_API_KEY_RPS="0",
            BASERUNNA_SIGNUPS_PER_HOUR_PER_CLIENT="0",
        )
    if workload == "idle_cost":
        # The setup signs up 1,000 owners from one client address. The per-client auth limit
        # turns off only at inf. Fewer PBKDF2 iterations keep the 1,000 password hashes short.
        env.update(
            BASERUNNA_AUTH_RPS_PER_CLIENT="inf",
            BASERUNNA_SIGNUPS_PER_HOUR_PER_CLIENT="0",
            BASERUNNA_PBKDF2_ITERATIONS="1000",
        )
    return env


def create_node_cgroup(cg, node_cg):
    """Creates the node cgroup with its memory limit and no swap, and delegates it to this user
    so that a node can create child cgroups for its workers. The node runs in the leaf `main`."""
    sudo_write("/sys/fs/cgroup/cgroup.subtree_control", CONTROLLERS)
    subprocess.run(["sudo", "mkdir", "-p", str(node_cg)], check=True)
    sudo_write(cg / "cgroup.subtree_control", CONTROLLERS)
    sudo_write(node_cg / "memory.max", NODE_MEMORY_MAX)
    sudo_write(node_cg / "memory.swap.max", "0")
    subprocess.run(
        ["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", str(node_cg)],
        check=True,
    )
    (node_cg / "main").mkdir()
    (node_cg / "cgroup.subtree_control").write_text(CONTROLLERS)


# Waits for one byte on the pipe in argv[1], then replaces itself with the node. If the parent
# exits first, the pipe closes with no byte, and the node does not start.
WAIT_THEN_EXEC = """
import os, sys
fd = int(sys.argv[1])
if os.read(fd, 1) != b"g":
    sys.exit(1)
os.close(fd)
os.execv(sys.argv[2], sys.argv[2:])
"""


def start_node_in_cgroup(node_bin, env, log, leaf):
    """Starts the node, moves it into the leaf cgroup before it runs, and returns its process.

    A wrapper process waits on a pipe until the parent has moved it, and then becomes the node
    with exec. So the node does not allocate any memory outside the cgroup. Moving a process
    between cgroups needs root, so the parent moves it with sudo."""
    go_read, go_write = os.pipe()
    node = subprocess.Popen(
        [sys.executable, "-I", "-c", WAIT_THEN_EXEC, str(go_read), str(node_bin)],
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        pass_fds=(go_read,),
    )
    os.close(go_read)
    sudo_write(leaf / "cgroup.procs", str(node.pid))
    os.write(go_write, b"g")
    os.close(go_write)
    return node


def memory_stat(node_cg):
    """Returns the node cgroup's anon, file, and kernel bytes. anon is process memory; file is
    page cache that the kernel can reclaim."""
    stat = {}
    try:
        for line in (node_cg / "memory.stat").read_text().splitlines():
            name, _, value = line.partition(" ")
            if name in ("anon", "file", "kernel"):
                stat[name] = int(value)
    except OSError:
        pass
    return stat


def teardown(node, s3, cg, node_cg):
    if node is not None and node.poll() is None:
        node.send_signal(signal.SIGTERM)
        try:
            node.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    if node_cg.is_dir():
        sudo_write(node_cg / "cgroup.kill", "1")
        procs = lambda: "".join(
            p.read_text() for p in node_cg.glob("**/cgroup.procs") if p.exists()
        )
        wait_until(lambda: not procs(), 5)
        dirs = sorted(
            (p for p in cg.glob("**/") if p.is_dir()),
            key=lambda p: len(p.parts),
            reverse=True,
        )
        for d in dirs:
            subprocess.run(["sudo", "rmdir", str(d)], stderr=subprocess.DEVNULL)
    if s3 is not None:
        s3.kill()
        s3.wait()


def exit_status(returncode):
    """Returns a process exit status as a shell reports it: 128 + N for a kill by signal N."""
    return 128 - returncode if returncode < 0 else returncode


def main():
    args = sys.argv[1:]
    if len(args) < 3:
        sys.exit(__doc__)
    workload, variant, out = args[0], args[1], Path(args[2]).resolve()
    rest = args[3:]
    node_arg = rest.pop(0) if rest and not rest[0].startswith("--") else None
    client_flags = rest
    target_dir = Path(
        os.environ.get("ISOLATION_TARGET_DIR", Path.home() / "target-isolation")
    )
    node_bin = Path(
        node_arg or os.environ.get("NODE_BIN") or target_dir / "release/baserunna"
    ).resolve()
    examples_dir = Path(os.environ.get("EXAMPLES_DIR", target_dir / "release/examples"))
    s3_port = os.environ.get("S3_PORT") or free_port()
    node_port = os.environ.get("NODE_PORT") or free_port()
    cg = Path(os.environ.get("ISO_CGROUP", "/sys/fs/cgroup/iso"))
    node_cg = cg / "node"
    store_latency_ms = os.environ.get("FAKE_S3_LATENCY_MS", DEFAULT_STORE_LATENCY_MS)

    if cg.exists():
        sys.exit(f"{cg} exists; another run is active or did not tear down")
    # Signals end the run through the normal teardown.
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: sys.exit(143))

    # Other nodes or compilers in the VM add noise. Record them; do not stop them.
    busy = subprocess.run(
        ["pgrep", "-a", "baserunna|cargo"], capture_output=True, text=True
    ).stdout.splitlines()
    vm = {
        "vcpus": os.cpu_count(),
        "memory_kib": keyed_value("/proc/meminfo", "MemTotal:") or None,
        "kernel": platform.release(),
    }

    work = Path(tempfile.mkdtemp())
    node = s3 = None
    try:
        # The store runs outside the node cgroup.
        s3_out = work / "s3.env"
        with open(s3_out, "w") as f:
            s3 = subprocess.Popen(
                [str(examples_dir / "fake_s3_server"), f"127.0.0.1:{s3_port}"],
                env=dict(os.environ, FAKE_S3_LATENCY_MS=store_latency_ms),
                stdout=f,
            )
        wait_until(lambda: "BASERUNNA_STORE" in s3_out.read_text(), 5)
        s3_env = dict(
            line.split("=", 1)
            for line in s3_out.read_text().splitlines()
            if "=" in line
        )

        create_node_cgroup(cg, node_cg)
        node_log = work / "node.log"
        with open(node_log, "w") as log:
            node = start_node_in_cgroup(
                node_bin,
                node_environment(workload, s3_env, node_port),
                log,
                node_cg / "main",
            )

        client_args = [
            str(examples_dir / "isolation"),
            "--node", f"http://127.0.0.1:{node_port}",
            "--cgroup", str(node_cg),
            "--workload", workload,
            "--out", str(out),
            "--node-pid", str(node.pid),
        ]  # fmt: skip
        if variant != "-":
            client_args += ["--variant", variant]
        out.parent.mkdir(parents=True, exist_ok=True)
        client_status = subprocess.run(client_args + client_flags).returncode

        node_exit = None if node.poll() is None else exit_status(node.returncode)
        # Only kills in the node's own leaf mean the node died; kills in worker cgroups are refusals.
        oom_kills = keyed_value(node_cg / "main/memory.events.local", "oom_kill")
        subtree_oom_kills = keyed_value(node_cg / "memory.events", "oom_kill")
        try:
            peak = int((node_cg / "memory.peak").read_text())
        except OSError:
            peak = None
        shutil.copy(node_log, out.with_suffix(".node.log"))
        if out.exists():
            result = json.loads(out.read_text())
            result["store_latency_ms"] = int(store_latency_ms)
            result["vm"] = vm
            result["vm_busy"] = bool(busy)
            result["vm_busy_processes"] = busy
            node_died = node_exit is not None or oom_kills > 0
            result["run"] = {
                "node_binary": str(node_bin),
                "node_exit_status": node_exit,
                "node_oom_kills": oom_kills,
                "subtree_oom_kills": subtree_oom_kills,
                "cgroup_memory_peak_bytes": peak,
                "memory_stat_end_bytes": memory_stat(node_cg),
                "client_exit_status": client_status,
                "node_died": node_died,
            }
            if node_died and result.get("conditions"):
                result["conditions"]["1_node_survives"] = False
                result["conditions"]["pass"] = False
            out.write_text(json.dumps(result, indent=2) + "\n")
            summary = {
                k: result.get(k)
                for k in ("workload", "variant", "conditions", "run", "error")
            }
            print(f"result: {out}")
            print(json.dumps(summary))
    finally:
        teardown(node, s3, cg, node_cg)
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
