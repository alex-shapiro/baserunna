"""Run a tenant isolation workload against a node inside a memory-limited cgroup.

Usage: python3 -I run.py <workload> <small|attack|-> <out.json> [node binary] [client flags]

Run this script inside a Linux VM with cgroup v2 and sudo access.
We recorded baselines on a VM with 4 vCPUs and 8 GiB of memory.
Do not compare results across VMs of different sizes.
The script writes the VM vCPU count, memory, and kernel version to each output file.

Environment variables:
- ISOLATION_TARGET_DIR: Cargo build directory. Default: $HOME/target-isolation.
- NODE_BIN: Node binary path. Default: $ISOLATION_TARGET_DIR/release/baserunna.
- HARNESS_BIN_DIR: Directory of the `isolation` and `fake_s3_server` binaries.
  Default: $ISOLATION_TARGET_DIR/release.
- S3_PORT and NODE_PORT: Fixed ports for the fake S3 server and node. Default: free ports.
- ISO_CGROUP: Parent cgroup path. Default: /sys/fs/cgroup/iso.

The script forwards arguments starting with `--` to the isolation client binary.
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
# Delay added to each fake S3 request to match remote object storage latency.
DEFAULT_STORE_LATENCY_MS = "20"


def free_port():
    with socket.socket() as probe_socket:
        probe_socket.bind(("127.0.0.1", 0))
        return str(probe_socket.getsockname()[1])


def sudo_write(path, text):
    """Write text to a root-owned file such as a cgroup control file."""
    subprocess.run(
        ["sudo", "tee", str(path)],
        input=text.encode(),
        stdout=subprocess.DEVNULL,
        check=True,
    )


def keyed_value(path, key):
    """Return the integer value after `key` in a whitespace-delimited file, or 0 if missing."""
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
        # The throughput benchmark uses 10 owners from a single client IP.
        # Set each limit to 0 to disable it.
        env.update(
            BASERUNNA_USER_RPS="0",
            BASERUNNA_API_KEY_RPS="0",
            BASERUNNA_SIGNUPS_PER_HOUR_PER_CLIENT="0",
        )
    if workload == "idle_cost":
        # The idle cost benchmark creates 1,000 owners from a single client IP.
        # Set the authentication rate limit to `inf` to disable it.
        # Use fewer PBKDF2 iterations so the node hashes passwords quickly.
        env.update(
            BASERUNNA_AUTH_RPS_PER_CLIENT="inf",
            BASERUNNA_SIGNUPS_PER_HOUR_PER_CLIENT="0",
            BASERUNNA_PBKDF2_ITERATIONS="1000",
        )
    return env


def create_node_cgroup(cgroup_root, node_cgroup):
    """Create the node cgroup with a memory limit and swap disabled.

    Delegate ownership to the current user so the node can create child cgroups.
    Create the `main` leaf cgroup for the node process.
    """
    sudo_write("/sys/fs/cgroup/cgroup.subtree_control", CONTROLLERS)
    subprocess.run(["sudo", "mkdir", "-p", str(node_cgroup)], check=True)
    sudo_write(cgroup_root / "cgroup.subtree_control", CONTROLLERS)
    sudo_write(node_cgroup / "memory.max", NODE_MEMORY_MAX)
    sudo_write(node_cgroup / "memory.swap.max", "0")
    subprocess.run(
        ["sudo", "chown", "-R", f"{os.getuid()}:{os.getgid()}", str(node_cgroup)],
        check=True,
    )
    (node_cgroup / "main").mkdir()
    (node_cgroup / "cgroup.subtree_control").write_text(CONTROLLERS)


# Wait for a start byte on the pipe in argv[1], then run os.execv on the node binary.
# If the parent exits early, os.read returns an empty byte string and the wrapper exits.
WAIT_THEN_EXEC = """
import os, sys
fd = int(sys.argv[1])
if os.read(fd, 1) != b"g":
    sys.exit(1)
os.close(fd)
os.execv(sys.argv[2], sys.argv[2:])
"""


def start_node_in_cgroup(node_bin, env, log, leaf_cgroup):
    """Start the node inside `leaf_cgroup` and return the child process.

    A wrapper process waits on a pipe while the parent moves it into `leaf_cgroup` with sudo.
    The wrapper then calls exec so the node allocates all memory inside the cgroup.
    """
    pipe_read_fd, pipe_write_fd = os.pipe()
    node = subprocess.Popen(
        [sys.executable, "-I", "-c", WAIT_THEN_EXEC, str(pipe_read_fd), str(node_bin)],
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        pass_fds=(pipe_read_fd,),
    )
    os.close(pipe_read_fd)
    sudo_write(leaf_cgroup / "cgroup.procs", str(node.pid))
    os.write(pipe_write_fd, b"g")
    os.close(pipe_write_fd)
    return node


def memory_stat(node_cgroup):
    """Return `anon`, `file`, and `kernel` byte counts from `memory.stat`.

    `anon` tracks process memory. `file` tracks reclaimable page cache.
    """
    stat = {}
    try:
        for line in (node_cgroup / "memory.stat").read_text().splitlines():
            name, _, value = line.partition(" ")
            if name in ("anon", "file", "kernel"):
                stat[name] = int(value)
    except OSError:
        pass
    return stat


def teardown(node, s3_server, cgroup_root, node_cgroup):
    if node is not None and node.poll() is None:
        node.send_signal(signal.SIGTERM)
        try:
            node.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    if node_cgroup.is_dir():
        sudo_write(node_cgroup / "cgroup.kill", "1")
        read_cgroup_procs = lambda: "".join(
            path.read_text()
            for path in node_cgroup.glob("**/cgroup.procs")
            if path.exists()
        )
        wait_until(lambda: not read_cgroup_procs(), 5)
        cgroup_dirs = sorted(
            (path for path in cgroup_root.glob("**/") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        )
        for cgroup_dir in cgroup_dirs:
            subprocess.run(
                ["sudo", "rmdir", str(cgroup_dir)], stderr=subprocess.DEVNULL
            )
    if s3_server is not None:
        s3_server.kill()
        s3_server.wait()


def exit_status(returncode):
    """Convert a subprocess return code into a shell exit status.

    Return 128 + signal if a signal stopped the process.
    """
    return 128 - returncode if returncode < 0 else returncode


def main():
    args = sys.argv[1:]
    if len(args) < 3:
        sys.exit(__doc__)
    workload, variant, result_path = args[0], args[1], Path(args[2]).resolve()
    remaining_args = args[3:]
    node_arg = (
        remaining_args.pop(0)
        if remaining_args and not remaining_args[0].startswith("--")
        else None
    )
    client_flags = remaining_args
    target_dir = Path(
        os.environ.get("ISOLATION_TARGET_DIR", Path.home() / "target-isolation")
    )
    node_bin = Path(
        node_arg or os.environ.get("NODE_BIN") or target_dir / "release/baserunna"
    ).resolve()
    harness_bin_dir = Path(os.environ.get("HARNESS_BIN_DIR", target_dir / "release"))
    s3_port = os.environ.get("S3_PORT") or free_port()
    node_port = os.environ.get("NODE_PORT") or free_port()
    cgroup_root = Path(os.environ.get("ISO_CGROUP", "/sys/fs/cgroup/iso"))
    node_cgroup = cgroup_root / "node"
    store_latency_ms = os.environ.get("FAKE_S3_LATENCY_MS", DEFAULT_STORE_LATENCY_MS)

    if cgroup_root.exists():
        sys.exit(f"{cgroup_root} exists; another run is active or did not clean up")
    # Convert termination signals into normal exit so the finally block runs teardown.
    for term_signal in (signal.SIGTERM, signal.SIGINT):
        signal.signal(term_signal, lambda *_: sys.exit(143))

    # Record other running nodes or Cargo builds in the VM without stopping them.
    busy_processes = subprocess.run(
        ["pgrep", "-a", "baserunna|cargo"], capture_output=True, text=True
    ).stdout.splitlines()
    vm = {
        "vcpus": os.cpu_count(),
        "memory_kib": keyed_value("/proc/meminfo", "MemTotal:") or None,
        "kernel": platform.release(),
    }

    work_dir = Path(tempfile.mkdtemp())
    node = s3_server = None
    try:
        # Run the fake S3 server outside the node cgroup.
        s3_env_path = work_dir / "s3.env"
        with open(s3_env_path, "w") as s3_env_file:
            s3_server = subprocess.Popen(
                [str(harness_bin_dir / "fake_s3_server"), f"127.0.0.1:{s3_port}"],
                env=dict(os.environ, FAKE_S3_LATENCY_MS=store_latency_ms),
                stdout=s3_env_file,
            )
        wait_until(lambda: "BASERUNNA_STORE" in s3_env_path.read_text(), 5)
        s3_env = dict(
            line.split("=", 1)
            for line in s3_env_path.read_text().splitlines()
            if "=" in line
        )

        create_node_cgroup(cgroup_root, node_cgroup)
        node_log = work_dir / "node.log"
        with open(node_log, "w") as log:
            node = start_node_in_cgroup(
                node_bin,
                node_environment(workload, s3_env, node_port),
                log,
                node_cgroup / "main",
            )

        client_args = [
            str(harness_bin_dir / "isolation"),
            "--node", f"http://127.0.0.1:{node_port}",
            "--cgroup", str(node_cgroup),
            "--workload", workload,
            "--out", str(result_path),
            "--node-pid", str(node.pid),
        ]  # fmt: skip
        if variant != "-":
            client_args += ["--variant", variant]
        result_path.parent.mkdir(parents=True, exist_ok=True)
        client_status = subprocess.run(client_args + client_flags).returncode

        node_exit = None if node.poll() is None else exit_status(node.returncode)
        # Count only OOM kills in the `main` leaf as node crashes.
        # The kernel uses OOM kills in worker cgroups to stop tenant work.
        node_oom_kills = keyed_value(
            node_cgroup / "main/memory.events.local", "oom_kill"
        )
        subtree_oom_kills = keyed_value(node_cgroup / "memory.events", "oom_kill")
        try:
            peak_memory_bytes = int((node_cgroup / "memory.peak").read_text())
        except OSError:
            peak_memory_bytes = None
        shutil.copy(node_log, result_path.with_suffix(".node.log"))
        if result_path.exists():
            result = json.loads(result_path.read_text())
            result["store_latency_ms"] = int(store_latency_ms)
            result["vm"] = vm
            result["vm_busy"] = bool(busy_processes)
            result["vm_busy_processes"] = busy_processes
            node_stopped = node_exit is not None or node_oom_kills > 0
            result["run"] = {
                "node_binary": str(node_bin),
                "node_exit_status": node_exit,
                "node_oom_kills": node_oom_kills,
                "subtree_oom_kills": subtree_oom_kills,
                "cgroup_memory_peak_bytes": peak_memory_bytes,
                "memory_stat_end_bytes": memory_stat(node_cgroup),
                "client_exit_status": client_status,
                "node_stopped": node_stopped,
            }
            if node_stopped and result.get("conditions"):
                result["conditions"]["1_node_running"] = False
                result["conditions"]["pass"] = False
            result_path.write_text(json.dumps(result, indent=2) + "\n")
            summary = {
                key: result.get(key)
                for key in ("workload", "variant", "conditions", "run", "error")
            }
            print(f"result: {result_path}")
            print(json.dumps(summary))
    finally:
        teardown(node, s3_server, cgroup_root, node_cgroup)
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
