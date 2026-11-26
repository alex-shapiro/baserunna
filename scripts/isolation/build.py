"""Build the node and isolation harness binaries inside the Linux VM.

Usage: python3 -I build.py

By default, this script writes binaries to $HOME/target-isolation.
Set $ISOLATION_TARGET_DIR to choose a different output directory.
"""

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def main():
    subprocess.run(["git", "log", "--oneline", "-1"], cwd=REPO_ROOT, check=True)
    target_dir = os.environ.get(
        "ISOLATION_TARGET_DIR", str(Path.home() / "target-isolation")
    )
    command = ["cargo", "build", "-q", "--release", "--bin", "baserunna"]
    command += ["--example", "isolation", "--example", "fake_s3_server"]
    subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=dict(os.environ, CARGO_TARGET_DIR=target_dir),
        check=True,
    )


if __name__ == "__main__":
    main()
