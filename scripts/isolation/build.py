"""Builds the node and the isolation harness from this checkout into
$ISOLATION_TARGET_DIR, which defaults to $HOME/target-isolation. Run inside the
Linux VM.

Usage: python3 -I build.py
"""

import os
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def main():
    subprocess.run(["git", "log", "--oneline", "-1"], cwd=REPO, check=True)
    target_dir = os.environ.get(
        "ISOLATION_TARGET_DIR", str(Path.home() / "target-isolation")
    )
    command = ["cargo", "build", "-q", "--release", "--bin", "baserunna"]
    command += ["--example", "isolation", "--example", "fake_s3_server"]
    subprocess.run(
        command, cwd=REPO, env=dict(os.environ, CARGO_TARGET_DIR=target_dir), check=True
    )


if __name__ == "__main__":
    main()
