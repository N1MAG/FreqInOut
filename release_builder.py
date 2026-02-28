from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def run(cmd: list[str]) -> int:
    print(f"[release_builder] Running: {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=ROOT, check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run release verification checks and optional EXE build."
    )
    parser.add_argument("--skip-preflight", action="store_true", help="Skip release preflight checks.")
    parser.add_argument(
        "--skip-compileall",
        action="store_true",
        help="Skip 'python -m compileall freqinout' verification.",
    )
    parser.add_argument("--build-exe", action="store_true", help="Run PyInstaller build after preflight.")
    args = parser.parse_args()

    if not args.skip_preflight:
        rc = run([sys.executable, "tools/release_preflight.py"])
        if rc != 0:
            print("[release_builder] ERROR: preflight checks failed.")
            return rc

    if not args.skip_compileall:
        rc = run([sys.executable, "-m", "compileall", "freqinout"])
        if rc != 0:
            print("[release_builder] ERROR: compileall verification failed.")
            return rc

    if args.build_exe:
        rc = run([sys.executable, "build_executable.py"])
        if rc != 0:
            print("[release_builder] ERROR: EXE build failed.")
            return rc

    print("[release_builder] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
