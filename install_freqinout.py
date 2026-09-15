
"""Create/update local virtual environment and install project requirements."""

import subprocess
import sys
from pathlib import Path


MIN_PYTHON = (3, 9)
MAX_PYTHON = (3, 13)


def main():
    if not (MIN_PYTHON <= sys.version_info[:2] <= MAX_PYTHON):
        supported = "3.9 through 3.13"
        raise SystemExit(
            f"FreqInOut requires Python {supported}; found "
            f"{sys.version_info.major}.{sys.version_info.minor}."
        )
    root = Path(__file__).parent
    venv = root / ".venv"
    if not venv.exists():
        subprocess.check_call([sys.executable, "-m", "venv", str(venv)])
    python = venv / ("Scripts" if sys.platform.startswith("win") else "bin") / (
        "python.exe" if sys.platform.startswith("win") else "python"
    )
    req = root / "requirements.txt"
    if req.exists():
        subprocess.check_call([str(python), "-m", "pip", "install", "-r", str(req)])
    if sys.platform.startswith("win"):
        run_hint = r".\.venv\Scripts\python.exe -m freqinout.main"
    else:
        run_hint = "./.venv/bin/python -m freqinout.main"
    print(f"Virtual environment ready. Run: {run_hint}")

if __name__ == "__main__":
    main()
