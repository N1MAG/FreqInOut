
"""Create/update local virtual environment and install project requirements."""

import subprocess
import sys
from pathlib import Path

def main():
    root = Path(__file__).parent
    venv = root / ".venv"
    if not venv.exists():
        subprocess.check_call([sys.executable, "-m", "venv", str(venv)])
    pip = venv / ("Scripts" if sys.platform.startswith("win") else "bin") / "pip"
    req = root / "requirements.txt"
    if req.exists():
        subprocess.check_call([str(pip), "install", "-r", str(req)])
    print("Virtualenv ready. Run: ./start-multi-rig.sh or python -m freqinout.main from the activated venv.")

if __name__ == "__main__":
    main()
