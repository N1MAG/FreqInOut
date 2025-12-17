# FreqInOut Installation Guide (Windows)

This guide assumes a fresh Windows system. Adjust paths as needed.

## 1) Prerequisites
- Python 3.11+ installed and on PATH
- Git (if cloning)
- Optional: FLRig/FLDigi/JS8Call/VarAC installed if you plan to auto-launch them

## 2) Get the code
Clone or download the repository:
```
git clone <your-repo-url> FreqInOut
cd FreqInOut
```

## 3) Create a virtual environment
```
python -m venv venv
```

## 4) Activate the virtual environment (PowerShell)
```
.\venv\Scripts\Activate.ps1
```

## 5) Install dependencies
```
pip install -r requirements.txt
```

If you will control JS8Call, also install:
```
pip install pyjs8call
```

## 6) Run FreqInOut
```
python -m freqinout.main
```

## 7) Configure paths
- Open the Settings tab and set executable paths for FLRig, FLDigi, FLMsg, FLAmp, VarAC, JS8Call.
- Set JS8Call DIRECTED.TXT path (for JS8 net control).

## 8) Data storage
- Settings and schedules are stored in `config/freqinout.db` (SQLite).
- Logs are under `%APPDATA%\FreqInOut\freqinout.log`.

## 9) Building an executable (optional)
If `build_executable.py` is provided, activate the venv then run:
```
python build_executable.py
```

## 10) Troubleshooting
- If saving settings fails on OneDrive, run the app from a local folder.
- Ensure JS8Call API port matches `js8_port` in settings (default 2442).
- For FLRig control, verify FLRig is running and reachable at 127.0.0.1:12345 (default).
