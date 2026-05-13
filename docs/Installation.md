# FreqInOut Installation Guide (Windows)

This guide assumes a fresh Windows system. Adjust paths as needed.

## 1) Prerequisites
- Python 3.9 through 3.13 installed and on PATH (3.11 recommended; 3.14 is not yet supported)
- Git (if cloning)
- Optional: FLRig/FLDigi/JS8Call/VarAC installed if you plan to auto-launch them

## 2) Get the code
Clone or download the repository:
```
git clone https://github.com/N1MAG/FreqInOut.git FreqInOut
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

`requirements.txt` includes `keyring`, which FIO uses for secure GPG signing passphrase storage in the operating system credential store. On Linux, make sure a supported keyring backend is available, such as Secret Service or KWallet; FIO will not fall back to plaintext storage.

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
- Watch the Settings left-nav for warning highlights. In `1.2.2`, these indicate sections where a companion field is still missing.
- Typical examples:
  - `JS8Call Settings`: if `JS8Call Install Folder` is set, also set host, TCP port, and `DIRECTED.TXT`.
  - `JS8Call Settings`: if `JS8Spotter Launch Path` is set, also set `JS8Spotter forms`.
  - `Fast Light Settings`: if `FLRig` or `FLDigi` executable paths are set, also set their endpoint fields; if `FLMsg` or `FLAmp` executable paths are set, also set their message folders.
  - `VarAC Settings`: if `VarAC Install Folder` is set, also set `Incoming Files`.

## 8) Data storage
- Settings and schedules are stored in the runtime profile under `FreqInOut\config\freqinout.db` (SQLite).
- On Windows, the default profile root is `%LOCALAPPDATA%\FreqInOut` (fallback `%APPDATA%\FreqInOut`).
- Logs are stored in the profile root as `freqinout.log`.

## 9) Building an executable (optional)
If `build_executable.py` is provided, activate the venv then run:
```
python build_executable.py
```

## 10) Troubleshooting
- If saving settings fails on OneDrive, run the app from a local folder.
- Ensure JS8Call API port matches `js8_port` in settings (default 2442).
- For FLRig control, verify FLRig is running and reachable at 127.0.0.1:12345 (default).

## 11) Linux installer
For Linux users, prefer the guided installer:
```
bash install_FreqInOut_linux.sh
```
This installer is the recommended end-user path. It keeps the installed app checkout runtime-focused, excluding `tests/` and other developer-only paths, so later `git pull` updates in that installed folder do not materialize the test suite.
