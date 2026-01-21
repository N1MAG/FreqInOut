# FreqInOut

FreqInOut is a desktop application for HF net control and operator tracking, with built-in scheduling, JS8Call integration, and live station mapping.

## Highlights

- Net control workflows for FLDigi and JS8Call
- Operator history with import/export and smart merge rules
- Map tab with live links from JS8 traffic and log fallbacks
- Scheduler with Operating Groups and automatic frequency changes
- Message viewing for JS8, FLMSG, FLAMP, and VarAC

## Quick Start

### Windows

```bash
git clone <your-repo-url> FreqInOut
cd FreqInOut
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m freqinout.main
```

### Linux (Debian/Ubuntu)

```bash
sudo apt-get install python3 python3-venv python3-pip
git clone <your-repo-url> FreqInOut
cd FreqInOut
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m freqinout.main
```

For QtWebEngine map support on Debian/Ubuntu:

```bash
sudo apt-get install libxcb-cursor0 libxcb-xinerama0
```

## Configuration Notes

- Set radio software paths and JS8Call DIRECTED.TXT in the Settings tab.
- Populate Operator History before expecting the Map tab to show full results.
- JS8 live ingest is used when available; log parsing is used as a fallback.

## Documentation

- Installation: `docs/Installation.md`
- User guide: `docs/guide.html`
- AppImage packaging: `docs/appimage.md`
- Changelog: `CHANGELOG.md`
- Contributing: `CONTRIBUTING.md`
- Code of Conduct: `CODE_OF_CONDUCT.md`

## License

GNU GPLv3 (see `LICENSE.md`).

