# FreqInOut Database Tools

This page covers the command-line database administration tools.

For a full reference of every delivered script (not just DB tools), see:

- `docs/tools-and-scripts.md`

## Quick Start (recommended)

Use the wrapper script from repo root (recommended):

```bash
bash tools/freqinout-db.sh status
```

Windows PowerShell equivalent:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status
```

Cross-platform Python entrypoint:

```bash
python tools/freqinout_db.py status
```

Available wrapper commands:

```bash
bash tools/freqinout-db.sh status
bash tools/freqinout-db.sh init all
bash tools/freqinout-db.sh truncate nets_all
bash tools/freqinout-db.sh table-show operator_checkins
bash tools/freqinout-db.sh table-export operator_checkins ./operator_checkins.json
bash tools/freqinout-db.sh backup
bash tools/freqinout-db.sh vacuum
```

## Safety Behavior

- Truncate operations require confirmation.
- Truncate operations automatically back up affected DB files first.
- Backups are saved under `config/backups/...`.

## Direct Python Tools

You can run the underlying Python tools directly:

```bash
python tools/db_admin.py -h
python tools/db_tools.py -h
```

Common examples:

```bash
python tools/db_admin.py --init all
python tools/db_admin.py --truncate nets_all --yes --show
python tools/db_tools.py --list-dbs
python tools/db_tools.py --table js8_links --table-show
python tools/db_tools.py --table js8_links --table-export ./js8_links.json
```

## Database Files

- Settings DB: runtime profile `config/freqinout.db`
- Nets/ops DB: runtime profile `config/freqinout_nets.db`
- Linux/macOS default profile root: `~/.freqinout`
- Windows default profile root: `%LOCALAPPDATA%\FreqInOut` (fallback `%APPDATA%\FreqInOut`)

## Propagation Regression Tests

Run the offline propagation regression suite:

```bash
python -m unittest discover -s tests -p "test_propagation*.py" -v
```

Coverage includes:
- ingest idempotency and checkpoint behavior
- modeled fallback behavior with no history
- overnight schedule window semantics
- modeled parity path used by ControlFreq and Map

## Propagation Calibration

Calibrate blend/gate constants from your local historical outcome dataset:

```bash
python tools/propagation_calibrate.py --fast
```

Apply calibrated values to settings after reviewing the JSON report:

```bash
python tools/propagation_calibrate.py --apply
```

Output report default path:
- runtime profile `config/propagation/prop_calibration_recommendation.json`
