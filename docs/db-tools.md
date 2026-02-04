# FreqInOut Database Tools

This page covers the command-line database administration tools.

## Quick Start (recommended)

Use the wrapper script from repo root:

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

- Settings DB: `config/freqinout.db`
- Nets/ops DB: `config/freqinout_nets.db`
