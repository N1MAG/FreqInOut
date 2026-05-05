# Contributing to FreqInOut

Thanks for your interest in contributing. This guide covers how to set up the project, propose changes, and submit a pull request.

## Quick Start

- Python 3.9 through 3.13 is supported (3.11 recommended; 3.14 is not yet supported).
- Clone the repo and set up a virtual environment:

```bash
git clone https://github.com/N1MAG/FreqInOut.git FreqInOut
cd FreqInOut
python -m venv venv
```

- Activate the virtual environment:
  - Windows PowerShell: `.\venv\Scripts\Activate.ps1`
  - Linux/macOS: `source venv/bin/activate`

- Install dependencies:

```bash
pip install -r requirements.txt
```

- Run the app:

```bash
python -m freqinout.main
```

See `docs/Installation.md` for more details and platform notes.

## What to Contribute

- Bug fixes and stability improvements.
- Performance improvements (especially map rendering, log ingestion, and DB writes).
- Docs updates (guide, install notes, examples).
- New integrations for radio software or hardware.

If you are unsure, open an issue first.

## Reporting Issues

Please include:

- FreqInOut version (`freqinout/version.py`).
- OS and Python version.
- Steps to reproduce.
- Expected vs actual behavior.
- Logs (if available), with any personal data removed.

## Pull Requests

- Keep PRs focused and small.
- Link to an existing issue when possible.
- Describe the behavior change and how you verified it.
- Include screenshots or short clips for UI changes.

## Release Readiness (Maintainer)

Before pushing a release commit, run:

```bash
python tools/release_preflight.py
```

Then verify packaging flows:
- Linux installer: guided install/update/repair paths.
- Windows build: PyInstaller + Inno setup.
- Docs: user-facing behavior matches current UI.

## Tests and Verification

There is no formal test suite yet. Please include manual verification steps, such as:

- "Open Settings tab, change X, save, restart, confirm Y"
- "Run FLDigi NCS tab, start net, save check-ins, end net"

## Coding Guidelines

- Prefer clear, readable code over cleverness.
- Keep UI responsive (avoid blocking the UI thread).
- Stream file reads when logs can be large.
- Avoid unnecessary database writes; batch when possible.
- Keep error handling user-friendly and log details via `freqinout.core.logger`.
- Follow existing UI patterns and naming.

## Dependencies

- Avoid adding new heavy dependencies unless required.
- Note new dependencies in PR description and update any docs if needed.

## Security and Privacy

- Do not commit API keys, secrets, or personal data.
- Avoid logging sensitive data like email addresses or tokens.

## License

By contributing, you agree that your contributions will be licensed under the GNU GPLv3 (see `LICENSE.md`).
