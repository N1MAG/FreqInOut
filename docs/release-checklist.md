# FreqInOut Release Checklist

Use this checklist before pushing a release commit, tagging, or building installers.

## 1) Versioning

- Update `freqinout/version.py` (`__version__`).
- Ensure matching version in:
  - `pyproject.toml` (`project.version`)
  - `installer.iss` (`AppVersion`)
  - `docs/guide.html` (`Current version`)
  - `CHANGELOG.md` (top release header)

## 2) Documentation

- `README.md` install paths and commands are current.
- `CONTRIBUTING.md` setup and release notes are current.
- `docs/Installation.md` is accurate for source installs.
- `docs/FreqInOut-linux-installer.md` and `.html` match installer behavior.
- `docs/tools-and-scripts.md` reflects current script behavior and examples.
- `SECURITY.md` contains valid private reporting contact info.

## 3) Installer / Packaging

- Linux: run installer in at least one fresh scenario and one update scenario.
- Linux: verify desktop launcher/icon behavior and logs.
- Windows: run `python build_executable.py`.
- Windows: update `installer.iss` and compile with Inno Setup.

## 4) Preflight

Run:

```bash
python tools/release_preflight.py
```

Fix any reported ERROR items before release.

## 5) Repo hygiene

- Confirm no local-only artifacts are staged (`dist/`, `build/`, DB files, logs).
- Confirm no placeholder/stub docs remain unintentionally.
- Confirm no mojibake text appears in docs/changelog.

## 6) Final release flow

1. Run preflight.
2. Build and smoke-test app.
3. Commit release changes.
4. Tag release.
5. Publish release notes from `CHANGELOG.md`.
