# FIO Single-Rig to Multi-Rig Upgrade Guide

Purpose: upgrade an existing production single-rig FIO install to the current multi-rig WIP on Linux while preserving a restorable backup of the production configuration.

## Upgrade Target

| Item | Value |
|---|---|
| WIP repository | `https://github.com/N1MAG/FreqInOut-internal-testing.git` |
| WIP branch | `wip/private-testing-multi-rig-1.2.3-not-ready` |
| Normal production config root | `$HOME/.freqinout` |
| Settings database | `$HOME/.freqinout/config/freqinout.db` |
| Nets/runtime database | `$HOME/.freqinout/config/freqinout_nets.db` |
| Installer backup root | `$HOME/.local/state/freqinout/backups` |
| In-app migration backup root | `$HOME/.freqinout/backups` |

## 1. Close FIO and Radio Companion Apps

Close FIO before starting the upgrade. Also close any companion app you do not want reading or writing configuration during the upgrade window, including FLRig, FLDigi, JS8Call, JS8Spotter, FLAMP, FLMsg, CommStat, and VarAC.

Check for a running FIO process:

```bash
pgrep -af 'freqinout.main|[f]reqinout' || true
```

If FIO appears in the output, close it normally from the app first.

## 2. Record the Current Production Paths

Confirm the existing production app folder. Common examples:

```bash
ls -la "$HOME/FreqInOut"
ls -la "$HOME/Apps/FreqInOut"
```

Confirm the production config database:

```bash
ls -la "$HOME/.freqinout/config"
```

Expected before upgrade:

```text
freqinout.db
```

If the production install uses a custom `FREQINOUT_CONFIG_DIR`, write that path down before proceeding and use it consistently for launch/testing.

## 3. Pull the Latest Multi-Rig WIP Installer

If your production install is already a git checkout:

```bash
cd "$HOME/FreqInOut"

git remote -v
git fetch origin
```

If the existing checkout points at the public repo, run the installer with the explicit WIP repo and branch in step 4. The installer will switch/update the checkout only after creating the pre-update backup.

If the production install is not a git checkout, the installer can move the old app folder aside and clone the WIP branch. It now creates the config backup before that replacement step.

## 4. Run the Upgrade Installer

From a checkout that contains `install_FreqInOut_linux.sh`, run:

```bash
bash install_FreqInOut_linux.sh \
  --dir "$HOME/FreqInOut" \
  --repo "https://github.com/N1MAG/FreqInOut-internal-testing.git" \
  --branch "wip/private-testing-multi-rig-1.2.3-not-ready"
```

Adjust `--dir` if the production app folder is not `$HOME/FreqInOut`.

During prompts:

- Answer that FIO is already installed.
- Choose the existing production install folder.
- Choose `Both` to update the app and refresh launcher/icon.
- If asked about changing branch/repo, continue with the WIP branch.
- If asked to replace a non-git folder, proceed only after confirming the backup message appears.

## 5. Confirm the Installer Backup

The installer should print a backup path like:

```text
Backup: $HOME/.local/state/freqinout/backups/freqinout-backup-YYYYMMDD-HHMMSS.tar.gz
```

Confirm the archive exists:

```bash
ls -lh "$HOME/.local/state/freqinout/backups"/freqinout-backup-*.tar.gz
```

The installer backup covers the production Linux config roots, including:

```text
$HOME/.freqinout
$HOME/.freqinout/config
$HOME/.freqinout/runtime/single-rig/config
$HOME/.freqinout/runtime/multi-rig/config
```

## 6. Migration Coverage

The production upgrade must be treated as a full migration. Existing FIO data is handled in three ways:

| Area | How it is handled |
|---|---|
| Settings and configuration | The complete FIO config root is backed up, then existing `kv` settings remain in `freqinout.db` and radio/software settings are copied into the default multi-rig radio, operating profile, JS8 instance, Fast Light config, and VarAC node. |
| FIO databases | Existing `freqinout.db` and `freqinout_nets.db` stay in the production config folder and are schema-upgraded in place. Operator history, traffic, schedules, SOP data, JS8 Expect data, VarAC local data, SITREP data, propagation outcomes, and local NCS/operator records remain available in those databases. |
| Schedules | Existing single-rig daily/net schedule rows are preserved in their original tables and converted into a default multi-rig Frequency Plan assigned to the migrated radio. |
| Paths | FIO-known radio app paths, JS8 profile/log paths, VarAC DB/INI paths, FLDigi log/check-in paths, FLMsg/FLAMP/VarAC message paths, and VarAC BBS/vault paths are copied into the applicable multi-rig records when a matching field exists. |
| Referenced external data folders | The in-app pre-migration backup includes FIO-known referenced message, log, check-in, VarAC BBS, and vault data paths in addition to the FIO config root. Missing referenced paths are recorded in the backup manifest instead of being silently ignored. |
| GPG/encryption key references | FIO settings such as the GPG executable path, trusted fingerprints, and selected signing fingerprint remain in `freqinout.db` and are backed up with the config root. |
| Saved signing passphrases | Saved GPG signing passphrases are stored in the Linux OS credential store through `keyring`, not inside FIO settings. On the same Linux user account, the upgraded FIO should continue to read them by fingerprint. They are not copied into FIO backups and should not be exported into plain files. |

## 7. Launch the Upgraded App Against Production Config

For the in-place production upgrade, do not use the isolated fresh-install config root. Launch normally:

```bash
freqinout
```

Or launch directly from the upgraded install:

```bash
"$HOME/FreqInOut/venv/bin/python" -m freqinout.main
```

Do not set `FREQINOUT_CONFIG_DIR` unless production already used a custom config root.

## 8. Expected First Launch Behavior

On first launch, the multi-rig build should detect existing single-rig FIO usage and defer conversion. It should not silently create a first radio from production settings until you confirm Multi-Rig setup.

In Settings, run the Multi-Rig setup flow:

1. Open Settings.
2. Review the Multi-Rig setup prompt/card.
3. Preview Configure Automatically if available.
4. Confirm the first radio display name, radio model, operating plan, and detected software roles.
5. Click Set up Multi-Rig.

Before database migration, FIO creates an in-app pre-migration backup under:

```text
$HOME/.freqinout/backups/pre-multirig-YYYYMMDD-HHMMSS
```

If that backup fails, migration is blocked and production settings are left unchanged.

## 9. Confirm Multi-Rig Migration

After setup completes, confirm the settings database has multi-rig records:

```bash
sqlite3 "$HOME/.freqinout/config/freqinout.db" \
  "SELECT name, control_backend, use_flrig, use_fldigi, use_js8call, use_varac FROM device_profiles;"

sqlite3 "$HOME/.freqinout/config/freqinout.db" \
  "SELECT name, scheduler_enabled, use_launch_control FROM operating_profiles;"

sqlite3 "$HOME/.freqinout/config/freqinout.db" \
  "SELECT host, port, profile_path, directed_path, forms_path FROM js8_instances;"

sqlite3 "$HOME/.freqinout/config/freqinout.db" \
  "SELECT flrig_path, flrig_host, flrig_port, fldigi_path, fldigi_host, fldigi_port FROM fast_light_configs;"

sqlite3 "$HOME/.freqinout/config/freqinout.db" \
  "SELECT install_path, db_path, ini_path, incoming_path FROM varac_nodes;"

sqlite3 "$HOME/.freqinout/config/freqinout.db" \
  "SELECT name, source_refs_json, schedule_refs_json, frequency_refs_json, group_refs_json FROM frequency_plans;"

sqlite3 "$HOME/.freqinout/config/freqinout.db" \
  "SELECT device_profile_id, frequency_plan_id, assignment_state, created_by FROM assigned_plans;"
```

Confirm the migration marker:

```bash
sqlite3 "$HOME/.freqinout/config/freqinout.db" \
  "SELECT key, value FROM kv WHERE key IN ('multi_rig_migration_version', 'multi_rig_migration_deferred', 'multi_rig_migration_completed_at_utc');"
```

Expected:

- `multi_rig_migration_version` is at least `2`.
- `multi_rig_migration_deferred` is false/cleared after setup.
- A completed-at timestamp exists.
- If production had daily/net schedules, a migrated Frequency Plan exists and is assigned to the migrated radio.

## 10. Post-Upgrade Acceptance Checks

| Done | Check |
|---|---|
| `[ ]` | Installer backup archive exists under `$HOME/.local/state/freqinout/backups`. |
| `[ ]` | In-app pre-migration backup exists under `$HOME/.freqinout/backups`. |
| `[ ]` | The first migrated radio has the expected name and control backend. |
| `[ ]` | FLRig/FLDigi paths, hosts, ports, logs, and check-in paths migrated. |
| `[ ]` | JS8Call host, port, profile path, directed path, and forms path migrated. |
| `[ ]` | VarAC install path, DB path, INI path, and incoming path migrated if configured. |
| `[ ]` | Existing daily/net schedule rows are still present and are represented by an assigned Frequency Plan. |
| `[ ]` | Operator history tables in `freqinout_nets.db` still contain the expected row counts. |
| `[ ]` | GPG signing key fingerprint/trust settings remain visible, and any saved passphrase is still available from the OS credential store if it existed before. |
| `[ ]` | Launch control is disabled until deliberately re-enabled. |
| `[ ]` | Station Health does not show no-error waiting states as warnings. |
| `[ ]` | Normal workflows open: Settings, Control Center, Health Details, Messages, Map, and selected radio panes. |

## 11. Optional Row Count Verification

Before upgrade, capture counts:

```bash
for table in \
  operator_checkins \
  local_operator_checkins \
  local_ncs_checkins \
  local_operator_reports \
  daily_schedule_tab \
  net_schedule_tab
do
  sqlite3 "$HOME/.freqinout/config/freqinout_nets.db" \
    "SELECT '$table', COUNT(*) FROM $table;" 2>/dev/null || echo "$table missing"
done
```

Run the same command after migration. Counts should not decrease. A missing table means that feature had not created data in the single-rig installation.

## 12. Rollback Notes

If the app update fails before migration, use the installer backup archive and any moved non-git install folder reported by the installer.

If migration fails after the in-app backup succeeds, keep the backup path shown in Settings and capture:

```bash
cp "$HOME/freqinout-install.log" "$HOME/freqinout-install-upgrade-failure.log"
cp "$HOME/.freqinout/freqinout.log" "$HOME/freqinout-runtime-upgrade-failure.log" 2>/dev/null || true
```

Do not run repeated migrations against the same production config until the failure is understood.
