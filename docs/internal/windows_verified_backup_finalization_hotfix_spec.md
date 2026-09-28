# Windows Verified-Backup Finalization Hotfix

Status: `Awaiting maintainer pass approval`

Governing contract: `project_delivery_rules.md`

## Operator reproduction

On an existing Windows station, `py -3.11 install_freqinout.py` locates the
profile under `%LOCALAPPDATA%\FreqInOut`, validates its databases, copies the
configuration into `backups\.pre-install-*`, verifies every copied hash and
database, and writes the manifest. Windows then repeatedly raises
`[WinError 5] Access is denied` while renaming that verified directory to the
preferred `pre-install-<timestamp>` name. The installer currently deletes the
verified staging backup and blocks installation even though only its friendly
folder name failed.

## Safety contract

The preferred backup-directory name is presentation metadata, not backup
integrity. The installer may continue with the existing staging name only when
all of these conditions are already true:

- every source database passed read-only SQLite validation;
- the entire configuration directory was copied;
- source and backup hashes match;
- every copied database passed read-only SQLite validation; and
- the backup manifest was written with `verification_status` set to
  `verified`.

Only `PermissionError` from the final same-directory rename qualifies for the
fallback. The installer first retries with short bounded backoff. If access is
still denied and the staging directory remains present, it prints an explicit
warning, reports that directory as the verified backup, records its exact path
in the installation receipt, and continues.

Copy failures, hash mismatches, database failures, manifest-write failures,
missing staging data, and non-permission rename errors remain fatal and retain
the existing cleanup behavior. The installer does not alter Windows ACLs, ask
the operator to disable security software, run elevated, or weaken any database
or hash check.

## Implementation boundary

1. Add one backup-finalization helper with bounded `PermissionError` retries.
2. Preserve and return the verified staging directory after persistent access
   denial.
3. Mark completed manifests as verified and rely on the existing receipt field
   to record the returned path.
4. Keep all other installation, backup, dependency, and launcher behavior
   unchanged.

## Acceptance gate

Automated acceptance must prove normal finalization, transient access-denial
recovery, persistent access-denial fallback, actual-path receipt recording,
content/hash/database validity of the retained backup, and fatal cleanup for a
non-permission finalization error. The broader installer and launcher contract
tests, changed-file compilation, and `git diff --check` must pass.

The operator gate remains open until the affected Windows station pulls the
private hotfix, reruns `py -3.11 install_freqinout.py`, sees either the preferred
or retained verified backup path followed by `Installation verified`, and then
starts FIO through `start-freqinout.cmd` without elevation.
