# VarAC BBS Location Summary and Copy Target Spec

## Scope

FreqInOut should treat VarAC BBS as enabled only when the synced VarAC BBS setting is on. Control Freq and Messages must avoid implying files are being served when VarAC BBS is disabled.

## Control Freq Summary

The Control Freq BBS row shows the live published BBS folder first:

- Disabled BBS: `VarAC BBS | 0 | Disabled`
- Missing or unset folder: `VarAC BBS | 0 | Not configured` or `Missing directory`
- Enabled live folder: live file count plus archive status.

When Managed BBS Vault is enabled, the detail column also summarizes enabled managed locations. The row remains compact and uses a full tooltip/searchable detail for the complete state.

Definitions:

- `Live`: files currently in the VarAC BBS folder.
- `Due now`: files at or beyond the configured auto-archive age.
- `Due soon`: files inside the final 24 hours before the archive threshold.
- `Location`: a managed vault source folder. Location counts include files in subfolders so the operator can see material that would be preserved by mirrored archive handling.

## Mirrored Archive

Archive destinations should preserve origin context:

- live BBS root files archive under `Archive/live/`
- managed location files archive under `Archive/locations/<location-name-or-id>/`
- unknown or legacy files may use `Archive/legacy/`

This avoids flattening multiple managed locations into one archive namespace and keeps subfolder structure intact when a source folder contains nested material.

## +BBS Copy Target Selection

The Messages `+BBS` action is available only when VarAC BBS is enabled and the selected row is a file-backed FLMSG, FLAMP, or VarAC message.

If there is only one valid BBS target, `+BBS` copies directly. If there are multiple targets, `+BBS` opens a compact target selector:

- Published BBS: writes directly to the live VarAC BBS folder.
- Managed Location: writes to a managed vault location source folder.

The default managed location is preselected when available. Copying to a managed location does not imply that location is currently published; the confirmation must say the file was copied to a managed location and should be published/refreshed before it is visible in VarAC BBS.
