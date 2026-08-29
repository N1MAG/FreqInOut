# VarAC BBS Location Summary and Copy Target Spec

## Scope

FreqInOut should treat VarAC BBS as enabled only when the synced VarAC BBS setting is on. Control Freq and Messages must avoid implying files are being served when VarAC BBS is disabled.

Multi-rig BBS ownership uses a safer two-layer model:

- `Managed BBS Library`: shared FIO-managed source content, locations, helper
  files, sweeper rules, and retention/access policy.
- `Live BBS`: the radio-specific folder that one VarAC instance serves on the
  air.

FIO may copy or publish library locations into FIO-A, FIO-B, or another selected
radio's live BBS folder, but separate VarAC instances should not share one live
BBS folder. This preserves simultaneous connections: one caller can browse
FIO-A's live BBS while another browses FIO-B's live BBS, and both are projected
from the shared library without racing over one mutable folder.

## Control Freq Summary

The Control Freq BBS row shows the live published BBS folder first:

- Disabled BBS: `VarAC BBS | 0 | Disabled`
- Missing or unset folder: `VarAC BBS | 0 | Not configured` or `Missing directory`
- Enabled live folder: live file count plus archive status.

When Managed BBS Library is enabled, the detail column also summarizes enabled managed locations. The row remains compact and uses a full tooltip/searchable detail for the complete state.

Definitions:

- `Live`: files currently in the VarAC BBS folder.
- `Due now`: files at or beyond the configured auto-archive age.
- `Due soon`: files inside the final 24 hours before the archive threshold.
- `Location`: a managed vault source folder. Location counts include files in subfolders so the operator can see material that would be preserved by mirrored archive handling.

## Mirrored Archive

Archive destinations should preserve origin context:

- live BBS root files archive under `Archive/live/`
- VarAC incoming files archive under `Archive/incoming/`
- VarAC outgoing files archive under `Archive/outgoing/`
- managed location files archive under `Archive/locations/<location-name-or-id>/`
- unknown or legacy files may use `Archive/legacy/`

This avoids flattening multiple managed locations into one archive namespace and keeps subfolder structure intact when a source folder contains nested material.

## FIO BBS File Management

Once a VarAC radio profile has BBS, incoming, outgoing, and archive paths
configured, FIO should be able to manage those files directly:

- archive or delete individual live BBS files;
- archive or delete incoming VarAC files;
- archive or delete outgoing VarAC files;
- archive or delete files copied into managed BBS Library locations;
- browse archived BBS files without implying they are still being served.

Manual file actions should be explicit and confirm destructive operations.
Archive actions should preserve relative folder structure whenever the source
folder is known. Removing a copied BBS artifact from a managed location or live
BBS folder must not delete the original FLMsg, FLAmp, or VarAC source message.

## +BBS Copy Target Selection

The Messages `+BBS` action is available only when VarAC BBS is enabled and the selected row is a file-backed FLMSG, FLAMP, or VarAC message.

If there is only one valid BBS target, `+BBS` copies directly. If there are multiple targets, `+BBS` opens a compact target selector:

- Published BBS: writes directly to the live VarAC BBS folder.
- Managed Location: writes to a managed library location source folder.

The default managed location is preselected when available. Copying to a managed location does not imply that location is currently published; the confirmation must say the file was copied to a managed location and should be published/refreshed before it is visible in VarAC BBS.
