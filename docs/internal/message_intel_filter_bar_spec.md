# Message Intel Filter Bar Spec

## Scope

This contract applies to FIO multi-rig message intelligence surfaces. Messages owns
the first reusable UI implementation. Ops Center and Map may consume the same core
rollup/filter contract without re-parsing source-native messages.

## Problem

Message search is powerful only when an operator already knows what to search for.
CommStat, FIOSpotter, JS8Call, Mesh, VarAC, FLMSG, and FLAmp already project source
traffic into FIO topics, status, severity, group, source, and map hints. The UI must
surface those projections as active choices instead of forcing operators to guess
keywords.

## Data Contract

- `topics` are normalized message-intelligence topics. CommStat categories and
  FIOSpotter-derived report concepts both flow into this field.
- `status_bucket` is the operator-facing status family:
  - `red`: critical, urgent, alert, or explicitly red.
  - `yellow`: warning, important, degraded, caution, or explicitly yellow/orange.
  - `green`: explicitly green, ok, normal, all clear, or functioning.
  - `info`: message traffic with no actionable color/status evidence.
- `source_family` remains the normalized source family used by projection and delete
  policy.
- `focus` is the operator's domain view. Source, topic, status, group, age, and
  search refine that focus.

## Filter Contract

- Status and topic filters compose with existing focus, source, group, age, type,
  status, sender, recipient, map-context, and search filters.
- Changing focus clears topic/status refinements and rebuilds source choices for
  that focus.
- Clear Filters clears refinements inside the current focus. It does not reset the
  focus to All.
- A selected topic filter matches exact normalized topic labels, not loose text.
- A selected status filter uses `status_bucket`, not raw source text.
- Empty topic/status filters are no-ops.

## Focus-Aware Source Contract

- `All`: all available message sources.
- `New`: all available message sources.
- `FLMSG/FLAMP`: FLMSG, FLAmp.
- `Spotter`: FIOSpotter and JS8Call-carried Spotter reports.
- `CommStat`: CommStat.
- `JS8Call`: JS8Call, CommStat, FIOSpotter.
- `Mesh`: MeshCore, Meshtastic, Mesh.
- `VarAC`: VarAC.
- `BBS`: BBS.

## UI Contract

- The Intel Filter Bar appears above the message table and below the source/group
  funnel controls.
- It uses compact clickable chips, not a large taxonomy panel.
- Status chips appear first and use stable labels: `Red`, `Yellow`, `Green`, `Info`.
- Topic chips show only active topics in the current view context. They are ordered
  by worst status, then count, then taxonomy order.
- Chip labels include counts, for example `Yellow 4` and `Power 7`.
- Active chips use primary styling. Red/yellow/green chips retain their semantic
  color role.
- If there is no projected intelligence in the current context, the bar collapses.
- Search remains available for exact keyword/callsign/grid work, but it is not the
  primary discovery path.

## Reuse Contract

The core rollup must be independent of Qt. It accepts message-like rows and returns
counts/chips that Messages, Ops Center, and Map can render differently while
sharing identical topic/status semantics.

Ops Center should reuse this contract for summary tiles:

- status totals across recent traffic.
- topic totals by worst status.
- actions to open Messages with the same topic/status/group/source filters.
- actions to open Map with equivalent topic/status/geography filters.

## Acceptance

- CommStat StatRep categories appear as topic chips when present.
- FIOSpotter report topics appear as topic chips when present.
- Selecting a status chip filters messages to that operator-facing status bucket.
- Selecting a topic chip filters messages to that exact topic.
- Selecting focus `JS8Call` offers JS8Call, CommStat, and FIOSpotter source
  refinements.
- Selecting focus `Mesh` offers only mesh-family refinements when present.
- Clear Filters clears selected topic/status chips without changing focus.
