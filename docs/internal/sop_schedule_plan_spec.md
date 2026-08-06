# SOP Schedule Plan Spec

## Intent

SOP scheduling becomes a first-class operational plan: where to be, when to be there,
what to do, and which radio can safely support it. It consumes HF Daily, HF Nets,
Net Resources, and one or more active SOPs, but it may diverge from those sources
and be saved as its own assignable object.

## Objects

### Unified Operational Projection

The operational projection is a side-by-side day view. It does not collapse every
source into a single winner. Instead it preserves simultaneous lanes so operators
can see multi-group and multi-radio obligations.

Sources:

- `HF`: daily baseline rows.
- `NET`: scheduled HF Net rows.
- `NET_RESOURCE`: known net resources not necessarily folded into HF Nets.
- `SOP`: SOP schedule layer rows.

Lane keys:

- `radio:<id>` when a row is explicitly assigned to a radio.
- `sop:<profile_id>` for SOP rows without radio assignment.
- `group:<group_name>` for group-scoped HF/Net/resource rows.
- `station` when no stronger identity exists.

Each lane has hourly cells for the projected day. Cells retain every matching
entry. More than one entry in a cell is a contention signal, not a hidden overwrite.

### SOP Schedule Plan

An SOP Schedule Plan is saved through the existing `frequency_plans` table with
category `sop_schedule`. It carries structured `schedule_refs` and provenance in
the same format RF Guard and Schedule Assignment already understand.

It is saved as one coherent object even when it contains multiple `radio:<id>`
lanes. RF Guard preflight validates each radio lane against that lane's entries
so one radio is not warned or blocked for another radio's work.

Expected payload fields:

- `category`: `sop_schedule`
- `source_refs`: includes `hf_daily`, `hf_nets`, `net_resources`, and/or `sop`
- `schedule_refs`: structured operational entries
- `frequency_refs`: unique band/frequency refs from entries
- `group_refs`: operating groups represented by the plan
- `notes`: JSON summary for lane counts, source counts, and generated timestamp

## RF Guard

Saving an SOP Schedule Plan may happen before radio assignment, but assignment must
run RF Guard when enabled. When a radio context is available, preflight should run
before save. Multi-radio plans must surface:

- unsupported antenna band
- same-band overlap groups
- close-frequency guard groups
- simultaneous TX risks where known

Known Net Resources are included when they are not already represented by HF Net
rows. If an HF Net row carries the same resource identity or the same
day/time/frequency/net signature, the resource is treated as already folded into
the schedule.

Recurring resources and SOP layers must honor `Weekly`, `Daily`, `Periodic`, and
`Bi-Weekly` rules for the projected week. Bi-weekly rows use
`biweekly_offset_weeks` against the ISO week number.

## UI Direction

The target UI mirrors FreqPlanner’s cell grid but adds lanes. Operators should be
able to scan the full day and answer:

- Which group/SOP/radio is active?
- What frequency or net should I monitor?
- What action should I take?
- Is RF Guard clean, warning, or blocked?

Clicking a cell opens an inspector. If the entry is source-backed, the inspector
offers source navigation. If the entry is plan-local, it edits the SOP Schedule
Plan entry directly. Resource-backed edits must distinguish “update this plan
only” from “update the master resource.”

For the first editable slice, saved `sop_schedule` plans render in SOP Lanes view
when selected in FreqPlanner. Editing a cell updates the selected plan only and
does not mutate HF Daily, HF Nets, SOP Builder, or master Net Resource rows. The
save path still runs RF Guard preflight, including per-radio lane validation and
same-plan sibling lane checks.

For resource-backed entries with a `resource_id`, the operator is prompted to
save the plan-local edit only or also update the master Net Resource. Master
resource updates are explicit, happen only after the SOP Schedule Plan save passes
RF Guard, and mark the resource as manually updated from an SOP Schedule Plan.

## Implementation Slices

1. Core operational projection and SOP Schedule Plan payload builder.
2. Draft SOP Schedule Plan creation from HF Daily + HF Nets + Net Resources + SOP.
3. RF Guard preflight for selected radio context and per-radio lanes.
4. Read-only lane grid in SOP Scheduler/FreqPlanner style.
5. Cell inspector and plan-local edits.
6. Optional master-resource update workflow.
