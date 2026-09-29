# Per-Radio Launch Control Gate Hotfix Specification

Status: `Awaiting maintainer pass approval`

Date: 2026-09-29

## Purpose

Make automatic and selected-radio startup deterministic and radio-oriented:
finish one radio's startup transaction before beginning the next, and do not
start companion applications for a radio whose configured control path cannot
produce a fresh, read-only radio-frequency readback.

## Observable problem

Launch Control already starts applications one at a time and waits for service
readiness. However, FLRig readiness currently means that FLRig's XML-RPC
service answers a version request. FLRig can satisfy that test while the
physical radio is powered off or disconnected. A failed FLRig row also blocks
only applications with an explicit FLRig dependency; unrelated heavy
applications such as VarAC or JS8Call can still start for the unavailable
radio.

The startup planner also applies dependency ordering by application name across
the station. Although normal saved rows usually appear radio-by-radio, that is
not a formal radio-stage guarantee and can allow one radio's emitted app name
to satisfy another radio's planning dependency.

## Required behavior

1. Active radios are ordered by existing `display_order`, then stable radio ID.
2. Each radio's launch rows are dependency-ordered within that radio. The
   configured control application is placed first when it is part of the
   startup bundle.
3. Launch Control completes or skips the current radio stage before considering
   the next radio.
4. The configured control application is started first when selected for
   startup. Existing exact-process and endpoint duplicate guards remain
   authoritative.
5. Before any remaining app for the radio starts, a worker-owned, fresh,
   read-only control check must return a positive frequency from the exact
   configured endpoint:
   - FLRig: XML-RPC `rig.get_vfo`;
   - RigCtlD: Hamlib `f` readback;
   - JS8Call: endpoint-scoped `RIG.GET_FREQ`/compatible frequency readback.
6. Manual-control and receive-only/manual observer profiles bypass the gate
   explicitly. No success is fabricated for an automated backend.
7. If the control check succeeds, the remaining radio apps continue through
   the existing sequential readiness and dependency behavior.
8. If the control app fails, times out, or fresh control readback cannot be
   obtained within the bounded gate timeout, every remaining row owned only by
   that radio is recorded as `blocked_radio_control`. The next radio stage then
   proceeds normally.
9. Deliberately shared launch identities retain the existing one-process
   deduplication and dependency rules. Sharing never changes a failed radio's
   gate state or authorizes its radio-unique applications. This hotfix does
   not split or migrate shared identities.
10. The same radio-stage rules govern FIO startup and the selected-radio
    **Start Startup Apps** action.

## Performance and safety contract

- No process walk, socket/XML-RPC call, database read, or control probe runs on
  the Qt GUI thread.
- The existing launch-owned process inventory, endpoint preflight, exact
  process identity, collision validation, self-launch guard, and cancellation
  behavior remain in force.
- The radio check is read-only. It never changes frequency, VFO, mode, PTT, or
  application configuration.
- A launch attempt requests fresh endpoint evidence and does not accept a prior
  cached success as proof for the current transaction.
- Control-app readiness is bounded to 30 seconds. A radio gate without a
  launchable control row is bounded to the existing 15-second endpoint
  preflight limit. No remaining row incurs another radio-control wait.
- The failure of one radio cannot stop, delay beyond its bounded gate, or
  contaminate another radio's stage.

## Operator feedback

Progress reports identify **Checking _radio name_ control**, **_radio name_
control ready**, or **Skipped—radio control unavailable**. The final summary
counts `blocked_radio_control` separately from application failures and normal
dependency blocks. Existing nonmodal cancellation remains available.

After powering on a skipped radio, the existing selected-radio **Start Startup
Apps** action is the retry path. Automatic retry loops are out of scope.

## Persistence and migration

No schema, migration, saved setting, launch bundle, or radio profile change is
permitted. Control-gate metadata is a transient projection from existing radio
profile fields into the immutable launch plan.

## Acceptance criteria

1. Two radios with heavy startup rows execute as complete, ordered radio
   stages rather than interleaving.
2. A responsive FLRig process with no positive radio frequency does not
   authorize FLDigi, VarAC, JS8Call, or helpers for that radio.
3. An already-running exact control instance with fresh frequency readback
   authorizes its radio without launching a duplicate.
4. A failed first radio produces `blocked_radio_control` for its remaining
   unique rows and the second healthy radio still launches.
5. Manual-control profiles retain existing startup behavior with an explicit
   bypass.
6. FLRig, RigCtlD, and JS8Call readback probes are exact-endpoint and worker
   owned.
7. Existing launch identity, JS8 default identity, shared dependency, VarAC,
   Windows subprocess, cancellation, and settings preview tests remain green.
8. No database or installer behavior changes.
9. Native two-radio operator testing remains required before approval.
