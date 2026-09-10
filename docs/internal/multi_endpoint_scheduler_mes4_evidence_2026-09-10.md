# Multi-Endpoint Scheduler MES-4 Evidence — 2026-09-10

Status: automated exit gate passed; MES-5 may begin. No physical SDR adapter is
claimed as FIO-verified by this scheduler-core package.

## Scope And Safety

MES-4 integrates receive-only desired states into the same coordinator and
failure-isolated endpoint lanes used for transceivers. It does not attach or
claim a production SDR++/SDRconnect/SDRangel hardware adapter; those exact
hardware/application/OS gates remain in the SDR receiver-control plan.

The implementation:

- adds a Qt-free `ReceiverControlClient` contract and immutable capability,
  identity, command, and state objects with no PTT or transmit method;
- provides a zero-I/O `ManualReceiverControl` fallback that never claims tuning
  or readback;
- gives a verified automated observer a normalized application-adapter,
  host/port, and canonical target endpoint identity;
- keeps disabled, manual, incomplete, and unverified observers as independent
  manual endpoints with no worker or endpoint I/O;
- branches receive-only schedule rows before `_apply_schedule_entry`, so they
  cannot enter FLRig/JS8/RigCtlD transmit-capable scheduling or PTT logic;
- performs receiver tune and readback on the same serialized endpoint worker,
  publishes target-specific cached status, and falls back to Manual tuning on
  failure;
- retains central RF/antenna/frontend arbitration for configured shared
  resources without acquiring PTT;
- uses target-qualified ownership so two VFO/device targets on one application
  are independent, while aliases for one target have one writer and conflicting
  desired states fail closed; and
- replaces the observer UI/runtime foreground TCP reachability probe with
  truthful configuration/manual wording and cached-verification expectations.

## Additive Configuration Migration

`device_profiles` gains nullable/defaulted fields for receiver application,
adapter, canonical target, control enablement, verification state, and bounded
verification evidence. Existing rows migrate to `manual`, disabled, and empty
evidence. Enabling control is rejected unless the profile is an observer, uses
a known receive-only adapter, has host/port/target, and has passed persisted
tune/readback verification.

The migration test removes the six fields from a populated configuration clone,
runs startup-owned schema repair, and verifies the existing profile and endpoint
data remain unchanged while safe defaults are restored. There is no destructive
or data-rewriting migration.

## Delegation And Review

- High-reasoning primary model: receiver-lane architecture, additive migration,
  coordinator/scheduler/runtime integration, safety and truthfulness review,
  delegated diff review, complete acceptance run, and gate decision.
- `gpt-5.6-terra` high: read-only observer/SDR/scheduler seam audit; a separate
  bounded package implemented the Qt-free receive-only contract and its four
  focused tests.
- `gpt-5.6-luna` high: seven deterministic five-endpoint, hung-SDR, no-transmit,
  manual fallback, alias/conflict, reconnect, and shutdown tests.

Primary integration review added eight production-path tests for receiver
tune/readback, observer routing isolation, runtime ownership/close, safe config
validation, manual fallback, canonical target identity, and additive migration.

## Acceptance Evidence

Focused receiver/MES-4 suite:

```text
QT_QPA_PLATFORM=offscreen .venv/bin/python -m pytest -q \
  tests/test_receiver_control.py \
  tests/test_scheduler_multi_endpoint_mes4.py \
  tests/test_scheduler_receiver_lane_integration.py
19 passed in 0.18s
```

Integrated scheduler, runtime, multi-rig, SOP, and station-safety gate:

```text
QT_QPA_PLATFORM=offscreen .venv/bin/python -m pytest -q \
  tests/test_receiver_control.py \
  tests/test_scheduler_multi_endpoint_mes4.py \
  tests/test_scheduler_receiver_lane_integration.py \
  tests/test_scheduler_*.py \
  tests/test_station_runtime_*.py \
  tests/test_multi_rig_wave4_phase_e_slice1.py \
  tests/test_multi_rig_wave4_phase_e_slice2.py \
  tests/test_multi_rig_wave4_phase_f_slice1.py \
  tests/test_multirig_shared_state_persistence.py \
  tests/test_busy_evidence_service.py \
  tests/test_ptt_conflict_service.py \
  tests/test_condition_sop_*.py
241 passed, 5 skipped in 7.59s
```

The skips are existing platform/optional-environment guards; all MES-4 tests ran
on this host. Python compilation and `git diff --check` also passed.

## Exit Checklist

- three transceiver plus two receiver intents coordinate concurrently: pass;
- a hung receiver does not delay a transceiver or peer receiver: pass;
- receive-only runtime and scheduler paths expose no PTT/transmit operation:
  pass;
- manual/no-API/unverified fallback performs zero endpoint I/O: pass;
- same-target aliases have one owner and conflicting intents fail closed: pass;
- distinct targets on one receiver application remain independent: pass;
- receiver failure can reconnect without affecting peer lanes: pass;
- shutdown is bounded and suppresses late receiver callbacks: pass;
- observer runtime never creates a transceiver client: pass;
- foreground observer status no longer probes an arbitrary TCP port: pass;
- additive migration preserves existing profiles and supplies safe defaults:
  pass; and
- destructive migration or production-data rewrite: none.

MES-5 may now address lifecycle churn, diagnostics, long synthetic soak, and
production qualification. Physical hardware evidence cannot be inferred from
the fake-adapter gate and must be recorded honestly when available.
