# JS8 Native-Default Identity And Multi-Endpoint Status Hotfix

Status: `Awaiting maintainer pass approval`

Governing contracts:

- `project_delivery_rules.md`
- `guided_radio_software_configuration_spec.md`
- `multi_endpoint_scheduler_concurrency_spec.md`

## Operator reproduction

An upgraded station retains its working native-default JS8Call profile on
`127.0.0.1:2442`. A second radio uses the same executable with
`--rig-name FT-710` on `127.0.0.1:2443`. When the named process is already
running, FIO credits it as the argument-free default process, suppresses the
default launch, and reports that the exact process is running while port 2442
is not ready. Repeated status checks may then report that the shared `js8net`
connection belongs to a different endpoint.

## Scope and invariants

This hotfix does not migrate, copy, rename, rewrite, or relink any JS8Call
profile, settings file, message directory, or database. The upgraded primary
radio continues to use the native default JS8Call namespace and its saved
executable and endpoint.

For a legacy/native-default JS8 launch identity:

- the configured executable must match;
- the observed command line must contain no `-r` or `--rig-name` selector;
- split and equals selector forms are equivalent; and
- a bare, repeated, or conflicting rig-name selector is not a default identity.

The empty-argument executable-match behavior remains unchanged for non-JS8
applications. Explicitly named JS8 instances retain their current
`--rig-name <radio>` identity.

JS8 status and control remain endpoint-scoped through
`JS8ApiClientRegistry`. A fallback status probe must receive the requested
port. The process-global legacy `js8net` fallback may serve only the endpoint
that owns it; a client for another endpoint must report unavailable rather than
credit, control, or warn about the sibling endpoint.

## Implementation boundary

1. Extend process matching with an optional excluded-option contract.
2. Apply `-r`/`--rig-name` exclusion only to the legacy/native-default JS8
   launch and selected-radio status identity.
3. Preserve the requested JS8 port when constructing a fallback control
   client.
4. Expose the process-global `js8net` fallback endpoint and reject cross-port
   fallback before invoking it.
5. Downgrade fallback-unavailable diagnostics to endpoint-specific debug
   evidence; do not emit the misleading repeated warning.

## Acceptance gate

Automated acceptance must prove:

- an argument-free native-default process matches;
- every supported rig-name selector form fails the default match;
- a named sibling cannot suppress the default launch or make its selected-radio
  status appear running;
- ports 2442 and 2443 receive distinct native API clients;
- a fallback status probe for 2443 cannot silently probe 2442; and
- a global fallback owned by 2442 is neither invoked nor warned about by a
  client for 2443.

The implementation gate requires focused launch/status/API tests, broader JS8
and scheduler regression tests, changed-file compilation, and
`git diff --check`. The operator gate remains open until the maintainer runs the
two configured JS8 instances and confirms that FIO launches the native default
on 2442 while the named sibling remains on 2443 without the shared-endpoint
warning.
