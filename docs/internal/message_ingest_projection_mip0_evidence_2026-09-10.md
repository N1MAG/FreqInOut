# Message Ingest Projection MIP-0 Evidence

Date: 2026-09-10

Status: exit gate passed; characterization only, with no production behavior
change

## Baseline

The production-shaped fixture contains 5,000 CommStat records, 5,000 SitRep
records, 1,000 Spotter records, representative JS8 and VarAC records, and 551
file records. Deterministic tests reproduce the September 10 failure shapes:

- one additional JS8 row changes the aggregate fingerprint and replays the
  current 5,000-row window;
- a normal message bundle invokes schema assurance for the outer projector and
  again for source, message, and reference writes;
- file-scan completion runs cache, BBS, observation, projection, VarAC, table,
  and signature follow-up work on the Qt thread before a queued heartbeat;
- an exclusive SQLite writer causes the projector to fail with a lock error;
- a mid-window exception rolls back projection and checkpoint state, and a
  later invocation can replay successfully;
- an invalid Unicode surrogate in a file path escapes the current boundary;
  and
- listing device profiles invokes runtime-primary normalization and issues an
  `UPDATE` from a nominal read path.

Static SQL-path accounting found that projection schema assurance performs 28
schema or introspection statements per invocation. For the observed 11,957-row
production projection, 5,000 artifact-bearing CommStat bundles and 6,957
standard bundles produce a lower bound of 1,144,556 schema/introspection
statements before projection DML and per-message Ops indexing.

## Gate Evidence

Command:

```text
./.venv/bin/python -m pytest -q tests/test_message_ingest_mip0_characterization.py
```

Result: 8 passed in 1.24 seconds.

`git diff --check` also passed. The fixture and tests intentionally capture the
pre-remediation behavior; later packages replace these expectations with the
bounded contracts while retaining the production shape.

## Ownership

- High-reasoning primary model: package boundaries, production-evidence
  correlation, concurrency/migration safety review, delegated-diff review, and
  exit-gate decision.
- `gpt-5.6-terra` high: core projection/SQLite and UI/thread read-only audits.
- `gpt-5.6-luna` high: bounded production-shaped characterization fixtures and
  focused test execution.

No destructive migration, source-data rewrite, or unrelated-file change was
performed.
