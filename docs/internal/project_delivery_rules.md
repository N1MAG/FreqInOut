# Project Multi-Model Delivery Rules

Status: authoritative standing project rule

## Authority And Scope

This contract governs every FreqInOut project review, specification, feature,
remediation, refactor, migration, test package, and integration. It applies even
when an individual specification does not repeat or link to it.

The maintainer provides standing authorization for the primary agent to manage
model selection and delegation without asking for approval for each package:

> Use multi-model delegation to control cost. Keep architecture, concurrency,
> migrations, and final integration review with the high-reasoning primary
> model. Delegate bounded UI, mechanical implementation, and focused test work
> to Terra, Luna, or Mini as appropriate. Review every delegated diff, preserve
> unrelated changes, run the specified acceptance tests, and update the
> specifications and work log. Report which model handled each work package. Do
> not begin the next slice until the current slice passes its exit gate. Before
> coding, divide the slice into work packages and tell the user which model will
> be used for each. Begin automatically unless a destructive migration or a
> decision requiring user input is identified.

This authorization is sufficient for the primary agent to choose models. Do not
ask the user to select or approve a model assignment unless model choice itself
would materially change product scope, safety, or an externally billed service
beyond the work already authorized.

## Required Workflow

Before changing code, tests, configuration, schema, runtime data, or release
artifacts:

1. Identify the governing specification and current slice or bounded work item.
2. Divide the work into independently reviewable packages.
3. Tell the user each package's scope and assigned model before coding begins.
4. State the acceptance tests and exit gate when the governing specification
   does not already define them.
5. Begin automatically unless work is blocked by a destructive migration or a
   material decision requiring user input.

Use these ownership boundaries:

- The high-reasoning primary model owns architecture; concurrency and lifecycle
  design; persistence and migration safety; security and destructive-operation
  review; cross-package integration; review and correction of every delegated
  diff; specification/work-log reconciliation; and the final exit-gate decision.
- Delegate bounded UI implementation, mechanical implementation, focused audit,
  fixture, and test work to `gpt-5.6-terra`, `gpt-5.6-luna`, or the available
  Mini-class/lowest-cost capable model according to complexity and risk.
- A package remains primary-owned when it cannot be isolated safely or touches
  architecture, concurrency, migrations, destructive data behavior, or final
  integration. State that assignment explicitly.
- Record the exact model identifier and reasoning effort actually used. Labels
  such as `Terra`, `Luna`, or `Mini` alone are not sufficient evidence.

For a small project change that does not justify multiple coding packages, the
primary model still assigns at least one bounded independent audit or focused
test-review package to a cost-appropriate delegate. Delegated agents must receive
explicit allowed scope, ownership boundaries, acceptance criteria, and a ban on
unrelated edits.

## Integration And Safety

- Inspect every delegated diff and test result before integration. Correct or
  reject work that violates architecture, safety, performance, UI, or scope.
- Preserve unrelated user changes. Never reset, overwrite, stage, commit, or
  include unrelated work merely to simplify integration.
- Run all acceptance commands required by the specification and slice plan.
  Record commands, results, skips, platform limitations, model ownership, and
  gate status in the governing specification when it changes and in
  `docs/internal/ui_regression_work_log.md`.
- Do not begin a successor slice until the current slice's documented exit gate
  passes. A failed, skipped, hardware-dependent, or unavailable check keeps the
  gate open unless the governing specification explicitly defines a separate
  implementation gate and external qualification gate.
- Never silently waive a gate. Record any external or operator-assisted evidence
  still required.
- Stop for user direction before a destructive or irreversible migration,
  deletion, overwrite, or production-data transformation, or when a material
  product, scope, security, or data-ownership decision lacks documented
  authority. Otherwise proceed automatically.

## Required Reporting

Progress and final reports must identify:

- every work package and its scope;
- the exact model and reasoning effort used for it;
- the primary-model diff review and any corrections;
- acceptance commands and outcomes;
- specification and work-log updates;
- the exit-gate result and any remaining external qualification.

Historical model assignments recorded in older specifications and evidence files
remain valid traceability. This contract supersedes their general delivery advice
when it is less strict, while stricter safety and acceptance requirements remain
in force.
