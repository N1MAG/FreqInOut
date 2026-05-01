---
description: Dual-agent Cascade and Codex workflow for dynamic implementation and review
---
# Dual-Agent Workflow: Cascade + Codex

Use this workflow when you want Cascade and Codex to contribute to the same FreqInOut task without colliding on edits.

## Purpose

Use Cascade as the primary investigator and implementer.
Use Codex as an active reviewer and challenger.
Keep the interaction dynamic, structured, and iterative.

## Rules

1. Use the prompt shell at `/Users/bill/RadioCode/FREQINOUT_PROMPT_SHELL.md`.
2. Use a task file under `/Users/bill/RadioCode/WORK` when possible.
3. Cascade owns direct edits unless you intentionally place Codex on a separate branch or worktree.
4. Do not have Cascade and Codex edit the same file concurrently.
5. Choose the primary worktree based on the task:
   - For shared fixes, start in `FreqInOut-single-rig` and then port to `FreqInOut-multi-rig`.
   - For `FreqInOut-multi-rig`-specific work, start in `FreqInOut-multi-rig` and do not backport unless explicitly requested.
6. Every round should produce a clear handoff back to the other agent.

## Recommended operating model

- Cascade investigates authoritative code paths.
- Cascade proposes the root cause, likely impact, and implementation plan.
- Codex challenges assumptions before or after the patch, depending on task size.
- Cascade implements and responds to Codex findings.
- Codex performs a final review pass.
- Cascade produces the final summary.

## Collaboration loop

### 1. Prepare the task

Create or update a task file using the prompt shell.
Include at minimum:
- Primary target
- Secondary target
- Propagation
- Mode
- Release target override if needed
- Task
- Intent
- Constraints
- Relevant areas
- Tests requested

### 2. Send the task to Cascade first

Use this prompt with Cascade:

```text
You are Cascade, the primary implementer for this FreqInOut task.

Use prompt shell:
`/Users/bill/RadioCode/FREQINOUT_PROMPT_SHELL.md`

Task file:
`[PATH_TO_TASK_FILE]`

Role:
- Investigate authoritative code paths first
- Implement the smallest correct fix
- If the task is shared across both worktrees, apply in `FreqInOut-single-rig` first and then port equivalent changes to `FreqInOut-multi-rig`
- If the task is specific to `FreqInOut-multi-rig`, implement there directly and do not backport unless explicitly requested
- Keep behavior aligned unless architecture requires an explicit adaptation

Required outputs before or during implementation:
1. Root cause summary
2. Likely affected files/modules
3. Risk areas / downstream consumers
4. Implementation plan
5. Validation plan

Collaboration requirements:
- Assume Codex will review your work critically
- Surface assumptions explicitly
- Call out any uncertainty before making invasive changes
- After implementing, produce a concise patch summary for Codex review

Ask Codex to specifically review for:
- missed edge cases
- missed consumers/callers
- schema/config/release-note impact
- lint/type/test gaps
- whether the fix is truly root-cause and minimal

Deliverables for Codex handoff:
- changed files
- root cause
- exact behavior changed
- validation performed
- anything intentionally deferred
```

### 3. Hand Cascade output to Codex

Use this prompt with Codex:

```text
You are Codex, acting as a second engineer reviewing a FreqInOut patch produced by Cascade.

Use prompt shell:
`/Users/bill/RadioCode/FREQINOUT_PROMPT_SHELL.md`

Task file:
`[PATH_TO_TASK_FILE]`

Review inputs:
- Cascade root cause summary
- Cascade implementation summary
- changed files
- diff or patch summary
- validation performed

Your job:
Challenge the implementation actively and dynamically.

Review goals:
1. Confirm or challenge root cause
2. Find missed callers, consumers, or propagation requirements
3. Identify DB/schema/config/migration impact
4. Identify UI consistency or cross-worktree mismatch
5. Suggest targeted lint, type-check, tests, or manual verification
6. Propose a safer or smaller alternative if the implementation is broader than necessary
7. Identify anything that should be added to changelog/release notes

Response format:
- Confirmed strengths
- Findings / concerns
- Severity for each finding: `[high | medium | low]`
- Recommended action
- Suggested validation additions
- Final recommendation:
  - `accept`
  - `accept with follow-ups`
  - `revise before merge`

Important constraints:
- Do not rewrite the feature unnecessarily
- Focus on correctness, regression risk, and maintainability
- Be explicit, critical, and actionable
```

### 4. Return Codex findings to Cascade

Ask Cascade to respond to each finding with one of:
- accepted
- rejected with rationale
- deferred with rationale

If Cascade accepts changes, let Cascade make the follow-up patch.
If the task is shared, Cascade should then port the accepted changes to the secondary worktree if needed.

### 5. Run a final Codex pass

After follow-up changes, ask Codex for one last review of:
- final changed files
- final behavior summary
- validation evidence
- release-note or changelog impact
- remaining risks

## Handoff template

Use this structured handoff between Cascade and Codex:

```text
Task:
[short task title]

Task file:
[PATH]

Release target:
[1.2.3 / override]

Propagation:
[shared / isolated]

Mode:
[implement / investigate]

Primary worktree:
[FreqInOut-single-rig / FreqInOut-multi-rig]

Secondary worktree:
[FreqInOut-multi-rig / FreqInOut-single-rig / blank]

Root cause:
[brief explanation]

Changed files:
- [worktree] [path]
- [worktree] [path]

Implementation summary:
- [change 1]
- [change 2]
- [change 3]

Behavior intentionally unchanged:
- [item]
- [item]

Potential risk areas:
- [risk]
- [risk]

Validation already run:
- [lint/type/test/manual/syntax]
- [result]

Validation not yet run:
- [check]
- [reason]

Questions for Codex:
- Did I miss any downstream consumers?
- Is this truly the smallest root-cause fix?
- Are schema/config/release-note implications fully covered?
- What additional tests or lint checks would you require?
```

## Dynamic review guidance

To keep the interaction dynamic rather than one-pass:

- Ask Codex to challenge assumptions, not just approve code.
- Ask Cascade to answer Codex findings point by point.
- Use at least two rounds for medium or high-risk tasks.
- For low-risk tasks, one implementation round and one review round is usually enough.
- If both agents are allowed to patch, put them on separate branches or worktrees and compare results before merging.

## Final expected output

The final implementation owner should provide:
- root cause
- files changed by worktree
- fix summary
- validation summary
- release/changelog impact
- remaining risks or deferred items

## Notes for FreqInOut

- For shared fixes, prefer `FreqInOut-single-rig` as the authoritative source and then port to `FreqInOut-multi-rig`.
- For `FreqInOut-multi-rig`-specific tasks, use `FreqInOut-multi-rig` as the primary worktree and do not backport unless explicitly requested.
- Keep behavior aligned for shared areas unless multi-rig architecture requires a deliberate adaptation.
- Include lint, type-check, test, or manual verification status in the final summary.
