# FIO Task-Oriented Workspace Design Guideline

Status: authoritative execution guideline for new and redesigned operator-facing
workspaces

## Authority And Scope

This guideline applies whenever an FIO tab, dialog, drawer, editor, or workflow
is newly created or meaningfully redesigned. It converts the product principles
in `multirig_product_ui_contract.md` into repeatable screen-design and acceptance
rules.

It does not replace that product contract, `project_delivery_rules.md`, or an
applicable feature, safety, persistence, or concurrency specification. On a
conflict, the product contract and the stricter feature or safety requirement
win.

A narrowly scoped defect or copy correction does not require a complete screen
redesign. It must still avoid introducing a prohibited pattern from this
guideline. When a change moves sections, changes the normal task sequence,
introduces a new editor/list relationship, or materially changes responsive
behavior, treat it as a redesign and apply the complete guideline.

## Design Objective

An FIO screen is successful when an operator can quickly answer:

1. What is this workspace for?
2. What state or item am I working with?
3. What should I do now or next?
4. Why is that action available, recommended, blocked, or unnecessary?
5. What will happen when I commit the action?

The UI should present an operator task, not reproduce a database row, settings
object, protocol packet, or internal state machine. FIO may retain extensive
technical evidence, but the normal view should translate it into operational
meaning and reveal detail when requested.

The Station Control Bar continues to own concise **Where** and **When** context.
The active workspace should use its space for **What** and **Why**, showing
task-specific Where/When detail only when the operator needs it to decide or act.

## Required Redesign Brief

Before implementation, every redesigned workspace records the following in its
governing specification or implementation plan:

- **Primary operator task:** one sentence beginning with an operator verb.
- **Starting context:** what the operator already selected or knows on entry.
- **Completion outcome:** the observable result of completing the task.
- **Task sequence:** the normal ordered decisions and actions.
- **Primary action:** the visually dominant commit or next-step action, if one
  exists.
- **Essential state and Why:** what must remain visible to support a safe
  decision and where its explanation/evidence is found.
- **Secondary and advanced work:** what remains available without occupying the
  primary scan path.
- **Workspace archetype:** which pattern below is used and why.
- **Responsive behavior:** wide, medium, and compact reading order, scroll
  ownership, and what progressively discloses.
- **Shared-theme and component reuse:** which existing theme tokens, style
  helpers, controls, and layout helpers are reused; identify any missing
  semantic treatment that must be added centrally before the screen uses it.
- **Performance boundary:** the bounded read model and the events that may
  refresh it; render, resize, paint, selection, and typing paths remain I/O-free.

If the team cannot state the primary task and normal sequence clearly, it is not
ready to implement the layout.

## Workspace Archetypes

Choose the pattern that matches the task. Do not force every screen into one
layout.

### Guided workflow

Use for compose, import, launch/configuration assistants, destructive changes,
and other staged operations.

Normal sequence:

`Context → choices → work/content → review → commit`

Show completed context concisely. Keep the current decision and the next action
prominent. Put validation beside the decision it explains. A sticky review or
commit footer is appropriate when the action would otherwise scroll out of
reach.

### Editor above bounded list

Use when the editor is compact, the fields benefit from full width, and the
operator normally edits one item before comparing or selecting another.

The editor uses its natural height. The list receives all remaining space. On a
wide screen, related editor sections may form balanced columns; on compact
screens they stack in task order. Do not allow a short editor to consume the
whole viewport or push the list entirely below the normal desktop fold.

### Dominant table with contextual inspector

Use when scanning, comparing, selecting, or monitoring many rows is the primary
task. The table remains dominant. The inspector may sit beside it on a wide
screen and stacks below it when either side would become difficult to read.

The inspector must use natural, top-packed height. Its size hints must not force
the page wider or scatter controls vertically. Selection changes update the
inspector from already-loaded data whenever possible.

### Operations or monitor surface

Use for Ops Center, Activity, health, traffic, and time-sensitive state.

Lead with the highest-priority current action or exception, followed by a
bounded summary. History and raw evidence are secondary. Stable state should
look calm; polling must not repeatedly reorder, resize, flash, or rebuild the
screen without a meaningful state change.

### Configuration by operator question

Use for settings, policies, software administration, and resource ownership.

Group controls by the question the operator is answering, such as `Who may
ask`, `Where it applies`, `Safety exceptions`, or `Launch behavior`. Do not use
persistence or schema order as the visible organization. A consuming workflow
selects reusable configuration and shows its resolved summary; full editing
belongs in the configuration's owner workspace with a clear `Manage` route.

## Information Hierarchy And Scan Path

Every screen needs one obvious reading path:

1. concise workspace purpose;
2. selected context and meaningful current state;
3. primary work surface;
4. review/Why information needed for the decision; and
5. primary completion or next-step action.

Apply these rules:

- Present context once. Do not repeat the same radio, target, form, policy,
  status, or schedule in a header, banner, editor, and footer.
- Use semantic chips or a compact summary for established context. Raw keys,
  source IDs, compatibility flags, ports, and storage paths belong in detail or
  diagnostics unless the operator must act on them.
- Use proximity, alignment, restrained contrast, and short section titles to
  establish groups. Do not rely on nested borders around every group.
- Keep related inputs close enough to scan without traversing the full window.
  Prefer labels above controls when values or labels are long.
- Place helper text beneath the control or group it explains. It must wrap at
  natural height and never compete with buttons in the same row.
- Lists lead with human identity, meaningful state, and immediate action.
  Protocol and runtime evidence belongs in selected-item detail.
- Constrain prose and compact editors to a readable content width instead of
  stretching short fields from one edge of a large display to the other.
- Empty space should clarify groups; it must not fragment one form or scatter
  successive controls down the viewport.

## Actions And Controls

- A committing workspace has one visually dominant primary action.
- Peer operational actions are allowed when the task genuinely requires them,
  but one remains the normal commit or next-step path.
- Refresh, Help, navigation, configuration routes, bulk maintenance, and rare
  actions use compact toolbars, contextual links, or overflow menus.
- Separate destructive actions from routine Save/Send/Apply actions. Explain
  impact, ownership, and recovery or reassignment before material deletion.
- Do not show full forms for mutually exclusive modes simultaneously. Show an
  explicit mode selector, reveal the selected mode, and retain only the concise
  resolved state needed from inactive or reusable configuration.
- Do not expose two controls that mean the same thing. Translate compatibility
  flags into one operator-visible state and keep conservative internal behavior.
- Use verbs that predict the result: `Save policy`, `Send now…`, `Add to BBS`,
  `Pause service`, or `Review radios`.
- Use an ellipsis when an action opens a required review or confirmation step
  rather than immediately performing the operation.

## Entity Selection, Chips, And Lookups

Use chips when the operator manages a small set of radios, callsigns, groups,
locations, policies, sources, or tags.

- Lookup and explicit custom entry may share one input when both are valid.
- Enter or Add commits the selected value and clears the input for the next
  value.
- Selected values remain visible while another lookup is performed.
- Normalize values before duplicate checking; never add duplicate chips.
- A removable chip has an accessible name that includes its value.
- Use a dropdown for one choice from a bounded list, not to hide a multi-value
  collection the operator must repeatedly review.
- Live lookup suggestions may contain only values from the operator's own
  configured or observed data. Permanent hints follow the UI Hint Neutrality
  Contract and never include a real or plausible callsign.

## Progressive Disclosure And Safety

The normal view shows the minimum controls needed for the common task. Advanced
limits, diagnostics, protocol fields, raw paths, import keys, and uncommon
maintenance remain discoverable through a clearly named drawer, tab, or detail.

Progressive disclosure must never hide:

- the target radio/source or scope that will be affected;
- an unsafe, blocked, stale, unknown, or unverified state;
- a required confirmation or destructive impact;
- unsaved changes;
- the reason a primary action is unavailable; or
- a safety prerequisite such as RF Guard, PTT/busy state, access policy, or
  schedule ownership.

Show a concise visible state with a direct Why or corrective-action route. Do not
make the operator open Advanced merely to learn why Send, QSY, Save, Delete, or
automatic operation is blocked.

## Responsive Layout And Scrolling

Responsive changes preserve task order and meaning; they do not merely shrink
controls.

- **Wide:** use columns only when both support the same immediate task. Keep the
  primary table, editor, form, message, map, or preview dominant.
- **Medium:** shorten secondary copy, wrap guidance, regroup actions, and stack
  a side inspector before either pane becomes difficult to scan.
- **Compact:** stack in task order and make the primary action reachable while
  the page scrolls vertically.
- Use Qt font metrics and actual content to choose widths and breakpoints. Do
  not encode one platform's screenshot dimensions as universal control sizes.
- Page-level horizontal scrolling is prohibited for normal workspaces. Tables,
  previews, logs, and code-like data may own bounded local horizontal overflow.
- Prefer one vertical scroll owner. Nested same-axis scrolling is limited to a
  deliberately constrained data surface, not adjacent setup forms.
- Preserve scroll position during typing, polling, preview updates, validation,
  and ordinary selection updates. Reset or reveal a region only after an
  intentional workflow transition, and coalesce that geometry change.
- A splitter is not a substitute for a responsive design. Set stretch and size
  policies so a child hint cannot starve the primary surface or expand the page.
- Wrapped evidence receives natural height in its own row. It must not overlap
  another label, control, or action.
- Status text changes must not produce repeated size oscillation, swipe/vanish
  behavior, or loss of the current selection.

### Font-Derived Height Requirement

All redesigned workspaces follow the complete Font-Derived Vertical Geometry
Contract in `ui_layout_standards.md`. Text-bearing controls, tabs, item-view
headers/rows, banners, chips, and wrapped or multiline content derive their
vertical floors from the active font, content, indicators/icons, and shared
padding/hit-target treatment. Lazy construction and font/theme changes must
republish those floors through coalesced cache-only geometry work. A global
guard or a literal pixel threshold is only a safety net and is not evidence that
the task surface, scroll owner, or responsive reading order is compliant.

## Visual Language

- Use the application-wide theme and shared component layer for primary,
  secondary, destructive, selected, warning, disabled, focus, and informational
  states. In the Qt application, `freqinout/gui/theme.py` is the current palette,
  application stylesheet, font-scale, control-sizing, combo-fitting, button-role,
  LED, and splitter-handle authority.
- Use one accent treatment for the selected mode or primary action; do not make
  every available action equally prominent.
- Use concise state chips or familiar icons where they improve recognition.
  Never rely on color alone.
- Use consistent small, medium, and section spacing tiers derived from the
  shared theme and font metrics. Do not solve one screen with arbitrary fixed
  pixel heights.
- Use dividers, cards, and borders sparingly. Visual containers should explain
  ownership or task grouping, not surround every row.
- Muted text is for supporting context, never required actions, critical
  warnings, or the only explanation of a disabled state.

### Shared Theme And Component Contract

Visual consistency is an implementation requirement, not discretionary polish.
A redesigned workspace must use the same visual vocabulary as the rest of FIO.

- Resolve colors through the shared theme and semantic roles. Do not add
  screen-local hexadecimal/RGB palettes or infer state colors from a particular
  Light or Dark theme.
- Use the global application stylesheet and shared helpers for fonts, text
  scale, control height, buttons, combo sizing, focus, tabs, disabled states,
  tables, splitter handles, and other treatments already provided centrally.
- Reuse established shared components or helper patterns for chips, banners,
  cards, tables, inputs, toolbars, scroll areas, selection, and icons when they
  express the needed interaction. A local imitation that merely looks similar
  is not a shared component.
- Do not hard-code a local font family, text size, text-bearing control height,
  spacing scale, radius, border, focus ring, selected-row color, disabled color,
  warning color, destructive color, or navigation-icon treatment.
- A local stylesheet is permitted only for layout or a truly screen-specific
  semantic that the global layer cannot express. It must derive every visual
  value from the resolved shared theme and active font metrics, cover all
  relevant states, and explain the exception in the governing specification.
- When the needed semantic or reusable control does not exist, add it to the
  shared theme/component layer first, with Light/Dark and Normal/Large Text
  behavior, then consume it from the feature screen. Do not solve the missing
  capability independently in several tabs.
- Shared input styling covers every control family, not only line edits and
  combo boxes. Numeric, decimal, date, time, and date/time editors must preserve
  readable values, prefixes/suffixes, step controls, focus, selected text, and
  enabled/disabled contrast in both Light and Dark themes. A platform-native
  subcontrol that becomes unreadable against the application palette is a
  shared-theme defect and is corrected centrally with a regression test.
- Main-navigation and workspace icons use the shared size, canvas, stroke,
  foreground/accent, selected, disabled, and high-DPI treatment. Feature icons
  must not introduce a visibly different color system.
- Platform-native rendering differences are acceptable only after the shared
  theme has been applied. Record and test any Qt or operating-system limitation
  that requires an exception.

## Feedback, Loading, And Failure States

- Preserve the last coherent read model while a background refresh is pending.
- Use calm, stable language such as `Refreshing…`, `Saved`, `Waiting for JS8Call`,
  or `Could not verify; configuration remains available`.
- Do not rapidly rotate status sentences in a fixed control bar or editor.
- Disable repeated commit actions for the active generation and provide one
  completion or failure result.
- An empty state explains what the list represents and the smallest next action.
- A configuration or endpoint failure must leave the administration surface
  usable so the operator can correct it.
- Preserve selection, draft values, expanded state, and scroll position across
  background publication unless the selected item was actually removed.

## Performance And Lifecycle Contract

Usability includes responsiveness. A visually improved screen fails if it makes
FIO slow or unstable.

- Resize, paint, theme, hover, splitter, selection, chip layout, validation, and
  keystroke paths perform no database, filesystem, process, endpoint, device,
  or network I/O.
- Activation reads are lazy, bounded, and performed off the UI thread when they
  could block. Publish immutable results only if their navigation/data
  generation still owns the visible screen.
- Filtering and sorting operate on the loaded bounded model unless the operator
  explicitly requests a new query.
- Polling publishes changed state; it does not rebuild the whole page or reset
  the operator's working context.
- Geometry requests are idempotent and coalesced. Do not start timers or queues
  from paint/resize loops that can perpetually invalidate layout.
- Tables and histories have explicit bounds, pagination, or virtualization.
- One slow or failed source must not prevent other independent data or controls
  from becoming usable.
- Screen deactivation cancels or generation-fences deferred callbacks so a
  hidden tab cannot reselect, resize, refresh, or navigate the current tab.

## Accessibility And Copy

- Support keyboard traversal, visible focus, accessible names/descriptions, and
  usable hit targets.
- Icon-only actions require a tooltip and accessible name; unfamiliar symbols
  retain a short visible label.
- Normal and Large Text must preserve full values and primary actions through
  wrapping, stacking, or scrolling. Never shrink the selected font to make a
  layout fit.
- Light and Dark themes preserve semantic contrast and do not use color as the
  only state indicator.
- Use operator vocabulary consistently. Explain a technical term the first time
  it is needed and avoid exposing implementation terms in normal labels.
- Hints describe the expected value, not a fictional configuration. Use
  `Callsign`, `Group`, `Radio`, or `Callsign or *` rather than realistic sample
  identities.

## Required Acceptance Matrix

Every meaningful redesign includes automated geometry/lifecycle checks and a
human visual review. Plain screenshots are product evidence; passing an
offscreen size test alone is not sufficient.

Exercise, as applicable:

- empty, populated, selected/editing, unsaved, loading, blocked/error, and stale
  states;
- long labels/values, wrapped evidence, and maximum reasonable chips/selections;
- Normal and Large Text;
- Light and Dark themes;
- Linux production baseline at 1920x1080;
- approximately 1000x700 and 900x560 compact windows;
- high-DPI scaling when a reported issue comes from a scaled display; and
- one-radio, two-radio-plus-Mesh, and three-radio shell cases when the shared
  shell or radio-scoped context is affected.

The exit gate verifies:

- the primary task, selected context, next action, and Why are immediately
  understandable;
- one clear scan path and action hierarchy exists;
- the primary surface receives the remaining useful workspace;
- no clipped or overlapping text/control, page horizontal scrolling, nested
  scroll trap, disruptive layout shift, or geometry feedback loop occurs;
- selection, draft, scroll, and expanded state remain stable during background
  updates;
- all new visual styling resolves through the shared theme/component layer, and
  selected, disabled, focus, warning, destructive, and primary states remain
  consistent in Light and Dark themes;
- no new screen-local palette, fixed text-bearing control metric, or duplicated
  shared component exists without a documented, tested exception;
- render/resize/typing paths remain cache-only and bounded;
- destructive and safety behavior remains explicit; and
- the applicable feature, lifecycle, concurrency, and persistence tests pass.

Record the tested sizes/states, model ownership, automated results, human visual
limitations, and any operator-assisted production check in the governing
specification and `ui_regression_work_log.md`.

## Prohibited Redesign Patterns

Do not introduce:

- a schema-shaped wall of controls with no normal task sequence;
- several equally prominent primary-looking actions;
- duplicated state or authorization controls with overlapping meanings;
- full advanced configuration permanently visible beside the common task;
- a short form stretched edge to edge across a large display;
- a narrow inspector that starves the table, message, map, form, or preview;
- an expanding action row or spacer that scatters related form controls;
- multiple adjacent vertical scrollbars for ordinary setup content;
- wrapped status/evidence in a row whose height cannot grow;
- bespoke screen-local colors, fonts, control metrics, focus/selection states,
  or cloned component styling that diverges from the shared theme;
- refresh-on-keystroke, query-on-paint, process discovery on resize, or
  timer-driven full-page reconstruction; or
- a responsive rule that hides safety state or changes the operator meaning of
  the workflow.

## Redesign Handoff Checklist

Before declaring a redesigned workspace complete:

1. Link this guideline and the governing product/feature contracts.
2. Include the completed redesign brief.
3. State the chosen archetype and normal task sequence.
4. Identify the primary surface, primary action, Why route, and scroll owner.
5. Identify the shared theme helpers/components used and any central additions;
   document and test every approved local styling exception.
6. Document what is progressively disclosed and why it is safe to hide.
7. Review long data, errors, empty state, Large Text, Light/Dark themes, selected,
   disabled, focus, warning, and destructive states, compact size, and
   high-DPI behavior.
8. Prove resize/paint/typing paths perform no I/O and deferred work is fenced.
9. Review every delegated diff, preserve unrelated work, and pass the governing
   exit gate before starting a successor slice.
