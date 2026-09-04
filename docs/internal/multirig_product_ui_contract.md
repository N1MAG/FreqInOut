# FIO Multi-Rig Product And UI Contract

## Status And Purpose

This is the concise product-level authority for planning, reviewing, and
implementing FIO multi-rig UI work. It records the operator model that must remain
clear even when older specifications contain more detailed or conflicting UI
directions.

Do not treat this document as a pixel-level design. It defines the information
hierarchy and the experience that responsive layouts must preserve.

## Product Center Of Gravity

FIO supports three connected operator questions:

1. **Where do I need to be?**
   Operating groups and the frequencies available to those groups establish the
   operating destination. The answer is radio-specific and may include band,
   frequency, mode, assigned plan, or group context.
2. **When do I need to be there?**
   The answer may come from a regular schedule or from an event that activates an
   SOP. It may vary with propagation and time zone. During an emergency or other
   event, the operator may need to be present on multiple radios at once.
3. **What do I do when I am there?**
   Available actions depend on the SOP, the current operating state, and the
   capabilities and status of the relevant tools and radios.

Together, these form the actionable operating/SOP context. The UI must present
the data in a way that guides the operator's decision and action, not merely expose
configuration and raw status.

## Shell And Workspace Responsibilities

The interface has two distinct responsibilities:

- The always-visible **Station Control Bar** supports **Where** and **When** for
  each radio.
- The active **tab workspace** focuses on **What** and clearly explains **Why** the
  action, recommendation, warning, or disabled state is appropriate.

Every tab must be reviewed against this division. Tabs should not duplicate large
Where/When displays when the control bar already provides that context. They may
show task-specific details when those details are necessary to explain or perform
the What.

## Station Control Bar Contract

"Always visible" means essential Where/When context remains immediately
available. It does not mean every detail and control is permanently expanded.

The presentation must adapt predictably to:

- available content width and height
- number of configured and active radios
- selected or primary radio
- light and dark themes
- Normal and Large text settings

Expected information hierarchy:

- The selected or primary radio receives the clearest Where/When presentation.
  Its primary context row begins with the radio name and current destination,
  for example `FIO-A · AMRRON 20M`; a generic `NOW` caption is insufficient
  because it does not explicitly bind the destination to a radio.
- The selected radio is omitted from the source header because its identity is
  already explicit in the primary context row. The source header shows alternate
  radios, Mesh sources, condition summaries, and time. Selecting an alternate
  radio moves it into the primary row and returns the previously selected radio
  to the header.
- Other active radios remain visibly identifiable with concise operating context.
- Configured but inactive radios remain accessible without consuming the same
  space as active radios.
- Attention states remain visible without causing unexpected layout growth or
  continuous workspace movement as status text changes.

Normal station scale is one active radio. Two active radios plus Mesh is a
realistic operating maximum for common use, and the design must remain usable for
the less-common three-radio operator. Mesh and similar sources must remain
distinguishable from commandable radios even when they share the awareness rail.

At minimum, the selected radio presentation should make radio identity, current
operating destination, schedule/event state, and next meaningful transition
understandable. Secondary detail may use a drawer, popover, selector, or dedicated
Station view.

The control bar must be content-efficient:

- Wide layouts must not reserve large unused regions.
- Compact layouts must not consume most of the height needed by the active tab.
- Multiple radios must not force each radio into a permanently full-size card.
- Controls should shorten, regroup, stack, or progressively disclose before the
  shell relies on broad horizontal scrolling.
- Layout dimensions should remain stable as polling results and status strings
  change.

The default quick-action set is `QSY`, `Hold/Resume`, the current primary SOP
action, and `Controls...`. Full and infrequent controls such as timed actions,
manual tuning details, and plan-management operations should remain readily
accessible without occupying permanent space.

When `Controls...` is expanded, preserve the selected-radio context row but do
not repeat its radio identity, current destination, state, next action, or plan
summary in a second card. Replace duplicate quick actions with one responsive
advanced-control tray for target selection, QSY timing, scheduler suspension,
resume, Health, and Plan. The exact layout and navigation mapping are defined in
`adaptive_shell_controls_navigation_spec.md`.

Next scheduled actions remain available in the summary. They begin receiving
additional visual prominence at 30 minutes and receive stronger prominence at 15
minutes. Urgency must be conveyed with text or iconography as well as color, and
the transition must not cause disruptive layout growth.

Essential attention states include off-schedule operation, unreachable or
unhealthy radios, required software unavailable, PTT/busy conflicts, plan
incompatibility, and overdue SOP actions. Each warning must route to a clear Why
or corrective action.

## Group Condition And SOP Context

Condition level is scoped to an operating group, not to the station globally and
not necessarily to one radio. For example, AMRRON, MAGNET, and a local operating
group may simultaneously have different condition levels and different SOP
actions.

The control bar should summarize enabled group conditions without turning each
condition into a large permanent panel. Each summary must preserve:

- operating-group identity
- current condition level and its effective severity
- whether an action is currently due
- warning/attention state when applicable
- a direct path to the corresponding SOP actions and Why

The active tab may provide the full actionable group view. Prefer one clearly
prioritized SOP action with a visible count or indication when additional actions
exist. The complete ordered action list belongs in the What/Why workspace.

Automatic condition changes derived from JS8Spotter or another message source
must retain group scope, source/provenance, observation time, trust or validation
state, and any applicable expiration/supersession rule. The UI must make it clear
whether a condition is confirmed, automatically applied under an approved rule,
or awaiting operator review. A received message must not silently become an
unexplained station-wide condition change.

Condition-message syntax is configured per operating group and must not be
hard-coded as MAGNET-specific application logic. Each enabled group may define a
validated condition token/template plus its number of levels, severity order,
display names, normal/default level, and allowed value mapping. MAGNET's planned
configuration is `MAGCON+<level>`; for example,
`N1MAG: @MR08 MAGCON+2`. Parsing a configured token and authorizing its sender are
separate decisions; do not treat a syntactically valid message as authoritative
until the sender/trust rule is satisfied.

The configuration UI must let the operator preview/test representative messages,
reject ambiguous or invalid patterns, and see the parsed group and level before
enabling automatic application. The parser should consume a constrained template
or similarly validated configuration rather than execute unrestricted user code.

A MAGNET condition message is authorized only when the sender resolves to an
operator for the affected group who both:

- has a role in the group's configured condition-authority role set, which
  defaults to `Hub` and `Alt-Hub`; and
- has the operator `trusted` flag set.

These requirements are cumulative. A trusted operator without an authorized role,
or an authorized-role operator without the trusted flag, must not change the
condition. The rejected message may remain visible as evidence, but the Why/audit
record must state which authorization requirement failed. Authority is
group-scoped unless an explicit cross-group authority rule is configured.

Both directed messages and broadcasts may set a condition when the sender meets
the affected group's role and trust requirements. A broadcast does not need a
group destination, but its configured condition token must resolve to one group
unambiguously. Ambiguous matches must not change any condition.

A condition change remains effective until another valid condition-level message
is received or the operator changes it manually. It does not expire merely
because time has passed. A group uses its configured condition-level syntax for
both escalation and stand-down; there is no separate stand-down command. The
newly authorized level replaces the current level. When multiple valid condition
messages for the same group conflict, the message FIO received most recently wins.
This receipt time is authoritative because a message becomes actionable only when
it reaches FIO, even if RF or internet routing delayed it for hours. The rule
applies consistently to CommStat internet or RF, Spotter RF traffic, JS8Call RF,
and future transports. Source/event time remains provenance, not transition
authority. A duplicate observation of the same retained message must be
idempotent rather than manufacture a new condition transition. Superseded
evidence must remain available for Why/audit history.

A per-group manual lock is the preferred safeguard for intentional operator
overrides. While locked, the operator may change the level manually and valid
incoming condition messages are retained but do not automatically replace it.
The UI must show a small lock icon and a compact pending-change count. Unlocking
must ask whether to keep the manual level or apply the newest received pending
level; pending messages must never be applied silently.

An accepted automatic change should produce brief nonmodal feedback and update a
compact persistent group summary containing the old/new level, source or sender,
receipt time, and resulting SOP-action count. Full evidence belongs in the
What/Why view or history, not permanently in the control bar. Unauthorized
messages remain auditable; avoid routine operator notifications unless an attempt
is suspicious, repeated, or otherwise needs attention.

## Visual Information Hierarchy

FIO already presents substantial text. Prefer concise graphical rendering when a
familiar symbol, shape, progression, or small visualization communicates state
more quickly and leaves attention for meaningful actions.

- Use a small lock icon for manual lock rather than permanently displaying the
  word `Lock`.
- Use compact icons or badges for familiar health, warning, connectivity,
  schedule, and pending-action states.
- Use restrained countdown/progress treatment for approaching schedule actions,
  with prominence increasing at the confirmed 30-minute and 15-minute thresholds.
- Reserve persistent prose for information the operator must interpret; place
  explanation and evidence in the What/Why workspace or an on-demand detail.
- Use visual prominence to lead the eye to the highest-priority actionable item,
  not to decoration or every available control.

Graphical presentation must remain accessible. Do not rely on color alone. Every
icon-only control or status needs a tooltip, accessible name/description, keyboard
focus behavior where interactive, and an adjacent short label when the symbol is
not broadly recognizable. Large Text mode must scale icons and hit targets without
turning every status into a large labeled control.

## Tab Workspace Contract

The active tab should own most of the usable workspace and make these questions
clear:

- What is the primary task on this tab?
- What should the operator do now or next?
- Why is that action recommended, available, blocked, or unnecessary?
- What evidence, state, provenance, or constraint supports that conclusion?

Controls remain available, but they must not dominate the task surface. Secondary
filters, configuration, and uncommon actions should use contextual panels,
drawers, overflow menus, compact toolbars, or progressive disclosure where that
preserves operator efficiency.

Tables, maps, editors, message content, and other primary work surfaces should
receive the remaining space instead of being compressed by permanently expanded
control panels.

## Accessibility And Density

Normal mode should feel compact and crisp, including on Linux production systems.
Large Text mode must remain fully usable for operators with impaired vision.

- Derive text-bearing control heights from font metrics; do not solve density by
  forcing one fixed height across text modes and platforms.
- Preserve readable text in Large Text mode, then adapt through wrapping,
  stacking, scrolling, or progressive disclosure.
- Do not shrink the user's selected font to preserve a dense layout.
- Validate Qt rendering on the target platforms; a layout that looks compact on
  one operating system may be oversized or clipped on another.

The current Linux production density baseline is a `1920x1080` display with FIO
Normal text selected. Shell refinements must be visually checked against that
baseline in addition to reduced-window and Large Text cases.

## Main Navigation

The main navigation layout may change when that materially improves the operator
experience. Navigation must remain workflow-oriented and must not compete with
the Station Control Bar for Where/When information.

The compact, collapsible left workflow rail must primarily navigate. It represents
the same master hierarchy as the full menu: Messages, Net Control, Operators,
Plans, Station, and Settings are groups whose child destinations appear in a
flyout; Ops, Map, and Help are direct destinations. It must not substitute an
arbitrary child such as JS8Call for the Net Control group. Use application-owned
icons plus short labels, and keep the active master group visible. Condition
summaries, radio state, and clocks that contribute to current operating context
belong in the control-bar shell rather than being duplicated in navigation.

Expanding or collapsing navigation must immediately recalculate the command-bar
density because it changes the available content width without necessarily
resizing the main window. In condensed mode, the command bar may grow vertically
to preserve the selected radio, Next state, and quick actions; it must not retain
a single-row height that clips or hides those controls.

## Implementation And Acceptance Rules

For each UI slice:

1. State which parts of Where, When, What, and Why the surface supports.
2. Identify the primary operator task and protect its workspace.
3. Reuse shared radio, plan, schedule, event, status, and action read models. Do
   not create separate UI-owned interpretations of those concepts.
4. Test the affected shell/tab interaction with one radio and multiple radios.
   Include one radio, two radios plus Mesh, and a three-radio stress case when the
   Station Control Bar or main shell is touched.
5. Review Normal and Large text in Light and Dark themes.
6. Review at normal desktop size, approximately `1000x700`, and approximately
   `900x560`, including Linux when the issue is platform-sensitive.
7. Verify that status updates do not produce distracting layout shifts and that
   the primary task remains reachable without unnecessary page-level horizontal
   scrolling.

Plain-language operator feedback and annotated screenshots are valid product
inputs. Convert them into observable objectives and acceptance criteria without
requiring the operator to prescribe the technical solution.
