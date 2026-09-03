# Message Inbox Controls Spec

## Scope

This contract applies to the FIO multi-rig Message Inbox control surface.
The controls must support fast operator triage without forcing the user to
interpret redundant labels or stale historical traffic.

## Control Contract

- Focus remains the primary mode selector.
- Age is the first refinement control and defaults to `Last 7 days`.
- `Any time` remains available for historical review, but it is not the default.
- Groups is one dropdown control. It owns both group selection and the
  configured/all group-list mode.
- Sources is one dropdown control. It owns both source selection and source
  status text.
- Separate static labels beside Groups and Sources are not required when the
  dropdown button text already names the control and current state.
- Clear Filters resets the inbox to the current focus and the default age window.
- Advanced Filters remains available for type/status/from/to work, but does not
  dominate the basic triage row.

## Label Contract

- Groups text examples:
  - `Groups: Configured`
  - `Groups: All`
  - `Groups: 2 selected`
- Sources text examples:
  - `Sources: All`
  - `Sources: JS8Call`
  - `Sources: 2 selected`
- Age text must be visible before group/source refinements in the basic row.

## Intel Contract

- Intel topic chips use canonical topic names.
- `Travel` and `Travel/Roads` are the same operator topic and render as
  `Travel/Roads`.
- Topic aliases should collapse before counts are calculated so chips do not
  split one operational concept into multiple buttons.

## Acceptance

- New Message Inbox sessions default to `Last 7 days`.
- Clear Filters returns Age to `Last 7 days`, not `Any time`.
- The basic refinement row starts with Age, then Groups, then Sources.
- There is no separate Configured/All button next to Groups.
- Group and source dropdown buttons show their current state directly.
- `Travel` and `Travel/Roads` produce one `Travel/Roads` intel chip.
