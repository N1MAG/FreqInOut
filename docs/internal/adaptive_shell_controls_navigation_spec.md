# Adaptive Shell Controls And Navigation Specification

## Purpose

This specification refines two shell interactions without changing their
underlying radio-command or screen-routing behavior:

- progressive disclosure of selected-radio controls; and
- the compact form of the main workflow navigation.

The product authority remains `multirig_product_ui_contract.md`. The shell keeps
**Where** and **When** visible; the selected tab retains the largest practical
area for **What** and **Why**.

## Selected-Radio Controls

The collapsed command bar is the normal operating state. It presents the selected
radio and current destination, current state, next scheduled transition, QSY,
Hold/Resume, the primary SOP action, and a Controls disclosure.

Opening Controls must not repeat the selected radio, current destination, current
state, next action, health as prose, or plan name already visible above it.
Instead, the quick QSY and Hold/Resume controls yield their space to one advanced
tray containing:

1. a plan-target selector;
2. QSY Now;
3. Timed QSY with duration choices;
4. schedule Suspend with duration and indefinite choices;
5. Resume when a manual or timed hold is active;
6. compact Health and Plan actions; and
7. a small Close control beside the persistent SOP action.

The tray uses the same RF-safe handlers, target model, hold-duration settings,
health summary, and schedule-assignment route as the existing controls. It does
not introduce a second command interpretation.

QSY target lists contain only alternate destinations. Any option whose normalized
frequency exactly matches the radio's currently reported frequency is omitted
from both the quick QSY menu and advanced target selector. If an assigned plan has
no alternate destinations, the selector states `No alternate QSY targets` rather
than offering a no-op command.

Responsive behavior:

| Density | Advanced-control arrangement |
| --- | --- |
| Roomy | One row when the content fits |
| Compact | Target/QSY row plus schedule/utility row |
| Condensed or Large Text | Three clean rows: target, QSY, schedule/utility |

The tray must not create page-level horizontal scrolling. Text-bearing controls
derive height from font metrics. Health and Plan may use compact familiar icons,
but require accessible names, tooltips, keyboard focus, and visible focus state.

## Compact Main Navigation

Collapsed navigation represents the full menu's master hierarchy, not a curated
set of child destinations. It uses application-owned icons plus short labels so
meaning does not depend on platform icon themes.

| Compact item | Full-menu behavior |
| --- | --- |
| Ops | Open Ops Center |
| Map | Open Map |
| Msgs | Messages flyout: Inbox, Compose |
| Net Control | Flyout: FLDigi / SSB, JS8Call, VHF/UHF |
| Calls | Operators flyout: HF Callsigns, Local Callsigns, Local Reports |
| Plans | Flyout: Plan Builder, SOP Builder, HF Daily, HF Nets, HF Peer Scheds |
| Station | Flyout: Control Center, Health Details |
| Settings | Flyout: Main, Radios |
| Help | Open Help |

The active compact item reflects the active master group. A grouped item opens a
flyout and does not silently choose one child. Flyout actions invoke the same full
navigation buttons, preserving Messages and Settings sub-context behavior.
Visible labels may abbreviate a master group to fit the compact rail (`Msgs` and
`Calls`), while its full group name remains in the accessible name and tooltip.
Do not label the Messages master button `Inbox`, because it also owns Compose.

The compact rail is approximately 72--88 logical pixels wide depending on the
selected text scale. It must retain the visible expand-navigation control and
must immediately trigger command-bar reflow when expanded or collapsed. Button
geometry must subtract both the navigation container margins and compact-widget
margins; icons, focus treatment, and the complete button border must remain
inside the available paint area on macOS and Linux.

## Acceptance Checks

- No repeated selected-radio/current/next card appears when Controls is open.
- Every advanced control remains reachable at 1920x1080 Normal Text, 1000x700,
  and approximately 900x560 Large Text without horizontal scrolling.
- One, two-plus-Mesh, and three-radio awareness states remain usable.
- Compact navigation contains all six grouped master menus and the three direct
  destinations, with the correct active-group indication.
- Map uses a map marker symbol rather than a folder; Messages uses an envelope;
  Net Control uses a radio/wave symbol.
- Light and Dark themes retain readable icons, labels, focus, and selection.
- No database or configuration schema change is required.
