# Actionable Traffic Summary Spec

## Product Intent

The traffic summary helps answer the workspace questions **what should I do**
and **why should I do it**. It complements the station control bar, which owns
the persistent **where** and **when** context.

Ops Center and Messages must render the same actionability projection. Ops
Center is the concise dashboard entry point; Messages owns the detailed traffic
review and source tools.

## Relevance Contract

- A message is relevant when it is addressed to the station callsign or to an
  operating/local group explicitly associated with the user.
- Explicit associations come from the configured HF operating groups,
  configured local-net groups, and the user's own HF Operator record.
- Group parent/child relationships are not inferred, expanded, hard coded, or
  required as additional configuration.
- A matching group name is exact after the existing group normalization.
- An operator may be associated with more than one group.

The current local-operator roster identifies service/category rather than group
membership. VHF, UHF, GMRS, MURS, and FRS categories therefore must not be
treated as group associations. Configured local-net group names are explicit
associations and may be used.

## Duty Contract

- `Hub`, `Hub-Alt`/`Alt-Hub`, `NCS`, and equivalent alternate-NCS roles share
  distribution duty: coordinate, respond, and help distribute impactful
  reports.
- `Peer` is a reporter role. It does not create a distribution duty.
- Role determines duty; explicit group association determines group relevance.
- An impactful event report addressed directly to a distribution-duty operator
  may require both reply and relay. Reply is shown first, while both filters
  include the item.

## Actionability Buckets

- **Reply**: direct event traffic to the station, or relevant traffic with an
  explicit request/question that the active source supports replying to.
- **Relay**: impactful relevant event traffic for a group where the user has a
  distribution-duty role. Direct impactful traffic also qualifies when the
  user has an explicit distribution-duty association.
- **Review**: relevant event traffic without a detected reply or relay duty.
- **Social**: direct non-event traffic. It remains visible but sorts after
  event-oriented traffic.

Event orientation uses the existing normalized severity, actionable flag,
message/form family, and intelligence topics. The projection is advisory;
operators retain judgment when source content is ambiguous.

## State Contract

- Received time remains the actionable time because delayed RF/internet routing
  can make an older authored message newly known to the station.
- Marking a message read does not mean reply, relay, or review duty is complete.
- This slice does not add action-completion persistence. A later workflow may
  add explicit acknowledge/defer/complete actions without redefining read state.

## Shared UI Contract

- The shared summary shows compact `Reply`, `Relay`, `Review`, and `Social`
  controls with counts.
- The highest-priority item supplies one concise `What` and `Why` line.
- Event reply sorts before relay, event review, non-event reply, and social
  traffic.
- Selecting a bucket filters Messages to that exact actionability predicate.
- Ops Center keeps legacy source/file counts under a collapsed `Sources`
  disclosure so source health remains available without dominating the
  dashboard.
- The summary uses theme-derived semantic styling and must remain usable with
  light/dark themes and larger text.

## Acceptance

- A message to `N1MAG` is relevant independent of group configuration.
- A message to `MR08` is relevant only when `MR08` is explicitly associated
  with the user; configuring `MAGNET` alone does not imply `MR08`.
- Hub, Hub-Alt/Alt-Hub, NCS, and ANCS classify the same impactful group report
  as Relay.
- Peer classifies that report as Review, not Relay.
- Event-oriented direct traffic is presented before newer social traffic.
- Changing a message from New to Read does not remove its operational action.
- Ops Center and Messages compute counts from the same Qt-free core functions.
