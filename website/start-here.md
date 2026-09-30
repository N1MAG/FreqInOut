---
title: Start Here
description: Choose the right FreqInOut setup path for a new station, an upgrade, or an existing installation.
---

# Start here

<div class="doc-badge-row">
  <span class="doc-badge">FIO 2.0</span>
  <span class="doc-badge">Windows · Linux · macOS</span>
  <span class="doc-badge">Choose the right path</span>
</div>

FreqInOut is a coordinated operations console for HF digital stations. It does not replace FLRig, FLDigi, FLMsg, FLAmp, JS8Call, VarAC, VARA, MeshCore, or Meshtastic. It helps those applications work together as one station.

## Which path fits your station?

<div class="decision-grid">
  <div class="decision-card">
    <strong>New to FIO</strong>
    <p>Install the current package, enter the station identity, and use Guided Add Radio for the first radio.</p>
  </div>
  <div class="decision-card">
    <strong>Upgrading single-radio FIO</strong>
    <p>Close FIO and companion applications, install the update, then review Upgrade Existing Station.</p>
  </div>
  <div class="decision-card">
    <strong>Already using FIO 2.0</strong>
    <p>Install the newer release over the established application and keep using the established station profile.</p>
  </div>
</div>

[Open the installation and upgrade guide →](/install/)

## Before you begin

- Know which computer and operating system will run FIO.
- Know where an existing source checkout was installed, if you use one.
- Close FIO and affected companion applications before an upgrade.
- Keep the radio manufacturer and model available for the guided setup.
- Treat a verified station backup as mandatory for an upgrade.

::: tip One radio is enough
You do not need to add another radio to use FIO 2.0. The multi-radio architecture keeps one-radio operation fully supported.
:::

## First successful session

The recommended order is:

1. Enter the station identity and location under **Configuration**.
2. Define the operating groups used by scheduling, filtering, access policies, and publication rules.
3. Add or review the first radio and select only the software it actually uses.
4. Review Software Administration and Launch Control for that radio.
5. Create or assign a frequency plan and schedule.
6. Open Ops Center, Messages, and Map to confirm the resulting station context.

<div class="expectation">
  <strong>Expected result</strong>
  FIO reopens with the same station profile, the configured radio appears under Configuration, and its selected application identities appear in Software Administration.
</div>

## Understand the operating model

FIO keeps three questions connected:

- **Where do I need to be?** Radio, group, band, frequency, and mode.
- **When do I need to be there?** Schedules, events, and the next meaningful transition.
- **What do I do when I am there?** The task, message, SOP action, or station service appropriate to that context.

The Station Control Bar keeps radio and schedule context visible. The active workspace focuses on the task and why an action is ready, blocked, or unnecessary.

## Continue

- [Install or upgrade FIO](/install/)
- [Configure your first radio](/guide/first-radio)
- [Connect a Local Mesh device](/integrations/mesh)
- [Prepare a support request](/support)
