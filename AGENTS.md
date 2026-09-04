# Agent Operating Rules

## Multi-Rig Product And UI Authority

- Before planning, reviewing, or implementing multi-rig UI work, read
  `docs/internal/multirig_product_ui_contract.md`.
- Treat that contract as the concise product-level authority for the relationship
  between the Station Control Bar and each tab workspace. If an older UI spec or
  implementation detail conflicts with it, stop and surface the conflict rather
  than silently following the older direction.
- In short: the always-visible Station Control Bar supports **Where** and **When**
  for each radio with responsive, stable presentation; the active tab should use
  the remaining workspace for **What** and a clear explanation of **Why**.

## Configuration And Database Safety

- Before diagnosing runtime behavior, confirm the actual FIO configuration root the user is running. Do not assume `~/.freqinout/config` is authoritative.
- Multi-rig development and lab sessions commonly use runtime-specific roots such as `/Users/bill/RadioCode/runtime/multi-rig/config` or profiles under `/Users/bill/RadioCode/WORK/MultiRig/TestLab/profiles/`.
- Prefer checking `FREQINOUT_CONFIG_DIR`, launch scripts, recent logs, and the active runtime profile before reading or reasoning from SQLite data.
- When inspecting radio/app configuration, use the correct database for the selected runtime profile:
  - `freqinout.db` contains Settings, `device_profiles`, `js8_instances`, `fast_light_configs`, `varac_nodes`, Launch Control settings, and app paths.
  - `freqinout_nets.db` contains traffic, messages, observations, schedules, and operator/map data.
- In multi-rig work, verify linked configuration rows as well as the visible radio row. For example, `device_profiles.js8_instance_id` may point to a `js8_instances` row that owns `install_path`, `spotter_launch_path`, and `commstat_launch_path`.
- If the observed UI behavior and the inspected database disagree, stop and identify the active configuration root before patching code. Acting on the wrong workspace or database is high-risk.
