# Multi-Rig Test Lab

This is a repeatable way to test multi-rig startup behavior without touching your production FIO configuration.

The harness creates disposable FIO config profiles under:

```text
/Users/bill/RadioCode/WORK/MultiRig/TestLab/profiles/
```

It can also start the RadioTools emulator lab from:

```text
/Users/bill/RadioTools
```

## What To Test

### Upgrade From Single-Rig

This profile starts with legacy single-rig settings already present. FIO should not migrate automatically. It should show the calm Settings card:

```text
FIO is using your current station setup.
```

Then use `Settings -> Multi-Rig Setup -> Set up Multi-Rig` to confirm the first radio.

### Fresh Install

This profile starts empty. FIO should create the default multi-rig runtime radio automatically. It should not show migration anxiety or single-rig upgrade wording.

Expected Settings card:

```text
Multi-Rig is ready.
```

## Commands

Run these from the multi-rig repo:

```bash
cd /Users/bill/RadioCode/FreqInOut-multi-rig
```

Prepare both profiles from scratch:

```bash
.venv/bin/python tools/multirig_test_lab.py prepare all --reset
```

Start the RadioTools emulator lab for one radio:

```bash
.venv/bin/python tools/multirig_test_lab.py lab start --mode single
```

Check emulator status:

```bash
.venv/bin/python tools/multirig_test_lab.py lab status
```

Check what FIO will think before opening the GUI:

```bash
.venv/bin/python tools/multirig_test_lab.py check upgrade
.venv/bin/python tools/multirig_test_lab.py check fresh
```

Run the single-rig upgrade profile:

```bash
.venv/bin/python tools/multirig_test_lab.py run upgrade
```

Run the fresh install profile:

```bash
.venv/bin/python tools/multirig_test_lab.py run fresh
```

Stop emulators:

```bash
.venv/bin/python tools/multirig_test_lab.py lab stop
```

## One-Command Variants

Reset and run the upgrade profile:

```bash
.venv/bin/python tools/multirig_test_lab.py run upgrade --prepare --reset
```

Reset and run the fresh profile:

```bash
.venv/bin/python tools/multirig_test_lab.py run fresh --prepare --reset
```

Reset and check without opening FIO:

```bash
.venv/bin/python tools/multirig_test_lab.py check upgrade --prepare --reset
.venv/bin/python tools/multirig_test_lab.py check fresh --prepare --reset
```

Print profile paths:

```bash
.venv/bin/python tools/multirig_test_lab.py paths
```

## Emulator Ports

The RadioTools lab uses profile `a` for the single-radio emulator path:

| Program | Host | Port |
|---|---|---:|
| FLRig mock | `127.0.0.1` | `12345` |
| FLDigi mock | `127.0.0.1` | `7362` |
| JS8 mock | `127.0.0.1` | `2442` |

The upgrade profile is seeded with these endpoints.

## Safe Reset

To fully reset the lab profiles:

```bash
rm -rf /Users/bill/RadioCode/WORK/MultiRig/TestLab/profiles
```

Then prepare them again:

```bash
.venv/bin/python tools/multirig_test_lab.py prepare all --reset
```

## What To Observe

For upgrade testing:

- Settings should say FIO is using the current station setup.
- `Set up Multi-Rig` should open the guided setup.
- The radio model should prefer the supported catalog.
- `Not Now` should pause setup and preserve the single-rig path.
- After setup, Settings should show `Multi-Rig is ready`.

For fresh install testing:

- FIO should start without a migration prompt.
- Settings should show `Runtime Radios`.
- A default primary radio should exist.

With emulators running:

- Station Health should see the FLRig, FLDigi, and JS8 endpoints.
- ControlFreq should be able to read the mock FLRig frequency/mode.
- Scheduler operations can be tested without touching real radios.
