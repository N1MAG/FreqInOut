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

### Production-Copy Upgrade Rehearsal

This is the closest rehearsal to your real station. It copies your current FIO config from:

```text
/Users/bill/Radio/FreqInOut/config
```

into:

```text
/Users/bill/RadioCode/WORK/MultiRig/TestLab/profiles/prod-upgrade
```

The copy is then pointed at the RadioTools emulators. The live FIO config is not changed.

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

Copy your current production FIO config into the lab:

```bash
.venv/bin/python tools/multirig_test_lab.py copy-production --reset
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
.venv/bin/python tools/multirig_test_lab.py check prod-upgrade
```

Run the single-rig upgrade profile:

```bash
.venv/bin/python tools/multirig_test_lab.py run upgrade
```

Run the fresh install profile:

```bash
.venv/bin/python tools/multirig_test_lab.py run fresh
```

Run the production-copy upgrade profile:

```bash
.venv/bin/python tools/multirig_test_lab.py run prod-upgrade
```

Stop emulators:

```bash
.venv/bin/python tools/multirig_test_lab.py lab stop
```

## Production-Copy Walkthrough

Use this when you want to rehearse the exact operator flow from your current single-rig setup.

Short version:

```bash
cd /Users/bill/RadioCode/FreqInOut-multi-rig
tools/multirig_upgrade_lab.sh copy
tools/multirig_upgrade_lab.sh start-single
tools/multirig_upgrade_lab.sh check
tools/multirig_upgrade_lab.sh run
```

After you complete Multi-Rig Setup in FIO:

```bash
tools/multirig_upgrade_lab.sh start-multi
tools/multirig_upgrade_lab.sh seed-extra
tools/multirig_upgrade_lab.sh run
```

Stop the emulators:

```bash
tools/multirig_upgrade_lab.sh stop
```

Detailed version:

1. Make the lab copy:

```bash
cd /Users/bill/RadioCode/FreqInOut-multi-rig
.venv/bin/python tools/multirig_test_lab.py copy-production --reset
```

2. Start one emulated radio:

```bash
.venv/bin/python tools/multirig_test_lab.py lab start --mode single
```

3. Confirm FIO sees this as an existing single-rig install:

```bash
.venv/bin/python tools/multirig_test_lab.py check prod-upgrade
```

Expected:

```text
startup_mode=existing_unmigrated
migration_current=False
```

4. Open FIO against the copied profile:

```bash
.venv/bin/python tools/multirig_test_lab.py run prod-upgrade
```

5. In FIO, go to Settings and choose `Set up Multi-Rig`. Confirm the first radio.

6. Close FIO, then restart the emulator lab with three radios:

```bash
.venv/bin/python tools/multirig_test_lab.py lab stop
.venv/bin/python tools/multirig_test_lab.py lab start --mode multi
```

7. Add the two extra emulated radios to the migrated FIO profile:

```bash
.venv/bin/python tools/multirig_test_lab.py seed-extra-radios prod-upgrade
```

8. Open FIO again:

```bash
.venv/bin/python tools/multirig_test_lab.py run prod-upgrade
```

Now you should have the migrated primary radio plus `Lab Radio B` and `Lab Radio C`, all pointed at RadioTools emulator ports.

## One-Command Variants

Reset and run the upgrade profile:

```bash
.venv/bin/python tools/multirig_test_lab.py run upgrade --prepare --reset
```

Reset and run the fresh profile:

```bash
.venv/bin/python tools/multirig_test_lab.py run fresh --prepare --reset
```

Reset and run the production-copy profile:

```bash
.venv/bin/python tools/multirig_test_lab.py run prod-upgrade --prepare --reset
```

Reset and check without opening FIO:

```bash
.venv/bin/python tools/multirig_test_lab.py check upgrade --prepare --reset
.venv/bin/python tools/multirig_test_lab.py check fresh --prepare --reset
.venv/bin/python tools/multirig_test_lab.py check prod-upgrade --prepare --reset
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

The upgrade and production-copy profiles are seeded with these endpoints.

When the lab is started in `multi` mode, the extra radios use:

| Radio | FLRig | FLDigi | JS8 |
|---|---:|---:|---:|
| `Lab Radio B` | `12346` | `7363` | `2443` |
| `Lab Radio C` | `12347` | `7364` | `2444` |

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
