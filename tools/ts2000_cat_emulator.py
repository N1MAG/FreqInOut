#!/usr/bin/env python3
"""Stateful Kenwood TS-2000 CAT emulator for the local GUI lab.

The emulator owns a pseudo-terminal and publishes its slave device through a
stable symlink. FLRig opens that symlink as if it were a physical TS-2000
serial port. This is local-lab infrastructure, not a FreqInOut radio backend.
"""

from __future__ import annotations

import argparse
import os
import pty
import select
import signal
import sys
import tempfile
import termios
import tty
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RadioState:
    freq_a: int = 14_074_000
    freq_b: int = 7_078_000
    mode: int = 2  # TS-2000 CAT mode 2 is USB.
    rx_vfo: int = 0
    tx_vfo: int = 0
    power: int = 50
    preamp: int = 0
    attenuator: int = 0
    auto_notch: int = 0
    manual_notch: int = 0
    notch_value: int = 40
    noise_reduction: int = 0
    noise_reduction_level: int = 5
    low_cut: int = 0
    high_cut: int = 0
    bandwidth: int = 2_400


def response_for(command: str, state: RadioState) -> str | None:
    """Apply one semicolon-terminated command and return any CAT response."""

    command = command.strip()
    if not command:
        return None

    if command == "ID;":
        return "ID019;"
    if command == "PS;":
        return "PS1;"
    if command == "IF;":
        freq = state.freq_a if state.rx_vfo == 0 else state.freq_b
        return f"IF{freq:011d}0001000+0000000000{state.mode}00000000;"

    if command == "FA;":
        return f"FA{state.freq_a:011d};"
    if command.startswith("FA") and command.endswith(";"):
        value = command[2:-1]
        if value.isdigit():
            state.freq_a = int(value)
        return None
    if command == "FB;":
        return f"FB{state.freq_b:011d};"
    if command.startswith("FB") and command.endswith(";"):
        value = command[2:-1]
        if value.isdigit():
            state.freq_b = int(value)
        return None

    if command == "MD;":
        return f"MD{state.mode};"
    if command.startswith("MD") and command.endswith(";"):
        value = command[2:-1]
        if value.isdigit():
            state.mode = int(value)
        return None
    if command == "FR;":
        return f"FR{state.rx_vfo};"
    if command.startswith("FR") and command.endswith(";"):
        value = command[2:-1]
        if value.isdigit():
            state.rx_vfo = int(value)
        return None
    if command == "FT;":
        return f"FT{state.tx_vfo};"
    if command.startswith("FT") and command.endswith(";"):
        value = command[2:-1]
        if value.isdigit():
            state.tx_vfo = int(value)
        return None

    query_values = {
        "AI;": "AI0;",
        "AG;": "AG100;",
        "BC;": f"BC{state.manual_notch};",
        "BP;": f"BP{state.notch_value:03d};",
        "FW;": f"FW{state.bandwidth:04d};",
        "MG;": "MG050;",
        "NB;": "NB0;",
        "NT;": f"NT{state.auto_notch};",
        "PA;": f"PA{state.preamp};",
        "PC;": f"PC{state.power:03d};",
        "RA;": f"RA0{state.attenuator};",
        "RG;": "RG055;",
        "RL;": f"RL{state.noise_reduction_level:03d};",
        "SH;": f"SH{state.high_cut:02d};",
        "SL;": f"SL{state.low_cut:02d};",
        "SM0;": "SM00035;",
        "SM1;": "SM10035;",
        "SQ;": "SQ000;",
    }
    if command in query_values:
        return query_values[command]

    if command == "NR;":
        return f"NR{state.noise_reduction};"
    if command.startswith("NR") and command.endswith(";"):
        value = command[2:-1]
        if value.isdigit():
            state.noise_reduction = int(value)
        return None

    mutable_prefixes = {
        "BC": "manual_notch",
        "BP": "notch_value",
        "FW": "bandwidth",
        "NT": "auto_notch",
        "PA": "preamp",
        "PC": "power",
        "RA": "attenuator",
        "RL": "noise_reduction_level",
        "SH": "high_cut",
        "SL": "low_cut",
    }
    for prefix, attribute in mutable_prefixes.items():
        if command.startswith(prefix) and command.endswith(";"):
            value = command[len(prefix) : -1]
            if value.isdigit():
                setattr(state, attribute, int(value))
            return None

    if command == "EX0120000;":
        return "EX01200000;"
    if command.startswith("EX"):
        return None

    # Kenwood setters/actions are not acknowledged. Replying would leave stale
    # data in FLRig's serial receive queue.
    if command.startswith(("RX", "TX", "UP", "DN", "BU", "BD", "SA")):
        return None

    # Unknown reads use the Kenwood CAT error response. FLRig can continue and
    # leave unchanged a control that this focused emulator does not model.
    return "?;"


def publish_symlink(link_path: Path, target: str) -> None:
    link_path.parent.mkdir(parents=True, exist_ok=True)
    temp_name = tempfile.mktemp(prefix=f".{link_path.name}.", dir=link_path.parent)
    os.symlink(target, temp_name)
    os.replace(temp_name, link_path)


def run(link_path: Path, *, freq_a: int, freq_b: int) -> int:
    master_fd, slave_fd = pty.openpty()
    tty.setraw(master_fd)
    tty.setraw(slave_fd)
    attributes = termios.tcgetattr(slave_fd)
    attributes[2] &= ~getattr(termios, "CRTSCTS", 0)
    termios.tcsetattr(slave_fd, termios.TCSANOW, attributes)
    slave_name = os.ttyname(slave_fd)
    publish_symlink(link_path, slave_name)
    state = RadioState(freq_a=freq_a, freq_b=freq_b)
    running = True

    def stop(_signum: int, _frame: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print(f"TS-2000 CAT emulator ready: {link_path} -> {slave_name}", flush=True)

    pending = bytearray()
    try:
        while running:
            readable, _, _ = select.select([master_fd], [], [], 0.5)
            if not readable:
                continue
            try:
                chunk = os.read(master_fd, 256)
            except InterruptedError:
                continue
            except OSError:
                break
            if not chunk:
                continue
            pending.extend(chunk)
            while b";" in pending:
                raw_command, _, remainder = pending.partition(b";")
                pending = bytearray(remainder)
                command = raw_command.decode("ascii", errors="ignore") + ";"
                response = response_for(command, state)
                if response:
                    os.write(master_fd, response.encode("ascii"))
    finally:
        os.close(slave_fd)
        os.close(master_fd)
        try:
            if link_path.is_symlink() and os.readlink(link_path) == slave_name:
                link_path.unlink()
        except FileNotFoundError:
            pass
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--device-link", required=True, type=Path)
    parser.add_argument("--frequency", type=int, default=14_074_000)
    parser.add_argument("--frequency-b", type=int, default=7_078_000)
    args = parser.parse_args()
    return run(args.device_link, freq_a=args.frequency, freq_b=args.frequency_b)


if __name__ == "__main__":
    sys.exit(main())
