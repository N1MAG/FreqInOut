"""
WFHZ test helper using pyFldigi if available, with a fallback to raw XML-RPC.

Requires:
  - FLDigi running with XML-RPC (rigctrl) enabled.
  - pyFldigi installed (optional; fallback uses xmlrpc.client).

Usage:
  python test_pyfldigi_wfhz.py --offset 900 --host 127.0.0.1 --port 7362
"""

import argparse
import sys
import xmlrpc.client

try:
    import pyfldigi  # type: ignore
except ImportError:
    pyfldigi = None


def set_get_wfhz_pyfldigi(offset: int):
    client = pyfldigi.Client()
    # Some pyFldigi versions expose wf or fldigi.wf with set_center/get_center
    wf = getattr(client, "wf", None)
    if wf is None and hasattr(client, "fldigi"):
        wf = getattr(client.fldigi, "wf", None)
    if wf is None:
        raise AttributeError("pyfldigi Client has no wf attribute")

    if hasattr(wf, "set_center"):
        wf.set_center(offset)
    elif hasattr(wf, "set_wf_center"):
        wf.set_wf_center(offset)
    else:
        raise AttributeError("pyfldigi wf missing set_center/set_wf_center")

    if hasattr(wf, "get_center"):
        return wf.get_center()
    if hasattr(wf, "get_wf_center"):
        return wf.get_wf_center()
    return None


def set_get_wfhz_xmlrpc(offset: int, host: str, port: int):
    proxy = xmlrpc.client.ServerProxy(f"http://{host}:{port}", allow_none=True)
    cmd = f"FLDIGI.WFHZ:{offset}"
    # Try fldigi.main.shell then main.shell
    for path in ("fldigi.main.shell", "main.shell"):
        try:
            fn = proxy
            for part in path.split("."):
                fn = getattr(fn, part)
            fn(cmd)
            return None
        except Exception:
            continue
    raise RuntimeError("All XML-RPC shell attempts failed")


def main() -> int:
    parser = argparse.ArgumentParser(description="Test FLDigi WFHZ via pyFldigi or XML-RPC")
    parser.add_argument("--offset", type=int, required=True, help="WFHZ offset in Hz (e.g., 900)")
    parser.add_argument("--host", default="127.0.0.1", help="FLDigi XML-RPC host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=7362, help="FLDigi XML-RPC port (default: 7362)")
    args = parser.parse_args()

    if pyfldigi is not None:
        try:
            cur = set_get_wfhz_pyfldigi(args.offset)
            print(f"Set WFHZ to {args.offset} Hz via pyFldigi")
            if cur is not None:
                print(f"WFHZ readback: {cur} Hz")
                if abs(cur - args.offset) > 1:
                    print("WARNING: WFHZ readback does not match requested value.")
                    return 2
            return 0
        except Exception as e:
            print(f"pyFldigi path failed: {e}; falling back to raw XML-RPC")

    try:
        set_get_wfhz_xmlrpc(args.offset, args.host, args.port)
        print(f"Set WFHZ to {args.offset} Hz via XML-RPC shell (no readback available).")
        return 0
    except Exception as e:
        print(f"Failed to set WFHZ via XML-RPC: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
