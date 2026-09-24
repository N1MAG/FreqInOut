"""Best-effort, PID-scoped titles for applications without a native option.

The helper never launches, stops, or selects a process by application name.
It changes only top-level windows that advertise the exact PID returned by the
launch call.  Failure is intentionally non-fatal: a display server or window
manager may deny cross-process title changes, especially under Wayland.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import platform


def set_process_window_title(
    pid: object,
    title: object,
    *,
    platform_name: str | None = None,
) -> bool:
    """Set one launched process's visible title where the OS permits it."""

    try:
        process_id = int(pid)
    except (TypeError, ValueError):
        return False
    value = str(title or "").strip()
    if process_id <= 0 or not value:
        return False
    system = str(platform_name or platform.system() or "").strip().casefold()
    try:
        if system == "windows":
            return _set_windows_title(process_id, value)
        if system == "linux":
            return _set_x11_title(process_id, value)
    except Exception:
        return False
    return False


def _set_windows_title(pid: int, title: str) -> bool:
    user32 = ctypes.windll.user32  # type: ignore[attr-defined]
    found = False
    callback_type = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p)  # type: ignore[attr-defined]
    user32.EnumWindows.argtypes = [callback_type, ctypes.c_void_p]
    user32.EnumWindows.restype = ctypes.c_bool
    user32.GetWindowThreadProcessId.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_ulong),
    ]
    user32.GetWindowThreadProcessId.restype = ctypes.c_ulong
    user32.IsWindowVisible.argtypes = [ctypes.c_void_p]
    user32.IsWindowVisible.restype = ctypes.c_bool
    user32.SetWindowTextW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
    user32.SetWindowTextW.restype = ctypes.c_bool

    @callback_type
    def _visit(hwnd: int, _param: int) -> bool:
        nonlocal found
        owner = ctypes.c_ulong()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
        if int(owner.value) == pid and user32.IsWindowVisible(hwnd):
            found = bool(user32.SetWindowTextW(hwnd, title)) or found
        return True

    user32.EnumWindows(_visit, 0)
    return found


def _set_x11_title(pid: int, title: str) -> bool:
    if not os.environ.get("DISPLAY"):
        return False
    library_name = ctypes.util.find_library("X11")
    if not library_name:
        return False
    x11 = ctypes.CDLL(library_name)
    display_type = ctypes.c_void_p
    window_type = ctypes.c_ulong
    atom_type = ctypes.c_ulong

    x11.XOpenDisplay.argtypes = [ctypes.c_char_p]
    x11.XOpenDisplay.restype = display_type
    x11.XDefaultRootWindow.argtypes = [display_type]
    x11.XDefaultRootWindow.restype = window_type
    x11.XInternAtom.argtypes = [display_type, ctypes.c_char_p, ctypes.c_int]
    x11.XInternAtom.restype = atom_type
    x11.XQueryTree.argtypes = [
        display_type,
        window_type,
        ctypes.POINTER(window_type),
        ctypes.POINTER(window_type),
        ctypes.POINTER(ctypes.POINTER(window_type)),
        ctypes.POINTER(ctypes.c_uint),
    ]
    x11.XQueryTree.restype = ctypes.c_int
    x11.XGetWindowProperty.argtypes = [
        display_type,
        window_type,
        atom_type,
        ctypes.c_long,
        ctypes.c_long,
        ctypes.c_int,
        atom_type,
        ctypes.POINTER(atom_type),
        ctypes.POINTER(ctypes.c_int),
        ctypes.POINTER(ctypes.c_ulong),
        ctypes.POINTER(ctypes.c_ulong),
        ctypes.POINTER(ctypes.POINTER(ctypes.c_ubyte)),
    ]
    x11.XGetWindowProperty.restype = ctypes.c_int
    x11.XChangeProperty.argtypes = [
        display_type,
        window_type,
        atom_type,
        atom_type,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_ubyte),
        ctypes.c_int,
    ]
    x11.XStoreName.argtypes = [display_type, window_type, ctypes.c_char_p]
    x11.XFree.argtypes = [ctypes.c_void_p]
    x11.XFlush.argtypes = [display_type]
    x11.XCloseDisplay.argtypes = [display_type]

    display = x11.XOpenDisplay(None)
    if not display:
        return False
    try:
        root = x11.XDefaultRootWindow(display)
        pid_atom = x11.XInternAtom(display, b"_NET_WM_PID", 1)
        title_atom = x11.XInternAtom(display, b"_NET_WM_NAME", 0)
        utf8_atom = x11.XInternAtom(display, b"UTF8_STRING", 0)
        if not pid_atom or not title_atom or not utf8_atom:
            return False
        candidates = _x11_windows(x11, display, root)
        changed = False
        encoded = title.encode("utf-8")
        buffer = (ctypes.c_ubyte * len(encoded)).from_buffer_copy(encoded)
        for window in candidates:
            if _x11_window_pid(x11, display, window, pid_atom) != pid:
                continue
            x11.XChangeProperty(
                display,
                window,
                title_atom,
                utf8_atom,
                8,
                0,
                buffer,
                len(encoded),
            )
            x11.XStoreName(display, window, encoded)
            changed = True
        if changed:
            x11.XFlush(display)
        return changed
    finally:
        x11.XCloseDisplay(display)


def _x11_windows(x11: object, display: object, root: int) -> tuple[int, ...]:
    pending = [(int(root), 0)]
    result: list[int] = []
    seen: set[int] = set()
    while pending and len(seen) < 4096:
        parent, depth = pending.pop()
        if parent in seen:
            continue
        seen.add(parent)
        root_return = ctypes.c_ulong()
        parent_return = ctypes.c_ulong()
        children = ctypes.POINTER(ctypes.c_ulong)()
        count = ctypes.c_uint()
        ok = x11.XQueryTree(
            display,
            parent,
            ctypes.byref(root_return),
            ctypes.byref(parent_return),
            ctypes.byref(children),
            ctypes.byref(count),
        )
        if not ok:
            continue
        try:
            values = tuple(int(children[index]) for index in range(int(count.value)))
        finally:
            if children:
                x11.XFree(children)
        result.extend(values)
        if depth < 8:
            pending.extend((value, depth + 1) for value in values)
    return tuple(dict.fromkeys(result))


def _x11_window_pid(x11: object, display: object, window: int, pid_atom: int) -> int:
    actual_type = ctypes.c_ulong()
    actual_format = ctypes.c_int()
    item_count = ctypes.c_ulong()
    remaining = ctypes.c_ulong()
    value = ctypes.POINTER(ctypes.c_ubyte)()
    status = x11.XGetWindowProperty(
        display,
        window,
        pid_atom,
        0,
        1,
        0,
        0,
        ctypes.byref(actual_type),
        ctypes.byref(actual_format),
        ctypes.byref(item_count),
        ctypes.byref(remaining),
        ctypes.byref(value),
    )
    if status != 0 or not value or not item_count.value:
        return 0
    try:
        if actual_format.value != 32:
            return 0
        return int(ctypes.cast(value, ctypes.POINTER(ctypes.c_ulong))[0])
    finally:
        x11.XFree(value)


__all__ = ["set_process_window_title"]
