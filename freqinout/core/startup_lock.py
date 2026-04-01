from __future__ import annotations


def try_acquire_single_instance_lock(lockfile: object) -> bool:
    """
    Acquire the single-instance lock once and let callers handle UX.
    """
    try:
        return bool(lockfile.tryLock(0))
    except Exception:
        return False
