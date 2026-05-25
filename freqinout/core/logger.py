
import logging
import logging.handlers
import os
import sys
import tempfile
import time
from collections import deque
from pathlib import Path

from freqinout.core.config_paths import get_config_dir as shared_get_config_dir

APP_NAME = "FreqInOut"
_RECENT_ISSUES_MAX = 200
_recent_issues = deque(maxlen=_RECENT_ISSUES_MAX)
_current_log_level_name = "INFO"

# Optional env override for log level (DEBUG/INFO/WARNING/ERROR/CRITICAL)
_ENV_LOG_LEVEL = os.getenv("FREQINOUT_LOG_LEVEL", "").strip().upper()
_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
    "DISABLED": None,
}

def _get_config_dir():
    """
    Determine a writable config/log directory using the shared runtime profile
    root, with local and temp fallbacks if that path is unavailable.
    """
    candidates = []
    try:
        candidates.append(Path(shared_get_config_dir()))
    except Exception:
        pass
    candidates.append(Path(os.path.abspath(os.path.join(os.getcwd(), f".{APP_NAME.lower()}"))))
    candidates.append(Path(tempfile.gettempdir()) / APP_NAME)

    for path in candidates:
        try:
            path = Path(path)
            path.mkdir(parents=True, exist_ok=True)
            # ensure writable
            test_file = path / ".write_test"
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink(missing_ok=True)  # type: ignore[arg-type]
            return str(path)
        except Exception:
            continue
    fallback = Path(tempfile.gettempdir()) / APP_NAME
    fallback.mkdir(parents=True, exist_ok=True)
    return str(fallback)

def _get_log_file():
    return os.path.join(_get_config_dir(), "freqinout.log")

def _supports_color():
    return sys.stdout.isatty()

class ColorFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[41m",
    }
    RESET = "\033[0m"
    def format(self, record):
        msg = super().format(record)
        if _supports_color():
            color = self.COLORS.get(record.levelname)
            if color:
                return f"{color}{msg}{self.RESET}"
        return msg


class ResilientRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """
    RotatingFileHandler variant that tolerates transient file locks on rollover.

    On Windows, antivirus/indexers or external viewers can briefly lock the log
    file, causing rename during doRollover() to raise PermissionError/WinError 32.
    This handler suppresses rollover attempts for a short cooldown and keeps
    app logging flowing to the active file.
    """

    def __init__(self, *args, rollover_retry_seconds: float = 30.0, **kwargs):
        super().__init__(*args, **kwargs)
        self._rollover_retry_seconds = max(1.0, float(rollover_retry_seconds))
        self._rollover_suppressed_until = 0.0

    @staticmethod
    def _is_lock_error(exc: Exception) -> bool:
        if isinstance(exc, PermissionError):
            return True
        if isinstance(exc, OSError):
            if getattr(exc, "winerror", None) == 32:
                return True
            # 13: permission denied, 16: device/resource busy
            if getattr(exc, "errno", None) in {13, 16}:
                return True
        return False

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if self.maxBytes > 0 and self.shouldRollover(record):
                now = time.monotonic()
                if now >= self._rollover_suppressed_until:
                    try:
                        self.doRollover()
                        self._rollover_suppressed_until = 0.0
                    except Exception as exc:
                        if self._is_lock_error(exc):
                            self._rollover_suppressed_until = now + self._rollover_retry_seconds
                        else:
                            raise
            logging.FileHandler.emit(self, record)
        except Exception:
            self.handleError(record)


class RecentIssueHandler(logging.Handler):
    """Always-on in-memory ring buffer for field diagnostics."""

    def __init__(self):
        super().__init__(logging.WARNING)
        self.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            _recent_issues.append(self.format(record))
        except Exception:
            pass


def _add_recent_issue_handler(logger: logging.Logger) -> None:
    if any(isinstance(h, RecentIssueHandler) for h in logger.handlers):
        return
    logger.addHandler(RecentIssueHandler())


def _has_output_handler(logger: logging.Logger) -> bool:
    return any(not isinstance(h, RecentIssueHandler) for h in logger.handlers)


def get_recent_issues() -> list[str]:
    return list(_recent_issues)


def setup_logger(name: str = "freqinout", log_to_console=True, log_level=logging.INFO):
    global _current_log_level_name
    # Allow env var override
    if _ENV_LOG_LEVEL in _LEVEL_MAP:
        log_level = _LEVEL_MAP[_ENV_LOG_LEVEL]
        _current_log_level_name = _ENV_LOG_LEVEL
    logger = logging.getLogger(name)
    if _has_output_handler(logger) and log_level is not None:
        # Update existing handlers if already configured
        for h in logger.handlers:
            if isinstance(h, RecentIssueHandler):
                h.setLevel(logging.WARNING)
            else:
                h.setLevel(log_level)
        _add_recent_issue_handler(logger)
        logger.setLevel(log_level)
        logger.disabled = False
        return logger

    logger.handlers = []
    _add_recent_issue_handler(logger)
    if log_level is None:
        _current_log_level_name = "DISABLED"
        logger.setLevel(logging.WARNING)
        logger.disabled = False
        return logger

    _current_log_level_name = logging.getLevelName(log_level)
    logger.setLevel(log_level)
    logger.disabled = False

    if log_to_console:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(log_level)
        ch.setFormatter(ColorFormatter("[%(levelname)s] %(message)s"))
        logger.addHandler(ch)

    log_file = _get_log_file()
    try:
        fh = ResilientRotatingFileHandler(
            log_file,
            maxBytes=2 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
            rollover_retry_seconds=30.0,
        )
        fh.setLevel(log_level)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(fh)
        if log_level <= logging.INFO:
            logger.info(f"Logger initialized. Log file: {log_file}")
    except Exception as e:
        logger.error("Logger: failed to open log file %s: %s", log_file, e)

    return logger

log = setup_logger()


def set_log_level(level_name: str) -> None:
    """
    Update the global logger level (both console and file handlers) at runtime.
    """
    level_name = level_name.strip().upper()
    global _current_log_level_name
    lvl = _LEVEL_MAP.get(level_name, logging.INFO)
    logger = logging.getLogger("freqinout")
    if lvl is None:
        _current_log_level_name = "DISABLED"
        for h in list(logger.handlers):
            logger.removeHandler(h)
        _add_recent_issue_handler(logger)
        logger.setLevel(logging.WARNING)
        logger.disabled = False
        return

    # Re-enable if previously disabled
    _current_log_level_name = logging.getLevelName(lvl)
    logger.disabled = False
    if not _has_output_handler(logger):
        setup_logger(name="freqinout", log_level=lvl)
        return
    logger.setLevel(lvl)
    for h in logger.handlers:
        if isinstance(h, RecentIssueHandler):
            h.setLevel(logging.WARNING)
        else:
            h.setLevel(lvl)


def get_log_level() -> str:
    """
    Return the current global log level name.
    """
    return _current_log_level_name
