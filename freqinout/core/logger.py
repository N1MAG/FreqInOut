
import logging
import logging.handlers
import os
import sys
import platform
from pathlib import Path

APP_NAME = "FreqInOut"

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
    Determine a writable config/log directory.
    Preferred: %APPDATA%\\FreqInOut on Windows, ~/.config/freqinout on others.
    Fallback: a `.freqinout` folder under the current working directory if the
    preferred path cannot be created (e.g., roaming profile or permissions issues).
    """
    candidates = []
    if platform.system() == "Windows":
        candidates.append(os.path.join(os.getenv("APPDATA", os.path.expanduser("~")), APP_NAME))
    else:
        candidates.append(os.path.expanduser(f"~/.config/{APP_NAME.lower()}"))
    # Local fallback
    candidates.append(os.path.abspath(os.path.join(os.getcwd(), f".{APP_NAME.lower()}")))

    for path in candidates:
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
            # ensure writable
            test_file = Path(path) / ".write_test"
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink(missing_ok=True)  # type: ignore[arg-type]
            return path
        except Exception:
            continue
    # Last resort: temp directory
    import tempfile
    fallback = os.path.join(tempfile.gettempdir(), APP_NAME)
    Path(fallback).mkdir(parents=True, exist_ok=True)
    return fallback

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

def setup_logger(name: str = "freqinout", log_to_console=True, log_level=logging.INFO):
    # Allow env var override
    if _ENV_LOG_LEVEL in _LEVEL_MAP:
        log_level = _LEVEL_MAP[_ENV_LOG_LEVEL]
    logger = logging.getLogger(name)
    if logger.handlers and log_level is not None:
        # Update existing handlers if already configured
        for h in logger.handlers:
            h.setLevel(log_level)
        logger.setLevel(log_level)
        logger.disabled = False
        return logger

    logger.handlers = []
    if log_level is None:
        logger.setLevel(logging.CRITICAL + 1)
        logger.disabled = True
        return logger

    logger.setLevel(log_level)
    logger.disabled = False

    if log_to_console:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(log_level)
        ch.setFormatter(ColorFormatter("[%(levelname)s] %(message)s"))
        logger.addHandler(ch)

    log_file = _get_log_file()
    try:
        fh = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=2 * 1024 * 1024, backupCount=5, encoding="utf-8"
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
    lvl = _LEVEL_MAP.get(level_name, logging.INFO)
    logger = logging.getLogger("freqinout")
    if lvl is None:
        logger.disabled = True
        for h in list(logger.handlers):
            logger.removeHandler(h)
        logger.setLevel(logging.CRITICAL + 1)
        return

    # Re-enable if previously disabled
    logger.disabled = False
    if not logger.handlers:
        setup_logger(name="freqinout", log_level=lvl)
        return
    logger.setLevel(lvl)
    for h in logger.handlers:
        h.setLevel(lvl)


def get_log_level() -> str:
    """
    Return the current global log level name.
    """
    logger = logging.getLogger("freqinout")
    return logging.getLevelName(logger.getEffectiveLevel())
