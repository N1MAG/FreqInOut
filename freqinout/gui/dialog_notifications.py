from __future__ import annotations

import os
from typing import Any, Callable

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import QApplication, QMessageBox, QWidget


_ORIGINAL_INFORMATION: Callable[..., Any] | None = None
_INSTALLED = False
_ACTIVE_INFORMATION_BOXES: list[QMessageBox] = []


def _button_value(button: Any) -> Any:
    try:
        return int(button)
    except Exception:
        return getattr(button, "value", button)


def _standard_button(name: str) -> Any:
    standard = getattr(QMessageBox, "StandardButton", None)
    if standard is not None and hasattr(standard, name):
        return getattr(standard, name)
    return getattr(QMessageBox, name)


def _ok_only(buttons: Any) -> bool:
    ok_value = _button_value(_standard_button("Ok"))
    no_button_value = _button_value(_standard_button("NoButton"))
    buttons_value = _button_value(buttons)
    return buttons_value in {ok_value, no_button_value}


def _timeout_for_text(text: object, *, default_timeout_ms: int) -> int:
    text_len = len(str(text or ""))
    scaled = int(default_timeout_ms) + min(24000, text_len * 35)
    return max(2500, min(30000, scaled))


def show_auto_closing_information(
    parent: QWidget | None,
    title: str,
    text: str,
    *,
    default_timeout_ms: int = 4500,
) -> Any:
    msg = QMessageBox(parent)
    msg.setIcon(QMessageBox.Icon.Information)
    msg.setWindowTitle(str(title or "Information"))
    msg.setText(str(text or ""))
    msg.setStandardButtons(_standard_button("Ok"))
    msg.setDefaultButton(_standard_button("Ok"))
    msg.setWindowModality(Qt.WindowModality.NonModal)
    msg.setModal(False)
    msg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)

    _ACTIVE_INFORMATION_BOXES.append(msg)

    def _remove_box() -> None:
        try:
            _ACTIVE_INFORMATION_BOXES.remove(msg)
        except ValueError:
            pass

    msg.finished.connect(lambda _result: _remove_box())
    QTimer.singleShot(_timeout_for_text(text, default_timeout_ms=default_timeout_ms), msg.accept)
    msg.show()
    return _standard_button("Ok")


def install_auto_closing_information_dialogs(*, default_timeout_ms: int = 4500) -> None:
    global _INSTALLED, _ORIGINAL_INFORMATION
    if _INSTALLED:
        return
    _ORIGINAL_INFORMATION = QMessageBox.information

    def _information(parent: QWidget | None, title: str, text: str, *args: Any, **kwargs: Any) -> Any:
        if os.environ.get("FREQINOUT_BLOCKING_INFO_DIALOGS") == "1":
            return _ORIGINAL_INFORMATION(parent, title, text, *args, **kwargs)  # type: ignore[misc]

        buttons = args[0] if len(args) >= 1 else kwargs.get("buttons", _standard_button("Ok"))
        if not _ok_only(buttons):
            return _ORIGINAL_INFORMATION(parent, title, text, *args, **kwargs)  # type: ignore[misc]

        app = QApplication.instance()
        if app is None:
            return _ORIGINAL_INFORMATION(parent, title, text, *args, **kwargs)  # type: ignore[misc]

        return show_auto_closing_information(
            parent,
            str(title or "Information"),
            str(text or ""),
            default_timeout_ms=default_timeout_ms,
        )

    QMessageBox.information = staticmethod(_information)  # type: ignore[method-assign]
    _INSTALLED = True


def uninstall_auto_closing_information_dialogs_for_tests() -> None:
    global _INSTALLED, _ORIGINAL_INFORMATION
    if not _INSTALLED or _ORIGINAL_INFORMATION is None:
        return
    QMessageBox.information = _ORIGINAL_INFORMATION  # type: ignore[method-assign]
    _ORIGINAL_INFORMATION = None
    _INSTALLED = False
