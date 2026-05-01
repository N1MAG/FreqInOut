from __future__ import annotations

from typing import Callable, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QPushButton, QTextEdit, QVBoxLayout

from freqinout.gui.theme import button_style, resolve_theme


class WorkspaceBucketCard(QFrame):
    def __init__(
        self,
        settings,
        *,
        title: str,
        read_only: bool = False,
        allow_paste: bool = False,
        on_paste: Optional[Callable[[], None]] = None,
        copy_label: str = "Copy",
        on_copy: Optional[Callable[[], None]] = None,
        secondary_label: str = "",
        on_secondary: Optional[Callable[[], None]] = None,
        parent=None,
    ):
        super().__init__(parent)
        self.settings = settings
        self._title = title
        self._read_only = read_only
        self._allow_paste = allow_paste
        self._on_paste = on_paste
        self._copy_label = copy_label
        self._on_copy = on_copy
        self._secondary_label = secondary_label
        self._on_secondary = on_secondary
        self._count = 0
        self._build_ui()

    def _build_ui(self) -> None:
        self.setFrameShape(QFrame.StyledPanel)
        self.setFrameShadow(QFrame.Raised)
        layout = QVBoxLayout(self)
        header = QHBoxLayout()
        self.title_label = QLabel(f"<b>{self._title}</b>")
        header.addWidget(self.title_label)
        self.count_label = QLabel("0")
        self.count_label.setAlignment(Qt.AlignCenter)
        self.count_label.setStyleSheet("QLabel { border: 1px solid #888888; padding: 1px 6px; border-radius: 8px; }")
        header.addWidget(self.count_label)
        header.addStretch()
        self.copy_btn: Optional[QPushButton] = None
        if self._copy_label:
            self.copy_btn = QPushButton(self._copy_label)
            theme = resolve_theme(self.settings)
            self.copy_btn.setStyleSheet(button_style("info", theme))
            header.addWidget(self.copy_btn)
        self.secondary_btn: Optional[QPushButton] = None
        if self._secondary_label:
            self.secondary_btn = QPushButton(self._secondary_label)
            theme = resolve_theme(self.settings)
            self.secondary_btn.setStyleSheet(button_style("muted", theme))
            header.addWidget(self.secondary_btn)
        self.paste_btn: Optional[QPushButton] = None
        if self._allow_paste:
            self.paste_btn = QPushButton("Paste")
            theme = resolve_theme(self.settings)
            self.paste_btn.setStyleSheet(button_style("eligible_success", theme))
            header.addWidget(self.paste_btn)
        layout.addLayout(header)

        self.text_edit = QTextEdit()
        self.text_edit.setAcceptRichText(False)
        self.text_edit.setReadOnly(self._read_only)
        layout.addWidget(self.text_edit)

        if self.copy_btn is not None:
            if self._on_copy is not None:
                self.copy_btn.clicked.connect(self._on_copy)
            else:
                self.copy_btn.clicked.connect(self._copy_to_clipboard)
        if self.secondary_btn is not None and self._on_secondary is not None:
            self.secondary_btn.clicked.connect(self._on_secondary)
        if self.paste_btn is not None:
            self.paste_btn.clicked.connect(self._paste_from_clipboard)

    def _copy_to_clipboard(self) -> None:
        text = self.text_edit.toPlainText()
        if text:
            from PySide6.QtWidgets import QApplication

            QApplication.clipboard().setText(text)

    def _paste_from_clipboard(self) -> None:
        if self._on_paste is not None:
            self._on_paste()
            return
        from PySide6.QtWidgets import QApplication

        self.text_edit.setPlainText(QApplication.clipboard().text())

    def set_title(self, title: str) -> None:
        self._title = title
        self.title_label.setText(f"<b>{title}</b>")

    def set_count(self, count: int) -> None:
        self._count = max(0, int(count))
        self.count_label.setText(str(self._count))

    def title(self) -> str:
        return self._title

    def set_read_only(self, read_only: bool) -> None:
        self._read_only = bool(read_only)
        self.text_edit.setReadOnly(self._read_only)
        if self.copy_btn is not None:
            self.copy_btn.setVisible(True)
        if self.secondary_btn is not None:
            self.secondary_btn.setVisible(True)
        if self.paste_btn is not None:
            self.paste_btn.setVisible(self._allow_paste)

    def set_text(self, text: str) -> None:
        self.text_edit.setPlainText(text or "")

    def text(self) -> str:
        return self.text_edit.toPlainText()

    def append_text(self, text: str) -> None:
        if text:
            self.text_edit.append(text)

    def set_placeholder(self, text: str) -> None:
        self.text_edit.setPlaceholderText(text)
