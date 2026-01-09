from __future__ import annotations

import html
import re
from pathlib import Path
from typing import List, Tuple

from PySide6.QtCore import Qt, QUrl
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QListWidget, QListWidgetItem, QTextBrowser, QLabel


class HelpTab(QWidget):
    """
    Displays docs/guide.html with a section index for quick navigation.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._doc_path = Path(__file__).resolve().parents[2] / "docs" / "guide.html"

        layout = QHBoxLayout(self)

        # Left: index with heading
        toc_col = QVBoxLayout()
        toc_col.setAlignment(Qt.AlignTop)
        toc_col.addWidget(QLabel("<b>Table of Contents</b>"))
        self.toc_list = QListWidget()
        self.toc_list.setMinimumWidth(220)
        self.toc_list.itemClicked.connect(self._on_toc_clicked)
        toc_col.addWidget(self.toc_list)
        layout.addLayout(toc_col)

        # Right: viewer
        viewer_col = QVBoxLayout()
        viewer_col.setAlignment(Qt.AlignTop)
        viewer_col.addWidget(QLabel("<h3>FreqInOut Guide</h3>"))

        self.viewer = QTextBrowser()
        if self._doc_path.exists():
            self.viewer.setSource(QUrl.fromLocalFile(str(self._doc_path)))
            self._build_toc()
        viewer_col.addWidget(self.viewer)

        layout.addLayout(viewer_col, stretch=1)

    def _parse_headings(self, html_text: str) -> List[Tuple[int, str, str]]:
        """
        Return a list of (level, anchor, text) for h1/h2/h3 tags that have ids.
        """
        headings: List[Tuple[int, str, str]] = []
        pattern = re.compile(
            r"<h([1-3])[^>]*?(?:id=\"([^\"]+)\")?[^>]*>(.*?)</h\\1>",
            flags=re.IGNORECASE | re.DOTALL,
        )
        # Fix backreference: </h\1> (single slash) is required; build a second pattern for safety.
        pattern_fix = re.compile(
            r"<h([1-3])[^>]*?(?:id=\"([^\"]+)\")?[^>]*>(.*?)</h\1>",
            flags=re.IGNORECASE | re.DOTALL,
        )
        matches = list(pattern_fix.finditer(html_text)) or list(pattern.finditer(html_text))
        for match in matches:
            level = int(match.group(1))
            anchor = match.group(2) or ""
            raw_text = match.group(3)
            # Strip tags and unescape entities
            text_clean = re.sub("<[^>]+>", "", raw_text)
            text_clean = html.unescape(text_clean).strip()
            if text_clean:
                headings.append((level, anchor, text_clean))
        return headings

    def _build_toc(self):
        """
        Parse headings from the guide to build a formatted index.
        """
        try:
            html_text = self._doc_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return
        headings = self._parse_headings(html_text)
        self.toc_list.clear()
        if not headings:
            self.toc_list.addItem("(No headings found)")
            return
        for level, anchor, text in headings:
            indent = "    " * (level - 1)
            prefix = "• " if level == 1 else "– "
            item = QListWidgetItem(f"{indent}{prefix}{text}")
            # Store anchor when available; otherwise store text for find() fallback
            item.setData(Qt.UserRole, anchor or text)
            font: QFont = item.font()
            if level == 1:
                font.setBold(True)
            item.setFont(font)
            self.toc_list.addItem(item)

    def _on_toc_clicked(self, item: QListWidgetItem):
        target = item.data(Qt.UserRole)
        if not target:
            return
        if target.startswith("#"):
            target = target.lstrip("#")
        base_url = QUrl.fromLocalFile(str(self._doc_path))
        if "#" in base_url.toString():
            base_url = QUrl.fromLocalFile(str(self._doc_path))
        # Try anchor if we have one
        if target and not target.strip().isspace() and "#" not in target and re.match(r"^[A-Za-z0-9_-]+$", target):
            self.viewer.setSource(QUrl(f"{base_url.toString()}#{target}"))
        else:
            # Fallback: find text in document
            doc = self.viewer.document()
            cursor = doc.find(str(target))
            if cursor and not cursor.isNull():
                self.viewer.setTextCursor(cursor)
