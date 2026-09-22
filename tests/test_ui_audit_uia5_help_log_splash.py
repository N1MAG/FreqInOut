"""Focused UIA-5 probes for Help, Log Viewer, and startup splash surfaces."""

from __future__ import annotations

import os
import inspect

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QEvent, Qt, QEventLoop, QTimer, QUrl
from PySide6.QtGui import QFont, QTextDocument
from PySide6.QtWidgets import QApplication, QBoxLayout, QWidget

from freqinout.gui.bounded_snapshot_worker import SnapshotWorkerController
from freqinout.gui.help_tab import HelpTab
from freqinout.gui.log_viewer import LogViewerTab
from freqinout.gui.startup_splash import StartupSplash


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _wait_until(predicate, timeout_ms: int = 2000) -> bool:
    loop = QEventLoop()
    poll = QTimer()
    poll.setInterval(10)
    poll.timeout.connect(lambda: loop.quit() if predicate() else None)
    deadline = QTimer()
    deadline.setSingleShot(True)
    deadline.timeout.connect(loop.quit)
    poll.start()
    deadline.start(timeout_ms)
    loop.exec()
    poll.stop()
    return bool(predicate())


def test_help_reflows_navigation_and_keeps_content_scroll_owned() -> None:
    _app()
    tab = HelpTab()
    try:
        assert _wait_until(lambda: bool(tab._document_html))
        assert tab.toc_list.count() > 0
        assert "Loading the FreqInOut guide" not in tab.viewer.toPlainText()
        for width, height in ((1920, 1080), (1000, 700), (900, 560)):
            tab.resize(width, height)
            tab._update_responsive_layout()
            _app().processEvents()
            assert tab.viewer.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert tab._help_layout.direction() == QBoxLayout.TopToBottom
        assert tab.toc_list.minimumWidth() == 0
        tab.resize(1920, 1080)
        tab._update_responsive_layout()
        assert tab._help_layout.direction() == QBoxLayout.LeftToRight
        assert tab.toc_list.minimumWidth() > tab.toc_title.sizeHint().width()
    finally:
        tab.close()
        tab.deleteLater()


def test_help_topic_theme_and_resize_paths_use_one_cached_document_snapshot(monkeypatch) -> None:
    calls: list[str] = []

    def read_snapshot(_path) -> str:
        calls.append("read")
        return '<html><body><h1 id="start">Start</h1><p>Guide body</p></body></html>'

    monkeypatch.setattr(HelpTab, "_read_document_snapshot", staticmethod(read_snapshot))
    tab = HelpTab()
    try:
        assert _wait_until(lambda: bool(tab._document_html))
        assert calls == ["read"]
        assert "Loading the FreqInOut guide" not in tab.viewer.toPlainText()
        assert "Guide body" in tab.viewer.toPlainText()
        assert tab.toc_list.count() == 1
        assert "Start" in tab.toc_list.item(0).text()
        assert tab.viewer.document().baseUrl() == QUrl.fromLocalFile(str(tab._doc_path))
        generation = tab._document_generation
        tab.open_anchor("start")
        tab.resize(900, 560)
        tab.apply_theme()
        _app().processEvents()
        assert calls == ["read"]
        assert tab._document_generation == generation
    finally:
        tab.shutdown()
        tab.close()
        tab.deleteLater()


def test_help_resolves_local_images_for_viewer_and_pdf_export(tmp_path) -> None:
    guide = tmp_path / "docs" / "guide.html"
    logo = tmp_path / "assets" / "logo.png"
    logo.parent.mkdir(parents=True)
    guide.parent.mkdir(parents=True)
    logo.write_bytes(b"png-placeholder")
    source = (
        '<img src="../assets/logo.png">'
        '<img src="https://example.invalid/remote.png">'
        '<img src="data:image/png;base64,abc">'
    )

    resolved = HelpTab._resolve_local_image_urls(source, guide)

    assert QUrl.fromLocalFile(str(logo)).toString() in resolved
    assert 'src="../assets/logo.png"' not in resolved
    assert 'src="https://example.invalid/remote.png"' in resolved
    assert 'src="data:image/png;base64,abc"' in resolved


def test_loaded_help_document_uses_absolute_logo_url() -> None:
    _app()
    tab = HelpTab()
    try:
        assert _wait_until(lambda: bool(tab._document_html))
        rendered_html = tab.viewer.document().toHtml()
        expected = QUrl.fromLocalFile(
            str((tab._doc_path.parent / "../assets/FreqInOut_logo.png").resolve())
        ).toString()
        assert expected in rendered_html
        resource = tab.viewer.document().resource(
            QTextDocument.ImageResource,
            QUrl(expected),
        )
        assert resource is not None
        assert not resource.isNull()
    finally:
        tab.shutdown()
        tab.close()
        tab.deleteLater()


def test_operating_groups_help_anchor_is_selectable_in_rendered_guide() -> None:
    _app()
    tab = HelpTab()
    try:
        assert _wait_until(lambda: bool(tab._document_html))
        tab.open_anchor("settings-hf-groups-details")
        selected = tab.toc_list.currentItem()
        assert selected is not None
        assert selected.data(Qt.UserRole) == "settings-hf-groups-details"
        assert "Operating Groups Details" in selected.text()
    finally:
        tab.shutdown()
        tab.close()
        tab.deleteLater()


def test_log_viewer_uses_qfont_for_explicit_log_font_and_releases_compact_floor(
    monkeypatch, tmp_path
) -> None:
    import freqinout.gui.log_viewer as module

    _app()
    monkeypatch.setattr(module, "_get_log_file", lambda: str(tmp_path / "fio.log"))
    (tmp_path / "fio.log").write_text("[INFO] first\n[ERROR] second\n", encoding="utf-8")
    tab = module.LogViewerTab()
    try:
        tab.level_combo.setCurrentText("ALL")
        assert _wait_until(lambda: "second" in tab.text.toPlainText())
        tab.resize(900, 560)
        tab._update_log_responsive_layout()
        assert tab.search_input.minimumWidth() == 0
        tab.font_spin.setValue(18)
        assert tab.text.font().pointSize() == 18
        assert "font-size" not in tab.text.styleSheet().lower()
        assert tab.text.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        generation = tab._snapshot_generation
        tab.search_input.setText("second")
        _app().processEvents()
        assert tab._snapshot_generation == generation
        assert "second" in tab.text.toPlainText()
        tab.resize(1920, 1080)
        tab._update_log_responsive_layout()
        assert tab.search_input.minimumWidth() == 180
    finally:
        tab.shutdown()
        tab.close()
        tab.deleteLater()


def test_startup_splash_scales_from_app_font_and_theme_without_raw_font_css() -> None:
    app = _app()
    original = QFont(app.font())
    try:
        normal = StartupSplash._build_pixmap("1.0")
        large_font = QFont(original)
        large_font.setPointSizeF(max(16.0, original.pointSizeF() * 1.5))
        app.setFont(large_font)
        large = StartupSplash._build_pixmap("1.0")
        assert large.width() >= normal.width()
        assert large.height() >= normal.height()
        assert large.width() > normal.width() or large.height() > normal.height()
    finally:
        app.setFont(original)


def test_log_tail_read_is_bounded_and_cache_render_paths_do_not_read(tmp_path) -> None:
    log_path = tmp_path / "large.log"
    log_path.write_text("".join(f"[INFO] line {index}\n" for index in range(1200)), encoding="utf-8")
    lines = LogViewerTab._read_log_tail_snapshot(str(log_path), max_lines=80, max_bytes=16 * 1024)
    assert len(lines) == 80
    assert lines[-1].endswith("line 1199\n")

    render_source = inspect.getsource(LogViewerTab._render_snapshot)
    search_source = inspect.getsource(LogViewerTab._search)
    theme_source = inspect.getsource(LogViewerTab._apply_theme)
    for source in (render_source, search_source, theme_source):
        assert "_read_log_tail_snapshot" not in source
        assert "open(" not in source
    assert "SnapshotWorkerController" in inspect.getsource(LogViewerTab._ensure_snapshot_worker)


def test_secondary_dialog_and_shell_floors_do_not_block_compact_viewport() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    main_source = (root / "freqinout/gui/main_window.py").read_text(encoding="utf-8")
    settings_source = (root / "freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    assert "layout.setSizeConstraint(QLayout.SetNoConstraint)" in main_source
    assert "self.setMinimumSize(0, 0)" in main_source
    assert "preview.setMinimumSize(640, 420)" not in settings_source
    assert "dlg.setMinimumSize(0, 0)" in settings_source
    assert "help_side = button_height_for_font(help_btn" in settings_source


def test_snapshot_worker_stops_when_receiver_is_deleted_without_close_event() -> None:
    app = _app()
    receiver = QWidget()
    controller = SnapshotWorkerController(receiver, lambda *_args: None)
    receiver.deleteLater()
    app.sendPostedEvents(None, QEvent.DeferredDelete)
    app.processEvents()
    assert controller._stopped is True
