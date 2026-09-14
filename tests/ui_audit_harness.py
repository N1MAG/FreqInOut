"""Small, deterministic Qt assertions for the UI conformance audit.

The audit deliberately avoids screenshots and platform-specific pixel values.
Assertions are derived from the widget's active font metrics, Qt's reported
row/tab geometry, and explicit scroll-owner properties.  The helpers are kept
in ``tests`` so a production widget never needs to import test machinery.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

from PySide6.QtCore import QEventLoop, QModelIndex, QRect, QSize, Qt
from PySide6.QtGui import QFont, QFontMetrics
from PySide6.QtWidgets import (
    QAbstractItemView,
    QAbstractScrollArea,
    QApplication,
    QTabBar,
    QTabWidget,
    QWidget,
)


class GeometryAuditError(AssertionError):
    """Raised when a widget violates one of the audit's geometry contracts."""


def ensure_qapplication() -> QApplication:
    """Return the process QApplication, creating the offscreen test app once."""

    app = QApplication.instance()
    if isinstance(app, QApplication):
        return app
    return QApplication([])


@dataclass(frozen=True)
class GeometrySnapshot:
    """Stable geometry evidence captured after event/layout settling."""

    geometry: QRect
    size_hint: QSize
    minimum_size_hint: QSize


def settle_geometry(widget: QWidget, *, passes: int = 3) -> GeometrySnapshot:
    """Flush queued layout work without sleeping or changing widget bounds.

    ``passes`` is intentionally bounded.  Calling ``adjustSize`` here would
    hide the very under-sized page being audited, so this helper only activates
    existing layouts and processes posted Qt events.
    """

    if not isinstance(widget, QWidget):
        raise GeometryAuditError("settle_geometry requires a QWidget root")
    if passes < 1:
        raise GeometryAuditError("settle_geometry passes must be positive")

    app = ensure_qapplication()
    widget.ensurePolished()
    for _ in range(passes):
        layout = widget.layout()
        if layout is not None:
            layout.activate()
            layout.invalidate()
            layout.activate()
        widget.updateGeometry()
        app.sendPostedEvents()
        app.processEvents(QEventLoop.ProcessEventsFlag.AllEvents)
    return GeometrySnapshot(widget.geometry(), widget.sizeHint(), widget.minimumSizeHint())


def font_line_height(widget: QWidget, *, font: QFont | None = None) -> int:
    """Return the active font's line spacing, independent of desktop DPI."""

    if not isinstance(widget, QWidget):
        raise GeometryAuditError("font_line_height requires a QWidget")
    metrics = QFontMetrics(font if font is not None else widget.font())
    return max(1, metrics.lineSpacing())


def font_derived_minimum_height(
    widget: QWidget,
    *,
    lines: int = 1,
    vertical_padding: int = 0,
    font: QFont | None = None,
    include_minimum_size_hint: bool = True,
) -> int:
    """Compute a vertical minimum from font, margins, and platform style hints."""

    if lines < 1:
        raise GeometryAuditError("font-derived minima require at least one line")
    margins = widget.contentsMargins()
    metric_floor = (
        font_line_height(widget, font=font) * lines
        + margins.top()
        + margins.bottom()
        + vertical_padding
    )
    hint_floors = [metric_floor]
    hint_names = ["sizeHint"]
    if include_minimum_size_hint:
        hint_names.insert(0, "minimumSizeHint")
    for hint_name in hint_names:
        try:
            hint = getattr(widget, hint_name)()
            if hint.isValid():
                hint_floors.append(int(hint.height()))
        except Exception:
            continue
    return max(hint_floors)


def assert_font_derived_vertical_minimum(
    widget: QWidget,
    *,
    lines: int = 1,
    vertical_padding: int = 0,
    font: QFont | None = None,
    measured_height: int | None = None,
) -> int:
    """Assert that a settled widget has room for its font-derived text.

    A caller may pass a measured child height when auditing a row or custom
    delegate.  Otherwise the widget's current geometry is used; unlike a
    screenshot this remains meaningful across fonts, themes, and DPI scales.
    """

    required = font_derived_minimum_height(
        widget,
        lines=lines,
        vertical_padding=vertical_padding,
        font=font,
    )
    actual = widget.height() if measured_height is None else int(measured_height)
    if actual < required:
        raise GeometryAuditError(
            f"{widget.objectName() or type(widget).__name__} height {actual} "
            f"is below font-derived minimum {required}"
        )
    return required


def _item_view_rows(view: QAbstractItemView) -> Iterable[tuple[object, int]]:
    model = view.model()
    if model is None:
        return

    def visit(parent: object) -> Iterable[tuple[object, int]]:
        row_count = model.rowCount(parent)
        column_count = model.columnCount(parent)
        for row in range(row_count):
            index = model.index(row, 0, parent)
            if index.isValid():
                yield index, row
            if column_count:
                yield from visit(index)

    yield from visit(QModelIndex())


def _is_row_hidden(view: QAbstractItemView, row: int, parent: QModelIndex) -> bool:
    try:
        return bool(view.isRowHidden(row, parent))
    except TypeError:
        return bool(view.isRowHidden(row))


def _view_row_height(view: QAbstractItemView, index: object, row: int) -> int:
    if hasattr(view, "rowHeight"):
        try:
            value = view.rowHeight(index)  # QTreeView signature
            if isinstance(value, int):
                return value
        except (TypeError, RuntimeError):
            pass
        try:
            value = view.rowHeight(row)  # QTableView signature
            if isinstance(value, int):
                return value
        except (TypeError, RuntimeError):
            pass
    rect = view.visualRect(index)
    return rect.height()


def assert_table_tree_headers_and_rows(
    view: QAbstractItemView,
    *,
    minimum_row_height: int | None = None,
    minimum_header_height: int | None = None,
    require_header: bool = True,
) -> None:
    """Check table/tree headers and every visible model row for font room."""

    if not isinstance(view, QAbstractItemView):
        raise GeometryAuditError("table/tree assertion requires QAbstractItemView")
    settle_geometry(view)
    model = view.model()
    if model is None:
        raise GeometryAuditError("table/tree view has no model")
    if hasattr(view, "horizontalHeader"):
        header = view.horizontalHeader()
    elif hasattr(view, "header"):
        header = view.header()
    else:
        header = None
    if require_header and header is None:
        raise GeometryAuditError("table/tree view has no horizontal header")
    if header is not None:
        # QHeaderView.minimumSizeHint() describes its generic scroll-area
        # viewport (often ~88 px), not the painted header thickness.
        expected_header = minimum_header_height or font_derived_minimum_height(
            header,
            include_minimum_size_hint=False,
        )
        actual_header = int(header.height())
        if actual_header < expected_header:
            raise GeometryAuditError(
                f"{type(view).__name__} header is shorter than its font/style floor "
                f"({actual_header} < {expected_header})"
            )

    expected_row = minimum_row_height or font_line_height(view)
    for index, row in _item_view_rows(view):
        if _is_row_hidden(view, row, index.parent()):
            continue
        height = _view_row_height(view, index, row)
        if height < expected_row:
            raise GeometryAuditError(
                f"{type(view).__name__} row {row} is shorter than font line spacing "
                f"({height} < {expected_row})"
            )


def assert_tab_bar_font_room(
    tabs: QTabWidget | QTabBar,
    *,
    minimum_tab_height: int | None = None,
    expected_labels: Sequence[str] | None = None,
) -> None:
    """Assert every tab rectangle and label meet the active font's height."""

    bar = tabs.tabBar() if isinstance(tabs, QTabWidget) else tabs
    if not isinstance(bar, QTabBar):
        raise GeometryAuditError("tab assertion requires QTabWidget or QTabBar")
    settle_geometry(bar)
    expected = minimum_tab_height or font_derived_minimum_height(bar)
    labels = [bar.tabText(index).strip() for index in range(bar.count())]
    if any(not label for label in labels):
        raise GeometryAuditError("tab bar contains an empty accessible label")
    if expected_labels is not None and labels != list(expected_labels):
        raise GeometryAuditError(f"tab labels {labels!r} do not match expected {list(expected_labels)!r}")
    for index in range(bar.count()):
        height = bar.tabRect(index).height()
        if height < expected:
            raise GeometryAuditError(
                f"tab {index} is shorter than its font/style floor ({height} < {expected})"
            )


def assert_geometry_settles(widget: QWidget, *, passes: int = 6, stable_passes: int = 2) -> GeometrySnapshot:
    """Require bounded event processing to converge on one geometry snapshot."""
    if passes < stable_passes or stable_passes < 2:
        raise GeometryAuditError("geometry settling needs at least two bounded stable passes")
    snapshots: list[GeometrySnapshot] = []
    for _ in range(passes):
        snapshots.append(settle_geometry(widget, passes=1))
    tail = snapshots[-stable_passes:]
    signatures = {
        (
            item.geometry.x(), item.geometry.y(), item.geometry.width(), item.geometry.height(),
            item.size_hint.width(), item.size_hint.height(),
            item.minimum_size_hint.width(), item.minimum_size_hint.height(),
        )
        for item in tail
    }
    if len(signatures) != 1:
        raise GeometryAuditError("widget geometry did not settle within the bounded event passes")
    return tail[-1]


def assert_page_horizontal_scroll_policy(
    page: QWidget | QAbstractScrollArea,
    *,
    allow_horizontal: bool = False,
) -> None:
    """Check explicitly marked page scroll owners, allowing local data overflow.

    A page scroll owner is a ``QScrollArea``/``QAbstractScrollArea`` carrying
    ``fio_scroll_owner == 'page'``.  Local tables/previews may instead carry
    ``fio_scroll_owner == 'local'`` and are intentionally not rejected.
    """

    roots = [page] if isinstance(page, QAbstractScrollArea) else page.findChildren(QAbstractScrollArea)
    owners = [
        scroll
        for scroll in roots
        if scroll.property("fio_scroll_owner") == "page"
        or scroll.property("fio_page_scroll_owner") is True
    ]
    if not owners:
        raise GeometryAuditError("page has no explicitly marked fio page scroll owner")
    for scroll in owners:
        policy = scroll.horizontalScrollBarPolicy()
        if allow_horizontal:
            if policy == Qt.ScrollBarAlwaysOff:
                raise GeometryAuditError("horizontal overflow was requested but page scrolling is disabled")
        elif policy != Qt.ScrollBarAlwaysOff:
            raise GeometryAuditError("normal workspace page scrolling must disable horizontal overflow")


def assert_lazy_page_theme_lifecycle(
    host: QWidget,
    loader: Callable[[], QWidget],
    apply_theme: Callable[[QWidget, str], None],
    *,
    themes: Sequence[str] = ("light", "dark"),
    point_increments: Sequence[int] = (0, 1, 3),
) -> QWidget:
    """Load one deferred page and verify each theme pass preserves geometry.

    The loader is deliberately injected, keeping this check independent from
    application settings, databases, and endpoint/runtime configuration.
    """

    if not isinstance(host, QWidget):
        raise GeometryAuditError("lazy-page lifecycle requires a QWidget host")
    if not themes:
        raise GeometryAuditError("lazy-page lifecycle requires at least one theme")
    page = loader()
    if not isinstance(page, QWidget):
        raise GeometryAuditError("lazy page loader did not return a QWidget")
    if page is host or not (page.parent() is host or host.isAncestorOf(page)):
        raise GeometryAuditError("loaded lazy page is not attached to its host")

    page.setProperty("fio_lazy_page_loaded", True)
    settle_geometry(page)
    base_font = QFont(page.font())
    for theme in themes:
        for increment in point_increments:
            scaled_font = QFont(base_font)
            if scaled_font.pointSize() > 0:
                scaled_font.setPointSize(max(1, scaled_font.pointSize() + int(increment)))
                page.setFont(scaled_font)
            apply_theme(page, str(theme))
            snapshot = assert_geometry_settles(page)
            if snapshot.geometry.width() <= 0 or snapshot.geometry.height() <= 0:
                raise GeometryAuditError(
                    f"lazy page collapsed for theme {theme!r}, font increment {increment}"
                )
    return page


# Stable audit manifest.  It intentionally mirrors source registration rather
# than importing MainWindow, whose constructor requires runtime services.
SCREEN_KEY_MANIFEST = (
    "ControlFreq",
    "Station Overview",
    "Managed BBS",
    "FIO Spotter",
    "Resources",
    "Shortwave",
    "FreqPlanner",
    "SOP",
    "Messages",
    "NCS-FLDigi/SSB",
    "NCS-JS8",
    "NCS-Local",
    "HF Operators",
    "Local Operators",
    "Local Reports",
    "Map",
    "HF Schedule",
    "Net Schedule",
    "Local Nets",
    "Peer Schedules",
    "Station Health",
    "Settings",
    "Help",
)
