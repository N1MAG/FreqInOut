from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.core.message_intel_filters import IntelFilterChip
from freqinout.gui.message_viewer_tab import MessageViewerTab


def _settings() -> SimpleNamespace:
    return SimpleNamespace(get=lambda _key, default=None: default)


def test_intel_topic_chips_render_neutral_even_with_red_rollup() -> None:
    chip = IntelFilterChip(
        kind="topic",
        value="Comms",
        label="Comms",
        count=31,
        status_bucket="red",
    )

    assert MessageViewerTab._intel_filter_button_role(chip) == "muted"


def test_intel_status_chips_use_subdued_semantic_roles() -> None:
    red = IntelFilterChip(kind="status", value="red", label="Red", count=13, status_bucket="red")
    yellow = IntelFilterChip(kind="status", value="yellow", label="Yellow", count=214, status_bucket="yellow")
    green = IntelFilterChip(kind="status", value="green", label="Green", count=2, status_bucket="green")

    assert MessageViewerTab._intel_filter_button_role(red) == "eligible_danger"
    assert MessageViewerTab._intel_filter_button_role(yellow) == "eligible_warning"
    assert MessageViewerTab._intel_filter_button_role(green) == "eligible_success"


def test_normal_js8_refresh_defers_source_ingest_when_background_unavailable() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = _settings()
    tab._is_shutting_down = False
    tab._last_js8_ingest_ts = 0.0
    tab._js8_ingest_interval_sec = 0.0
    tab._js8_display_snapshot_fp = None
    tab._message_rows = []

    called = {"direct": 0}
    tab._load_structured_message_projections = lambda force=False, rebuild=False: None
    tab._js8_display_fingerprint = lambda: (("stable", 1, 1),)
    tab._request_background_ingest = lambda *args, **kwargs: False
    tab._ingest_js8_runtime_messages = lambda: called.__setitem__("direct", called["direct"] + 1)
    tab._populate_messages_table = lambda force=False: None

    tab._refresh_js8_messages(force=False, rebuild=False)

    assert called["direct"] == 0


def test_normal_varac_refresh_defers_source_ingest_when_background_unavailable() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = _settings()
    tab._is_shutting_down = False
    tab._last_varac_ingest_ts = 0.0
    tab._varac_ingest_interval_sec = 0.0
    tab._message_rows = []

    called = {"direct": 0}
    tab._message_sources_fingerprint = lambda: (("stable", 1),)
    tab._request_background_ingest = lambda *args, **kwargs: False
    tab._load_varac_from_local = lambda force=False, rebuild=False: None
    tab._populate_messages_table = lambda force=False: None

    import freqinout.gui.message_viewer_tab as module

    original = module.ingest_varac_for_runtime_sources
    try:
        module.ingest_varac_for_runtime_sources = lambda _settings: called.__setitem__("direct", called["direct"] + 1)
        tab._refresh_varac_messages(force=False, rebuild=False)
    finally:
        module.ingest_varac_for_runtime_sources = original

    assert called["direct"] == 0
