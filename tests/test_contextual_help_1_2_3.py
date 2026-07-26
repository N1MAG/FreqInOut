from __future__ import annotations

from pathlib import Path

from freqinout.gui.help_registry import HELP_CONTEXTS, get_help_context, resolve_help_host


CORE_HELP_KEYS = [
    "help.glossary",
    "help.plan-context",
    "tab.controlfreq",
    "tab.messages",
    "messages.compose",
    "messages.bbs",
    "messages.compose-setup",
    "tab.map",
    "map.paths",
    "tab.hf-daily",
    "tab.hf-nets",
    "tab.sop-builder",
    "tab.settings",
    "settings.operator",
    "settings.freqinout",
    "settings.js8call",
    "settings.fast-light",
    "settings.hf-groups",
    "settings.local-comms",
    "settings.varac",
    "settings.message-auth",
    "settings.launch-control",
    "settings.logging",
]


def test_core_help_contexts_exist() -> None:
    for key in CORE_HELP_KEYS:
        context = get_help_context(key)
        assert context.key == key
        assert context.anchor
        assert context.title


def test_registered_help_anchors_exist_in_guide() -> None:
    guide_path = Path(__file__).resolve().parents[1] / "docs" / "guide.html"
    html = guide_path.read_text(encoding="utf-8", errors="ignore")
    missing = [ctx.anchor for ctx in HELP_CONTEXTS.values() if f'id="{ctx.anchor}"' not in html]
    assert not missing, f"Missing help anchors in guide.html: {missing}"


def test_sop_builder_context_help_is_registered_and_wired() -> None:
    context = get_help_context("tab.sop-builder")
    source = Path("freqinout/gui/sop_tab.py").read_text(encoding="utf-8")

    assert context.anchor == "sop-builder"
    assert "plan context cue" in context.summary
    assert "from freqinout.gui.help_registry import resolve_help_host" in source
    assert 'self.help_btn = QPushButton("Help")' in source
    assert 'self.help_btn.setToolTip("Open SOP Builder help.")' in source
    assert 'self.help_btn.clicked.connect(lambda: self._open_context_help("tab.sop-builder"))' in source
    assert "def _open_context_help(self, context_key: str) -> None:" in source


class _DummyNode:
    def __init__(self, parent=None, parent_widget=None, *, has_help=False):
        self._parent = parent
        self._parent_widget = parent_widget
        if has_help:
            self.open_context_help = lambda _key=None: None

    def parent(self):
        return self._parent

    def parentWidget(self):
        return self._parent_widget

    def window(self):
        return self._parent_widget or self._parent


def test_resolve_help_host_walks_parent_chain() -> None:
    host = _DummyNode(has_help=True)
    middle = _DummyNode(parent=host)
    leaf = _DummyNode(parent=middle)
    assert resolve_help_host(leaf) is host
