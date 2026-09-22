from __future__ import annotations

import re
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
    "tab.bbs",
    "tab.map",
    "tab.station-health",
    "tab.ncs-fldigi",
    "map.paths",
    "tab.hf-daily",
    "tab.hf-nets",
    "tab.local-nets",
    "tab.tools-resources",
    "tab.sop-builder",
    "tab.settings",
    "settings.operator",
    "settings.freqinout",
    "settings.js8call",
    "settings.fast-light",
    "settings.hf-groups",
    "settings.local-comms",
    "settings.local-mesh",
    "settings.varac",
    "settings.message-auth",
    "settings.condition-alerts",
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
    missing = [ctx.anchor for ctx in HELP_CONTEXTS.values() if html.count(f'id="{ctx.anchor}"') != 1]
    assert not missing, f"Missing or duplicate help anchors in guide.html: {missing}"


def test_every_static_context_help_key_is_registered() -> None:
    project_root = Path(__file__).resolve().parents[1]
    literal_pattern = re.compile(
        r'(?:help_context_key\s*=\s*|(?:open_context_help|_open_context_help)\(\s*)["\']([^"\']+)["\']'
    )
    used_keys: set[str] = set()
    for source_path in (project_root / "freqinout").rglob("*.py"):
        used_keys.update(literal_pattern.findall(source_path.read_text(encoding="utf-8", errors="ignore")))

    missing = sorted(key for key in used_keys if key not in HELP_CONTEXTS)
    assert not missing, f"Static context-help keys silently falling back to overview: {missing}"


def test_guide_fragment_links_resolve_to_unique_anchors() -> None:
    guide_path = Path(__file__).resolve().parents[1] / "docs" / "guide.html"
    html = guide_path.read_text(encoding="utf-8", errors="ignore")
    anchors = re.findall(r'\bid="([^"]+)"', html)
    duplicate_anchors = sorted(anchor for anchor in set(anchors) if anchors.count(anchor) != 1)
    broken_links = sorted(
        fragment
        for fragment in set(re.findall(r'href="#([^"]+)"', html))
        if anchors.count(fragment) != 1
    )

    assert not duplicate_anchors, f"Duplicate guide anchors: {duplicate_anchors}"
    assert not broken_links, f"Broken guide fragment links: {broken_links}"


def test_main_menu_guide_covers_current_primary_navigation_labels() -> None:
    project_root = Path(__file__).resolve().parents[1]
    guide_path = project_root / "docs" / "guide.html"
    html = guide_path.read_text(encoding="utf-8", errors="ignore")
    menu_section = html.split('<h3 id="main-menu-order">', 1)[1].split("<h3", 1)[0]
    main_window_source = (project_root / "freqinout" / "gui" / "main_window.py").read_text(
        encoding="utf-8", errors="ignore"
    )
    nav_block = main_window_source.split("_nav_specs = [", 1)[1].split("\n        ]", 1)[0]
    expected_labels = re.findall(r'^\s*\("([^"]+)",\s*"[^"]+"\),', nav_block, re.MULTILINE)
    expected_labels.append("Shortwave")
    missing = [label for label in expected_labels if menu_section.count(f"<code>{label}</code>") != 1]
    assert not missing, f"Current primary navigation labels missing from guide: {missing}"
    for stale_label in ("ControlFreq", "FreqPlanner", "Station Health", "HF Peer Schedules"):
        assert f"<code>{stale_label}</code>" not in menu_section
    assert menu_section.count("<td>Configuration</td>") == 1
    assert "<td>Settings</td>" not in menu_section


def test_configuration_is_the_user_facing_main_navigation_label() -> None:
    project_root = Path(__file__).resolve().parents[1]
    source = (project_root / "freqinout" / "gui" / "main_window.py").read_text(encoding="utf-8")
    context = get_help_context("tab.settings")

    assert '("Main", "Settings")' in source
    assert 'if screen == "Settings":\n            return "Configuration"' in source
    assert '("Config", "Configuration", "Configuration", "settings.svg")' in source
    assert 'raw["Configuration"] = raw.get("Settings")' in source
    assert context.title == "Configuration Help"


def test_guide_uses_reference_oriented_section_language() -> None:
    guide_path = Path(__file__).resolve().parents[1] / "docs" / "guide.html"
    html = guide_path.read_text(encoding="utf-8", errors="ignore")

    assert '<h3 id="main-menu-learning-map">Tabs Explained</h3>' in html
    assert "<th>Tab</th><th>Purpose</th><th>What's There</th><th>Why It Matters</th>" in html
    assert '<h3 id="main-menu-section-explainers">Sections and Sub-Tabs</h3>' in html
    assert "<th>Section or Sub-Tab</th><th>Description</th>" in html
    assert "<strong>Operational use</strong>" in html
    assert "<strong>Overview</strong>" in html
    assert "<strong>Related diagnostics</strong>" in html
    assert "HF SOP Review Workflow" in html
    assert "Net/SOP Conflict Review Workflow" in html
    assert "Post-Save Behavior for an Active HF SOP" in html

    conversational_labels = (
        "Explain Each Tab",
        "Explain Each Section and Sub-Tab",
        "Explain This",
        "Explain this to me",
        "Plain Explanation",
        "Why you use it",
        "Plain-language workflow",
        "Step-by-Step:",
        "What does Resume Schedule do?",
    )
    assert not [label for label in conversational_labels if label in html]


def test_quick_start_operating_groups_link_targets_the_registered_help_section() -> None:
    guide_path = Path(__file__).resolve().parents[1] / "docs" / "guide.html"
    html = guide_path.read_text(encoding="utf-8", errors="ignore")
    quick_start = html.split('<h2 id="quick-start">', 1)[1].split("<h2", 1)[0]
    operating_groups = html.split('<h3 id="settings-hf-groups-details">', 1)[1].split("<h3", 1)[0]
    context = get_help_context("settings.hf-groups")

    assert context.anchor == "settings-hf-groups-details"
    assert quick_start.count('href="#settings-hf-groups-details"') == 1
    assert 'href="#settings-hf-groups"' not in quick_start
    assert quick_start.index("Configure <a") < quick_start.index("Build your operating plan")
    assert "before building schedules" in quick_start
    assert "Without them" in quick_start
    for label in (
        "Scheduling requirement",
        "Minimum useful configuration",
        "Groups and configurations",
        "Known groups",
        "Auto-Tune on QSY",
        "Change impact",
    ):
        assert f"<strong>{label}</strong>" in operating_groups
    for control in (
        "Add Group",
        "Add Configuration",
        "Save Changes",
        "Delete Configuration",
        "View Frequencies",
        "Enable Group",
    ):
        assert f"<code>{control}</code>" in html


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


def test_local_nets_and_resources_context_help_are_registered_and_wired() -> None:
    local_context = get_help_context("tab.local-nets")
    resources_context = get_help_context("tab.tools-resources")
    local_source = Path("freqinout/gui/local_nets_tab.py").read_text(encoding="utf-8")
    resources_source = Path("freqinout/gui/resources_tab.py").read_text(encoding="utf-8")

    assert local_context.anchor == "local-nets"
    assert resources_context.anchor == "tools-resources"
    assert 'host.open_context_help("tab.local-nets")' in local_source
    assert 'host.open_context_help("tab.tools-resources")' in resources_source


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
