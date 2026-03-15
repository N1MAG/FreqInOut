from __future__ import annotations

from typing import Iterable, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QScrollArea,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.station_runtime_manager import DeviceRuntimeSnapshot, StationRuntimeManager
from freqinout.gui.theme import led_style, resolve_theme


def _state_badge_style(state: str, theme: dict[str, str]) -> str:
    normalized = str(state or "idle").strip().lower()
    if normalized == "ok":
        bg = theme["success"]
    elif normalized == "warn":
        bg = theme["warning"]
    elif normalized == "error":
        bg = theme["danger"]
    else:
        bg = theme["surface_alt"]
    fg = theme.get("text_on_accent", "#ffffff") if normalized in {"ok", "warn", "error"} else theme["text"]
    return (
        f"background: {bg}; color: {fg}; border-radius: 10px; padding: 3px 8px; "
        f"border: 1px solid {theme['border']}; font-weight: 600;"
    )


class StationOverviewTab(QWidget):
    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.settings = SettingsManager()
        self._runtime_manager: Optional[StationRuntimeManager] = None
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        self.summary_label = QLabel("No active station runtimes.")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        self.scroll = QScrollArea(self)
        self.scroll.setWidgetResizable(True)
        self.scroll.setFrameShape(QFrame.NoFrame)
        self.scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        layout.addWidget(self.scroll, 1)

        self.cards_container = QWidget(self.scroll)
        self.cards_layout = QVBoxLayout(self.cards_container)
        self.cards_layout.setContentsMargins(0, 0, 0, 0)
        self.cards_layout.setSpacing(10)
        self.cards_layout.addStretch(1)
        self.scroll.setWidget(self.cards_container)

    def set_runtime_manager(self, manager: Optional[StationRuntimeManager]) -> None:
        self._runtime_manager = manager
        self.refresh_from_manager(force=True)

    def set_tab_active(self, active: bool) -> None:
        if active:
            self.refresh_from_manager()

    def apply_theme(self) -> None:
        self.refresh_from_manager(force=True)

    def refresh_from_manager(self, *, force: bool = False) -> None:
        manager = self._runtime_manager
        if manager is None:
            self._rebuild_cards([])
            return
        try:
            self.settings.reload()
        except Exception:
            pass
        snapshots = manager.get_runtime_snapshots(force=force)
        self._rebuild_cards(snapshots)

    def _clear_cards(self) -> None:
        while self.cards_layout.count() > 1:
            item = self.cards_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

    def _rebuild_cards(self, snapshots: Iterable[DeviceRuntimeSnapshot]) -> None:
        snaps = list(snapshots)
        self._clear_cards()
        if not snaps:
            self.summary_label.setText("No active station runtimes. Activate one or more device profiles in Settings.")
            return

        primary = next((snap for snap in snaps if snap.runtime_primary), None)
        primary_name = primary.name if primary is not None else "none"
        self.summary_label.setText(
            f"{len(snaps)} active device profile{'s' if len(snaps) != 1 else ''}. "
            f"Primary compatibility device: {primary_name}."
        )
        for snap in snaps:
            self.cards_layout.insertWidget(self.cards_layout.count() - 1, self._build_runtime_card(snap))

    def _build_runtime_card(self, snapshot: DeviceRuntimeSnapshot) -> QWidget:
        theme = resolve_theme(self.settings)
        card = QFrame(self.cards_container)
        card.setObjectName("stationRuntimeCard")
        card.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        card.setStyleSheet(
            f"QFrame#stationRuntimeCard {{ border: 1px solid {theme['border']}; border-radius: 10px; "
            f"background: {theme['surface']}; }}"
        )

        layout = QVBoxLayout(card)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(8)

        header = QHBoxLayout()
        title = QLabel(snapshot.name or f"Device {snapshot.device_profile_id}")
        title.setStyleSheet("font-size: 15px; font-weight: 700;")
        header.addWidget(title, 1)
        if snapshot.runtime_primary:
            badge = QLabel("Primary")
            badge.setStyleSheet(_state_badge_style("ok", theme))
            header.addWidget(badge, 0, Qt.AlignRight)
        active_badge = QLabel("Active")
        active_badge.setStyleSheet(_state_badge_style(snapshot.overall_state, theme))
        header.addWidget(active_badge, 0, Qt.AlignRight)
        layout.addLayout(header)

        meta = QLabel(
            f"{snapshot.control_backend.upper()} | {snapshot.endpoint_summary} | "
            f"Deployment: {snapshot.deployment_mode.upper()} | "
            f"Operating Profile: {snapshot.assigned_operating_profile_name or 'Unassigned'} | "
            f"Assignment: {snapshot.assignment_state.replace('_', ' ').title() or 'Unassigned'}"
        )
        meta.setWordWrap(True)
        layout.addWidget(meta)

        policy_notes = []
        if not snapshot.scheduler_enabled:
            policy_notes.append("Scheduler Off")
        if not snapshot.use_messages:
            policy_notes.append("Messages Off")
        if not snapshot.use_map:
            policy_notes.append("Map Off")
        if not snapshot.use_net_control_tabs:
            policy_notes.append("NetCtrl Off")
        if not snapshot.use_background_ingest:
            policy_notes.append("Ingest Off")
        if not snapshot.use_launch_control:
            policy_notes.append("Launch Off")
        policy_label = QLabel("Policy: " + (", ".join(policy_notes) if policy_notes else "Full shell"))
        policy_label.setWordWrap(True)
        layout.addWidget(policy_label)

        status_row = QHBoxLayout()
        led = QLabel()
        led.setFixedSize(14, 14)
        led.setStyleSheet(led_style(snapshot.overall_state, theme))
        status_row.addWidget(led, 0, Qt.AlignTop)
        summary = QLabel(snapshot.status_summary or "Runtime status unavailable.")
        summary.setWordWrap(True)
        status_row.addWidget(summary, 1)
        layout.addLayout(status_row)

        if snapshot.warning_text:
            warning = QLabel(snapshot.warning_text)
            warning.setWordWrap(True)
            warning.setStyleSheet(f"color: {theme['warning']};")
            layout.addWidget(warning)

        chips = QHBoxLayout()
        chips.setSpacing(6)
        for service_name, info in snapshot.service_states.items():
            chip = QLabel(service_name.replace("_API", ""))
            chip.setStyleSheet(_state_badge_style(str(info.get("state", "idle") or "idle"), theme))
            tooltip = str(info.get("tooltip", "") or "").strip()
            if tooltip:
                chip.setToolTip(tooltip)
            chips.addWidget(chip)
        chips.addStretch(1)
        layout.addLayout(chips)
        return card
