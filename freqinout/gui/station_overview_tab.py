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
        self._tab_active = False
        self._refresh_dirty = False
        self._last_render_signature: tuple[object, ...] = tuple()
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        self.summary_label = QLabel("No active station runtimes.")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        self.alerts_label = QLabel("")
        self.alerts_label.setWordWrap(True)
        self.alerts_label.setVisible(False)
        layout.addWidget(self.alerts_label)

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
        self._refresh_dirty = True
        self.refresh_from_manager(force=True)

    def set_tab_active(self, active: bool) -> None:
        self._tab_active = bool(active)
        if active:
            self.refresh_from_manager(force=self._refresh_dirty)

    def apply_theme(self) -> None:
        self._refresh_dirty = True
        self.refresh_from_manager(force=True)

    def refresh_from_manager(self, *, force: bool = False) -> None:
        if not force and not self._tab_active:
            return
        manager = self._runtime_manager
        if manager is None:
            self._rebuild_cards([])
            return
        snapshots = manager.get_runtime_snapshots(force=force)
        signature = self._overview_signature(snapshots)
        if not force and signature == self._last_render_signature:
            return
        self._refresh_dirty = False
        self._last_render_signature = signature
        self._rebuild_cards(snapshots)

    def _clear_cards(self) -> None:
        while self.cards_layout.count() > 1:
            item = self.cards_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

    @staticmethod
    def _service_signature(service_states: dict[str, dict[str, object]]) -> tuple[object, ...]:
        return tuple(
            (
                name,
                str(info.get("state", "idle") or "idle"),
                str(info.get("tooltip", "") or ""),
            )
            for name, info in sorted(service_states.items())
        )

    def _overview_signature(self, snapshots: Iterable[DeviceRuntimeSnapshot]) -> tuple[object, ...]:
        rendered: list[tuple[object, ...]] = []
        for snapshot in snapshots:
            rendered.append(
                (
                    int(snapshot.device_profile_id or 0),
                    str(snapshot.name or ""),
                    str(snapshot.control_backend or ""),
                    str(snapshot.deployment_mode or ""),
                    bool(snapshot.runtime_primary),
                    str(snapshot.endpoint_summary or ""),
                    str(snapshot.assigned_operating_profile_name or ""),
                    str(snapshot.assignment_state or ""),
                    bool(snapshot.scheduler_enabled),
                    bool(snapshot.use_messages),
                    bool(snapshot.use_map),
                    bool(snapshot.use_background_ingest),
                    bool(snapshot.use_launch_control),
                    bool(snapshot.use_net_control_tabs),
                    bool(snapshot.control_ready),
                    str(snapshot.overall_state or ""),
                    str(snapshot.status_summary or ""),
                    str(snapshot.warning_text or ""),
                    str(snapshot.ptt_group or ""),
                    bool(snapshot.ptt_active),
                    bool(snapshot.shared_ptt_blocked),
                    str(snapshot.shared_ptt_status_text or ""),
                    str(snapshot.observer_follow_source_name or ""),
                    str(snapshot.observer_follow_summary or ""),
                    str(snapshot.varac_cluster_name or ""),
                    str(snapshot.varac_cluster_summary or ""),
                    str(snapshot.current_frequency_label or ""),
                    str(snapshot.current_band or ""),
                    str(snapshot.antenna_group or ""),
                    str(snapshot.frontend_group or ""),
                    str(snapshot.amplifier_group or ""),
                    str(snapshot.swap_role or ""),
                    str(snapshot.swap_summary or ""),
                    self._service_signature(snapshot.service_states),
                )
            )
        return tuple(rendered)

    def _rebuild_cards(self, snapshots: Iterable[DeviceRuntimeSnapshot]) -> None:
        snaps = list(snapshots)
        self._clear_cards()
        self.alerts_label.setVisible(False)
        self.alerts_label.clear()
        if not snaps:
            self.summary_label.setText("No active station runtimes. Activate one or more device profiles in Settings.")
            self._last_render_signature = tuple()
            return

        theme = resolve_theme(self.settings)
        primary = next((snap for snap in snaps if snap.runtime_primary), None)
        primary_name = primary.name if primary is not None else "none"
        observer_count = len([snap for snap in snaps if snap.device_class == "observer"])
        varac_cluster_members = len([snap for snap in snaps if snap.varac_cluster_name])
        warning_count = len(
            [
                snap
                for snap in snaps
                if (
                    str(snap.overall_state or "").strip().lower() in {"warn", "error"}
                    or snap.warning_text
                    or snap.shared_ptt_blocked
                )
            ]
        )
        swap_active = any(str(snap.swap_role or "").strip() for snap in snaps)
        self.summary_label.setText(
            f"{len(snaps)} active device profile{'s' if len(snaps) != 1 else ''}. "
            f"Station default: {primary_name}. "
            f"Warnings: {warning_count}. "
            f"Observer profiles: {observer_count}. "
            f"VarAC cluster members: {varac_cluster_members}."
            + (" Temporary swap active." if swap_active else "")
        )
        warning_lines = [snap.warning_text for snap in snaps if snap.warning_text]
        blocked_names = [snap.name or f"Device {snap.device_profile_id}" for snap in snaps if snap.shared_ptt_blocked]
        if blocked_names:
            preview = ", ".join(blocked_names[:3])
            extra = "" if len(blocked_names) <= 3 else f", +{len(blocked_names) - 3} more"
            warning_lines.append(
                f"Shared PTT is blocking {len(blocked_names)} device profile{'s' if len(blocked_names) != 1 else ''}: {preview}{extra}."
            )
        if warning_lines:
            self.alerts_label.setText(" ".join(dict.fromkeys([line for line in warning_lines if line])))
            self.alerts_label.setStyleSheet(f"color: {theme['warning']}; font-weight: 600;")
            self.alerts_label.setVisible(True)
        for snap in snaps:
            self.cards_layout.insertWidget(self.cards_layout.count() - 1, self._build_runtime_card(snap, theme))

    @staticmethod
    def _merged_service_states(service_states: dict[str, dict[str, object]]) -> list[tuple[str, str, str]]:
        return [
            (
                str(name or "").replace("_API", "") or "Service",
                str(info.get("state", "idle") or "idle"),
                str(info.get("tooltip", "") or "").strip(),
            )
            for name, info in sorted((service_states or {}).items())
        ]

    def _build_runtime_card(self, snapshot: DeviceRuntimeSnapshot, theme: dict[str, str]) -> QWidget:
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
        if snapshot.device_class == "observer":
            role_badge = QLabel("Observer / SDR")
            role_badge.setStyleSheet(_state_badge_style("idle", theme))
            header.addWidget(role_badge, 0, Qt.AlignRight)
        if snapshot.runtime_primary:
            badge = QLabel("Station Default")
            badge.setStyleSheet(_state_badge_style("ok", theme))
            header.addWidget(badge, 0, Qt.AlignRight)
        header_badge = QLabel("Active")
        header_badge.setStyleSheet(_state_badge_style(snapshot.overall_state, theme))
        header.addWidget(header_badge, 0, Qt.AlignRight)
        layout.addLayout(header)

        meta = QLabel(
            f"Control: {snapshot.endpoint_summary or snapshot.control_backend.upper()} | "
            f"Deployment: {snapshot.deployment_mode.upper()} | "
            f"PTT Group: {snapshot.ptt_group or 'None'} | "
            f"Assigned Plan: {snapshot.assigned_operating_profile_name or 'Unassigned'} | "
            f"Assignment: {snapshot.assignment_state.replace('_', ' ').title() or 'Unassigned'}"
        )
        meta.setWordWrap(True)
        layout.addWidget(meta)

        frequency_bits = " ".join(part for part in (snapshot.current_frequency_label, snapshot.current_band) if part).strip()
        resource_bits = []
        if snapshot.antenna_group:
            resource_bits.append(f"Antenna: {snapshot.antenna_group}")
        if snapshot.frontend_group:
            resource_bits.append(f"Front-End: {snapshot.frontend_group}")
        if snapshot.amplifier_group:
            resource_bits.append(f"Amplifier: {snapshot.amplifier_group}")
        tuning_text = f"Tuning: {frequency_bits or 'Unavailable'}"
        if resource_bits:
            tuning_text += " | " + " | ".join(resource_bits)
        tuning_label = QLabel(tuning_text)
        tuning_label.setWordWrap(True)
        layout.addWidget(tuning_label)

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
        status_row.addWidget(led, 0)
        status_text = QLabel(snapshot.status_summary or "No status summary available.")
        status_text.setWordWrap(True)
        status_row.addWidget(status_text, 1)
        layout.addLayout(status_row)

        if snapshot.shared_ptt_status_text:
            ptt_label = QLabel(snapshot.shared_ptt_status_text)
            ptt_label.setWordWrap(True)
            if snapshot.shared_ptt_blocked:
                ptt_label.setStyleSheet(f"color: {theme['warning']}; font-weight: 600;")
            elif snapshot.ptt_active:
                ptt_label.setStyleSheet(f"color: {theme.get('info', theme['text'])}; font-weight: 600;")
            layout.addWidget(ptt_label)

        if snapshot.observer_follow_summary:
            observer_label = QLabel(snapshot.observer_follow_summary)
            observer_label.setWordWrap(True)
            observer_label.setStyleSheet(f"color: {theme.get('info', theme['text'])}; font-weight: 600;")
            layout.addWidget(observer_label)

        if snapshot.varac_cluster_summary:
            varac_label = QLabel(snapshot.varac_cluster_summary)
            varac_label.setWordWrap(True)
            varac_label.setStyleSheet(
                f"color: {theme.get('accent', theme.get('info', theme['text'])) if snapshot.varac_gateway_handler else theme.get('info', theme['text'])}; font-weight: 600;"
            )
            layout.addWidget(varac_label)

        if snapshot.swap_summary:
            swap_label = QLabel(snapshot.swap_summary)
            swap_label.setWordWrap(True)
            swap_label.setStyleSheet(f"color: {theme.get('info', theme['text'])}; font-weight: 600;")
            layout.addWidget(swap_label)

        if snapshot.warning_text:
            warning_label = QLabel(snapshot.warning_text)
            warning_label.setWordWrap(True)
            warning_label.setStyleSheet(f"color: {theme['warning']};")
            layout.addWidget(warning_label)

        services = self._merged_service_states(snapshot.service_states)
        if services:
            service_layout = QVBoxLayout()
            service_layout.setContentsMargins(0, 0, 0, 0)
            service_layout.setSpacing(4)
            for label, state, tooltip in services:
                row = QHBoxLayout()
                row.setContentsMargins(0, 0, 0, 0)
                row.setSpacing(6)
                row_led = QLabel()
                row_led.setFixedSize(12, 12)
                row_led.setStyleSheet(led_style(state, theme))
                row.addWidget(row_led, 0)
                row_label = QLabel(label)
                row.addWidget(row_label, 0)
                row_summary = QLabel(tooltip or state.title())
                row_summary.setWordWrap(True)
                row.addWidget(row_summary, 1)
                service_layout.addLayout(row)
            layout.addLayout(service_layout)
        return card
