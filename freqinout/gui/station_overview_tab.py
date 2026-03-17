from __future__ import annotations
from typing import Iterable, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.station_runtime_manager import DeviceRuntimeSnapshot, StationRuntimeManager
from freqinout.gui.async_status_broker import AsyncStatusBroker
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
        self._status_broker: Optional[AsyncStatusBroker] = None
        self._station_context = None
        self._station_context_syncing = False
        self._tab_active = False
        self._refresh_dirty = False
        self._last_render_signature: tuple[object, ...] = tuple()
        self._build_ui()

    def set_status_broker(self, broker: Optional[AsyncStatusBroker]) -> None:
        if self._status_broker is broker:
            return
        if self._status_broker is not None:
            try:
                self._status_broker.station_snapshots_ready.disconnect(self._on_async_station_snapshots_ready)
            except Exception:
                pass
            try:
                self._status_broker.station_snapshots_failed.disconnect(self._on_async_station_snapshots_failed)
            except Exception:
                pass
        self._status_broker = broker
        if self._status_broker is not None:
            self._status_broker.station_snapshots_ready.connect(self._on_async_station_snapshots_ready)
            self._status_broker.station_snapshots_failed.connect(self._on_async_station_snapshots_failed)

    def set_station_context(self, context: object) -> None:
        if self._station_context is context:
            return
        if self._station_context is not None:
            try:
                self._station_context.snapshots_changed.disconnect(self._on_station_context_snapshots_changed)
            except Exception:
                pass
            try:
                self._station_context.selection_changed.disconnect(self._on_station_context_selection_changed)
            except Exception:
                pass
        self._station_context = context
        if self._station_context is not None:
            try:
                self._station_context.snapshots_changed.connect(self._on_station_context_snapshots_changed)
                self._station_context.selection_changed.connect(self._on_station_context_selection_changed)
            except Exception:
                pass
        self._refresh_dirty = True
        self.refresh_from_manager(force=True)

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
        if self._status_broker is not None:
            if not self._tab_active:
                self._refresh_dirty = True
                return
            self._status_broker.request_station_snapshots(
                store=manager.store,
                settings=manager.settings,
                force=force,
            )
            return
        snapshots = manager.get_runtime_snapshots(force=force)
        signature = self._overview_signature(snapshots)
        if not force and signature == self._last_render_signature:
            return
        self._refresh_dirty = False
        self._last_render_signature = signature
        self._rebuild_cards(snapshots)

    def _on_async_station_snapshots_ready(self, snapshots: object) -> None:
        try:
            rows = list(snapshots or [])
        except Exception:
            rows = []
        signature = self._overview_signature(rows)
        if signature == self._last_render_signature:
            self._refresh_dirty = False
            return
        self._refresh_dirty = False
        self._last_render_signature = signature
        self._rebuild_cards(rows)

    def _on_async_station_snapshots_failed(self, error_text: str) -> None:
        self._refresh_dirty = True

    def _clear_cards(self) -> None:
        while self.cards_layout.count() > 1:
            item = self.cards_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

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
        attention_lines = self._station_attention_lines(snaps)
        attention_count = len(attention_lines)
        self.summary_label.setText(
            f"{len(snaps)} active device profile{'s' if len(snaps) != 1 else ''}. "
            f"Station default: {primary_name}. "
            f"Observer profiles: {observer_count}. "
            f"VarAC cluster members: {varac_cluster_members}. "
            f"Attention items: {attention_count}."
        )
        attention_text = " ".join(attention_lines)
        if attention_text:
            self.alerts_label.setText(attention_text)
            self.alerts_label.setStyleSheet(f"color: {theme['warning']}; font-weight: 600;")
            self.alerts_label.setVisible(True)
        for snap in snaps:
            self.cards_layout.insertWidget(self.cards_layout.count() - 1, self._build_runtime_card(snap, theme))

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
                    str(snapshot.device_class or ""),
                    str(snapshot.control_backend or ""),
                    str(snapshot.deployment_mode or ""),
                    bool(snapshot.runtime_primary),
                    str(snapshot.overall_state or ""),
                    str(snapshot.endpoint_summary or ""),
                    str(snapshot.ptt_group or ""),
                    bool(snapshot.ptt_active),
                    str(snapshot.current_frequency_label or ""),
                    str(snapshot.current_band or ""),
                    str(snapshot.antenna_group or ""),
                    str(snapshot.frontend_group or ""),
                    str(snapshot.amplifier_group or ""),
                    str(snapshot.assigned_operating_profile_name or ""),
                    str(snapshot.assignment_state or ""),
                    bool(snapshot.scheduler_enabled),
                    bool(snapshot.use_messages),
                    bool(snapshot.use_map),
                    bool(snapshot.use_background_ingest),
                    bool(snapshot.use_launch_control),
                    bool(snapshot.use_net_control_tabs),
                    str(snapshot.status_summary or ""),
                    str(snapshot.warning_text or ""),
                    bool(snapshot.shared_ptt_blocked),
                    str(snapshot.shared_ptt_status_text or ""),
                    str(snapshot.observer_follow_summary or ""),
                    str(snapshot.varac_cluster_summary or ""),
                    str(snapshot.swap_role or ""),
                    str(snapshot.swap_summary or ""),
                    self._service_signature(snapshot.service_states),
                )
            )
        return tuple(rendered)

    @staticmethod
    def _station_attention_lines(snaps: Iterable[DeviceRuntimeSnapshot]) -> list[str]:
        rows = list(snaps)
        if not rows:
            return []
        lines: list[str] = []
        primary = next((snap for snap in rows if snap.runtime_primary), None)
        if primary is not None and str(primary.deployment_mode or "").strip().lower() == "minimal":
            primary_name = primary.name or f"Device {primary.device_profile_id}"
            lines.append(
                f"{primary_name} is primary in Minimal mode; Map, Messages, FreqPlanner, startup launch, and background ingest stay suppressed."
            )
        swap_summary = next((snap.swap_summary for snap in rows if snap.swap_role == "target" and snap.swap_summary), "")
        if swap_summary:
            lines.append(swap_summary)
        blocked_names = [snap.name or f"Device {snap.device_profile_id}" for snap in rows if snap.shared_ptt_blocked]
        if blocked_names:
            preview = ", ".join(blocked_names[:3])
            extra = "" if len(blocked_names) <= 3 else f", +{len(blocked_names) - 3} more"
            lines.append(
                f"Shared PTT is blocking {len(blocked_names)} device profile{'s' if len(blocked_names) != 1 else ''}: {preview}{extra}."
            )
        warning_names = [
            snap.name or f"Device {snap.device_profile_id}"
            for snap in rows
            if str(snap.overall_state or "").strip().lower() in {"warn", "error"} or snap.warning_text
        ]
        if warning_names:
            preview = ", ".join(warning_names[:3])
            extra = "" if len(warning_names) <= 3 else f", +{len(warning_names) - 3} more"
            lines.append(
                f"{len(warning_names)} runtime warning{'s' if len(warning_names) != 1 else ''} need attention: {preview}{extra}."
            )
        return lines

    @staticmethod
    def _control_summary(snapshot: DeviceRuntimeSnapshot) -> str:
        endpoint_summary = str(snapshot.endpoint_summary or "").strip()
        if endpoint_summary:
            return endpoint_summary
        return str(snapshot.control_backend or "unknown").replace("_", " ").upper()

    @staticmethod
    def _merged_service_states(service_states: dict[str, dict[str, object]]) -> list[tuple[str, str, str]]:
        severity = {"error": 4, "warn": 3, "ok": 2, "idle": 1}
        merged: dict[str, dict[str, object]] = {}
        for service_name, info in (service_states or {}).items():
            label = str(service_name or "").replace("_API", "") or "Service"
            state = str((info or {}).get("state", "idle") or "idle").strip().lower()
            tooltip = str((info or {}).get("tooltip", "") or "").strip()
            current = merged.setdefault(label, {"state": state, "tooltips": []})
            if severity.get(state, 0) > severity.get(str(current.get("state", "idle")), 0):
                current["state"] = state
            tooltips = current.setdefault("tooltips", [])
            if tooltip and tooltip not in tooltips:
                tooltips.append(tooltip)
        return [
            (
                label,
                str(info.get("state", "idle") or "idle"),
                "\n".join(str(item) for item in info.get("tooltips", []) if str(item).strip()),
            )
            for label, info in merged.items()
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
        if snapshot.runtime_primary:
            badge = QLabel("Station Default")
            badge.setStyleSheet(_state_badge_style("ok", theme))
            header.addWidget(badge, 0, Qt.AlignRight)
        selected_snapshot = None
        if self._station_context is not None and hasattr(self._station_context, "selected_snapshot"):
            try:
                selected_snapshot = self._station_context.selected_snapshot()
            except Exception:
                selected_snapshot = None
        if (
            selected_snapshot is not None
            and int(getattr(selected_snapshot, "device_profile_id", 0) or 0) == int(snapshot.device_profile_id or 0)
            and snapshot.device_class != "observer"
        ):
            badge = QLabel("Selected Radio")
            badge.setStyleSheet(_state_badge_style("warn", theme))
            header.addWidget(badge, 0, Qt.AlignRight)
        active_badge = QLabel("Active")
        active_badge.setStyleSheet(_state_badge_style(snapshot.overall_state, theme))
        header.addWidget(active_badge, 0, Qt.AlignRight)
        if snapshot.device_class != "observer" and self._station_context is not None:
            select_btn = QPushButton("Select")
            select_btn.setCursor(Qt.PointingHandCursor)
            select_btn.clicked.connect(
                lambda _checked=False, device_profile_id=int(snapshot.device_profile_id): self._select_radio(device_profile_id)
            )
            header.addWidget(select_btn, 0, Qt.AlignRight)
        layout.addLayout(header)

        meta = QLabel(
            f"Class: {snapshot.device_class.replace('_', ' ').title()} | "
            f"Control: {self._control_summary(snapshot)} | "
            f"Deployment: {snapshot.deployment_mode.upper()} | "
            f"PTT Group: {snapshot.ptt_group or 'None'} | "
            f"Operating Profile: {snapshot.assigned_operating_profile_name or 'Unassigned'} | "
            f"Assignment: {snapshot.assignment_state.replace('_', ' ').title() or 'Unassigned'}"
        )
        meta.setWordWrap(True)
        layout.addWidget(meta)

        tuning_label = QLabel(
            "Tuning: "
            + (
                " ".join(part for part in (snapshot.current_frequency_label, snapshot.current_band) if part).strip()
                or "Unavailable"
            )
            + " | "
            + f"Antenna: {snapshot.antenna_group or 'None'} | "
            + f"Front-End: {snapshot.frontend_group or 'None'} | "
            + f"Amplifier: {snapshot.amplifier_group or 'None'}"
        )
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
        status_row.addWidget(led, 0, Qt.AlignTop)
        summary = QLabel(snapshot.status_summary or "Runtime status unavailable.")
        summary.setWordWrap(True)
        status_row.addWidget(summary, 1)
        layout.addLayout(status_row)

        if snapshot.shared_ptt_status_text:
            ptt_label = QLabel(snapshot.shared_ptt_status_text)
            ptt_label.setWordWrap(True)
            if snapshot.shared_ptt_blocked:
                ptt_label.setStyleSheet(f"color: {theme['warning']}; font-weight: 600;")
            elif snapshot.ptt_active:
                ptt_label.setStyleSheet(f"color: {theme['info']}; font-weight: 600;")
            layout.addWidget(ptt_label)

        if snapshot.swap_summary:
            swap_label = QLabel(snapshot.swap_summary)
            swap_label.setWordWrap(True)
            swap_label.setStyleSheet(f"color: {theme['accent']}; font-weight: 600;")
            layout.addWidget(swap_label)

        if snapshot.observer_follow_summary:
            observer_label = QLabel(snapshot.observer_follow_summary)
            observer_label.setWordWrap(True)
            observer_label.setStyleSheet(f"color: {theme['info']}; font-weight: 600;")
            layout.addWidget(observer_label)

        if snapshot.varac_cluster_summary:
            varac_label = QLabel(snapshot.varac_cluster_summary)
            varac_label.setWordWrap(True)
            varac_label.setStyleSheet(
                f"color: {theme['accent'] if snapshot.varac_gateway_handler else theme['info']}; font-weight: 600;"
            )
            layout.addWidget(varac_label)

        if snapshot.warning_text:
            warning = QLabel(snapshot.warning_text)
            warning.setWordWrap(True)
            warning.setStyleSheet(f"color: {theme['warning']};")
            layout.addWidget(warning)

        chips = QHBoxLayout()
        chips.setSpacing(6)
        for label, state, tooltip in self._merged_service_states(snapshot.service_states):
            chip = QLabel(label)
            chip.setStyleSheet(_state_badge_style(state, theme))
            if tooltip:
                chip.setToolTip(tooltip)
            chips.addWidget(chip)
        chips.addStretch(1)
        layout.addLayout(chips)
        return card

    def _select_radio(self, device_profile_id: int) -> None:
        if self._station_context is None:
            return
        try:
            self._station_context.set_selected_device_profile(int(device_profile_id))
        except Exception:
            pass

    def _on_station_context_snapshots_changed(self, _snapshots: object) -> None:
        self._refresh_dirty = True
        if self._tab_active:
            self.refresh_from_manager(force=True)

    def _on_station_context_selection_changed(self, _snapshot: object) -> None:
        self._refresh_dirty = True
        if self._tab_active:
            self.refresh_from_manager(force=True)
