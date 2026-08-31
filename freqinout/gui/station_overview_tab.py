from __future__ import annotations

from typing import Iterable, Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QAbstractScrollArea,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QScrollArea,
    QSizePolicy,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.busy_state_service import BusyStateService
from freqinout.core.scheduler_manual_control_service import SchedulerManualControlService
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
    CONTROL_CENTER_HEALTH_COLUMN = 4
    health_details_requested = Signal(int, str)

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.settings = SettingsManager()
        self._runtime_manager: Optional[StationRuntimeManager] = None
        self._busy_state_service: Optional[BusyStateService] = None
        self._manual_control_service: Optional[SchedulerManualControlService] = None
        self._tab_active = False
        self._refresh_dirty = False
        self._last_render_signature: tuple[object, ...] = tuple()
        self._control_center_snapshots: list[DeviceRuntimeSnapshot] = []
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

        control_title = QLabel("<b>Station Control Center</b>")
        control_title.setAccessibleName("Station Control Center")
        layout.addWidget(control_title)

        self.control_center_tabs = QTabWidget(self)
        self.control_center_tabs.setObjectName("stationControlCenterSourceTabs")
        self.control_center_tabs.setAccessibleName("Station Control Center source tabs")
        layout.addWidget(self.control_center_tabs, 1)

        self.control_center_overview_page = QWidget(self.control_center_tabs)
        overview_layout = QVBoxLayout(self.control_center_overview_page)
        overview_layout.setContentsMargins(0, 0, 0, 0)
        overview_layout.setSpacing(8)

        self.control_center_table = QTableWidget(0, 6)
        self.control_center_table.setObjectName("stationControlCenterTable")
        self.control_center_table.setAccessibleName("Station Control Center table")
        self.control_center_table.setHorizontalHeaderLabels(
            ["Radio / SDR", "Now", "Control State", "Next Schedule Action", "Health", "Actions"]
        )
        self.control_center_table.verticalHeader().setVisible(False)
        self.control_center_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.control_center_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.control_center_table.setSelectionMode(QTableWidget.SingleSelection)
        self.control_center_table.setAlternatingRowColors(True)
        self.control_center_table.setSizeAdjustPolicy(QAbstractScrollArea.AdjustIgnored)
        self.control_center_table.setMinimumHeight(124)
        self.control_center_table.setMaximumHeight(220)
        self.control_center_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.control_center_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.control_center_table.cellClicked.connect(self._on_control_center_cell_clicked)
        self.control_center_table.cellDoubleClicked.connect(self._on_control_center_cell_clicked)
        header = self.control_center_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.Stretch)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.control_center_empty_label = QLabel(
            "No active station runtimes. Activate one or more device profiles in Settings to populate Station Control Center."
        )
        self.control_center_empty_label.setObjectName("stationControlCenterEmptyState")
        self.control_center_empty_label.setWordWrap(True)
        self.control_center_empty_label.setVisible(False)
        overview_layout.addWidget(self.control_center_empty_label, 0)
        overview_layout.addWidget(self.control_center_table, 0)
        overview_layout.addStretch(1)
        self.control_center_tabs.addTab(self.control_center_overview_page, "Overview")
        self._update_control_center_empty_state()

        self.cards_container = QWidget(self)
        self.cards_layout = QVBoxLayout(self.cards_container)
        self.cards_layout.setContentsMargins(0, 0, 0, 0)
        self.cards_layout.setSpacing(0)

    def set_runtime_manager(self, manager: Optional[StationRuntimeManager]) -> None:
        self._runtime_manager = manager
        store = getattr(manager, "store", None) if manager is not None else None
        self._busy_state_service = BusyStateService(store) if store is not None else None
        self._manual_control_service = SchedulerManualControlService(store) if store is not None else None
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
        if hasattr(self, "control_center_tabs"):
            while self.control_center_tabs.count() > 1:
                widget = self.control_center_tabs.widget(1)
                self.control_center_tabs.removeTab(1)
                if widget is not None:
                    widget.deleteLater()
        while self.cards_layout.count():
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
                    self._control_center_row_values(snapshot),
                )
            )
        return tuple(rendered)

    def _control_center_signature(self, snapshots: Iterable[DeviceRuntimeSnapshot]) -> tuple[object, ...]:
        return tuple(self._control_center_row_values(snapshot) for snapshot in snapshots)

    def _rebuild_cards(self, snapshots: Iterable[DeviceRuntimeSnapshot]) -> None:
        snaps = list(snapshots)
        self._clear_cards()
        self._refresh_control_center_table(snaps)
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
            self._add_source_tab(snap, theme)
        self._sync_source_tab_badges(snaps)

    @staticmethod
    def _source_tab_label(snapshot: DeviceRuntimeSnapshot) -> str:
        name = str(snapshot.name or f"Device {snapshot.device_profile_id}").strip()
        state = str(snapshot.overall_state or "idle").strip().lower()
        marker = " !" if state in {"warn", "error"} or snapshot.warning_text or snapshot.shared_ptt_blocked else ""
        role = " SDR" if snapshot.device_class == "observer" else ""
        return f"{name}{role}{marker}"

    def _add_source_tab(self, snapshot: DeviceRuntimeSnapshot, theme: dict[str, str]) -> None:
        page = QWidget(self.control_center_tabs)
        page_layout = QVBoxLayout(page)
        page_layout.setContentsMargins(0, 0, 0, 0)
        page_layout.setSpacing(0)
        scroll = QScrollArea(page)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        page_layout.addWidget(scroll, 1)
        body = QWidget(scroll)
        body_layout = QVBoxLayout(body)
        body_layout.setContentsMargins(0, 0, 0, 0)
        body_layout.setSpacing(10)
        body_layout.addWidget(self._build_runtime_card(snapshot, theme, parent=body), 0)
        body_layout.addStretch(1)
        scroll.setWidget(body)
        self.control_center_tabs.addTab(page, self._source_tab_label(snapshot))

        legacy_marker = QLabel(str(snapshot.name or f"Device {snapshot.device_profile_id}"), self.cards_container)
        legacy_marker.setVisible(False)
        self.cards_layout.addWidget(legacy_marker)

    def _sync_source_tab_badges(self, snapshots: Iterable[DeviceRuntimeSnapshot]) -> None:
        if not hasattr(self, "control_center_tabs"):
            return
        self.control_center_tabs.setTabToolTip(
            0,
            "Overview of all active radios, SDRs, and future station sources.",
        )
        for index, snapshot in enumerate(list(snapshots), start=1):
            if index >= self.control_center_tabs.count():
                break
            tooltip = (
                f"{snapshot.name or f'Device {snapshot.device_profile_id}'} | "
                f"{self._control_state_text(snapshot)} | "
                f"{snapshot.status_summary or 'No status summary available.'}"
            )
            if snapshot.warning_text:
                tooltip += f" | {snapshot.warning_text}"
            self.control_center_tabs.setTabToolTip(index, tooltip)

    @staticmethod
    def _manual_state_label(raw_state: str) -> str:
        state = str(raw_state or "on_schedule").strip().lower()
        labels = {
            "on_schedule": "On Schedule",
            "manual_hold": "Manual Hold",
            "manual_suspend": "Suspended",
            "manual_qsy": "Manual Control",
            "busy_hold": "Busy Hold",
            "unavailable": "Unavailable",
        }
        return labels.get(state, "On Schedule")

    def _manual_state_for(self, snapshot: DeviceRuntimeSnapshot) -> tuple[str, str]:
        service = self._manual_control_service
        if service is None:
            return ("on_schedule", "")
        try:
            state = service.get_state(int(snapshot.device_profile_id or 0))
        except Exception:
            return ("on_schedule", "")
        suffix = ""
        if state.hold_until_utc:
            suffix = str(state.hold_until_utc or "").replace("T", " ").replace("Z", "Z")
        return (state.state, suffix)

    def _busy_label_for(self, snapshot: DeviceRuntimeSnapshot) -> str:
        if snapshot.ptt_active or snapshot.shared_ptt_blocked:
            return "Busy: PTT"
        service = self._busy_state_service
        if service is None:
            return ""
        try:
            busy = service.state_for_radio(int(snapshot.device_profile_id or 0))
        except Exception:
            return ""
        if not busy.busy:
            return ""
        summary = str(busy.summary or busy.reason_code or "Busy").strip()
        if summary.lower().startswith("busy"):
            return summary
        return f"Busy: {summary}"

    def _control_state_text(self, snapshot: DeviceRuntimeSnapshot) -> str:
        busy_label = self._busy_label_for(snapshot)
        if busy_label:
            return busy_label
        if snapshot.device_class == "observer":
            return "Monitor"
        state, suffix = self._manual_state_for(snapshot)
        label = self._manual_state_label(state)
        if suffix and state in {"manual_hold", "manual_suspend", "manual_qsy"}:
            return f"{label} until {suffix}"
        if not snapshot.scheduler_enabled:
            return "Scheduler Off"
        return label

    @staticmethod
    def _next_schedule_text(snapshot: DeviceRuntimeSnapshot) -> str:
        if snapshot.device_class == "observer":
            return snapshot.observer_follow_summary or "Receive-only monitor"
        if not snapshot.assigned_operating_profile_name:
            return "No assigned plan"
        if not snapshot.scheduler_enabled:
            return "Scheduler disabled"
        return f"Assigned plan: {snapshot.assigned_operating_profile_name}"

    @staticmethod
    def _health_text(snapshot: DeviceRuntimeSnapshot) -> str:
        services = []
        for label, state, _tooltip in StationOverviewTab._merged_service_states(snapshot.service_states):
            family = label.upper().replace("FLRIG", "FL").replace("FLDIGI", "FL")
            family = family.replace("JS8CALL", "JS8").replace("VARAC", "VA")
            state_code = str(state or "idle").strip().lower()
            marker = "ok" if state_code == "ok" else ("!" if state_code in {"warn", "error"} else "-")
            services.append(f"{family} {marker}")
        if services:
            return " | ".join(services[:4])
        if snapshot.overall_state:
            return str(snapshot.overall_state).title()
        return "No health data"

    def _control_center_row_values(self, snapshot: DeviceRuntimeSnapshot) -> tuple[str, str, str, str, str, str]:
        role = "SDR" if snapshot.device_class == "observer" else "HF"
        radio = f"{snapshot.name or f'Device {snapshot.device_profile_id}'} ({role})"
        now = " ".join(part for part in (snapshot.current_frequency_label, snapshot.current_band) if part).strip()
        return (
            radio,
            now or "Unavailable",
            self._control_state_text(snapshot),
            self._next_schedule_text(snapshot),
            self._health_text(snapshot),
            "Read-only",
        )

    def _refresh_control_center_table(self, snapshots: Iterable[DeviceRuntimeSnapshot]) -> None:
        table = self.control_center_table
        self._control_center_snapshots = list(snapshots)
        rows = [self._control_center_row_values(snapshot) for snapshot in self._control_center_snapshots]
        table.setRowCount(len(rows))
        for row_idx, values in enumerate(rows):
            for col_idx, value in enumerate(values):
                item = QTableWidgetItem(str(value or ""))
                if col_idx in {1, 2, 5}:
                    item.setTextAlignment(Qt.AlignCenter)
                else:
                    item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                if col_idx == self.CONTROL_CENTER_HEALTH_COLUMN:
                    item.setToolTip("Open Health Details for this radio or SDR.")
                table.setItem(row_idx, col_idx, item)
        table.resizeRowsToContents()
        self._update_control_center_empty_state()

    def _update_control_center_empty_state(self) -> None:
        if not hasattr(self, "control_center_empty_label") or not hasattr(self, "control_center_table"):
            return
        has_rows = self.control_center_table.rowCount() > 0
        self.control_center_empty_label.setVisible(not has_rows)
        self.control_center_table.setVisible(has_rows)

    def _on_control_center_cell_clicked(self, row: int, column: int) -> None:
        if int(column) == self.CONTROL_CENTER_HEALTH_COLUMN:
            self._request_health_details_for_row(row)

    def _request_health_details_for_row(self, row: int) -> None:
        try:
            snapshot = self._control_center_snapshots[int(row)]
        except Exception:
            return
        self.health_details_requested.emit(int(snapshot.device_profile_id or 0), str(snapshot.name or "").strip())

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

    def _build_runtime_card(
        self,
        snapshot: DeviceRuntimeSnapshot,
        theme: dict[str, str],
        *,
        parent: QWidget | None = None,
    ) -> QWidget:
        card = QFrame(parent or self)
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
            f"Operating Model: {snapshot.assigned_operating_profile_name or 'Unassigned'} | "
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
