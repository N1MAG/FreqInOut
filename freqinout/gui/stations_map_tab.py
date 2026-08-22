from __future__ import annotations

import datetime
import html
import json
import shutil
import sqlite3
import urllib.request
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Dict, Optional, Set
import math
import time
import logging
import sys
import queue

from PySide6.QtCore import QUrl, Qt, QTimer, QCoreApplication, QSize
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QCheckBox,
    QComboBox,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QFrame,
    QGridLayout,
    QScrollArea,
    QSplitter,
    QStackedWidget,
    QToolButton,
    QStyle,
    QHeaderView,
    QTableWidget,
    QTableWidgetItem,
)
from freqinout.core.config_paths import get_config_dir

_WEBENGINE_IMPORT_ERROR = None
try:
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from PySide6.QtWebEngineCore import QWebEnginePage
except Exception as exc:  # pragma: no cover - optional dependency
    QWebEngineView = None
    QWebEnginePage = None
    _WEBENGINE_IMPORT_ERROR = exc

JS8NET_PATH = Path(__file__).resolve().parents[2] / "third_party" / "js8net" / "js8net-main"
if JS8NET_PATH.exists():
    sys.path.insert(0, str(JS8NET_PATH))
try:
    import js8net  # type: ignore
except Exception:
    js8net = None
from freqinout.core.logger import log
from freqinout.core.js8_spotter_forms import (
    PURPOSE_HAZARD,
    PURPOSE_INFRASTRUCTURE,
    PURPOSE_WEATHER,
    form_codes_enabled_for,
    forms_enabled_for,
)
from freqinout.core.perf_metrics import emit_span, span as perf_span
from freqinout.core.operator_activity import (
    load_js8_direct_contact_summary,
    load_operator_activity_summary,
)
from freqinout.core.observation_queries import ObservationQuery, map_observation_rows
from freqinout.core.rf_pins import delete_rf_pins, list_rf_pins, save_rf_pin
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.js8_log_link_indexer import JS8LogLinkIndexer
from freqinout.core.js8_runtime_ingest import ingest_js8_links_for_runtime_sources
from freqinout.core.js8_source_context import resolve_js8_source_context
from freqinout.core.plan_context_service import PlanContextService
from freqinout.core.propagation_service import PropagationService
from freqinout.core.sitrep_metadata import source_family_label, source_short_label, transport_label
from freqinout.core.varac_runtime_ingest import ingest_varac_for_runtime_sources
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.support_reporting import build_support_summary, bullet_lines
from freqinout.radio_interface.js8_rx_hub import JS8RxHub
from freqinout.gui.qsy_helper import current_scheduler_freq
from freqinout.gui.help_registry import resolve_help_host
from freqinout.gui.plan_context_label import PlanContextLabel
from freqinout.gui.theme import (
    resolve_theme,
    resolve_ui_text_scale,
    BAND_COLORS_DARK,
    BAND_COLORS_LIGHT,
    button_style,
)
from freqinout.utils.timezones import get_timezone


def _ensure_webengine_imported() -> bool:
    global QWebEngineView, QWebEnginePage, _WEBENGINE_IMPORT_ERROR
    if QWebEngineView is not None:
        return True
    try:
        from PySide6.QtWebEngineWidgets import QWebEngineView as _QWebEngineView
        from PySide6.QtWebEngineCore import QWebEnginePage as _QWebEnginePage
    except Exception as exc:  # pragma: no cover - optional dependency
        _WEBENGINE_IMPORT_ERROR = exc
        log.warning("Qt WebEngine import failed: %s", exc, exc_info=True)
        return False
    QWebEngineView = _QWebEngineView
    QWebEnginePage = _QWebEnginePage
    _WEBENGINE_IMPORT_ERROR = None
    return True


USA_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]
LOWER48_STATES = [s for s in USA_STATES if s not in {"AK", "HI"}]

CANADA_PROVINCES = ["AB", "BC", "MB", "NB", "NL", "NT", "NS", "NU", "ON", "PE", "QC", "SK", "YT"]

USA_FRAME = ((7.0, -172.0), (83.0, -50.0))  # lat_min, lon_min, lat_max, lon_max

# FEMA regions mapping (states -> region id)
FEMA_REGIONS = {
    "R01": ["CT", "ME", "MA", "NH", "RI", "VT"],
    "R02": ["NJ", "NY", "PR", "VI"],
    "R03": ["DC", "DE", "MD", "PA", "VA", "WV"],
    "R04": ["AL", "FL", "GA", "KY", "MS", "NC", "SC", "TN"],
    "R05": ["IL", "IN", "MI", "MN", "OH", "WI"],
    "R06": ["AR", "LA", "NM", "OK", "TX"],
    "R07": ["IA", "KS", "MO", "NE"],
    "R08": ["CO", "MT", "ND", "SD", "UT", "WY"],
    "R09": ["AZ", "CA", "HI", "NV", "GU", "AS", "MP"],
    "R10": ["AK", "ID", "OR", "WA"],
}
STATE_TO_FEMA_REGION = {state: region for region, states in FEMA_REGIONS.items() for state in states}

PROP_BANDS = ["80M", "40M", "30M", "20M", "15M", "10M"]
PROP_BAND_COLORS = {
    "80M": "#4B2E83",
    "40M": "#1E88E5",
    "30M": "#00897B",
    "20M": "#43A047",
    "15M": "#FB8C00",
    "10M": "#E53935",
}
PROP_DEFAULT_PROFILES = {
    "80M": {"ideal_km": 600, "spread_km": 1200, "day": 0.35, "night": 1.0},
    "40M": {"ideal_km": 1200, "spread_km": 1800, "day": 0.65, "night": 0.95},
    "30M": {"ideal_km": 1800, "spread_km": 2200, "day": 0.8, "night": 0.85},
    "20M": {"ideal_km": 2800, "spread_km": 2600, "day": 1.0, "night": 0.6},
    "15M": {"ideal_km": 3600, "spread_km": 3000, "day": 0.9, "night": 0.35},
    "10M": {"ideal_km": 4200, "spread_km": 3600, "day": 0.8, "night": 0.2},
}

RF_PIN_TOPICS = (
    "General Intel",
    "Comms",
    "Infrastructure",
    "Power",
    "Water",
    "Fuel",
    "Travel/Roads",
    "Fire",
    "Weather",
    "Shelter",
    "Medical",
)


class _RfPinDialog(QDialog):
    def __init__(self, parent: Optional[QWidget] = None, *, pin: Optional[Any] = None) -> None:
        super().__init__(parent)
        self._source_ref = str(getattr(pin, "source_ref", "") or "").strip()
        self.setWindowTitle("Edit RF Pin" if self._source_ref else "Add RF Pin")
        self.title_edit = QLineEdit(self)
        self.title_edit.setPlaceholderText("Short label shown in map details")
        self.group_edit = QLineEdit(self)
        self.group_edit.setPlaceholderText("Operating group or target, optional")
        self.grid_edit = QLineEdit(self)
        self.grid_edit.setPlaceholderText("Grid square, e.g. DM79QJ")
        self.state_edit = QLineEdit(self)
        self.state_edit.setPlaceholderText("State/province, optional")
        self.topic_combo = QComboBox(self)
        self.topic_combo.addItems(list(RF_PIN_TOPICS))
        self.summary_edit = QLineEdit(self)
        self.summary_edit.setPlaceholderText("Brief note, optional")

        form = QFormLayout(self)
        form.setContentsMargins(18, 18, 18, 18)
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(8)
        form.addRow("Pin Name", self.title_edit)
        form.addRow("Group", self.group_edit)
        form.addRow("Grid", self.grid_edit)
        form.addRow("State", self.state_edit)
        form.addRow("Topic", self.topic_combo)
        form.addRow("Summary", self.summary_edit)
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel, self)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        form.addRow(buttons)
        if pin is not None:
            self._load_pin(pin)

    def _load_pin(self, pin: Any) -> None:
        self.title_edit.setText(str(getattr(pin, "subject", "") or "").strip())
        self.group_edit.setText(str(getattr(pin, "to_target", "") or "").strip().lstrip("@"))
        self.grid_edit.setText(str(getattr(pin, "grid", "") or "").strip().upper())
        self.state_edit.setText(str(getattr(pin, "state", "") or "").strip().upper())
        topics = tuple(getattr(pin, "observed_topics", ()) or ())
        if topics:
            idx = self.topic_combo.findText(str(topics[0]).strip())
            if idx >= 0:
                self.topic_combo.setCurrentIndex(idx)
        self.summary_edit.setText(str(getattr(pin, "summary", "") or "").strip())

    def pin_payload(self) -> Dict[str, Any]:
        label = self.title_edit.text().strip()
        group = self.group_edit.text().strip().lstrip("@")
        grid = self.grid_edit.text().strip().upper()
        state = self.state_edit.text().strip().upper()
        topic = self.topic_combo.currentText().strip()
        summary = self.summary_edit.text().strip()
        created_utc = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
        ref_seed = label or grid or state or "pin"
        payload: Dict[str, Any] = {
            "raw_reference": self._source_ref or f"rf_pin:{int(time.time() * 1000)}:{ref_seed}",
            "label": label or summary or "RF Pin",
            "group": group,
            "groups": [group] if group else [],
            "to_target": group,
            "grid": grid,
            "state": state,
            "topics": [topic] if topic else [],
            "summary": summary or label or "RF map pin",
            "source_app": "FIO",
            "created_utc": created_utc,
            "event_utc": created_utc,
            "status": "PIN",
            "pin_kind": "operator",
        }
        return payload


class _RfPinManagerDialog(QDialog):
    COL_LABEL = 0
    COL_GROUP = 1
    COL_TOPIC = 2
    COL_AREA = 3
    COL_SUMMARY = 4

    def __init__(self, db_path: Path, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Manage RF Pins")
        self._db_path = Path(db_path)
        self._deleted = False
        self._changed = False
        self._pins_by_ref: Dict[str, Any] = {}
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)
        self.table = QTableWidget(0, 5, self)
        self.table.setHorizontalHeaderLabels(["Pin", "Group", "Topic", "Area", "Summary"])
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.ExtendedSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(self.COL_LABEL, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_GROUP, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_TOPIC, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_AREA, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_SUMMARY, QHeaderView.Stretch)
        layout.addWidget(self.table)

        actions = QHBoxLayout()
        self.edit_button = QPushButton("Edit Selected", self)
        self.delete_button = QPushButton("Delete Selected", self)
        self.close_button = QPushButton("Close", self)
        self.edit_button.clicked.connect(self._edit_selected)
        self.delete_button.clicked.connect(self._delete_selected)
        self.close_button.clicked.connect(self.accept)
        actions.addStretch(1)
        actions.addWidget(self.edit_button)
        actions.addWidget(self.delete_button)
        actions.addWidget(self.close_button)
        layout.addLayout(actions)
        self._load_rows()

    @property
    def changed(self) -> bool:
        return self._changed or self._deleted

    def _load_rows(self) -> None:
        try:
            pins = list_rf_pins(self._db_path, limit=500)
        except Exception as exc:
            log.warning("StationsMap: failed to list RF pins: %s", exc, exc_info=True)
            pins = ()
        self.table.setRowCount(0)
        self._pins_by_ref = {}
        for pin in pins:
            source_ref = str(pin.source_ref or "").strip()
            if source_ref:
                self._pins_by_ref[source_ref] = pin
            row = self.table.rowCount()
            self.table.insertRow(row)
            topic = ", ".join(str(t).strip() for t in pin.observed_topics if str(t).strip())
            area = " / ".join(part for part in (pin.state, pin.grid) if str(part).strip())
            values = [
                pin.subject or "RF Pin",
                pin.to_target,
                topic,
                area,
                pin.summary,
            ]
            for col, value in enumerate(values):
                item = QTableWidgetItem(str(value or ""))
                if col == self.COL_LABEL:
                    item.setData(Qt.UserRole, source_ref)
                self.table.setItem(row, col, item)
        self.edit_button.setEnabled(self.table.rowCount() > 0)
        self.delete_button.setEnabled(self.table.rowCount() > 0)

    def _selected_source_refs(self) -> List[str]:
        refs: List[str] = []
        for index in self.table.selectionModel().selectedRows():
            item = self.table.item(index.row(), self.COL_LABEL)
            source_ref = str(item.data(Qt.UserRole) if item is not None else "").strip()
            if source_ref:
                refs.append(source_ref)
        return refs

    def _edit_selected(self) -> None:
        refs = self._selected_source_refs()
        if len(refs) != 1:
            QMessageBox.information(self, "Manage RF Pins", "Select one RF pin to edit.")
            return
        pin = self._pins_by_ref.get(refs[0])
        if pin is None:
            QMessageBox.warning(self, "Manage RF Pins", "FIO could not find the selected RF pin.")
            return
        dialog = _RfPinDialog(self, pin=pin)
        if dialog.exec() != QDialog.Accepted:
            return
        payload = dialog.pin_payload()
        if not payload.get("grid"):
            QMessageBox.warning(self, "Edit RF Pin", "Add a grid square so FIO can place the RF pin on the map.")
            return
        try:
            save_rf_pin(self._db_path, payload)
        except Exception as exc:
            log.warning("StationsMap: failed to update RF pin: %s", exc, exc_info=True)
            QMessageBox.warning(self, "Edit RF Pin", f"FIO could not update this RF pin.\n{exc}")
            return
        self._changed = True
        self._load_rows()

    def _delete_selected(self) -> None:
        refs = self._selected_source_refs()
        if not refs:
            QMessageBox.information(self, "Manage RF Pins", "Select one or more RF pins to delete.")
            return
        answer = QMessageBox.question(
            self,
            "Delete RF Pins",
            f"Delete {len(refs)} selected RF pin(s)?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if answer != QMessageBox.Yes:
            return
        try:
            delete_rf_pins(self._db_path, refs)
        except Exception as exc:
            log.warning("StationsMap: failed to delete RF pins: %s", exc, exc_info=True)
            QMessageBox.warning(self, "Delete RF Pins", f"FIO could not delete the selected RF pin(s).\n{exc}")
            return
        self._deleted = True
        self._load_rows()

US_STATE_NAMES = {
    "AL": "ALABAMA",
    "AK": "ALASKA",
    "AZ": "ARIZONA",
    "AR": "ARKANSAS",
    "CA": "CALIFORNIA",
    "CO": "COLORADO",
    "CT": "CONNECTICUT",
    "DE": "DELAWARE",
    "FL": "FLORIDA",
    "GA": "GEORGIA",
    "HI": "HAWAII",
    "ID": "IDAHO",
    "IL": "ILLINOIS",
    "IN": "INDIANA",
    "IA": "IOWA",
    "KS": "KANSAS",
    "KY": "KENTUCKY",
    "LA": "LOUISIANA",
    "ME": "MAINE",
    "MD": "MARYLAND",
    "MA": "MASSACHUSETTS",
    "MI": "MICHIGAN",
    "MN": "MINNESOTA",
    "MS": "MISSISSIPPI",
    "MO": "MISSOURI",
    "MT": "MONTANA",
    "NE": "NEBRASKA",
    "NV": "NEVADA",
    "NH": "NEW HAMPSHIRE",
    "NJ": "NEW JERSEY",
    "NM": "NEW MEXICO",
    "NY": "NEW YORK",
    "NC": "NORTH CAROLINA",
    "ND": "NORTH DAKOTA",
    "OH": "OHIO",
    "OK": "OKLAHOMA",
    "OR": "OREGON",
    "PA": "PENNSYLVANIA",
    "RI": "RHODE ISLAND",
    "SC": "SOUTH CAROLINA",
    "SD": "SOUTH DAKOTA",
    "TN": "TENNESSEE",
    "TX": "TEXAS",
    "UT": "UTAH",
    "VT": "VERMONT",
    "VA": "VIRGINIA",
    "WA": "WASHINGTON",
    "WV": "WEST VIRGINIA",
    "WI": "WISCONSIN",
    "WY": "WYOMING",
}

# Canada province names (uppercase for matching)
CANADA_PROVINCE_NAMES = {
    "AB": "ALBERTA",
    "BC": "BRITISH COLUMBIA",
    "MB": "MANITOBA",
    "NB": "NEW BRUNSWICK",
    "NL": "NEWFOUNDLAND AND LABRADOR",
    "NT": "NORTHWEST TERRITORIES",
    "NS": "NOVA SCOTIA",
    "NU": "NUNAVUT",
    "ON": "ONTARIO",
    "PE": "PRINCE EDWARD ISLAND",
    "QC": "QUEBEC",
    "SK": "SASKATCHEWAN",
    "YT": "YUKON",
}

# Reverse map for fast lookup of full state names -> abbreviation
US_STATE_ABBR_FROM_NAME = {name: abbr for abbr, name in US_STATE_NAMES.items()}
CANADA_PROV_ABBR_FROM_NAME = {name: abbr for abbr, name in CANADA_PROVINCE_NAMES.items()}

STATE_CENTERS = {
    # USA
    "AL": (32.806, -86.792),
    "AK": (61.370, -152.404),
    "AZ": (33.729, -111.431),
    "AR": (34.969, -92.373),
    "CA": (36.116, -119.681),
    "CO": (39.059, -105.311),
    "CT": (41.600, -72.755),
    "DE": (38.910, -75.527),
    "FL": (27.766, -81.686),
    "GA": (33.040, -83.643),
    "HI": (21.094, -157.498),
    "ID": (44.240, -114.478),
    "IL": (40.349, -88.987),
    "IN": (39.849, -86.258),
    "IA": (42.011, -93.210),
    "KS": (38.526, -96.726),
    "KY": (37.668, -84.670),
    "LA": (31.169, -91.867),
    "ME": (44.693, -69.381),
    "MD": (39.063, -76.802),
    "MA": (42.230, -71.531),
    "MI": (44.182, -84.506),
    "MN": (46.729, -94.685),
    "MS": (32.741, -89.678),
    "MO": (38.457, -92.288),
    "MT": (46.921, -110.454),
    "NE": (41.125, -98.268),
    "NV": (38.313, -117.055),
    "NH": (43.193, -71.572),
    "NJ": (40.058, -74.406),
    "NM": (34.307, -106.018),
    "NY": (42.165, -74.948),
    "NC": (35.630, -79.807),
    "ND": (47.551, -101.002),
    "OH": (40.388, -82.764),
    "OK": (35.565, -96.928),
    "OR": (43.804, -120.554),
    "PA": (41.203, -77.194),
    "RI": (41.580, -71.477),
    "SC": (33.837, -81.163),
    "SD": (44.299, -99.438),
    "TN": (35.747, -86.692),
    "TX": (31.054, -97.564),
    "UT": (40.150, -111.862),
    "VT": (44.045, -72.709),
    "VA": (37.769, -78.169),
    "WA": (47.400, -121.490),
    "WV": (38.491, -80.954),
    "WI": (44.268, -89.616),
    "WY": (42.756, -107.302),
    # Canada (rough centroids)
    "AB": (53.933, -116.576),
    "BC": (54.000, -125.000),
    "MB": (53.760, -98.813),
    "NB": (46.565, -66.461),
    "NL": (53.135, -57.660),
    "NT": (64.824, -124.845),
    "NS": (44.682, -63.744),
    "NU": (70.299, -83.107),
    "ON": (50.000, -85.000),
    "PE": (46.250, -63.000),
    "QC": (52.939, -73.549),
    "SK": (52.939, -106.450),
    "YT": (64.282, -135.000),
}

# Simple city list (major/medium) for labels; keep compact
CITIES = [
    # USA
    ("New York", 40.7128, -74.0060, 8804190),
    ("Los Angeles", 34.0522, -118.2437, 3898747),
    ("Chicago", 41.8781, -87.6298, 2746388),
    ("Houston", 29.7604, -95.3698, 2304580),
    ("Phoenix", 33.4484, -112.0740, 1608139),
    ("San Antonio", 29.4241, -98.4936, 1434625),
    ("San Diego", 32.7157, -117.1611, 1386932),
    ("Dallas", 32.7767, -96.7970, 1288457),
    ("San Jose", 37.3382, -121.8863, 1035317),
    ("Austin", 30.2672, -97.7431, 964177),
    ("Jacksonville", 30.3322, -81.6557, 949611),
    ("Fort Worth", 32.7555, -97.3308, 918915),
    ("Columbus", 39.9612, -82.9988, 906528),
    ("Charlotte", 35.2271, -80.8431, 879709),
    ("Indianapolis", 39.7684, -86.1581, 882039),
    ("San Francisco", 37.7749, -122.4194, 873965),
    ("Seattle", 47.6062, -122.3321, 737015),
    ("Denver", 39.7392, -104.9903, 716000),
    ("Washington", 38.9072, -77.0369, 689545),
    ("Boston", 42.3601, -71.0589, 675647),
    ("El Paso", 31.7619, -106.4850, 678815),
    ("Nashville", 36.1627, -86.7816, 689447),
    ("Detroit", 42.3314, -83.0458, 639111),
    ("Oklahoma City", 35.4676, -97.5164, 681054),
    ("Portland", 45.5051, -122.6750, 652503),
    ("Las Vegas", 36.1699, -115.1398, 641903),
    ("Memphis", 35.1495, -90.0490, 633104),
    ("Louisville", 38.2527, -85.7585, 617638),
    ("Baltimore", 39.2904, -76.6122, 576498),
    ("Milwaukee", 43.0389, -87.9065, 577222),
    ("Albuquerque", 35.0844, -106.6504, 564559),
    ("Tucson", 32.2226, -110.9747, 542629),
    ("Seattle", 47.6062, -122.3321, 737015),
    ("Fresno", 36.7378, -119.7871, 542107),
    ("Sacramento", 38.5816, -121.4944, 524943),
    ("Kansas City", 39.0997, -94.5786, 508090),
    ("Mesa", 33.4152, -111.8315, 504258),
    ("Atlanta", 33.7490, -84.3880, 498715),
    ("Colorado Springs", 38.8339, -104.8214, 478961),
    ("Omaha", 41.2565, -95.9345, 486051),
    ("Raleigh", 35.7796, -78.6382, 469124),
    ("Miami", 25.7617, -80.1918, 439890),
    ("Minneapolis", 44.9778, -93.2650, 429954),
    ("Tulsa", 36.1539, -95.9928, 413066),
    ("New Orleans", 29.9511, -90.0715, 383997),
    ("Wichita", 37.6872, -97.3301, 397532),
    ("Cleveland", 41.4993, -81.6944, 372624),
    ("Tampa", 27.9506, -82.4572, 384959),
    ("Bakersfield", 35.3733, -119.0187, 407615),
    ("Aurora", 39.7294, -104.8319, 386261),
    ("Honolulu", 21.3069, -157.8583, 345510),
    ("St Louis", 38.6270, -90.1994, 302838),
    ("Pittsburgh", 40.4406, -79.9959, 302971),
    ("Cincinnati", 39.1031, -84.5120, 309317),
    ("Anchorage", 61.2181, -149.9003, 288121),
    ("Boise", 43.6150, -116.2023, 235684),
    # Canada
    ("Toronto", 43.6510, -79.3470, 2731571),
    ("Vancouver", 49.2827, -123.1207, 662248),
    ("Calgary", 51.0447, -114.0719, 1239000),
    ("Montreal", 45.5019, -73.5674, 1780000),
    ("Ottawa", 45.4215, -75.6972, 934243),
    ("Winnipeg", 49.8951, -97.1384, 705244),
    ("Halifax", 44.6488, -63.5752, 439819),
    ("Edmonton", 53.5461, -113.4938, 1063000),
    ("Quebec City", 46.8139, -71.2080, 542298),
    ("Hamilton", 43.2557, -79.8711, 579200),
    ("London", 42.9849, -81.2453, 422324),
    # Mexico
    ("Mexico City", 19.4326, -99.1332, 9209944),
    ("Guadalajara", 20.6597, -103.3496, 1495000),
    ("Monterrey", 25.6866, -100.3161, 1135000),
    ("Tijuana", 32.5149, -117.0382, 1880000),
    ("Cancun", 21.1619, -86.8515, 888797),
    ("Leon", 21.1220, -101.6841, 1570000),
    ("Merida", 20.9674, -89.5926, 892363),
]


@dataclass
class StationPoint:
    callsign: str
    grid: str
    heard_by: Optional[str] = None
    name: str = ""
    state: str = ""
    group: str = ""
    groups: List[str] = field(default_factory=list)
    trusted: bool = True
    lat: float = 0.0
    lon: float = 0.0


WEATHER_REPORT_MAX_AGE_SEC = 12 * 60 * 60
ALERT_REPORT_MAX_AGE_SEC = 24 * 60 * 60
INFRASTRUCTURE_REPORT_MAX_AGE_SEC = 24 * 60 * 60
WEATHER_CLUSTER_DEGREES = 0.75

WEATHER_SEVERITY_RANK = {
    "unknown": 0,
    "routine": 1,
    "caution": 2,
    "severe": 3,
}


def maidenhead_to_latlon(grid: str) -> Optional[tuple[float, float]]:
    """
    Convert Maidenhead grid locator to lat/lon (center of square/locator).
    Supports 4- or 6-character locators.
    """
    grid = (grid or "").strip().upper()
    if len(grid) < 4:
        return None
    try:
        lon = (ord(grid[0]) - ord("A")) * 20.0 + int(grid[2]) * 2.0 + 1.0 / 24.0
        lat = (ord(grid[1]) - ord("A")) * 10.0 + int(grid[3]) * 1.0 + 1.0 / 48.0
        if len(grid) >= 6:
            lon += (ord(grid[4]) - ord("A")) / 12.0
            lat += (ord(grid[5]) - ord("A")) / 24.0
        lon -= 180.0
        lat -= 90.0
        return lat, lon
    except Exception:
        return None


def maidenhead_grid4_bounds(grid: str) -> Optional[tuple[float, float, float, float]]:
    grid = (grid or "").strip().upper()
    if len(grid) < 4:
        return None
    if not re.match(r"^[A-R]{2}[0-9]{2}$", grid[:4]):
        return None
    lon_min = (ord(grid[0]) - ord("A")) * 20.0 + int(grid[2]) * 2.0 - 180.0
    lat_min = (ord(grid[1]) - ord("A")) * 10.0 + int(grid[3]) * 1.0 - 90.0
    lon_max = lon_min + 2.0
    lat_max = lat_min + 1.0
    return (lat_min, lon_min, lat_max, lon_max)


class StationsMapTab(QWidget):
    """
    Displays JS8Call-heard stations on an OSM-based map with a Maidenhead overlay.
    USA/Canada stations are shown; map tiles are streamed from OSM (requires network).
    """

    def __init__(self, parent=None, *, plan_context_service: Optional[PlanContextService] = None):
        super().__init__(parent)
        self.plan_context_service = plan_context_service
        self.show_callsigns = False
        self.stations: List[StationPoint] = []
        self._map_file: Optional[Path] = None
        self._map_cache_dir = get_config_dir() / "cache"
        self._managed_map_file = self._map_cache_dir / "stations_map_view.html"
        self._asset_dir = Path(__file__).resolve().parents[2] / "config" / "leaflet"
        self._geojson_path = self._asset_dir / "us_states.geojson"
        self._geojson_canada = self._asset_dir / "canada_provinces.geojson"
        self._geojson_mexico = self._asset_dir / "mexico_states.geojson"
        self._cities_geojson = self._asset_dir / "cities_na_1k.geojson"

        self.show_callsigns = False
        self.show_cities = False
        self.show_states = False
        self.show_station_markers = True
        self.show_link_paths = True
        self.show_grids = False
        self.show_grid_labels = False  # driven by the "Show grids" toggle
        self.show_weather_reports = True
        self.show_alert_reports = True
        self.show_infrastructure_reports = True
        self.show_regions = False
        self.show_city_labels = False
        self.city_pop_min = 100000
        self.link_mode = "off"
        self.link_value = ""
        self.relay_target = ""
        self._now_reachable_enabled: bool = False
        self._now_reachable_callsigns: Set[str] = set()
        self._now_reachable_meta: Dict[str, Dict] = {}
        self._refresh_links_button: Optional[QPushButton] = None
        self._now_reachable_button: Optional[QPushButton] = None
        self._now_reachable_label: Optional[QLabel] = None
        self._sitrep_status_only_enabled: bool = False
        self._observation_focus_enabled: bool = False
        self._observation_focus_mode: str = ""
        self._sitrep_status_button: Optional[QPushButton] = None
        self._map_view_status_label: Optional[QLabel] = None
        self._map_all_stations_button: Optional[QPushButton] = None
        self._map_hf_reports_button: Optional[QPushButton] = None
        self._map_local_reports_button: Optional[QPushButton] = None
        self._map_reports_button: Optional[QPushButton] = None
        self._map_rf_pins_button: Optional[QPushButton] = None
        self._map_add_rf_pin_button: Optional[QPushButton] = None
        self._map_manage_rf_pins_button: Optional[QPushButton] = None
        self._map_topic_filter_combo: Optional[QComboBox] = None
        self.selected_band = "All"
        self.recency_seconds: Optional[int] = None
        self.operator_rows: List[Dict] = []
        self.operator_index: Dict[str, Dict] = {}
        self._operator_groups: List[str] = []
        self._sitrep_report_groups: List[str] = []
        self._operator_regions: List[str] = []
        self._last_map_view: Optional[Dict[str, float]] = None

        # Settings handle so SettingsTab import works; JS8 indexer may be added later
        try:
            self.settings = SettingsManager()
        except Exception:
            self.settings = None

        self._last_js8_load_ts: float = 0.0
        self._last_exit_ts: float = 0.0
        try:
            if self.settings:
                self._last_js8_load_ts = float(self.settings.get("js8_links_last_load_utc", 0) or 0)
                self._last_exit_ts = float(self.settings.get("last_exit_utc", 0) or 0)
        except Exception:
            self._last_js8_load_ts = 0.0
            self._last_exit_ts = 0.0
        self._js8_timer: Optional[QTimer] = None
        self._js8_rx_timer: Optional[QTimer] = None
        self._js8_rx_hub: Optional[JS8RxHub] = None
        self._js8_rx_registered = False
        self._js8_indexer: Optional[JS8LogLinkIndexer] = None
        self._is_shutting_down = False
        self._js8_polling = False
        self._js8_live_source_context_cache: Dict[str, object] = {
            "endpoint": "",
            "expires": 0.0,
            "context": {},
        }
        self._background_ingest_controller = None
        self._js8_net_started = False
        self._map_initialized = False
        self._pending_map_payload: Optional[Dict[str, List[Dict]]] = None
        self._last_map_payload_sig: Optional[str] = None
        self._map_query_cache: Dict[tuple[str, str], tuple[float, object]] = {}
        self._last_map_render_input_sig: Optional[tuple] = None
        self._last_map_config: Optional[tuple] = None
        self._last_map_render_ts: float = 0.0
        self._stations_revision: int = 0
        self._render_pending: bool = False
        self._pending_refresh_level: int = 0
        self._pending_refresh_reason: str = ""
        self._pending_refresh_preserve_view: object = True
        self._map_visible: bool = False
        self._map_dirty: bool = False
        self._ingest_started: bool = False
        self._deferred_initial_ingest_pending: bool = False
        self._map_load_ok: bool = False
        self._map_page_loading: bool = False
        self._render_requested_during_load: bool = False
        self._render_requested_during_load_level: int = 0
        self._map_runtime_state: str = "cold"
        self._map_runtime_detail: str = "Map has not been opened yet."
        self._map_last_error: str = ""
        self._map_last_event_ts: float = 0.0
        self._map_marker_count: int = 0
        self._map_link_count: int = 0
        self._app_active: bool = True
        self.prop_overlay_enabled: bool = False
        self.prop_adaptive_enabled: bool = True
        self.prop_mode: str = "blended"
        self.prop_window_hours: int = 6
        self._prop_region_scores: Dict[str, Dict] = {}
        self._prop_best_band_info: Dict[str, str] = {}
        self._last_prop_region_filter: str = ""
        self.prop_overlay_chk: Optional[QCheckBox] = None
        self.prop_adaptive_chk: Optional[QCheckBox] = None
        self.prop_badge: Optional[QLabel] = None
        self.prop_mode_combo: Optional[QComboBox] = None
        self.prop_window_combo: Optional[QComboBox] = None
        self.prop_target_type_combo: Optional[QComboBox] = None
        self.prop_target_value_combo: Optional[QComboBox] = None
        self._prop_target_syncing: bool = False
        self._map_stack: Optional[QStackedWidget] = None
        self._map_loading_label: Optional[QLabel] = None
        self._controls_button: Optional[QPushButton] = None
        self._controls_drawer_open: bool = False
        self._controls_drawer_threshold: int = 1280
        self._drawer_mode: bool = False
        self._main_splitter: Optional[QSplitter] = None
        self._controls_panel: Optional[QWidget] = None
        self._controls_handle_button: Optional[QToolButton] = None
        self._controls_top_spacer: Optional[QWidget] = None
        self._map_filter_bar: Optional[QWidget] = None
        base = Path(__file__).resolve().parents[2]
        self._prop_service = PropagationService(
            default_profiles=PROP_DEFAULT_PROFILES,
            profiles_path=base / "config" / "propagation" / "prop_profiles.json",
            climatology_db_path=base / "config" / "propagation" / "prop_climatology.db",
            outcome_db_path=get_config_dir() / "config" / "freqinout_nets.db",
            db_index_mode="floor5",
        )
        self._presence_weights_cache: Optional[Dict[str, Dict]] = None
        self._presence_weights_ts: float = 0.0
        self._query_cache: Dict[tuple, tuple[float, Any]] = {}
        self._query_cache_ttl_sec: float = 3.0
        self._prop_prewarm_done: bool = False
        self._initial_data_loaded: bool = False
        self._map_refresh_timer = QTimer(self)
        self._map_refresh_timer.setSingleShot(True)
        self._map_refresh_timer.setInterval(160)
        self._map_refresh_timer.timeout.connect(self._flush_requested_map_refresh)

        self._build_ui()
        self._refresh_group_filter_options()
        self._refresh_region_filter_options()
        self._load_display_preferences()
        self._refresh_band_options()
        self.apply_theme()
        QTimer.singleShot(0, self._prewarm_map_perf_caches)

    def _ensure_initial_data_loaded(self) -> None:
        if self._initial_data_loaded:
            return
        with perf_span("map.initial_data_load", settings=self.settings, min_ms=10.0):
            self._load_operator_history()
        self._initial_data_loaded = True

    def _bool_setting(self, key: str, default: bool = False) -> bool:
        if not self.settings:
            return default
        try:
            val = self.settings.get(key, default)
        except Exception:
            return default
        if isinstance(val, bool):
            return val
        if val is None:
            return default
        sval = str(val).strip().lower()
        return sval in ("1", "true", "yes", "on")

    def _load_display_preferences(self):
        if not self.settings:
            return
        def apply_chk(chk: QCheckBox, attr: str, key: str, default: bool = False):
            val = self._bool_setting(key, default)
            setattr(self, attr, val)
            chk.blockSignals(True)
            chk.setChecked(val)
            chk.blockSignals(False)

        apply_chk(self.show_calls_chk, "show_callsigns", "map_show_callsigns", False)
        apply_chk(self.show_regions_chk, "show_regions", "map_show_regions", False)
        apply_chk(self.show_states_chk, "show_states", "map_show_states", False)
        apply_chk(self.show_cities_chk, "show_cities", "map_show_cities", False)
        apply_chk(self.show_grid_labels_chk, "show_grids", "map_show_grids", False)
        apply_chk(self.map_stations_chk, "show_station_markers", "map_show_station_markers", True)
        apply_chk(self.map_links_chk, "show_link_paths", "map_show_link_paths", True)
        apply_chk(self.map_weather_chk, "show_weather_reports", "map_show_weather_reports", True)
        apply_chk(self.map_alerts_chk, "show_alert_reports", "map_show_alert_reports", True)
        apply_chk(self.map_infrastructure_chk, "show_infrastructure_reports", "map_show_infrastructure_reports", True)
        # Map propagation overlay defaults OFF on every app launch.
        self.prop_overlay_enabled = False
        if self.prop_overlay_chk is not None:
            self.prop_overlay_chk.blockSignals(True)
            self.prop_overlay_chk.setChecked(False)
            self.prop_overlay_chk.blockSignals(False)
        try:
            self.prop_adaptive_enabled = self._bool_setting("map_prop_adaptive", True)
        except Exception:
            pass
        try:
            mode = (self.settings.get("map_prop_mode", "blended") or "blended").strip().lower()
        except Exception:
            mode = "blended"
        if mode == "adaptive":
            mode = "actual"
        if mode not in ("model", "actual", "blended"):
            mode = "blended"
        self.prop_mode = mode
        if self.prop_mode_combo is not None:
            idx_mode = self.prop_mode_combo.findData(mode)
            if idx_mode >= 0:
                self.prop_mode_combo.blockSignals(True)
                self.prop_mode_combo.setCurrentIndex(idx_mode)
                self.prop_mode_combo.blockSignals(False)
        try:
            self.prop_window_hours = int(self.settings.get("map_prop_window_hours", 6) or 6)
        except Exception:
            self.prop_window_hours = 6
        if self.prop_window_combo is not None:
            idx_window = self.prop_window_combo.findData(int(self.prop_window_hours or 6))
            if idx_window >= 0:
                self.prop_window_combo.blockSignals(True)
                self.prop_window_combo.setCurrentIndex(idx_window)
                self.prop_window_combo.blockSignals(False)
        # show_grid_labels mirrors show_grids
        self.show_grid_labels = self.show_grids

        try:
            idx = int(self.settings.get("map_city_pop_idx", 4) or 4)
        except Exception:
            idx = 4
        idx = max(0, min(idx, self.city_pop_combo.count() - 1))
        self.city_pop_combo.blockSignals(True)
        self.city_pop_combo.setCurrentIndex(idx)
        self.city_pop_combo.blockSignals(False)
        try:
            self.city_pop_min = int(self.city_pop_combo.itemData(idx))
        except Exception:
            pass
        self._sync_city_pop_enabled()
        self.show_city_labels = self.show_cities
        self._refresh_prop_target_controls()

    def _save_display_preferences(self):
        if not self.settings:
            return
        try:
            self.settings.set("map_show_callsigns", int(self.show_calls_chk.isChecked()))
            self.settings.set("map_show_regions", int(self.show_regions_chk.isChecked()))
            self.settings.set("map_show_states", int(self.show_states_chk.isChecked()))
            self.settings.set("map_show_cities", int(self.show_cities_chk.isChecked()))
            self.settings.set("map_show_grids", int(self.show_grid_labels_chk.isChecked()))
            self.settings.set("map_show_station_markers", int(self.map_stations_chk.isChecked()))
            self.settings.set("map_show_link_paths", int(self.map_links_chk.isChecked()))
            self.settings.set("map_show_weather_reports", int(self.map_weather_chk.isChecked()))
            self.settings.set("map_show_alert_reports", int(self.map_alerts_chk.isChecked()))
            self.settings.set("map_show_infrastructure_reports", int(self.map_infrastructure_chk.isChecked()))
            self.settings.set("map_city_pop_idx", self.city_pop_combo.currentIndex())
            if self.prop_overlay_chk is not None:
                self.settings.set("map_prop_overlay", int(self.prop_overlay_chk.isChecked()))
            else:
                self.settings.set("map_prop_overlay", int(bool(self.prop_overlay_enabled)))
            self.settings.set("map_prop_adaptive", int(bool(self.prop_adaptive_enabled)))
            self.settings.set("map_prop_mode", (self.prop_mode or "blended").lower())
            self.settings.set("map_prop_window_hours", int(self.prop_window_hours or 6))
        except Exception:
            pass

    def _start_js8_ingest_timer(self):
        try:
            app = QCoreApplication.instance()
            if app:
                app.aboutToQuit.connect(self._record_exit_time)
        except Exception:
            pass
        self._js8_timer = QTimer(self)
        self._js8_timer.setInterval(5 * 60 * 1000)  # 5 minutes
        self._js8_timer.timeout.connect(lambda: self._auto_ingest_and_refresh(initial=False))
        if self._map_visible and self._app_active and not self._is_shutting_down:
            self._js8_timer.start()
        # Start display refresh timer (separate from ingest) using selected interval
        # JS8 RX live ingestion timer
        self._start_js8_rx_listener()

    def _prewarm_map_perf_caches(self) -> None:
        if self._is_shutting_down or self._prop_prewarm_done:
            return
        try:
            self._load_prop_db_cache()
        except Exception:
            pass
        try:
            self._load_recent_calls_by_band(self._prop_window_seconds())
        except Exception:
            pass
        self._prop_prewarm_done = True

    def _start_js8_rx_listener(self):
        """
        Attach to JS8 RX hub and ingest live traffic in real time.
        """
        if not self.settings:
            return
        host = (self.settings.get("js8_host", "") or "").strip() or "127.0.0.1"
        try:
            port = int(self.settings.get("js8_port", 2442) or 2442)
        except Exception:
            port = 2442
        if self._js8_rx_hub is None or self._js8_rx_hub.endpoint() != (host, port):
            if self._js8_rx_hub is not None and self._js8_rx_registered:
                try:
                    self._js8_rx_hub.unregister_listener(self._on_js8_rx_messages)
                except Exception:
                    pass
                self._js8_rx_registered = False
            self._js8_rx_hub = JS8RxHub.instance(host, port)
        if not self._js8_rx_registered:
            self._js8_rx_hub.register_listener(self._on_js8_rx_messages)
            self._js8_rx_registered = True
        self._js8_rx_hub.start(host, port)

    def _get_js8_indexer(self) -> Optional[JS8LogLinkIndexer]:
        if self._js8_indexer is None:
            try:
                db_path = get_config_dir() / "config" / "freqinout_nets.db"
                self._js8_indexer = JS8LogLinkIndexer(self.settings, db_path)
            except Exception as e:
                log.debug("StationsMap: indexer init failed: %s", e)
                self._js8_indexer = None
        return self._js8_indexer

    def _js8_live_source_context(self) -> Dict[str, str]:
        if not self.settings:
            return {}
        host = (self.settings.get("js8_host", "") or "").strip() or "127.0.0.1"
        try:
            port = int(self.settings.get("js8_port", 2442) or 2442)
        except Exception:
            port = 2442
        endpoint = f"{host}:{port}".strip().lower()
        cache = getattr(self, "_js8_live_source_context_cache", {}) or {}
        if (
            str(cache.get("endpoint", "") or "") == endpoint
            and float(cache.get("expires", 0.0) or 0.0) > time.time()
            and isinstance(cache.get("context"), dict)
        ):
            return dict(cache.get("context") or {})
        context = resolve_js8_source_context(self.settings, host=host, port=port)
        if context:
            self._js8_live_source_context_cache = {
                "endpoint": endpoint,
                "expires": time.time() + 30.0,
                "context": context,
            }
            return context
        self._js8_live_source_context_cache = {
            "endpoint": endpoint,
            "expires": time.time() + 30.0,
            "context": {},
        }
        return {}

    def _schedule_render(self) -> None:
        self._request_map_refresh(level="medium", reason="schedule", preserve_view=True)

    def _flush_scheduled_render(self) -> None:
        self._flush_requested_map_refresh()

    @staticmethod
    def _refresh_level_rank(level: str) -> int:
        normalized = str(level or "").strip().lower()
        return {"light": 1, "medium": 2, "full": 3}.get(normalized, 2)

    @staticmethod
    def _refresh_level_name(rank: int) -> str:
        mapping = {1: "light", 2: "medium", 3: "full"}
        return mapping.get(int(rank or 0), "medium")

    def _emit_map_event(self, event: str, **meta: object) -> None:
        payload = {
            "event": str(event or "").strip(),
            "state": getattr(self, "_map_runtime_state", "cold"),
            "visible": bool(getattr(self, "_map_visible", False)),
            "initialized": bool(getattr(self, "_map_initialized", False)),
            "loading": bool(getattr(self, "_map_page_loading", False)),
            "markers": int(getattr(self, "_map_marker_count", 0) or 0),
            "links": int(getattr(self, "_map_link_count", 0) or 0),
        }
        payload.update(meta)
        try:
            log.info("MAP|%s", json.dumps(payload, sort_keys=True, default=str))
        except Exception:
            log.info("MAP|%s", payload)
        self._map_last_event_ts = time.time()

    def _cached_map_value(
        self,
        cache_name: str,
        cache_key: object,
        loader,
        *,
        ttl_sec: float,
        force: bool = False,
    ):
        if force or ttl_sec <= 0:
            value = loader()
            try:
                key_sig = json.dumps(cache_key, sort_keys=True, default=str)
                self._map_query_cache[(str(cache_name or ""), key_sig)] = (time.time(), value)
            except Exception:
                pass
            return value
        try:
            key_sig = json.dumps(cache_key, sort_keys=True, default=str)
        except Exception:
            key_sig = str(cache_key)
        cache_key_tuple = (str(cache_name or ""), key_sig)
        now_ts = time.time()
        cached = self._map_query_cache.get(cache_key_tuple)
        if cached is not None:
            cached_ts, cached_value = cached
            if (now_ts - float(cached_ts or 0.0)) <= float(ttl_sec):
                return cached_value
        value = loader()
        self._map_query_cache[cache_key_tuple] = (now_ts, value)
        return value

    @staticmethod
    def _path_stat_fingerprint(path: str | Path | None) -> Tuple[str, int, int]:
        try:
            p = Path(path) if path is not None else None
        except Exception:
            p = None
        if p is None:
            return ("", 0, 0)
        try:
            stat = p.stat()
            return (str(p), int(stat.st_size), int(stat.st_mtime_ns))
        except Exception:
            return (str(p), 0, 0)

    @staticmethod
    def _map_band_filter_signature(band_filter: object) -> Tuple[Tuple[str, str], ...]:
        if isinstance(band_filter, dict):
            try:
                return tuple(sorted((str(k), str(v)) for k, v in band_filter.items()))
            except Exception:
                return tuple()
        return (("value", str(band_filter or "")),)

    def _nets_db_fingerprint(self) -> Tuple[str, int, int]:
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return ("", 0, 0)
        return self._path_stat_fingerprint(db_path)

    def _set_station_points(self, points: List[StationPoint]) -> None:
        self.stations = list(points)
        self._stations_revision += 1

    def _set_map_runtime_state(self, state: str, detail: str = "", *, error: str = "") -> None:
        normalized = str(state or "cold").strip().lower() or "cold"
        self._map_runtime_state = normalized
        self._map_runtime_detail = str(detail or "").strip()
        self._map_last_error = str(error or "").strip()
        self._update_map_support_card()

    def _request_map_refresh(self, *, level: str = "medium", reason: str = "", preserve_view: object = True) -> None:
        if getattr(self, "_is_shutting_down", False):
            return
        requested_rank = self._refresh_level_rank(level)
        if not getattr(self, "_app_active", True):
            self._map_dirty = True
            self._pending_refresh_level = max(getattr(self, "_pending_refresh_level", 0), requested_rank)
            self._pending_refresh_reason = reason or getattr(self, "_pending_refresh_reason", "")
            self._pending_refresh_preserve_view = preserve_view
            self._emit_map_event("render_deferred_inactive", level=self._refresh_level_name(requested_rank), reason=reason)
            return
        if not getattr(self, "_map_visible", False):
            self._map_dirty = True
            self._pending_refresh_level = max(getattr(self, "_pending_refresh_level", 0), requested_rank)
            self._pending_refresh_reason = reason or getattr(self, "_pending_refresh_reason", "")
            self._pending_refresh_preserve_view = preserve_view
            return
        if getattr(self, "_map_page_loading", False):
            self._map_dirty = True
            self._render_requested_during_load = True
            self._render_requested_during_load_level = max(
                getattr(self, "_render_requested_during_load_level", 0),
                requested_rank,
            )
            self._pending_refresh_reason = reason or getattr(self, "_pending_refresh_reason", "")
            self._pending_refresh_preserve_view = preserve_view
            self._emit_map_event("render_queued_while_loading", level=self._refresh_level_name(requested_rank), reason=reason)
            return
        self._pending_refresh_level = max(getattr(self, "_pending_refresh_level", 0), requested_rank)
        if reason:
            self._pending_refresh_reason = reason
        self._pending_refresh_preserve_view = preserve_view
        delay_ms = 90 if requested_rank <= 1 else 180
        if requested_rank >= 3:
            delay_ms = 30
        self._render_pending = True
        timer = getattr(self, "_map_refresh_timer", None)
        if timer is None:
            self._schedule_render()
            return
        timer.start(delay_ms)
        self._emit_map_event("render_requested", level=self._refresh_level_name(requested_rank), reason=reason)

    def _flush_requested_map_refresh(self) -> None:
        if self._is_shutting_down:
            return
        rank = max(int(self._pending_refresh_level or 0), 2 if self._map_dirty else 0)
        reason = self._pending_refresh_reason or "coalesced"
        preserve_view = self._pending_refresh_preserve_view
        self._pending_refresh_level = 0
        self._pending_refresh_reason = ""
        self._pending_refresh_preserve_view = True
        self._render_pending = False
        if not self._map_visible or not self._app_active:
            self._map_dirty = True
            return
        if self._map_page_loading:
            self._map_dirty = True
            self._render_requested_during_load = True
            self._render_requested_during_load_level = max(self._render_requested_during_load_level, rank)
            return
        if rank <= 0:
            return
        now_ts = time.time()
        if rank < 3 and now_ts - self._last_map_render_ts < 0.75:
            self._pending_refresh_level = max(self._pending_refresh_level, rank)
            self._pending_refresh_reason = reason or self._pending_refresh_reason
            self._pending_refresh_preserve_view = preserve_view
            self._render_pending = True
            self._map_refresh_timer.start(220)
            return
        self._last_map_render_ts = now_ts
        self._perform_map_refresh(level=self._refresh_level_name(rank), reason=reason, preserve_view=preserve_view)

    def _perform_map_refresh(self, *, level: str, reason: str, preserve_view: object = True) -> None:
        self._emit_map_event("render_started", level=level, reason=reason)
        if level == "full" or not self._map_initialized:
            self._set_map_runtime_state("loading", "Refreshing the map surface and rebuilding overlays.")
        elif self._map_runtime_state != "degraded":
            self._set_map_runtime_state("loading", "Refreshing map data.")
        try:
            with perf_span("map.render_call", settings=self.settings, meta={"source": reason, "level": level}, min_ms=10.0):
                self._render_map(preserve_view=preserve_view)
        except Exception as exc:
            self._enter_map_degraded(
                "Map refresh did not complete cleanly. You can retry without restarting FIO.",
                reason=reason,
                exc=exc,
            )
            return
        if not self._map_page_loading and self._map_runtime_state != "degraded":
            self._set_map_runtime_state(
                "ready",
                f"Map is ready with {int(self._map_marker_count)} station markers and {int(self._map_link_count)} links.",
            )
        self._emit_map_event("render_completed", level=level, reason=reason)

    def _enter_map_degraded(self, detail: str, *, reason: str = "", exc: Exception | None = None) -> None:
        error_text = str(exc or "").strip()
        self._set_map_runtime_state("degraded", detail, error=error_text)
        self._emit_map_event("degraded", reason=reason, error=error_text or detail)

    def _retry_map_render(self) -> None:
        self._emit_map_event("retry_requested")
        self._request_map_refresh(level="full", reason="retry", preserve_view=True)

    def _reload_map_data(self) -> None:
        self._emit_map_event("reload_data_requested")
        self._ensure_initial_data_loaded()
        self._auto_ingest_and_refresh(initial=False)

    def _map_support_summary(self) -> str:
        top_lines = [
            f"State: {self._map_runtime_state.title()}",
            f"Visible: {'Yes' if self._map_visible else 'No'}",
            f"Markers: {int(self._map_marker_count)}",
            f"Links: {int(self._map_link_count)}",
            f"Page Loading: {'Yes' if self._map_page_loading else 'No'}",
            f"WebEngine Ready: {'Yes' if self._map_initialized else 'No'}",
        ]
        sections = [
            (
                "Current Detail",
                bullet_lines(
                    [
                        self._map_runtime_detail,
                        f"Last error: {self._map_last_error}" if self._map_last_error else "",
                        f"Active band filter: {self.selected_band or 'All'}",
                        f"Recency filter: {self.recency_combo.currentText()}" if hasattr(self, "recency_combo") else "",
                        f"Link mode: {self.link_mode_combo.currentText()}" if hasattr(self, "link_mode_combo") else "",
                        f"Group filter: {self.group_filter_combo.currentText()}" if hasattr(self, "group_filter_combo") else "",
                        f"Region filter: {self.region_filter_combo.currentText()}" if hasattr(self, "region_filter_combo") else "",
                    ]
                ),
            ),
        ]
        return build_support_summary("FreqInOut Multi-Rig Map Diagnostics", top_lines, sections=sections)

    def _copy_map_diagnostics(self) -> None:
        QApplication.clipboard().setText(self._map_support_summary())
        if hasattr(self, "_map_copy_summary_btn") and self._map_copy_summary_btn is not None:
            self._map_copy_summary_btn.setText("Copied")
            QTimer.singleShot(1500, lambda: self._map_copy_summary_btn.setText("Copy Diagnostics"))

    def _update_map_support_card(self) -> None:
        card = getattr(self, "_map_support_card", None)
        if card is None:
            return
        label = getattr(self, "_map_support_label", None)
        if label is not None:
            text = self._map_runtime_detail or "Map is standing by."
            label.setText(f"Map Status: {self._map_runtime_state.title()}. {text}")
            label.setToolTip(self._map_support_summary())
        theme = self._theme_snapshot()
        border = theme.get("border", "#cccccc")
        role = "muted"
        if self._map_runtime_state == "degraded":
            border = theme.get("danger", "#b3261e")
            role = "danger"
        elif self._map_runtime_state in {"loading", "warming"}:
            border = theme.get("warning", "#c99700")
            role = "warning"
        elif self._map_runtime_state == "ready":
            border = theme.get("success", theme.get("accent", "#2a6fd3"))
            role = "success"
        bg = theme.get("surface_alt", theme.get("surface", "#f7f7f7"))
        fg = theme.get("text", "#222222")
        card.setStyleSheet(
            "QFrame {"
            f" background: {bg};"
            f" border: 1px solid {border};"
            " border-radius: 6px;"
            "}"
            " QLabel {"
            f" color: {fg};"
            " border: none;"
            " background: transparent;"
            "}"
        )
        if getattr(self, "_map_retry_btn", None) is not None:
            self._map_retry_btn.setStyleSheet(button_style("warning" if role in {"warning", "danger"} else "secondary", theme))
        if getattr(self, "_map_reload_btn", None) is not None:
            self._map_reload_btn.setStyleSheet(button_style("secondary", theme))
        if getattr(self, "_map_copy_summary_btn", None) is not None:
            self._map_copy_summary_btn.setStyleSheet(button_style("secondary", theme))
            self._map_copy_summary_btn.setVisible(self._map_runtime_state in {"loading", "warming", "degraded"})
        if getattr(self, "_map_support_help_btn", None) is not None:
            self._map_support_help_btn.setStyleSheet(button_style("muted", theme))

    def _start_map_ingest_lifecycle(self) -> None:
        if self._ingest_started:
            return
        self._ingest_started = True
        self._start_js8_ingest_timer()
        # Initial ingest to catch up since last run (looks back to last exit time if available)
        QTimer.singleShot(500, lambda: self._auto_ingest_and_refresh(initial=True))

    def _maybe_start_map_ingest(self) -> bool:
        if self._ingest_started or not self._deferred_initial_ingest_pending:
            return False
        if not self._map_load_ok:
            return False
        self._deferred_initial_ingest_pending = False
        self._start_map_ingest_lifecycle()
        return True

    def set_map_visible(self, is_visible: bool) -> None:
        is_visible = bool(is_visible)
        if self._map_visible == is_visible:
            return
        self._map_visible = is_visible
        if not self._map_visible:
            if self._js8_timer is not None:
                self._js8_timer.stop()
            return
        if not self._app_active:
            self._map_dirty = True
            self._set_map_runtime_state("warming", "Preparing the map view.")
            self._emit_map_event("activation_deferred_inactive")
            return
        if self._map_visible and not self._ingest_started:
            self._deferred_initial_ingest_pending = True
            self._maybe_start_map_ingest()
        elif self._map_visible and self._js8_timer is not None and self._ingest_started and not self._js8_timer.isActive():
            self._js8_timer.start()
        if self._map_visible:
            self._set_map_runtime_state("warming", "Preparing the map view and refreshing station data.")
            self._emit_map_event("activation_started")
            self._map_dirty = True
            QTimer.singleShot(0, self._on_map_visible_deferred)

    def set_app_active(self, active: bool) -> None:
        self._app_active = bool(active)
        if not self._app_active:
            self._map_dirty = True
            if self._js8_timer is not None:
                self._js8_timer.stop()
            self._emit_map_event("ui_paused_inactive")
            return
        if self._map_visible and not self._is_shutting_down:
            if self._js8_timer is not None and self._ingest_started and not self._js8_timer.isActive():
                self._js8_timer.start()
            self._set_map_runtime_state("warming", "Resuming map view.")
            self._emit_map_event("ui_resumed")
            QTimer.singleShot(0, self._on_map_visible_deferred)

    def _on_js8_rx_messages(self, messages: List[dict]) -> None:
        """
        Consume js8net rx messages and upsert live observations into js8_links.
        """
        if self._is_shutting_down or self._js8_polling or not self._map_visible:
            return
        if not self.settings:
            return
        indexer = self._get_js8_indexer()
        if indexer is None:
            return
        start_ts = time.time()
        try:
            my_call = (self.settings.get("operator_callsign", "") or "").upper()
        except Exception:
            my_call = ""

        updated = False
        observations: List[tuple] = []
        self._js8_polling = True
        try:
            source_context = self._js8_live_source_context()
            try:
                if any("F!" in str((msg.get("params", {}) or {}).get("TEXT") or msg.get("value") or "").upper() for msg in messages if isinstance(msg, dict)):
                    MessageIngestor(self.settings).ingest_spotter_from_js8_events(
                        messages,
                        source_radio_id=source_context.get("source_radio_id", ""),
                        js8_instance_id=source_context.get("js8_instance_id", ""),
                    )
            except Exception as exc:
                log.debug("StationsMap: Spotter live ingest failed: %s", exc)
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                try:
                    ts = float(msg.get("time") or time.time())
                except Exception:
                    ts = time.time()
                params = msg.get("params", {})
                mtype = (msg.get("type") or "").strip().upper()
                freq_hz = None
                try:
                    fval = params.get("FREQ")
                    if fval not in (None, ""):
                        freq_hz = float(fval)
                except Exception:
                    freq_hz = None
                origin = ""
                dest = ""
                snr = None
                is_spotter = 0
                if mtype == "RX.SPOT":
                    origin = str(params.get("CALL") or "").upper()
                    dest = my_call
                    snr = params.get("SNR")
                    is_spotter = 1
                elif mtype == "RX.DIRECTED":
                    origin = str(params.get("FROM") or "").upper()
                    dest = str(params.get("TO") or params.get("CALL") or "").upper()
                    snr = params.get("SNR") or params.get("EXTRA")
                    if not dest:
                        dest = my_call
                else:
                    continue
                if not origin or not dest:
                    continue
                try:
                    snr_val = float(snr)
                except Exception:
                    snr_val = None
                observations.append(
                    (
                        ts,
                        origin,
                        dest,
                        snr_val,
                        freq_hz,
                        is_spotter,
                        source_context.get("source_id", ""),
                        source_context.get("app_instance_id", ""),
                        source_context.get("source_radio_id", ""),
                    )
                )
                updated = True
        finally:
            self._js8_polling = False
        if observations:
            try:
                indexer.ingest_live_batch(observations)
            except Exception as e:
                log.debug("StationsMap: live ingest batch failed: %s", e)
        if updated:
            self._last_js8_load_ts = max(self._last_js8_load_ts, time.time())
            self._schedule_render()
        elapsed = time.time() - start_ts
        if elapsed > 0.5:
            log.debug("StationsMap: js8 rx ingest took %.2fs", elapsed)

    def _record_exit_time(self):
        try:
            if self.settings:
                now_ts = time.time()
                self.settings.set("last_exit_utc", now_ts)
                self._last_exit_ts = now_ts
        except Exception:
            pass

    def _ingest_js8_logs(self, since_ts: Optional[float] = None) -> int:
        """
        Run JS8 log ingestion (DIRECTED/ALL) and persist last load timestamp.
        """
        if not self.settings:
            return 0
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            result = ingest_js8_links_for_runtime_sources(self.settings, db_path, since_ts=since_ts)
            self._last_js8_load_ts = result.latest_ts
            try:
                self.settings.set("js8_links_last_load_utc", result.latest_ts)
            except Exception:
                pass
            if result.used_runtime_sources:
                log.info("StationsMap: JS8 traffic ingested from runtime sources (%s rows)", result.inserted)
            else:
                log.info("StationsMap: JS8 traffic ingested from legacy JS8 settings (%s rows)", result.inserted)
            return result.inserted
        except Exception as e:
            log.error("StationsMap: JS8 log ingest failed: %s", e)
            return 0

    def _request_background_ingest(self, *kinds: str) -> bool:
        parent = self.parent()
        controller = getattr(parent, "background_ingest", None)
        if controller is None:
            return False
        try:
            if hasattr(controller, "is_running") and not controller.is_running():
                return False
            self._connect_background_ingest_notifications(controller)
            if hasattr(controller, "request_refresh"):
                controller.request_refresh(*kinds)
                return True
        except Exception as exc:
            log.debug("StationsMap: background ingest request failed: %s", exc)
        return False

    def _connect_background_ingest_notifications(self, controller: object) -> None:
        if self._background_ingest_controller is controller:
            return
        signal = getattr(controller, "job_finished", None)
        if signal is None:
            return
        try:
            signal.connect(self._on_background_ingest_finished)
            self._background_ingest_controller = controller
        except Exception as exc:
            log.debug("StationsMap: background ingest signal connect failed: %s", exc)

    def _on_background_ingest_finished(self, job_name: str) -> None:
        if self._is_shutting_down:
            return
        if str(job_name or "").strip().lower() in {"js8_links", "varac"}:
            self._schedule_render()

    def _auto_ingest_and_refresh(self, initial: bool = False):
        """
        Background ingest and refresh map. Used on timer and manual refresh.
        """
        if self._is_shutting_down:
            return
        if not self._app_active:
            self._map_dirty = True
            self._emit_map_event("ingest_deferred_inactive")
            return
        since = None
        if initial:
            since = max(self._last_js8_load_ts, self._last_exit_ts)
        elif self._js8_rx_hub and self._js8_rx_hub.is_active():
            since = self._last_js8_load_ts
        if not self._request_background_ingest("js8_links", "varac"):
            self._ingest_js8_logs(since_ts=since)
            try:
                ingest_varac_for_runtime_sources(self.settings)
            except Exception:
                pass
        self._schedule_render()

    def shutdown(self) -> None:
        self._is_shutting_down = True
        try:
            if self._js8_timer:
                self._js8_timer.stop()
        except Exception:
            pass
        try:
            if self._js8_rx_hub and self._js8_rx_registered:
                self._js8_rx_hub.unregister_listener(self._on_js8_rx_messages)
                self._js8_rx_registered = False
        except Exception:
            pass
        try:
            if self.web is not None:
                try:
                    self.web.stop()
                except Exception:
                    pass
                try:
                    self.web.loadFinished.disconnect(self._on_map_load_finished)
                except Exception:
                    pass
                try:
                    self.web.hide()
                except Exception:
                    pass
                try:
                    self.web.setParent(None)
                except Exception:
                    pass
                try:
                    self.web.setUrl(QUrl("about:blank"))
                except Exception:
                    pass
                try:
                    page = self.web.page()
                    if page is not None:
                        try:
                            if QWebEnginePage is not None:
                                self.web.setPage(QWebEnginePage(self.web))
                        except Exception:
                            pass
                        try:
                            page.setParent(None)
                        except Exception:
                            pass
                        page.deleteLater()
                except Exception:
                    pass
                try:
                    self.web.deleteLater()
                except Exception:
                    pass
                self.web = None
        except Exception:
            pass
        try:
            QCoreApplication.processEvents()
        except Exception:
            pass
        try:
            if self._managed_map_file.exists():
                self._managed_map_file.unlink()
        except Exception:
            pass


    @staticmethod
    def _parse_link_selection(data) -> tuple[str, str]:
        """
        Normalize link-mode selection from the combo. PySide may return list/tuple.
        """
        if isinstance(data, (list, tuple)) and len(data) >= 2:
            return str(data[0]), str(data[1])
        if isinstance(data, str):
            txt = data.strip()
            # Accept stringified tuple/list e.g. "('all', '')" or "['all', '']"
            if txt.startswith(("(", "[")) and "," in txt:
                inner = txt.strip("()[]")
                parts = [p.strip().strip("'\"") for p in inner.split(",")]
                if len(parts) >= 2:
                    return parts[0], parts[1]
        return "off", ""

    @staticmethod
    def _hex_to_rgba(value: str, alpha: float) -> str:
        try:
            raw = (value or "").strip().lstrip("#")
            if len(raw) != 6:
                raise ValueError("invalid hex color")
            r = int(raw[0:2], 16)
            g = int(raw[2:4], 16)
            b = int(raw[4:6], 16)
        except Exception:
            r, g, b = 0, 0, 0
        a = max(0.0, min(1.0, float(alpha)))
        return f"rgba({r}, {g}, {b}, {a:.2f})"

    def _theme_snapshot(self, force_reload: bool = False) -> Dict[str, str]:
        if force_reload and self.settings is not None:
            try:
                if hasattr(self.settings, "reload"):
                    self.settings.reload()
            except Exception:
                pass
        return resolve_theme(self.settings)

    def apply_theme(self) -> None:
        theme = self._theme_snapshot(force_reload=True)
        if self._controls_button is not None:
            self._controls_button.setStyleSheet(button_style("muted", theme))
        if getattr(self, "_help_button", None) is not None:
            self._help_button.setStyleSheet(button_style("secondary", theme))
        if getattr(self, "_paths_help_button", None) is not None:
            self._paths_help_button.setStyleSheet(button_style("secondary", theme))
        if self._refresh_links_button is not None:
            self._refresh_links_button.setStyleSheet(button_style("primary", theme))
        if self._map_add_rf_pin_button is not None:
            self._map_add_rf_pin_button.setStyleSheet(button_style("secondary", theme))
        if self._map_manage_rf_pins_button is not None:
            self._map_manage_rf_pins_button.setStyleSheet(button_style("secondary", theme))
        self._update_now_reachable_button_visual(bool(self._now_reachable_enabled), theme=theme)
        self._update_sitrep_status_button_visual(self._current_map_mode_key() == "sitrep", theme=theme)
        self._update_map_mode_buttons(theme=theme)
        self._update_map_view_status_label(theme=theme)
        self._sync_map_control_button_widths()
        self._update_splitter_indicator_state(theme=theme)
        try:
            target_ctx = self._prop_target_context()
            target_label = str(target_ctx.get("label") or "National")
            if self.prop_overlay_enabled:
                region_scores = self._compute_region_scores("")
                state_scores = self._compute_state_scores()
                best_band, best_score = self._best_band_for_target(target_ctx, region_scores, state_scores)
                self._update_prop_badge(target_label, best_band, best_score, theme=theme)
            else:
                self._update_prop_badge(target_label, "", 0.0, theme=theme)
        except Exception:
            # Keep theme updates resilient if propagation data is unavailable.
            self._update_prop_badge("National", "", 0.0, theme=theme)
        self._update_map_support_card()

    def _open_context_help(self, context_key: str) -> None:
        host = resolve_help_host(self)
        if host is not None and hasattr(host, "open_context_help"):
            try:
                host.open_context_help(context_key)
            except Exception:
                pass

    def _sync_map_control_button_widths(self) -> None:
        for button in (
            self._refresh_links_button,
            self._now_reachable_button,
            self._sitrep_status_button,
            getattr(self, "_paths_help_button", None),
        ):
            if button is None:
                continue
            try:
                button.setMinimumWidth(max(button.minimumWidth(), button.sizeHint().width() + 6))
            except Exception:
                continue

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 4, 8, 8)
        layout.setSpacing(6)

        top_row = QHBoxLayout()
        top_row.setContentsMargins(0, 0, 0, 0)
        top_row.setSpacing(6)
        self._controls_button = QPushButton("Show Filters & Layers")
        self._controls_button.setVisible(False)
        self._controls_button.clicked.connect(self._toggle_controls_drawer)
        top_row.addWidget(self._controls_button, alignment=Qt.AlignLeft)
        self._help_button = QPushButton("Help")
        self._help_button.setToolTip("Open focused help for Map controls and overlays.")
        self._help_button.clicked.connect(lambda: self._open_context_help("tab.map"))
        top_row.addWidget(self._help_button, alignment=Qt.AlignLeft)
        top_row.addStretch(1)
        layout.addLayout(top_row)

        map_context_text = (
            "Map uses the current radio and Frequency Plan context when reviewing station and traffic overlays."
        )
        self.plan_context_label = PlanContextLabel(
            "map",
            service=self.plan_context_service,
            fallback_text=map_context_text,
            create_service=self.plan_context_service is not None,
        )
        self.plan_context_label.setToolTip(
            "Use this context to confirm which radio and assigned Frequency Plan Map overlays are being reviewed against."
        )
        self.plan_context_label.setVisible(False)
        layout.addWidget(self.plan_context_label)
        if self.plan_context_service is not None:
            self.plan_context_label.refresh_context(refresh=True)

        self._map_support_card = QFrame(self)
        support_layout = QHBoxLayout(self._map_support_card)
        support_layout.setContentsMargins(10, 8, 10, 8)
        support_layout.setSpacing(8)
        self._map_support_label = QLabel("Map Status: Cold. Map has not been opened yet.")
        self._map_support_label.setWordWrap(True)
        support_layout.addWidget(self._map_support_label, 1)
        self._map_retry_btn = QPushButton("Retry")
        self._map_retry_btn.clicked.connect(self._retry_map_render)
        support_layout.addWidget(self._map_retry_btn)
        self._map_reload_btn = QPushButton("Reload Data")
        self._map_reload_btn.clicked.connect(self._reload_map_data)
        support_layout.addWidget(self._map_reload_btn)
        self._map_copy_summary_btn = QPushButton("Copy Diagnostics")
        self._map_copy_summary_btn.clicked.connect(self._copy_map_diagnostics)
        support_layout.addWidget(self._map_copy_summary_btn)
        self._map_support_help_btn = QPushButton("Help")
        self._map_support_help_btn.clicked.connect(lambda: self._open_context_help("tab.map"))
        support_layout.addWidget(self._map_support_help_btn)
        layout.addWidget(self._map_support_card)

        splitter = QSplitter(Qt.Horizontal, self)
        splitter.setHandleWidth(14)
        self._main_splitter = splitter
        layout.addWidget(splitter, stretch=1)

        controls_scroll = QScrollArea(self)
        controls_scroll.setWidgetResizable(True)
        controls_scroll.setFrameShape(QFrame.NoFrame)
        controls_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        controls_scroll.setMinimumWidth(220)
        controls_scroll.setMaximumWidth(320)

        self._controls_panel = QWidget()
        controls_layout = QVBoxLayout(self._controls_panel)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(8)
        self._controls_top_spacer = QWidget(self._controls_panel)
        self._controls_top_spacer.setFixedHeight(0)
        controls_layout.addWidget(self._controls_top_spacer)
        controls_scroll.setWidget(self._controls_panel)
        splitter.addWidget(controls_scroll)

        self.link_mode_combo = QComboBox()
        self.link_mode_combo.addItem("Off", ("off", ""))
        self.link_mode_combo.addItem("My Station", ("my_station", ""))
        self.link_mode_combo.addItem("All", ("all", ""))
        self.link_mode_combo.setCurrentText("My Station")

        self.group_filter_combo = QComboBox()
        self.group_filter_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        self.group_filter_combo.setMinimumContentsLength(14)
        self.group_filter_combo.view().setMinimumWidth(220)

        self.region_filter_combo = QComboBox()
        self.region_filter_combo.setSizeAdjustPolicy(QComboBox.AdjustToContents)
        self.region_filter_combo.setMinimumContentsLength(12)
        self.region_filter_combo.view().setMinimumWidth(200)

        self.band_combo = QComboBox()
        self.recency_combo = QComboBox()
        self.recency_combo.addItems(["Any", "15m", "30m", "1h", "3h", "6h", "12h", "24h", "7d"])
        self.recency_combo.setCurrentText("3h")
        self.recency_seconds = 3 * 60 * 60

        self.relay_target_combo = QComboBox()
        self.relay_target_combo.setEditable(True)
        self.relay_target_combo.setInsertPolicy(QComboBox.NoInsert)
        self.relay_target_combo.setDuplicatesEnabled(False)
        self.relay_target_combo.setMinimumWidth(180)
        try:
            self.relay_target_combo.view().setMinimumWidth(360)
        except Exception:
            pass
        try:
            relay_edit = self.relay_target_combo.lineEdit()
            if relay_edit is not None:
                relay_edit.setPlaceholderText("Search Callsign or Name...")
        except Exception:
            pass
        relay_completer = self.relay_target_combo.completer()
        if relay_completer:
            relay_completer.setFilterMode(Qt.MatchContains)
            relay_completer.setCaseSensitivity(Qt.CaseInsensitive)

        layers_layout = self._add_collapsible_group(controls_layout, "Map Detail", expanded=True)
        self.show_calls_chk = QCheckBox("Callsigns")
        self.show_regions_chk = QCheckBox("Regions")
        self.show_states_chk = QCheckBox("States")
        self.show_cities_chk = QCheckBox("Cities")
        self.show_grid_labels_chk = QCheckBox("Grids")
        layer_grid = QGridLayout()
        layer_grid.setContentsMargins(0, 0, 0, 0)
        layer_grid.setHorizontalSpacing(8)
        layer_grid.setVerticalSpacing(6)
        layer_grid.addWidget(self.show_calls_chk, 0, 0)
        layer_grid.addWidget(self.show_regions_chk, 0, 1)
        layer_grid.addWidget(self.show_states_chk, 1, 0)
        layer_grid.addWidget(self.show_cities_chk, 1, 1)
        layer_grid.addWidget(self.show_grid_labels_chk, 2, 0)
        layers_layout.addLayout(layer_grid)

        self.city_pop_combo = QComboBox()
        self._city_pop_options = [
            ("1M+", 1_000_000),
            ("750k+", 750_000),
            ("500k+", 500_000),
            ("250k+", 250_000),
            ("100k+", 100_000),
            ("75k+", 75_000),
            ("50k+", 50_000),
            ("25k+", 25_000),
            ("10k+", 10_000),
            ("5k+", 5_000),
            ("<5k", 0),
        ]
        for label, val in self._city_pop_options:
            self.city_pop_combo.addItem(label, val)
        self.city_pop_combo.setCurrentIndex(4)
        pop_grid = QGridLayout()
        pop_grid.setContentsMargins(0, 0, 0, 0)
        pop_grid.setHorizontalSpacing(8)
        pop_grid.addWidget(QLabel("Population"), 0, 0)
        pop_grid.addWidget(self.city_pop_combo, 0, 1)
        layers_layout.addLayout(pop_grid)

        prop_layout = self._add_collapsible_group(controls_layout, "Propagation Forecast", expanded=True)
        self.prop_overlay_chk = QCheckBox("Enable Propagation Overlay")
        prop_layout.addWidget(self.prop_overlay_chk)

        self.prop_mode_combo = QComboBox()
        self.prop_mode_combo.addItem("Actual", "actual")
        self.prop_mode_combo.addItem("Blended", "blended")
        self.prop_mode_combo.addItem("Modeled", "model")

        self.prop_window_combo = QComboBox()
        self.prop_window_combo.addItem("1h", 1)
        self.prop_window_combo.addItem("3h", 3)
        self.prop_window_combo.addItem("6h", 6)
        self.prop_window_combo.addItem("12h", 12)
        self.prop_window_combo.addItem("24h", 24)
        self.prop_window_combo.addItem("7 Days", 168)

        self.prop_target_type_combo = QComboBox()
        self.prop_target_type_combo.addItem("Region", "REGION")
        self.prop_target_type_combo.addItem("State", "STATE")
        self.prop_target_type_combo.addItem("Operator", "OPERATOR")

        self.prop_target_value_combo = QComboBox()
        self.prop_target_value_combo.setEditable(True)
        self.prop_target_value_combo.setInsertPolicy(QComboBox.NoInsert)
        self.prop_target_value_combo.setDuplicatesEnabled(False)
        try:
            self.prop_target_value_combo.view().setMinimumWidth(260)
        except Exception:
            pass

        prop_grid = QGridLayout()
        prop_grid.setContentsMargins(0, 0, 0, 0)
        prop_grid.setHorizontalSpacing(8)
        prop_grid.setVerticalSpacing(6)
        prop_grid.addWidget(QLabel("Mode"), 0, 0)
        prop_grid.addWidget(self.prop_mode_combo, 0, 1)
        prop_grid.addWidget(QLabel("Window"), 1, 0)
        prop_grid.addWidget(self.prop_window_combo, 1, 1)
        prop_grid.addWidget(QLabel("Target"), 2, 0)
        prop_grid.addWidget(self.prop_target_type_combo, 2, 1)
        prop_grid.addWidget(QLabel("Value"), 3, 0)
        prop_grid.addWidget(self.prop_target_value_combo, 3, 1)
        prop_layout.addLayout(prop_grid)

        self.prop_badge = QLabel("Best Band: --")
        self.prop_badge.setWordWrap(True)
        theme = resolve_theme(self.settings)
        self.prop_badge.setStyleSheet(
            f"font-weight: bold; color: {theme.get('info', theme.get('accent', '#1E88E5'))};"
        )
        prop_layout.addWidget(self.prop_badge)
        controls_layout.addStretch()

        map_container = QWidget(self)
        map_layout = QVBoxLayout(map_container)
        map_layout.setContentsMargins(0, 0, 0, 0)
        map_layout.setSpacing(4)
        splitter.addWidget(map_container)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([260, 1084])

        # Keep filters highly accessible at the top of the map content area.
        self.link_mode_combo.setMinimumWidth(120)
        self.group_filter_combo.setMinimumWidth(170)
        self.region_filter_combo.setMinimumWidth(150)
        self.band_combo.setMinimumWidth(120)
        self.recency_combo.setMinimumWidth(90)
        self.relay_target_combo.setMinimumWidth(180)
        self._refresh_links_button = QPushButton("Refresh Links")
        self._refresh_links_button.clicked.connect(lambda: self._auto_ingest_and_refresh(initial=False))
        self._map_all_stations_button = QPushButton("All Stations")
        self._map_all_stations_button.setToolTip("Return to the normal station map view.")
        self._map_all_stations_button.clicked.connect(self.focus_all_stations)
        self._map_hf_reports_button = QPushButton("HF Reports")
        self._map_hf_reports_button.setToolTip("Show HF-derived Spotter, SitRep, and field-report observations.")
        self._map_hf_reports_button.clicked.connect(self.focus_hf_reports)
        self._map_local_reports_button = QPushButton("Local Reports")
        self._map_local_reports_button.setToolTip("Show confirmed local operator and NCS field reports.")
        self._map_local_reports_button.clicked.connect(self.focus_local_reports)
        self._map_reports_button = QPushButton("Reports")
        self._map_reports_button.setToolTip("Show HF and confirmed local reports together.")
        self._map_reports_button.clicked.connect(self.focus_reports)
        self._map_rf_pins_button = QPushButton("RF Pins")
        self._map_rf_pins_button.setToolTip("Show operator-curated RF pins on the map.")
        self._map_rf_pins_button.clicked.connect(self.focus_rf_pins)
        self._now_reachable_button = QPushButton("Peer Sched Now")
        self._now_reachable_button.setCheckable(True)
        self._update_now_reachable_button_visual(False)
        self._sitrep_status_button = QPushButton("SitRep Status")
        self._sitrep_status_button.setCheckable(True)
        self._update_sitrep_status_button_visual(False)
        self.map_stations_chk = QCheckBox("Stations")
        self.map_links_chk = QCheckBox("Links")
        self.map_weather_chk = QCheckBox("Weather")
        self.map_alerts_chk = QCheckBox("Alerts/Intel")
        self.map_infrastructure_chk = QCheckBox("Infrastructure/Utilities")
        self.map_stations_chk.setToolTip("Show or hide station markers on the map.")
        self.map_links_chk.setToolTip("Show or hide path lines on the map.")
        self.map_weather_chk.setToolTip("Show or hide mapped Weather / Storm reports.")
        self.map_alerts_chk.setToolTip("Show or hide mapped awareness and warning reports.")
        self.map_infrastructure_chk.setToolTip("Show or hide mapped infrastructure and utility status reports.")
        self._paths_help_button = QPushButton("Paths Help")
        self._paths_help_button.setToolTip("Open focused help for Paths, Paths To, and Peer Sched Now.")
        self._paths_help_button.clicked.connect(lambda: self._open_context_help("map.paths"))
        self._map_add_rf_pin_button = QPushButton("Add RF Pin")
        self._map_add_rf_pin_button.setToolTip(
            "Add an operator-curated RF observation to the map using a grid, topic, and short note."
        )
        self._map_add_rf_pin_button.clicked.connect(self._on_add_rf_pin_clicked)
        self._map_manage_rf_pins_button = QPushButton("Manage Pins")
        self._map_manage_rf_pins_button.setToolTip("Review or delete saved RF pins.")
        self._map_manage_rf_pins_button.clicked.connect(self._on_manage_rf_pins_clicked)
        self._map_topic_filter_combo = QComboBox()
        self._map_topic_filter_combo.addItem("All Topics")
        self._map_topic_filter_combo.addItems(list(RF_PIN_TOPICS))
        self._map_topic_filter_combo.setToolTip(
            "Filter mapped reports and RF pins by message-intelligence topic."
        )
        self._map_topic_filter_combo.setMinimumWidth(150)
        for button in (
            self._refresh_links_button,
            self._map_all_stations_button,
            self._map_hf_reports_button,
            self._map_local_reports_button,
            self._map_reports_button,
            self._map_rf_pins_button,
            self._now_reachable_button,
            self._sitrep_status_button,
            self._paths_help_button,
            self._map_add_rf_pin_button,
            self._map_manage_rf_pins_button,
        ):
            try:
                button.setMinimumWidth(button.sizeHint().width() + 6)
            except Exception:
                pass
        self._now_reachable_label = QLabel("")
        self._now_reachable_label.setWordWrap(True)
        self._now_reachable_label.setVisible(False)
        self._map_view_status_label = QLabel("Map View: All Stations")
        self._map_view_status_label.setWordWrap(True)
        filter_bar = QFrame(map_container)
        self._map_filter_bar = filter_bar
        mode_actions_row = QWidget(filter_bar)
        mode_actions_layout = QHBoxLayout(mode_actions_row)
        mode_actions_layout.setContentsMargins(0, 0, 0, 0)
        mode_actions_layout.setSpacing(8)
        for button in (
            self._map_all_stations_button,
            self._map_hf_reports_button,
            self._map_local_reports_button,
            self._map_reports_button,
            self._map_rf_pins_button,
            self._sitrep_status_button,
            self._now_reachable_button,
        ):
            mode_actions_layout.addWidget(button, 0)
        mode_actions_layout.addStretch(1)
        path_actions_row = QWidget(filter_bar)
        path_actions_layout = QHBoxLayout(path_actions_row)
        path_actions_layout.setContentsMargins(0, 0, 0, 0)
        path_actions_layout.setSpacing(8)
        path_actions_layout.addWidget(self.relay_target_combo, 1)
        path_actions_layout.addWidget(self._refresh_links_button, 0)
        path_actions_layout.addWidget(self._paths_help_button, 0)
        filter_grid = QGridLayout(filter_bar)
        filter_grid.setContentsMargins(0, 0, 0, 0)
        filter_grid.setHorizontalSpacing(10)
        filter_grid.setVerticalSpacing(8)
        filter_grid.addWidget(QLabel("View Mode"), 0, 0, alignment=Qt.AlignTop)
        filter_grid.addWidget(mode_actions_row, 0, 1, 1, 5)
        filter_grid.addWidget(QLabel("Group"), 1, 0)
        filter_grid.addWidget(self.group_filter_combo, 1, 1)
        filter_grid.addWidget(QLabel("Region"), 1, 2)
        filter_grid.addWidget(self.region_filter_combo, 1, 3)
        filter_grid.addWidget(QLabel("Band"), 1, 4)
        filter_grid.addWidget(self.band_combo, 1, 5)
        filter_grid.addWidget(QLabel("Since"), 2, 0)
        filter_grid.addWidget(self.recency_combo, 2, 1)
        filter_grid.addWidget(QLabel("Paths"), 2, 2)
        filter_grid.addWidget(self.link_mode_combo, 2, 3)
        filter_grid.addWidget(QLabel("Topic"), 2, 4)
        filter_grid.addWidget(self._map_topic_filter_combo, 2, 5)
        filter_grid.addWidget(QLabel("Paths to"), 3, 0, alignment=Qt.AlignTop)
        filter_grid.addWidget(path_actions_row, 3, 1, 1, 5)
        layer_toggle_row = QWidget(filter_bar)
        layer_toggle_layout = QHBoxLayout(layer_toggle_row)
        layer_toggle_layout.setContentsMargins(0, 0, 0, 0)
        layer_toggle_layout.setSpacing(14)
        layer_toggle_layout.addWidget(self.map_stations_chk, 0)
        layer_toggle_layout.addWidget(self.map_links_chk, 0)
        layer_toggle_layout.addWidget(self.map_weather_chk, 0)
        layer_toggle_layout.addWidget(self.map_alerts_chk, 0)
        layer_toggle_layout.addWidget(self.map_infrastructure_chk, 0)
        layer_toggle_layout.addStretch(1)
        pins_row = QWidget(filter_bar)
        pins_layout = QHBoxLayout(pins_row)
        pins_layout.setContentsMargins(0, 0, 0, 0)
        pins_layout.setSpacing(8)
        pins_layout.addWidget(self._map_add_rf_pin_button, 0)
        pins_layout.addWidget(self._map_manage_rf_pins_button, 0)
        pins_layout.addStretch(1)
        filter_grid.addWidget(QLabel("Intelligence"), 4, 0)
        filter_grid.addWidget(layer_toggle_row, 4, 1, 1, 5)
        filter_grid.addWidget(QLabel("RF Pins"), 5, 0)
        filter_grid.addWidget(pins_row, 5, 1, 1, 5)
        filter_grid.addWidget(self._map_view_status_label, 6, 0, 1, 6, alignment=Qt.AlignLeft)
        filter_grid.addWidget(self._now_reachable_label, 7, 0, 1, 6, alignment=Qt.AlignLeft)
        filter_grid.setColumnStretch(6, 1)
        map_layout.addWidget(filter_bar)

        if _ensure_webengine_imported():
            self._map_stack = QStackedWidget(map_container)
            loading_widget = QWidget(self._map_stack)
            loading_layout = QVBoxLayout(loading_widget)
            loading_layout.setContentsMargins(0, 0, 0, 0)
            loading_layout.addStretch()
            self._map_loading_label = QLabel("Loading map...")
            self._map_loading_label.setAlignment(Qt.AlignCenter)
            loading_layout.addWidget(self._map_loading_label)
            loading_layout.addStretch()
            self._map_stack.addWidget(loading_widget)
            # Defer WebEngine view construction until first Map activation so
            # Windows startup does not pay the visible helper-window cost.
            self.web = None
            self._map_stack.setCurrentIndex(0)
            map_layout.addWidget(self._map_stack)
        else:
            self.web = None
            self._map_stack = None
            self._map_loading_label = None
            map_layout.addWidget(QLabel("Qt WebEngine is not available. Map preview disabled."))

        self.show_calls_chk.stateChanged.connect(self._on_show_calls_changed)
        self.show_regions_chk.stateChanged.connect(self._on_show_regions_changed)
        self.show_states_chk.stateChanged.connect(self._on_show_states_changed)
        self.show_cities_chk.stateChanged.connect(self._on_show_cities_changed)
        self.show_grid_labels_chk.stateChanged.connect(self._on_show_grid_labels_changed)
        self.map_stations_chk.stateChanged.connect(self._on_map_stations_changed)
        self.map_links_chk.stateChanged.connect(self._on_map_links_changed)
        self.map_weather_chk.stateChanged.connect(self._on_map_weather_changed)
        self.map_alerts_chk.stateChanged.connect(self._on_map_alerts_changed)
        self.map_infrastructure_chk.stateChanged.connect(self._on_map_infrastructure_changed)
        self.city_pop_combo.currentIndexChanged.connect(self._on_city_pop_changed)
        self.link_mode_combo.currentIndexChanged.connect(self._on_link_mode_changed)
        self._map_topic_filter_combo.currentIndexChanged.connect(self._on_map_topic_filter_changed)
        self.group_filter_combo.currentIndexChanged.connect(self._on_group_filter_changed)
        self.region_filter_combo.currentIndexChanged.connect(self._on_region_filter_changed)
        self.band_combo.currentIndexChanged.connect(self._on_band_changed)
        self.recency_combo.currentIndexChanged.connect(self._on_recency_changed)
        self.relay_target_combo.currentIndexChanged.connect(
            lambda _idx: self._on_relay_target_changed(self.relay_target_combo.currentText())
        )
        try:
            line_edit = self.relay_target_combo.lineEdit()
            if line_edit is not None:
                line_edit.editingFinished.connect(
                    lambda: self._on_relay_target_changed(self.relay_target_combo.currentText())
                )
        except Exception:
            pass
        if self._now_reachable_button is not None:
            self._now_reachable_button.toggled.connect(self._on_now_reachable_toggled)
        if self._sitrep_status_button is not None:
            self._sitrep_status_button.toggled.connect(self._on_sitrep_status_toggled)
        self.prop_overlay_chk.stateChanged.connect(self._on_prop_overlay_changed)
        self.prop_mode_combo.currentIndexChanged.connect(self._on_prop_mode_changed)
        self.prop_window_combo.currentIndexChanged.connect(self._on_prop_window_changed)
        self.prop_target_type_combo.currentIndexChanged.connect(self._on_prop_target_type_changed)
        self.prop_target_value_combo.currentTextChanged.connect(self._on_prop_target_value_changed)

        self._sync_city_pop_enabled()
        try:
            self._on_link_mode_changed(self.link_mode_combo.currentIndex())
        except Exception:
            pass
        self._install_splitter_indicator()
        self._sync_controls_top_alignment()
        QTimer.singleShot(0, self._update_drawer_mode)

    def _add_collapsible_group(self, parent_layout: QVBoxLayout, title: str, expanded: bool) -> QVBoxLayout:
        section = QFrame(self)
        section.setFrameShape(QFrame.StyledPanel)
        section_layout = QVBoxLayout(section)
        section_layout.setContentsMargins(6, 6, 6, 6)
        section_layout.setSpacing(6)
        header = QToolButton(section)
        header.setText(title)
        header.setCheckable(True)
        header.setChecked(bool(expanded))
        header.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        header.setArrowType(Qt.DownArrow if expanded else Qt.RightArrow)
        header.setStyleSheet("font-weight: 600;")
        body = QWidget(section)
        body_layout = QVBoxLayout(body)
        body_layout.setContentsMargins(0, 0, 0, 0)
        body_layout.setSpacing(6)

        def _toggle(opened: bool) -> None:
            body.setVisible(bool(opened))
            header.setArrowType(Qt.DownArrow if opened else Qt.RightArrow)

        header.toggled.connect(_toggle)
        _toggle(bool(expanded))
        section_layout.addWidget(header)
        section_layout.addWidget(body)
        parent_layout.addWidget(section)
        return body_layout

    def _toggle_controls_drawer(self) -> None:
        self._set_controls_drawer_open(not self._controls_drawer_open)

    def _install_splitter_indicator(self) -> None:
        splitter = self._main_splitter
        if splitter is None:
            return
        try:
            handle = splitter.handle(1)
        except Exception:
            handle = None
        if handle is None:
            return
        btn = QToolButton(handle)
        btn.setCursor(Qt.PointingHandCursor)
        btn.setToolTip("Collapse map controls")
        btn.setText("")
        btn.setAutoRaise(True)
        btn.setIconSize(btn.sizeHint())
        btn.clicked.connect(self._toggle_controls_drawer)
        btn.raise_()
        self._controls_handle_button = btn
        self._update_splitter_indicator_state()
        self._position_splitter_indicator()

    def _position_splitter_indicator(self) -> None:
        btn = self._controls_handle_button
        splitter = self._main_splitter
        if btn is None or splitter is None:
            return
        try:
            handle = splitter.handle(1)
        except Exception:
            return
        if handle is None:
            return
        w = max(10, handle.width() - 2)
        h = handle.height()
        bw = min(w, 12)
        bh = min(max(26, h // 8), 42)
        x = max(0, (handle.width() - bw) // 2)
        y = max(4, (h - bh) // 2)
        btn.setGeometry(x, y, bw, bh)
        btn.setIconSize(QSize(max(8, bw - 3), max(10, bh - 12)))

    def _update_splitter_indicator_state(self, theme: Optional[Dict[str, str]] = None) -> None:
        btn = self._controls_handle_button
        splitter = self._main_splitter
        if btn is None or splitter is None:
            return
        if theme is None:
            theme = self._theme_snapshot()
        style = self.style() if hasattr(self, "style") else None
        if style is not None:
            icon = (
                style.standardIcon(QStyle.SP_ArrowLeft)
                if self._controls_drawer_open
                else style.standardIcon(QStyle.SP_ArrowRight)
            )
            btn.setIcon(icon)
        btn.setToolTip("Collapse map controls" if self._controls_drawer_open else "Expand map controls")
        btn.setVisible(True)
        try:
            handle = splitter.handle(1)
            if handle is not None:
                handle.setCursor(Qt.SplitHCursor)
                handle.setToolTip("Drag to resize, or click chevron to show/hide map controls")
                border_color = self._hex_to_rgba(theme.get("border", "#2A313A"), 0.80)
                handle_bg = self._hex_to_rgba(theme.get("surface_alt", "#202632"), 0.55)
                button_bg = self._hex_to_rgba(theme.get("surface", "#171B21"), 0.90)
                button_hover = self._hex_to_rgba(theme.get("surface_alt", "#202632"), 0.95)
                button_border = self._hex_to_rgba(theme.get("focus", theme.get("accent", "#4C9BD3")), 0.75)
                handle.setStyleSheet(
                    "QSplitterHandle {"
                    f" background: {handle_bg};"
                    f" border-left: 1px solid {border_color};"
                    f" border-right: 1px solid {border_color};"
                    " }"
                )
                btn.setStyleSheet(
                    "QToolButton {"
                    f" background: {button_bg};"
                    f" border: 1px solid {button_border};"
                    " border-radius: 4px; padding: 1px; }"
                    f"QToolButton:hover {{ background: {button_hover}; }}"
                )
        except Exception:
            pass

    def _sync_controls_top_alignment(self) -> None:
        if self._controls_top_spacer is None or self._map_filter_bar is None:
            return
        h = self._map_filter_bar.height()
        if h <= 0:
            try:
                h = self._map_filter_bar.sizeHint().height()
            except Exception:
                h = 0
        self._controls_top_spacer.setFixedHeight(max(0, int(h)))

    def _set_controls_drawer_open(self, open_drawer: bool) -> None:
        if self._main_splitter is None:
            return
        self._controls_drawer_open = bool(open_drawer)
        total = max(1, self.width())
        panel_width = min(300, max(220, int(total * 0.27)))
        if self._controls_drawer_open:
            self._main_splitter.setSizes([panel_width, max(1, total - panel_width)])
        else:
            self._main_splitter.setSizes([0, total])
        if self._controls_button is not None:
            self._controls_button.setText("Hide Filters & Layers" if self._controls_drawer_open else "Show Filters & Layers")
            self._controls_button.setVisible(self._drawer_mode)
        self._update_splitter_indicator_state()
        self._position_splitter_indicator()

    def _update_drawer_mode(self) -> None:
        narrow = self.width() < self._controls_drawer_threshold
        if narrow != self._drawer_mode:
            self._drawer_mode = narrow
            if not narrow:
                # On transition back to wide layouts, restore visible controls panel.
                self._controls_drawer_open = True
            if self._controls_button is not None:
                self._controls_button.setVisible(narrow)
        self._sync_controls_top_alignment()
        self._set_controls_drawer_open(self._controls_drawer_open)
        self._update_splitter_indicator_state()
        self._position_splitter_indicator()

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._update_drawer_mode()
        self._position_splitter_indicator()
        self._sync_controls_top_alignment()

    def _sync_city_pop_enabled(self) -> None:
        enabled = bool(self.show_cities or self.show_states)
        self.city_pop_combo.setEnabled(enabled)

    def _query_cache_get(self, key: tuple, ttl_sec: Optional[float] = None):
        ttl = self._query_cache_ttl_sec if ttl_sec is None else max(0.0, float(ttl_sec))
        item = self._query_cache.get(key)
        if not item:
            return None
        ts, value = item
        if (time.time() - float(ts)) > ttl:
            self._query_cache.pop(key, None)
            return None
        return value

    def _query_cache_set(self, key: tuple, value: Any) -> None:
        self._query_cache[key] = (time.time(), value)
        # Keep cache bounded.
        if len(self._query_cache) > 200:
            try:
                oldest_key = min(self._query_cache.items(), key=lambda kv: kv[1][0])[0]
                self._query_cache.pop(oldest_key, None)
            except Exception:
                pass

    def _clear_report_query_caches(self) -> None:
        """Clear map/report caches after user-visible report inputs change."""
        try:
            self._map_query_cache.clear()
        except Exception:
            pass
        try:
            self._query_cache.clear()
        except Exception:
            pass

    def _load_operator_activity_summary(self) -> Dict[str, Dict[str, object]]:
        cache_key = ("operator_activity_summary",)
        cached = self._query_cache_get(cache_key, ttl_sec=2.0)
        if isinstance(cached, dict):
            return {str(k): (dict(v) if isinstance(v, dict) else {}) for k, v in cached.items()}
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as e:
            log.debug("StationsMap: failed to resolve operator activity DB path: %s", e)
            return {}
        if not db_path.exists():
            return {}
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            summary = load_operator_activity_summary(conn)
        except Exception as e:
            log.debug("StationsMap: failed to load operator activity summary: %s", e)
            return {}
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
        self._query_cache_set(cache_key, {k: dict(v) for k, v in summary.items()})
        return summary

    def _load_js8_direct_contact_summary(self, my_call: str) -> Dict[str, Dict[str, object]]:
        my_call = (my_call or "").strip().upper()
        if not my_call:
            return {}
        cache_key = ("js8_direct_contact_summary", my_call)
        cached = self._query_cache_get(cache_key, ttl_sec=2.0)
        if isinstance(cached, dict):
            return {str(k): (dict(v) if isinstance(v, dict) else {}) for k, v in cached.items()}
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as e:
            log.debug("StationsMap: failed to resolve direct-contact DB path: %s", e)
            return {}
        if not db_path.exists():
            return {}
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            summary = load_js8_direct_contact_summary(conn, my_call)
        except Exception as e:
            log.debug("StationsMap: failed to load JS8 direct-contact summary: %s", e)
            return {}
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
        self._query_cache_set(cache_key, {k: dict(v) for k, v in summary.items()})
        return summary

    # ------------- Data helpers ------------- #
    def _load_operator_history(self):
        """
        Load operator_checkins (callsign, name, state, grid, group1-3) and plot as stations.
        Grid is preferred; if missing, fall back to state centroid when available.
        """
        perf_start = time.perf_counter()
        pts: List[StationPoint] = []
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as e:
            log.error("StationsMap: failed to resolve DB path: %s", e)
            self._set_station_points(pts)
            emit_span(
                "map.load_operator_history",
                (time.perf_counter() - perf_start) * 1000.0,
                settings=self.settings,
                meta={"stations": 0, "status": "resolve_db_failed"},
                min_ms=5.0,
            )
            return
        if not db_path.exists():
            self._set_station_points(pts)
            emit_span(
                "map.load_operator_history",
                (time.perf_counter() - perf_start) * 1000.0,
                settings=self.settings,
                meta={"stations": 0, "status": "db_missing"},
                min_ms=5.0,
            )
            return
        raw_rows = []
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    callsign,
                    IFNULL(name,''),
                    IFNULL(state,''),
                    IFNULL(grid,''),
                    IFNULL(group1,''),
                    IFNULL(group2,''),
                    IFNULL(group3,''),
                    groups_json,
                    COALESCE(trusted,0)
                FROM operator_checkins
                ORDER BY callsign COLLATE NOCASE
                """
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.error("StationsMap: failed to load operator history: %s", e)
            self._set_station_points(pts)
            emit_span(
                "map.load_operator_history",
                (time.perf_counter() - perf_start) * 1000.0,
                settings=self.settings,
                meta={"stations": 0, "status": "query_failed"},
                min_ms=5.0,
            )
            return

        my_call = (self.settings.get("operator_callsign", "") or "").strip().upper()
        my_grid = (self.settings.get("operator_grid6", "") or "").strip().upper()

        for cs, name, state, grid, g1, g2, g3, gj, trusted in rows:
            cs = (cs or "").strip().upper()
            if not cs:
                continue
            grid = (grid or "").strip().upper()
            if my_call and cs == my_call and my_grid:
                grid = my_grid
            state = (state or "").strip().upper()
            state_abbr = state
            if state_abbr and len(state_abbr) > 2:
                if state_abbr in US_STATE_ABBR_FROM_NAME:
                    state_abbr = US_STATE_ABBR_FROM_NAME[state_abbr]
                elif state_abbr in CANADA_PROV_ABBR_FROM_NAME:
                    state_abbr = CANADA_PROV_ABBR_FROM_NAME[state_abbr]
            latlon = None
            placement = ""
            grid4 = grid[:4] if len(grid) >= 4 else ""
            if len(grid) >= 6:
                latlon = maidenhead_to_latlon(grid)
                placement = "grid6"
            if not latlon and grid4 and state_abbr:
                bounds = maidenhead_grid4_bounds(grid4)
                state_center = STATE_CENTERS.get(state_abbr)
                if bounds and state_center:
                    lat = min(max(state_center[0], bounds[0]), bounds[2])
                    lon = min(max(state_center[1], bounds[1]), bounds[3])
                    latlon = (lat, lon)
                    placement = "state+grid4"
            if not latlon and state_abbr:
                latlon = STATE_CENTERS.get(state_abbr)
                placement = "state"
            if not latlon and grid4:
                latlon = maidenhead_to_latlon(grid4)
                placement = "grid4"
            if not latlon:
                continue
            lat, lon = latlon
            if not self._is_usa_canada(lat, lon):
                continue
            log.debug(
                "StationsMap: station placement %s cs=%s state=%s grid=%s",
                placement or "unknown",
                cs,
                state_abbr or state,
                grid,
            )
            groups: List[str] = []
            try:
                if gj:
                    parsed = json.loads(gj)
                    if isinstance(parsed, list):
                        groups = [str(x) for x in parsed if str(x).strip()]
            except Exception:
                groups = []
            if not groups:
                groups = [g for g in (g1, g2, g3) if g]
            group = groups[0] if groups else ""
            pts.append(
                StationPoint(
                    callsign=cs,
                    grid=grid,
                    name=(name or "").strip(),
                    state=state_abbr or state,
                    group=(group or "").strip(),
                    groups=groups,
                    trusted=bool(trusted),
                    lat=lat,
                    lon=lon,
                )
            )

        self._set_station_points(pts)
        # store raw operator rows for path filters
        op_rows = []
        for r in rows:
            cs_val = (r[0] or "").strip()
            if not cs_val:
                continue
            parsed_groups: List[str] = []
            try:
                if len(r) > 7 and r[7]:
                    maybe = json.loads(r[7])
                    if isinstance(maybe, list):
                        parsed_groups = [str(g) for g in maybe if str(g).strip()]
            except Exception:
                parsed_groups = []
            op_rows.append(
                {
                    "callsign": cs_val.upper(),
                    "name": (r[1] or "").strip(),
                    "state": (r[2] or "").strip().upper(),
                    "group1": (r[4] or "").strip(),
                    "group2": (r[5] or "").strip(),
                    "group3": (r[6] or "").strip(),
                    "groups": parsed_groups,
                }
            )
        self.operator_rows = op_rows
        self._rebuild_operator_index()
        if hasattr(self, "link_mode_combo"):
            self._refresh_link_mode_options()
        if hasattr(self, "group_filter_combo"):
            self._refresh_group_filter_options()
        if hasattr(self, "region_filter_combo"):
            self._refresh_region_filter_options()
        if hasattr(self, "relay_target_combo"):
            self._refresh_relay_targets()
        if hasattr(self, "prop_target_type_combo") and self.prop_target_type_combo is not None:
            self._refresh_prop_target_controls()
        if self._now_reachable_enabled:
            self._now_reachable_meta = self._compute_now_reachable_snapshot()
            self._now_reachable_callsigns = set(self._now_reachable_meta.keys())
            self._update_now_reachable_summary()
        emit_span(
            "map.load_operator_history",
            (time.perf_counter() - perf_start) * 1000.0,
            settings=self.settings,
            meta={"stations": len(self.stations), "rows": len(rows)},
            min_ms=5.0,
        )

    def update_stations(self, stations: List[Dict]):
        pts: List[StationPoint] = []
        for s in stations:
            cs = (s.get("callsign") or "").strip().upper()
            grid = (s.get("grid") or "").strip().upper()
            heard_by = (s.get("heard_by") or "").strip().upper() or None
            if not cs or not grid:
                continue
            ll = maidenhead_to_latlon(grid)
            if not ll:
                continue
            lat, lon = ll
            if not self._is_usa_canada(lat, lon):
                continue
            pts.append(StationPoint(callsign=cs, grid=grid, heard_by=heard_by, lat=lat, lon=lon))
        self._set_station_points(pts)
        self._request_map_refresh(level="full", reason="operator_history_loaded")

    def _daily_schedule_freqs(self) -> List[float]:
        """
        Return unique list of frequencies (MHz) from daily_schedule_tab if present.
        """
        freqs: List[float] = []
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return freqs
        if not db_path.exists():
            return freqs
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_schedule_tab'")
            if not cur.fetchone():
                conn.close()
                return freqs
            cur.execute("SELECT DISTINCT frequency FROM daily_schedule_tab WHERE frequency IS NOT NULL")
            for (f,) in cur.fetchall():
                try:
                    freqs.append(round(float(f), 3))
                except Exception:
                    continue
            conn.close()
        except Exception as e:
            log.warning("StationsMap: failed reading daily_schedule_tab: %s", e)
        return sorted({v for v in freqs if v})

    def _refresh_band_options(self):
        priority = ["20M", "40M", "80M"]
        remaining = ["160M", "60M", "30M", "17M", "15M", "12M", "10M", "6M", "2M"]
        bands = ["All"] + priority + remaining
        self.band_combo.blockSignals(True)
        self.band_combo.clear()
        self.band_combo.addItem("All", {"type": "all"})
        for b in bands[1:]:
            self.band_combo.addItem(b, {"type": "band", "value": b})
        for freq in self._daily_schedule_freqs():
            label = f"{freq:.3f} MHz"
            self.band_combo.addItem(label, {"type": "freq", "value": freq})
        self.band_combo.setCurrentIndex(0)
        self.band_combo.blockSignals(False)

    def _rebuild_operator_index(self):
        """
        Build a quick lookup for operator metadata (state, groups, FEMA region).
        """
        idx: Dict[str, Dict] = {}
        groups: Set[str] = set()
        for r in self.operator_rows:
            cs = (r.get("callsign") or "").upper()
            if not cs:
                continue
            state_raw = (r.get("state") or "").upper()
            state_abbr = state_raw
            if state_abbr and len(state_abbr) > 2:
                if state_abbr in US_STATE_ABBR_FROM_NAME:
                    state_abbr = US_STATE_ABBR_FROM_NAME[state_abbr]
                elif state_abbr in CANADA_PROV_ABBR_FROM_NAME:
                    state_abbr = CANADA_PROV_ABBR_FROM_NAME[state_abbr]
            region = STATE_TO_FEMA_REGION.get(state_abbr)
            group_set = {
                str(g).strip().upper()
                for g in (
                    r.get("group1") or "",
                    r.get("group2") or "",
                    r.get("group3") or "",
                    *(r.get("groups") or []),
                )
                if str(g).strip()
            }
            idx[cs] = {"state": state_abbr, "region": region, "groups": group_set}
            groups.update(group_set)
        self.operator_index = idx
        self._operator_groups = sorted(groups)
        self._operator_regions = sorted({v.get("region") for v in idx.values() if v.get("region")})

    def _marker_station_matches_filters(
        self,
        callsign: str,
        *,
        group_filter: str = "",
        region_filter: str = "",
        my_call: str = "",
        allow_self: bool = False,
    ) -> bool:
        cs = (callsign or "").strip().upper()
        if not cs:
            return False
        if allow_self and my_call and cs == my_call:
            return True
        meta = self.operator_index.get(cs, {})
        group_key = (group_filter or "").strip().upper()
        if group_key:
            groups = {str(g).strip().upper() for g in (meta.get("groups") or set()) if str(g).strip()}
            if group_key not in groups:
                return False
        region_key = (region_filter or "").strip().upper()
        if region_key and str(meta.get("region") or "").strip().upper() != region_key:
            return False
        return True

    def _refresh_group_filter_options(self):
        current = self.group_filter_combo.currentData() if hasattr(self, "group_filter_combo") else None
        group_values = sorted({str(g).strip().upper() for g in (self._operator_groups + self._sitrep_report_groups) if str(g).strip()})
        self.group_filter_combo.blockSignals(True)
        self.group_filter_combo.clear()
        self.group_filter_combo.addItem("All", "")
        for g in group_values:
            self.group_filter_combo.addItem(g, g)
        if current and self.group_filter_combo.findData(current) >= 0:
            self.group_filter_combo.setCurrentIndex(self.group_filter_combo.findData(current))
        else:
            self.group_filter_combo.setCurrentIndex(0)
        self.group_filter_combo.blockSignals(False)

    def _refresh_region_filter_options(self):
        current = self.region_filter_combo.currentData() if hasattr(self, "region_filter_combo") else None
        self.region_filter_combo.blockSignals(True)
        self.region_filter_combo.clear()
        self.region_filter_combo.addItem("All", "")
        for reg in self._operator_regions:
            self.region_filter_combo.addItem(f"Region {reg}", reg)
        if current and self.region_filter_combo.findData(current) >= 0:
            self.region_filter_combo.setCurrentIndex(self.region_filter_combo.findData(current))
        else:
            self.region_filter_combo.setCurrentIndex(0)
        self.region_filter_combo.blockSignals(False)

    def _refresh_link_mode_options(self):
        current_data = self.link_mode_combo.currentData() if hasattr(self, "link_mode_combo") else None
        self.link_mode_combo.blockSignals(True)
        self.link_mode_combo.clear()
        self.link_mode_combo.addItem("Off", ("off", ""))
        self.link_mode_combo.addItem("My Station", ("my_station", ""))
        self.link_mode_combo.addItem("All", ("all", ""))
        restore_idx = self.link_mode_combo.findData(current_data) if current_data else -1
        if restore_idx >= 0:
            self.link_mode_combo.setCurrentIndex(restore_idx)
        else:
            self.link_mode_combo.setCurrentIndex(0)
        self.link_mode_combo.blockSignals(False)
        self.link_mode, self.link_value = self._parse_link_selection(self.link_mode_combo.currentData())

    def _refresh_relay_targets(self):
        current_text = self.relay_target_combo.currentText() if hasattr(self, "relay_target_combo") else ""
        current_call = self._relay_target_callsign_from_text(current_text)
        entries: list[tuple[str, str, str, str]] = []
        for row in self.operator_rows:
            cs = (row.get("callsign") or "").strip().upper()
            if not cs:
                continue
            if self._now_reachable_enabled and self._now_reachable_callsigns and cs not in self._now_reachable_callsigns:
                continue
            name = (row.get("name") or "").strip()
            state = self._normalize_state_abbr(row.get("state") or "")
            groups = row.get("groups") if isinstance(row.get("groups"), list) else []
            if not groups:
                groups = [
                    (row.get("group1") or "").strip(),
                    (row.get("group2") or "").strip(),
                    (row.get("group3") or "").strip(),
                ]
            group_list = [g for g in groups if g]
            group_text = ", ".join(group_list[:3]) if group_list else "--"
            entries.append((cs, name, state, group_text))
        entries = sorted(entries, key=lambda it: (it[0], it[1], it[2], it[3]))
        self.relay_target_combo.blockSignals(True)
        self.relay_target_combo.clear()
        self.relay_target_combo.addItem("")
        for cs, name, state, group_text in entries:
            name_txt = name if name else "--"
            state_txt = state if state else "--"
            status = ""
            if self._now_reachable_enabled:
                meta = self._now_reachable_meta.get(cs, {})
                qsy_text = str(meta.get("qsy_text") or "").strip()
                if qsy_text:
                    status = f" | {qsy_text}"
            label = f"{cs} | {name_txt} | {state_txt} | {group_text}{status}"
            self.relay_target_combo.addItem(label, cs)
        if current_call:
            idx = self.relay_target_combo.findData(current_call)
            if idx >= 0:
                self.relay_target_combo.setCurrentIndex(idx)
            elif current_text:
                self.relay_target_combo.setEditText(current_text)
        elif current_text:
                self.relay_target_combo.setEditText(current_text)
        self.relay_target_combo.blockSignals(False)

    def _parse_frequency_mhz(self, value) -> Optional[float]:
        try:
            txt = str(value).strip()
            if not txt:
                return None
            match = re.search(r"[-+]?\d+(?:[.,]\d+)?", txt)
            if not match:
                return None
            return float(match.group(0).replace(",", "."))
        except Exception:
            return None

    @staticmethod
    def _day_to_index(day_val: str) -> Optional[int]:
        txt = (day_val or "").strip().lower()
        if not txt:
            return None
        names = [
            "sunday",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
        ]
        for idx, name in enumerate(names):
            if txt.startswith(name[:3]) or name.startswith(txt[:3]):
                return idx
        return None

    def _schedule_day_indices(self, day_val: str) -> List[int]:
        txt = (day_val or "ALL").strip().upper()
        if txt in {"", "ALL", "DAILY"}:
            return list(range(7))
        parts = re.split(r"[,/;|]+", txt)
        out: List[int] = []
        for part in parts:
            idx = self._day_to_index(part)
            if idx is not None and idx not in out:
                out.append(idx)
        return out

    def _schedule_active_now(
        self,
        day_val: str,
        today_name: str,
        yesterday_name: str,
        now_min: int,
        start_min: int,
        end_min: int,
    ) -> bool:
        """
        Backward-compatible helper for tests and legacy callers.
        """
        if start_min < 0 or start_min > 1439 or end_min < 0 or end_min > 1439:
            return False
        if start_min == end_min:
            return False
        day_indices = self._schedule_day_indices(day_val)
        if not day_indices:
            return False
        today_idx = self._day_to_index(today_name)
        yesterday_idx = self._day_to_index(yesterday_name)
        if today_idx is None:
            return False
        if start_min < end_min:
            return today_idx in day_indices and start_min <= now_min < end_min
        if today_idx in day_indices and now_min >= start_min:
            return True
        return yesterday_idx in day_indices and now_min < end_min if yesterday_idx is not None else False

    def _minutes_until_end(self, now_min: int, start_min: int, end_min: int) -> Optional[int]:
        """
        Backward-compatible helper for tests and legacy callers.
        """
        if start_min < 0 or start_min > 1439 or end_min < 0 or end_min > 1439:
            return None
        if start_min == end_min:
            return None
        if start_min < end_min:
            if start_min <= now_min < end_min:
                return end_min - now_min
            return None
        if now_min >= start_min:
            return (24 * 60 - now_min) + end_min
        if now_min < end_min:
            return end_min - now_min
        return None

    def _schedule_minutes_to_end(
        self,
        day_val: str,
        now_utc: datetime.datetime,
        start_min: int,
        end_min: int,
    ) -> Optional[int]:
        if start_min < 0 or start_min > 1439 or end_min < 0 or end_min > 1439:
            return None
        if start_min == end_min:
            return None
        day_indices = self._schedule_day_indices(day_val)
        if not day_indices:
            return None
        now_day_idx = (now_utc.weekday() + 1) % 7  # Sunday=0
        now_min = now_utc.hour * 60 + now_utc.minute
        candidates: List[int] = []
        for day_idx in day_indices:
            if start_min < end_min:
                if day_idx == now_day_idx and start_min <= now_min < end_min:
                    candidates.append(end_min - now_min)
                continue
            # Overnight interval
            if day_idx == now_day_idx and now_min >= start_min:
                candidates.append((24 * 60 - now_min) + end_min)
            if ((day_idx + 1) % 7) == now_day_idx and now_min < end_min:
                candidates.append(end_min - now_min)
        if not candidates:
            return None
        return max(0, min(candidates))

    def _load_my_active_schedule_freqs(self, now_utc: datetime.datetime) -> Set[float]:
        out: Set[float] = set()
        try:
            nets_db_path = get_config_dir() / "config" / "freqinout_nets.db"
            settings_db_path = get_config_dir() / "config" / "freqinout.db"
        except Exception:
            return out

        def _add_rows(db_path: Path, table_name: str) -> None:
            if not db_path.exists():
                return
            try:
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,),
                )
                if not cur.fetchone():
                    conn.close()
                    return
                cur.execute(
                    f"SELECT day_utc, start_utc, end_utc, frequency FROM {table_name}"
                )
                for day_utc, start_utc, end_utc, frequency in cur.fetchall():
                    start_min = self._parse_hhmm_minutes(start_utc)
                    end_min = self._parse_hhmm_minutes(end_utc)
                    if start_min is None or end_min is None:
                        continue
                    minutes_to_end = self._schedule_minutes_to_end(
                        str(day_utc or "ALL"),
                        now_utc,
                        start_min,
                        end_min,
                    )
                    if minutes_to_end is None:
                        continue
                    freq_mhz = self._parse_frequency_mhz(frequency)
                    if freq_mhz is None:
                        continue
                    out.add(round(freq_mhz, 6))
                conn.close()
            except Exception:
                pass

        try:
            _add_rows(settings_db_path, "daily_schedule_tab")
            _add_rows(nets_db_path, "daily_schedule_tab")
            _add_rows(nets_db_path, "net_schedule_tab")
            _add_rows(settings_db_path, "net_schedule_tab")
        except Exception:
            return out
        return out

    def _compute_now_reachable_snapshot(self) -> Dict[str, Dict]:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        active_freqs = self._load_my_active_schedule_freqs(now_utc)
        try:
            sched_freq = current_scheduler_freq(self.window())
            sched_freq_mhz = self._parse_frequency_mhz(sched_freq)
            if sched_freq_mhz is not None:
                active_freqs.add(round(sched_freq_mhz, 6))
        except Exception:
            pass
        if not active_freqs:
            return {}
        active_peer = self._load_peer_schedule_presence(now_utc)
        out: Dict[str, Dict] = {}
        for entry in active_peer:
            callsign = (entry.get("callsign") or "").strip().upper()
            if not callsign:
                continue
            freq_mhz = self._parse_frequency_mhz(entry.get("frequency"))
            if freq_mhz is None:
                continue
            if not any(abs(freq_mhz - mine) <= 0.001 for mine in active_freqs):
                continue
            minutes_to_end = entry.get("minutes_to_end")
            try:
                mins = int(minutes_to_end) if minutes_to_end is not None else None
            except Exception:
                mins = None
            qsy_soon = bool(mins is not None and mins <= 10)
            if mins is None:
                qsy_text = "QSY ?"
            elif qsy_soon:
                qsy_text = f"QSY in {mins}m"
            else:
                qsy_text = f"Stable {mins}m"
            out[callsign] = {
                "frequency": round(freq_mhz, 6),
                "minutes_to_end": mins,
                "qsy_soon": qsy_soon,
                "qsy_text": qsy_text,
            }
        return out

    def _update_now_reachable_summary(self) -> None:
        if self._now_reachable_label is None:
            return
        # Reachable details are shown in map legend/tooltip; keep header compact.
        self._now_reachable_label.setText("")
        self._now_reachable_label.setVisible(False)

    def _update_now_reachable_button_visual(self, enabled: bool, theme: Optional[Dict[str, str]] = None) -> None:
        if self._now_reachable_button is None:
            return
        if theme is None:
            theme = self._theme_snapshot()
        self._now_reachable_button.setText("Peer Sched Now")
        if enabled:
            self._now_reachable_button.setStyleSheet(button_style("eligible_info", theme))
            self._now_reachable_button.setToolTip(
                "Peer Sched Now is ON: map filters to peers whose peer schedule currently matches your active schedule frequency."
            )
        else:
            self._now_reachable_button.setStyleSheet(button_style("muted", theme))
            self._now_reachable_button.setToolTip(
                "Show peers whose peer schedule currently matches your active schedule frequency."
            )

    def _update_sitrep_status_button_visual(self, enabled: bool, theme: Optional[Dict[str, str]] = None) -> None:
        if self._sitrep_status_button is None:
            return
        if theme is None:
            theme = self._theme_snapshot()
        self._sitrep_status_button.setText("SitRep")
        if enabled:
            self._sitrep_status_button.setStyleSheet(button_style("eligible_info", theme))
            self._sitrep_status_button.setToolTip("Map View: SitRep Status. Show only Red/Yellow/Green stations.")
        else:
            self._sitrep_status_button.setStyleSheet(button_style("muted", theme))
            self._sitrep_status_button.setToolTip(
                "Show only stations with known SitRep status (Red/Yellow/Green). This view overrides map filters."
            )

    def _current_map_mode_key(self) -> str:
        if bool(getattr(self, "_now_reachable_enabled", False)):
            return "peer"
        if bool(getattr(self, "_observation_focus_enabled", False)):
            focus_mode = str(getattr(self, "_observation_focus_mode", "") or "").strip().lower()
            if focus_mode == "hf_reports":
                return "hf"
            if focus_mode == "local_reports":
                return "local"
            if focus_mode == "all_reports":
                return "reports"
            if focus_mode == "rf_pins":
                return "pins"
        if bool(getattr(self, "_sitrep_status_only_enabled", False)):
            return "sitrep"
        return "all"

    def _update_map_mode_buttons(self, theme: Optional[Dict[str, str]] = None) -> None:
        if theme is None:
            theme = self._theme_snapshot()
        mode_key = self._current_map_mode_key()
        buttons = (
            (getattr(self, "_map_all_stations_button", None), "all"),
            (getattr(self, "_map_hf_reports_button", None), "hf"),
            (getattr(self, "_map_local_reports_button", None), "local"),
            (getattr(self, "_map_reports_button", None), "reports"),
            (getattr(self, "_map_rf_pins_button", None), "pins"),
            (getattr(self, "_sitrep_status_button", None), "sitrep"),
            (getattr(self, "_now_reachable_button", None), "peer"),
        )
        for button, key in buttons:
            if button is None:
                continue
            button.setStyleSheet(button_style("eligible_info" if key == mode_key else "muted", theme))

    def _map_view_status_text(self) -> str:
        mode_key = self._current_map_mode_key()
        if mode_key == "peer":
            return "Map View: Peer Schedule Now"
        if mode_key == "hf":
            return "Map View: HF Reports"
        if mode_key == "local":
            return "Map View: Local Reports"
        if mode_key == "reports":
            return "Map View: Reports"
        if mode_key == "pins":
            return "Map View: RF Pins"
        if mode_key == "sitrep":
            return "Map View: SitRep Status"
        return "Map View: All Stations"

    def _update_map_view_status_label(self, theme: Optional[Dict[str, str]] = None) -> None:
        label = getattr(self, "_map_view_status_label", None)
        if label is None:
            return
        if theme is None:
            theme = self._theme_snapshot()
        text = self._map_view_status_text()
        label.setText(text)
        active = text != "Map View: All Stations"
        color = theme.get("accent", "#0078A8") if active else theme.get("text_secondary", "#5B6773")
        label.setStyleSheet(f"font-weight: bold; color: {color};")
        label.setToolTip(
            "Shows the current map review context. HF Reports and Local Reports use separate observation filters."
        )

    def _on_now_reachable_toggled(self, checked: bool) -> None:
        self._now_reachable_enabled = bool(checked)
        if self._now_reachable_enabled and self._sitrep_status_only_enabled and self._sitrep_status_button is not None:
            # Pin modes are mutually exclusive; Peer Sched Now takes precedence when enabled.
            self._sitrep_status_button.blockSignals(True)
            self._sitrep_status_button.setChecked(False)
            self._sitrep_status_button.blockSignals(False)
            self._sitrep_status_only_enabled = False
            self._observation_focus_enabled = False
            self._observation_focus_mode = ""
            self._update_sitrep_status_button_visual(False)
            self._update_map_mode_buttons()
        if self._now_reachable_enabled:
            snapshot = self._compute_now_reachable_snapshot()
            self._now_reachable_meta = snapshot
            self._now_reachable_callsigns = set(snapshot.keys())
            # Ensure links are visible for the operator-centric view.
            try:
                mode, _ = self._parse_link_selection(self.link_mode_combo.currentData())
                if str(mode).lower() == "off":
                    self.link_mode_combo.setCurrentText("My Station")
            except Exception:
                pass
        else:
            self._now_reachable_meta = {}
            self._now_reachable_callsigns = set()
        self._update_now_reachable_button_visual(self._now_reachable_enabled)
        self._update_map_mode_buttons()
        self._update_map_view_status_label()
        self._update_now_reachable_summary()
        self._refresh_relay_targets()
        self._request_map_refresh(level="medium", reason="reachable_toggle")

    def _on_sitrep_status_toggled(self, checked: bool) -> None:
        self._sitrep_status_only_enabled = bool(checked)
        if not self._sitrep_status_only_enabled:
            self._observation_focus_enabled = False
            self._observation_focus_mode = ""
        if self._sitrep_status_only_enabled and self._now_reachable_enabled and self._now_reachable_button is not None:
            # Pin modes are mutually exclusive; SitRep takes precedence when enabled.
            self._now_reachable_button.blockSignals(True)
            self._now_reachable_button.setChecked(False)
            self._now_reachable_button.blockSignals(False)
            self._now_reachable_enabled = False
            self._now_reachable_meta = {}
            self._now_reachable_callsigns = set()
            self._update_now_reachable_button_visual(False)
            self._update_now_reachable_summary()
            self._refresh_relay_targets()
        if self._sitrep_status_only_enabled:
            # Keep links available by default when this mode is enabled.
            try:
                mode, _ = self._parse_link_selection(self.link_mode_combo.currentData())
                if str(mode).lower() == "off":
                    self.link_mode_combo.setCurrentText("My Station")
            except Exception:
                pass
        self._update_sitrep_status_button_visual(self._current_map_mode_key() == "sitrep")
        self._update_map_mode_buttons()
        self._update_map_view_status_label()
        self._request_map_refresh(level="medium", reason="sitrep_toggle")

    def focus_all_stations(self) -> None:
        """Return to the normal station map view."""
        self._sitrep_status_only_enabled = False
        self._observation_focus_enabled = False
        self._observation_focus_mode = ""
        self._now_reachable_enabled = False
        self._now_reachable_meta = {}
        self._now_reachable_callsigns = set()
        for button in (getattr(self, "_sitrep_status_button", None), getattr(self, "_now_reachable_button", None)):
            if button is None:
                continue
            try:
                button.blockSignals(True)
                button.setChecked(False)
                button.blockSignals(False)
            except Exception:
                pass
        self._update_sitrep_status_button_visual(False)
        self._update_now_reachable_button_visual(False)
        self._update_map_mode_buttons()
        self._update_map_view_status_label()
        self._update_now_reachable_summary()
        self._refresh_relay_targets()
        self._request_map_refresh(level="medium", reason="all_stations_map_focus")

    def _set_report_focus_mode(self, mode: str) -> None:
        """Open a temporary map focus for HF, local, or combined report review."""
        self._sitrep_status_only_enabled = True
        self._observation_focus_enabled = True
        self._observation_focus_mode = str(mode or "all_reports").strip().lower()
        self.show_station_markers = True
        self.show_weather_reports = True
        self.show_alert_reports = True
        self.show_infrastructure_reports = True
        if self._now_reachable_enabled and self._now_reachable_button is not None:
            self._now_reachable_button.blockSignals(True)
            self._now_reachable_button.setChecked(False)
            self._now_reachable_button.blockSignals(False)
            self._now_reachable_enabled = False
            self._now_reachable_meta = {}
            self._now_reachable_callsigns = set()
            self._update_now_reachable_button_visual(False)
            self._update_now_reachable_summary()
            self._refresh_relay_targets()
        for widget, value in (
            (getattr(self, "_sitrep_status_button", None), self._observation_focus_mode == "sitrep"),
            (getattr(self, "map_stations_chk", None), True),
            (getattr(self, "map_weather_chk", None), True),
            (getattr(self, "map_alerts_chk", None), True),
            (getattr(self, "map_infrastructure_chk", None), True),
        ):
            if widget is None:
                continue
            try:
                widget.blockSignals(True)
                widget.setChecked(value)
                widget.blockSignals(False)
            except Exception:
                pass
        try:
            if hasattr(self, "group_filter_combo"):
                self.group_filter_combo.blockSignals(True)
                self.group_filter_combo.setCurrentIndex(0)
                self.group_filter_combo.blockSignals(False)
            if hasattr(self, "band_combo"):
                self.band_combo.blockSignals(True)
                self.band_combo.setCurrentIndex(0)
                self.band_combo.blockSignals(False)
            if hasattr(self, "recency_combo"):
                idx = self.recency_combo.findText("7d")
                if idx >= 0:
                    self.recency_combo.blockSignals(True)
                    self.recency_combo.setCurrentIndex(idx)
                    self.recency_combo.blockSignals(False)
                    self.recency_seconds = 7 * 24 * 60 * 60
        except Exception:
            pass
        self._update_sitrep_status_button_visual(self._current_map_mode_key() == "sitrep")
        self._update_map_mode_buttons()
        self._update_map_view_status_label()
        self._request_map_refresh(level="medium", reason=f"{self._observation_focus_mode}_map_focus")

    def focus_hf_reports(self) -> None:
        """Open a map focus for HF-derived Spotter/SitRep field reports."""
        self._set_report_focus_mode("hf_reports")

    def focus_local_reports(self) -> None:
        """Open a map focus for confirmed local operator and NCS reports."""
        self._set_report_focus_mode("local_reports")

    def focus_reports(self) -> None:
        """Open a map focus for HF and confirmed local reports together."""
        self._set_report_focus_mode("all_reports")

    def focus_rf_pins(self) -> None:
        """Open a map focus for operator-curated RF pins."""
        self._set_report_focus_mode("rf_pins")

    def focus_spotter_reports(self) -> None:
        """Compatibility alias for the previous Spotter map action."""
        self.focus_hf_reports()

    def _on_add_rf_pin_clicked(self) -> None:
        dialog = _RfPinDialog(self)
        if dialog.exec() != QDialog.Accepted:
            return
        payload = dialog.pin_payload()
        if not payload.get("grid") and not payload.get("state"):
            QMessageBox.warning(
                self,
                "Add RF Pin",
                "Add a grid square or state/province so FIO can place the RF pin in context.",
            )
            return
        if not payload.get("grid"):
            QMessageBox.warning(
                self,
                "Add RF Pin",
                "A state-only pin can be saved later when rollup markers are supported. Add a grid square for this map pin.",
            )
            return
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            save_rf_pin(db_path, payload)
        except Exception as exc:
            log.warning("StationsMap: failed to save RF pin: %s", exc, exc_info=True)
            QMessageBox.warning(self, "Add RF Pin", f"FIO could not save this RF pin.\n{exc}")
            return
        self._clear_report_query_caches()
        if not bool(getattr(self, "_observation_focus_enabled", False)):
            self._set_report_focus_mode("rf_pins")
        else:
            self._request_map_refresh(level="medium", reason="rf_pin_saved")
        label = str(payload.get("label") or "RF Pin")
        status = getattr(self, "_map_view_status_label", None)
        if status is not None:
            status.setText(f"RF Pin saved: {label}")

    def _on_manage_rf_pins_clicked(self) -> None:
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as exc:
            QMessageBox.warning(self, "Manage RF Pins", f"FIO could not open RF pin storage.\n{exc}")
            return
        dialog = _RfPinManagerDialog(db_path, self)
        dialog.exec()
        if not dialog.changed:
            return
        self._clear_report_query_caches()
        self._request_map_refresh(level="medium", reason="rf_pin_changed")

    def _include_legacy_spotter_report_layers(self) -> bool:
        """Return False when Local Reports should exclude HF Spotter-only report layers."""
        if not bool(getattr(self, "_observation_focus_enabled", False)):
            return True
        focus_mode = str(getattr(self, "_observation_focus_mode", "") or "").strip().lower()
        return focus_mode not in {"local_reports", "rf_pins"}

    def _relay_target_callsign_from_text(self, text: str) -> str:
        txt = (text or "").strip()
        if not txt:
            return ""
        if hasattr(self, "relay_target_combo"):
            idx = self.relay_target_combo.findText(txt, Qt.MatchFixedString)
            if idx >= 0:
                data = self.relay_target_combo.itemData(idx)
                if data:
                    return str(data).strip().upper()
        first = txt.split("|", 1)[0].strip().upper()
        if first in self.operator_index:
            return first
        exact_name = txt.upper()
        for row in self.operator_rows:
            name = (row.get("name") or "").strip().upper()
            if name and name == exact_name:
                return (row.get("callsign") or "").strip().upper()
        return first

    def _load_js8_links(
        self,
        band_filter=None,
        my_call: str = "",
        link_selection: Optional[tuple[str, str]] = None,
        relay_target: Optional[str] = None,
        group_filter: str = "",
        region_filter: str = "",
        reachable_callsigns: Optional[Set[str]] = None,
        max_age_sec: Optional[int] = None,
    ) -> tuple[List[Dict], Dict[str, Dict]]:
        """
        Load recent JS8 links from js8_links table, returning only pairs with known positions.
        Returns (links, station_stats) where station_stats keyed by callsign contains:
          last_seen (ts), last_spotter (ts), avg_snr, max_snr
        """
        links: List[Dict] = []
        # Build position map from current stations
        pos_map: Dict[str, tuple[float, float]] = {}
        for pt in self.stations:
            pos_map[pt.callsign.upper()] = (pt.lat, pt.lon)

        if isinstance(link_selection, (list, tuple)) and len(link_selection) >= 2:
            mode, selection_value = link_selection[0], link_selection[1]
        else:
            mode, selection_value = "off", ""
        selection_value = (selection_value or "").upper() if mode == "region" else (selection_value or "")
        relay_target = (relay_target or "").strip().upper()
        group_filter = (group_filter or "").strip().upper()
        region_filter = (region_filter or "").strip().upper()
        reachable_calls = {c.strip().upper() for c in (reachable_callsigns or set()) if c}
        band_sig = ""
        try:
            band_sig = json.dumps(band_filter or {"type": "all"}, sort_keys=True, default=str)
        except Exception:
            band_sig = str(band_filter or {"type": "all"})
        cache_key = (
            "js8_links",
            band_sig,
            (my_call or "").strip().upper(),
            mode,
            selection_value,
            relay_target,
            group_filter,
            region_filter,
            ",".join(sorted(reachable_calls)),
            int(max_age_sec or 0),
            len(self.stations),
            len(self.operator_index),
        )
        cached = self._query_cache_get(cache_key, ttl_sec=2.0)
        if isinstance(cached, tuple) and len(cached) == 2:
            cached_links, cached_stats = cached
            if isinstance(cached_links, list) and isinstance(cached_stats, dict):
                return (
                    [dict(x) for x in cached_links if isinstance(x, dict)],
                    {
                        str(k): (dict(v) if isinstance(v, dict) else v)
                        for k, v in cached_stats.items()
                    },
                )
        if mode == "off" and not relay_target:
            return links, {}

        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as e:
            log.error("StationsMap: failed to resolve DB path for links: %s", e)
            return links
        if not db_path.exists():
            return links

        ts_cut = None
        if max_age_sec and max_age_sec > 0:
            ts_cut = time.time() - max_age_sec

        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            if ts_cut:
                cur.execute(
                    "SELECT ts, origin, destination, snr, band, freq_hz, is_spotter FROM js8_links WHERE ts >= ?",
                    (ts_cut,),
                )
            else:
                cur.execute("SELECT ts, origin, destination, snr, band, freq_hz, is_spotter FROM js8_links")
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.error("StationsMap: failed to load js8_links: %s", e)
            return links, {}

        # Defensive recency filter in Python too (covers odd SQLite typing differences across platforms)
        if ts_cut:
            before = len(rows)
            rows = [r for r in rows if r and len(r) > 0 and isinstance(r[0], (int, float)) and r[0] >= ts_cut]
            if log.isEnabledFor(logging.DEBUG):
                log.debug("StationsMap: recency filter %s removed %s rows", max_age_sec, before - len(rows))

        # keep best SNR per pair with filters
        best: Dict[tuple[str, str], Optional[float]] = {}
        stat: Dict[str, Dict] = {}
        relay_best: Dict[tuple[str, str], Optional[float]] = {}
        my_partners: Set[str] = set()
        target_partners: Set[str] = set()
        direct_snrs: Dict[tuple[str, str], List[float]] = {}
        direct_counts: Dict[tuple[str, str], int] = {}

        def _freq_to_band(freq_mhz: Optional[float]) -> str:
            if freq_mhz is None:
                return ""
            bands = [
                ("160M", 1.8, 2.0),
                ("80M", 3.5, 4.0),
                ("60M", 5.0, 5.5),
                ("40M", 7.0, 7.3),
                ("30M", 10.1, 10.15),
                ("20M", 14.0, 14.35),
                ("17M", 18.068, 18.168),
                ("15M", 21.0, 21.45),
                ("12M", 24.89, 24.99),
                ("10M", 28.0, 29.7),
                ("6M", 50.0, 54.0),
                ("2M", 144.0, 148.0),
            ]
            for name, lo, hi in bands:
                if lo <= freq_mhz <= hi:
                    return name
            return ""

        def _station_matches_filters(cs: str) -> bool:
            if not cs:
                return False
            if group_filter:
                groups = self.operator_index.get(cs, {}).get("groups", set())
                if group_filter not in groups:
                    return False
            if region_filter:
                region = self.operator_index.get(cs, {}).get("region")
                if region != region_filter:
                    return False
            return True

        for ts, o, d, snr, band, freq_hz, is_spotter in rows:
            o = (o or "").upper()
            d = (d or "").upper()
            if o == "" or d == "" or o not in pos_map or d not in pos_map:
                continue
            bf = band_filter or {"type": "all"}
            try:
                freq_mhz = float(freq_hz) / 1_000_000.0 if freq_hz is not None else None
            except Exception:
                freq_mhz = None
            band_val = (band or "").upper() or _freq_to_band(freq_mhz)
            if bf.get("type") == "band":
                if band_val != str(bf.get("value")).upper():
                    continue
            elif bf.get("type") == "freq":
                target_f = bf.get("value")
                if freq_mhz is None or target_f is None or abs(freq_mhz - target_f) > 0.001:
                    continue

            match_o = _station_matches_filters(o)
            match_d = _station_matches_filters(d)

            include = False
            if relay_target:
                if my_call and (my_call in {o, d} or relay_target in {o, d}):
                    include = True
            elif mode == "my_station":
                include = bool(my_call) and my_call in {o, d}
            elif mode == "all":
                include = True
            elif mode == "region" and selection_value:
                region_o = self.operator_index.get(o, {}).get("region")
                region_d = self.operator_index.get(d, {}).get("region")
                if region_o == selection_value and region_d == selection_value:
                    include = True
                elif my_call and my_call in {o, d}:
                    other = d if o == my_call else o
                    include = self.operator_index.get(other, {}).get("region") == selection_value
            elif mode == "group" and selection_value:
                groups_o = self.operator_index.get(o, {}).get("groups", set())
                groups_d = self.operator_index.get(d, {}).get("groups", set())
                if selection_value in groups_o and selection_value in groups_d:
                    include = True
                elif my_call and my_call in {o, d}:
                    other = d if o == my_call else o
                    include = selection_value in self.operator_index.get(other, {}).get("groups", set())
            if include and group_filter:
                if my_call and my_call in {o, d}:
                    other = d if o == my_call else o
                    include = group_filter in self.operator_index.get(other, {}).get("groups", set())
                else:
                    include = group_filter in self.operator_index.get(o, {}).get("groups", set()) and group_filter in self.operator_index.get(d, {}).get("groups", set())
            if include and region_filter:
                if my_call and my_call in {o, d}:
                    other = d if o == my_call else o
                    include = self.operator_index.get(other, {}).get("region") == region_filter
                else:
                    region_o = self.operator_index.get(o, {}).get("region")
                    region_d = self.operator_index.get(d, {}).get("region")
                    include = region_o == region_filter and region_d == region_filter
            if include and reachable_calls:
                include = (o in reachable_calls or d in reachable_calls)
                if include and my_call:
                    include = my_call in {o, d}
            if not include:
                include = False

            key = tuple(sorted((o, d)))
            try:
                snr_val = float(snr)
            except Exception:
                snr_val = None

            record_stats = False
            if mode == "all":
                record_stats = match_o
            elif mode == "my_station":
                record_stats = bool(my_call) and o != my_call and my_call in {o, d} and match_o
            if snr_val is not None and record_stats and my_call and my_call in {o, d} and o != my_call:
                direct_snrs.setdefault(key, []).append(snr_val)
                direct_counts[key] = direct_counts.get(key, 0) + 1

            def _record_station(cs: str, other: str, ts_val, snr_value, spotted: bool, is_origin: bool, band_name: str):
                s = stat.setdefault(
                    cs,
                    {
                        "last_seen": 0,
                        "last_spotter": 0,
                        "snrs": [],
                        "snrs_excl_my": [],
                        "snr_excl_my_count": 0,
                        "last_band": "",
                        "last_band_ts": 0,
                    },
                )
                if ts_val and ts_val > s["last_seen"]:
                    s["last_seen"] = ts_val
                if spotted and ts_val and ts_val > s["last_spotter"]:
                    s["last_spotter"] = ts_val
                if snr_value is not None and is_origin:
                    s["snrs"].append(snr_value)
                    if my_call and other != my_call:
                        s["snrs_excl_my"].append(snr_value)
                        s["snr_excl_my_count"] += 1
                if is_origin and band_name and ts_val and ts_val > s.get("last_band_ts", 0):
                    s["last_band"] = band_name
                    s["last_band_ts"] = ts_val
            if record_stats:
                _record_station(o, d, ts, snr_val, bool(is_spotter), True, band_val)

            if not include:
                continue

            if relay_target:
                if key not in relay_best or (snr_val is not None and (relay_best[key] is None or snr_val > relay_best[key])):
                    relay_best[key] = snr_val
                if my_call and my_call in {o, d}:
                    other = d if o == my_call else o
                    my_partners.add(other)
                if relay_target in {o, d}:
                    other = d if o == relay_target else o
                    target_partners.add(other)
            else:
                if key not in best or (snr_val is not None and (best[key] is None or snr_val > best[key])):
                    best[key] = snr_val

        def _add_link(key_map: Dict[tuple[str, str], Optional[float]], a: str, b: str):
            k = tuple(sorted((a, b)))
            if k not in key_map:
                return
            p1 = pos_map.get(a)
            p2 = pos_map.get(b)
            if not p1 or not p2:
                return
            links.append(
                {
                    "origin": a,
                    "destination": b,
                    "lat1": p1[0],
                    "lon1": p1[1],
                    "lat2": p2[0],
                    "lon2": p2[1],
                    "snr": key_map[k],
                }
            )

        if relay_target and my_call:
            mutual = my_partners & target_partners
            _add_link(relay_best, my_call, relay_target)
            for other in sorted(mutual):
                _add_link(relay_best, my_call, other)
                _add_link(relay_best, relay_target, other)
        else:
            for (o, d), snr_val in best.items():
                _add_link(best, o, d)

        # finalize stats: avg/max
        stats_out: Dict[str, Dict] = {}
        for cs, data in stat.items():
            snrs = data.get("snrs", [])
            avg_snr = sum(snrs) / len(snrs) if snrs else None
            max_snr = max(snrs) if snrs else None
            snrs_excl_my = data.get("snrs_excl_my", [])
            if not my_call:
                snrs_excl_my = snrs
            avg_snr_excl_my = sum(snrs_excl_my) / len(snrs_excl_my) if snrs_excl_my else None
            direct_snr = None
            direct_count = 0
            if my_call:
                key = tuple(sorted((my_call, cs)))
                vals = direct_snrs.get(key, [])
                if vals:
                    direct_snr = sum(vals) / len(vals)
                    direct_count = direct_counts.get(key, 0)
            try:
                seen_fmt = datetime.datetime.utcfromtimestamp(data.get("last_seen") or 0).strftime("%Y-%m-%d %H:%M:%S UTC") if data.get("last_seen") else ""
            except Exception:
                seen_fmt = ""
            try:
                spotter_fmt = datetime.datetime.utcfromtimestamp(data.get("last_spotter") or 0).strftime("%Y-%m-%d %H:%M:%S UTC") if data.get("last_spotter") else ""
            except Exception:
                spotter_fmt = ""
            stats_out[cs] = {
                "last_seen": data.get("last_seen") or 0,
                "last_spotter": data.get("last_spotter") or 0,
                "last_seen_fmt": seen_fmt,
                "last_spotter_fmt": spotter_fmt,
                "avg_snr": avg_snr,
                "max_snr": max_snr,
                "direct_snr": direct_snr,
                "avg_snr_excl_my": avg_snr_excl_my,
                "direct_count": direct_count,
                "avg_snr_count": data.get("snr_excl_my_count", 0),
                "last_band": data.get("last_band") or "",
            }
        self._query_cache_set(
            cache_key,
            (
                [dict(x) for x in links],
                {str(k): (dict(v) if isinstance(v, dict) else v) for k, v in stats_out.items()},
            ),
        )
        return links, stats_out

    def _load_varac_links(
        self,
        band_filter=None,
        my_call: str = "",
        link_selection: Optional[tuple[str, str]] = None,
        group_filter: str = "",
        region_filter: str = "",
        reachable_callsigns: Optional[Set[str]] = None,
        max_age_sec: Optional[int] = None,
    ) -> List[Dict]:
        band_sig = ""
        try:
            band_sig = json.dumps(band_filter or {"type": "all"}, sort_keys=True, default=str)
        except Exception:
            band_sig = str(band_filter or {"type": "all"})
        cache_key = (
            "varac_links",
            band_sig,
            (my_call or "").strip().upper(),
            str(link_selection or ""),
            (group_filter or "").strip().upper(),
            (region_filter or "").strip().upper(),
            ",".join(sorted({c.strip().upper() for c in (reachable_callsigns or set()) if c})),
            int(max_age_sec or 0),
            len(self.stations),
            len(self.operator_index),
        )
        cached = self._query_cache_get(cache_key, ttl_sec=2.0)
        if isinstance(cached, list):
            return [dict(x) for x in cached if isinstance(x, dict)]
        links: List[Dict] = []
        pos_map: Dict[str, tuple[float, float]] = {}
        for pt in self.stations:
            pos_map[pt.callsign.upper()] = (pt.lat, pt.lon)

        if isinstance(link_selection, (list, tuple)) and len(link_selection) >= 2:
            mode, selection_value = link_selection[0], link_selection[1]
        else:
            mode, selection_value = "off", ""
        selection_value = (selection_value or "").upper() if mode == "region" else (selection_value or "")
        group_filter = (group_filter or "").strip().upper()
        region_filter = (region_filter or "").strip().upper()
        reachable_calls = {c.strip().upper() for c in (reachable_callsigns or set()) if c}
        if mode == "off":
            return links

        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return links
        if not db_path.exists():
            return links

        ts_cut = None
        if max_age_sec and max_age_sec > 0:
            ts_cut = time.time() - max_age_sec
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            if ts_cut:
                cur.execute(
                    "SELECT ts, origin, destination, snr, band, freq_hz FROM varac_links WHERE ts >= ?",
                    (ts_cut,),
                )
            else:
                cur.execute("SELECT ts, origin, destination, snr, band, freq_hz FROM varac_links")
            rows = cur.fetchall()
            conn.close()
        except Exception:
            return links

        def _freq_to_band(freq_mhz: Optional[float]) -> str:
            if freq_mhz is None:
                return ""
            bands = [
                ("160M", 1.8, 2.0),
                ("80M", 3.5, 4.0),
                ("60M", 5.0, 5.5),
                ("40M", 7.0, 7.3),
                ("30M", 10.1, 10.15),
                ("20M", 14.0, 14.35),
                ("17M", 18.068, 18.168),
                ("15M", 21.0, 21.45),
                ("12M", 24.89, 24.99),
                ("10M", 28.0, 29.7),
                ("6M", 50.0, 54.0),
                ("2M", 144.0, 148.0),
            ]
            for name, lo, hi in bands:
                if lo <= freq_mhz <= hi:
                    return name
            return ""

        def _station_matches_filters(cs: str) -> bool:
            if not cs:
                return False
            if group_filter:
                groups = self.operator_index.get(cs, {}).get("groups", set())
                if group_filter not in groups:
                    return False
            if region_filter:
                region = self.operator_index.get(cs, {}).get("region")
                if region != region_filter:
                    return False
            return True

        for ts, o, d, snr, band, freq_hz in rows:
            o = (o or "").upper()
            d = (d or "").upper()
            if o == "" or d == "" or o not in pos_map or d not in pos_map:
                continue
            bf = band_filter or {"type": "all"}
            try:
                freq_mhz = float(freq_hz) / 1_000_000.0 if freq_hz is not None else None
            except Exception:
                freq_mhz = None
            band_val = (band or "").upper() or _freq_to_band(freq_mhz)
            if bf.get("type") == "band":
                if band_val != str(bf.get("value")).upper():
                    continue
            elif bf.get("type") == "freq":
                target_f = bf.get("value")
                if freq_mhz is None or target_f is None or abs(freq_mhz - target_f) > 0.001:
                    continue

            include = False
            match_o = _station_matches_filters(o)
            match_d = _station_matches_filters(d)
            if mode == "my_station":
                include = bool(my_call) and my_call in {o, d}
            elif mode == "all":
                include = True
            elif mode == "region" and selection_value:
                region_o = self.operator_index.get(o, {}).get("region")
                region_d = self.operator_index.get(d, {}).get("region")
                if region_o == selection_value and region_d == selection_value:
                    include = True
                elif my_call and my_call in {o, d}:
                    other = d if o == my_call else o
                    include = self.operator_index.get(other, {}).get("region") == selection_value
            elif mode == "group" and selection_value:
                groups_o = self.operator_index.get(o, {}).get("groups", set())
                groups_d = self.operator_index.get(d, {}).get("groups", set())
                if selection_value in groups_o and selection_value in groups_d:
                    include = True
                elif my_call and my_call in {o, d}:
                    other = d if o == my_call else o
                    include = selection_value in self.operator_index.get(other, {}).get("groups", set())
            if include and group_filter:
                if my_call and my_call in {o, d}:
                    other = d if o == my_call else o
                    include = group_filter in self.operator_index.get(other, {}).get("groups", set())
                else:
                    include = group_filter in self.operator_index.get(o, {}).get("groups", set()) and group_filter in self.operator_index.get(d, {}).get("groups", set())
            if include and region_filter:
                if my_call and my_call in {o, d}:
                    other = d if o == my_call else o
                    include = self.operator_index.get(other, {}).get("region") == region_filter
                else:
                    region_o = self.operator_index.get(o, {}).get("region")
                    region_d = self.operator_index.get(d, {}).get("region")
                    include = region_o == region_filter and region_d == region_filter
            if include and reachable_calls:
                include = (o in reachable_calls or d in reachable_calls)
                if include and my_call:
                    include = my_call in {o, d}
            if not include:
                include = False

            if not include:
                continue
            p1 = pos_map.get(o)
            p2 = pos_map.get(d)
            if not p1 or not p2:
                continue
            try:
                snr_val = float(snr)
            except Exception:
                snr_val = None
            links.append(
                {
                    "origin": o,
                    "destination": d,
                    "lat1": p1[0],
                    "lon1": p1[1],
                    "lat2": p2[0],
                    "lon2": p2[1],
                    "snr": snr_val,
                }
            )

        self._query_cache_set(cache_key, [dict(x) for x in links])
        return links

    def _load_varac_stats(self, max_age_sec: Optional[int] = None) -> Dict[str, Dict]:
        cache_key = ("varac_stats", int(max_age_sec) if max_age_sec else None)
        cached = self._query_cache_get(cache_key)
        if isinstance(cached, dict):
            return {
                str(k): dict(v) if isinstance(v, dict) else v
                for k, v in cached.items()
            }
        stats: Dict[str, Dict] = {}
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return stats
        if not db_path.exists():
            return stats
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT callsign, last_seen_ts, last_band, last_freq_hz, last_snr
                FROM varac_callsign_stats
                """
            )
            rows = cur.fetchall()
            cur.execute(
                """
                SELECT callsign, AVG(snr) FROM (
                    SELECT origin AS callsign, snr FROM varac_links WHERE snr IS NOT NULL
                    UNION ALL
                    SELECT destination AS callsign, snr FROM varac_links WHERE snr IS NOT NULL
                )
                GROUP BY callsign
                """
            )
            avg_rows = cur.fetchall()
            conn.close()
        except Exception:
            return stats
        ts_cut = None
        if max_age_sec and max_age_sec > 0:
            ts_cut = time.time() - max_age_sec
        avg_map = {str(c or "").strip().upper(): float(a) for c, a in avg_rows if c and a is not None}
        def _freq_to_band(freq_hz: Optional[float]) -> str:
            if not freq_hz:
                return ""
            try:
                mhz = float(freq_hz) / 1_000_000.0
            except Exception:
                return ""
            bands = [
                ("160M", 1.8, 2.0),
                ("80M", 3.5, 4.0),
                ("60M", 5.0, 5.5),
                ("40M", 7.0, 7.3),
                ("30M", 10.1, 10.15),
                ("20M", 14.0, 14.35),
                ("17M", 18.068, 18.168),
                ("15M", 21.0, 21.45),
                ("12M", 24.89, 24.99),
                ("10M", 28.0, 29.7),
                ("6M", 50.0, 54.0),
                ("2M", 144.0, 148.0),
            ]
            for name, lo, hi in bands:
                if lo <= mhz <= hi:
                    return name
            return ""
        for cs, last_seen_ts, last_band, last_freq_hz, last_snr in rows:
            last_seen_val = float(last_seen_ts or 0.0)
            if ts_cut and last_seen_val and last_seen_val < ts_cut:
                continue
            band_val = (last_band or "").strip().upper()
            if band_val in {"NA", "N/A"}:
                band_val = ""
            if not band_val and last_freq_hz not in (None, ""):
                try:
                    band_val = _freq_to_band(float(last_freq_hz))
                except Exception:
                    band_val = ""
            stats[(cs or "").strip().upper()] = {
                "last_seen_ts": last_seen_val,
                "last_band": band_val,
                "last_freq_hz": float(last_freq_hz) if last_freq_hz not in (None, "") else None,
                "last_snr": float(last_snr) if last_snr not in (None, "") else None,
                "avg_snr": avg_map.get((cs or "").strip().upper()),
            }
        self._query_cache_set(cache_key, dict(stats))
        return stats

    def _load_js8_presence(self) -> Set[str]:
        cache_key = ("js8_presence",)
        cached = self._query_cache_get(cache_key)
        if isinstance(cached, (set, list, tuple)):
            return {str(c) for c in cached}
        calls: Set[str] = set()
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return calls
        if not db_path.exists():
            return calls
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT origin FROM js8_links")
            calls.update({str(r[0]).strip().upper() for r in cur.fetchall() if r and r[0]})
            cur.execute("SELECT DISTINCT destination FROM js8_links")
            calls.update({str(r[0]).strip().upper() for r in cur.fetchall() if r and r[0]})
            conn.close()
        except Exception:
            return calls
        self._query_cache_set(cache_key, set(calls))
        return calls

    def _load_fldigi_presence(self) -> Set[str]:
        cache_key = ("fldigi_presence",)
        cached = self._query_cache_get(cache_key)
        if isinstance(cached, (set, list, tuple)):
            return {str(c) for c in cached}
        calls: Set[str] = set()
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return calls
        if not db_path.exists():
            return calls
        def _base_callsign(val: str) -> str:
            cs_norm = (val or "").strip().upper()
            if not cs_norm:
                return ""
            cs_norm = re.sub(r"/(P|M|MM|QRP|SOTA|ROVER|[A-Z0-9]{1,4})$", "", cs_norm)
            match = re.search(r"\b[A-Z]{1,2}\d[A-Z0-9]{1,4}\b", cs_norm)
            if match:
                return match.group(0)
            return cs_norm
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT callsign FROM fldigi_checkins")
            checkins = {
                _base_callsign(str(r[0]).strip().upper())
                for r in cur.fetchall()
                if r and r[0]
            }
            cur.execute("SELECT callsign FROM fldigi_file_senders")
            senders = {
                _base_callsign(str(r[0]).strip().upper())
                for r in cur.fetchall()
                if r and r[0]
            }
            conn.close()
        except Exception:
            return calls
        out = {c for c in (checkins | senders) if c}
        self._query_cache_set(cache_key, set(out))
        return out

    @staticmethod
    def _settings_bool(settings: SettingsManager, key: str, default: bool) -> bool:
        try:
            value = settings.get(key, default)
        except Exception:
            value = default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        txt = str(value or "").strip().lower()
        if txt in {"1", "true", "yes", "on", "enabled"}:
            return True
        if txt in {"0", "false", "no", "off", "disabled"}:
            return False
        return bool(default)

    @staticmethod
    def _safe_float(value: object, default: float = 0.0) -> float:
        try:
            return float(value or 0.0)
        except Exception:
            return float(default)

    @staticmethod
    def _sitrep_status_label(status_key: str) -> str:
        key = (status_key or "").strip().lower()
        if key == "red":
            return "Not Functioning"
        if key == "yellow":
            return "Partially Functioning"
        if key == "green":
            return "Functioning"
        return "Unknown"

    @staticmethod
    def _sitrep_status_chip(status_key: str) -> str:
        key = (status_key or "").strip().lower()
        if key == "red":
            return "R"
        if key == "yellow":
            return "Y"
        if key == "green":
            return "G"
        return "?"

    @staticmethod
    def _source_short_label(source: str) -> str:
        return source_short_label(source)

    @classmethod
    def _encode_source_chips(cls, source_summary: Dict[str, str]) -> str:
        if not source_summary:
            return ""
        parts: List[str] = []
        for source in sorted(source_summary.keys()):
            key = str(source_summary.get(source) or "unknown").strip().lower()
            if key not in {"red", "yellow", "green", "unknown", "not_reported"}:
                key = "unknown"
            if key == "not_reported":
                key = "unknown"
            parts.append(f"{cls._source_short_label(source)}:{cls._sitrep_status_chip(key)}")
        return " ".join(parts)

    @staticmethod
    def _decode_source_summary(value: object) -> Dict[str, str]:
        txt = str(value or "").strip()
        if not txt:
            return {}
        try:
            obj = json.loads(txt)
            if isinstance(obj, dict):
                out: Dict[str, str] = {}
                for raw_src, raw_status in obj.items():
                    src = str(raw_src or "").strip().upper()
                    status = str(raw_status or "").strip().lower()
                    if not src:
                        continue
                    if status not in {"red", "yellow", "green", "unknown", "not_reported"}:
                        status = "unknown"
                    out[src] = "unknown" if status == "not_reported" else status
                return out
        except Exception:
            pass
        return {}

    @staticmethod
    def _sitrep_conflict(source_summary: Dict[str, str]) -> bool:
        vals = {
            str(v or "").strip().lower()
            for v in source_summary.values()
            if str(v or "").strip().lower() in {"red", "yellow", "green", "unknown"}
        }
        return len(vals) > 1

    @staticmethod
    def _sitrep_age_text(ts_val: float) -> str:
        ts = float(ts_val or 0.0)
        if ts <= 0:
            return ""
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        age = max(0, int(now - ts))
        if age < 60:
            return f"{age}s"
        mins, sec = divmod(age, 60)
        if mins < 60:
            return f"{mins}m {sec}s"
        hrs, mins = divmod(mins, 60)
        if hrs < 24:
            return f"{hrs}h {mins}m"
        days, hrs = divmod(hrs, 24)
        return f"{days}d {hrs}h"

    def _load_spotter_station_status(self) -> Dict[str, Dict]:
        unified_enabled = self._settings_bool(self.settings, "sitrep_unified_map_enabled", True)
        cache_key = ("spotter_station_status", unified_enabled)
        cached = self._query_cache_get(cache_key)
        if isinstance(cached, dict):
            return {
                str(k): dict(v) if isinstance(v, dict) else v
                for k, v in cached.items()
            }
        statuses: Dict[str, Dict] = {}
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return statuses
        if not db_path.exists():
            return statuses
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT from_call, status_key, status_label, updated_utc_ts, updated_utc_str, response_code,
                           status_source, status_source_detail
                    FROM spotter_station_status
                    """
                )
            except Exception:
                cur.execute(
                    """
                    SELECT from_call, status_key, status_label, updated_utc_ts, updated_utc_str, response_code,
                           '' AS status_source, '' AS status_source_detail
                    FROM spotter_station_status
                    """
                )
            rows = cur.fetchall()
            fused_rows = []
            if unified_enabled:
                try:
                    cur.execute(
                        """
                        SELECT
                            callsign,
                            effective_status,
                            latest_event_ts,
                            latest_event_ts_utc,
                            latest_report_group,
                            latest_transport_mode,
                            latest_state_code,
                            latest_state_confidence,
                            latest_geo_confidence,
                            latest_brevity_summary,
                            source_summary_json
                        FROM sitrep_latest_by_callsign
                        """
                    )
                    fused_rows = cur.fetchall()
                except Exception:
                    fused_rows = []
            conn.close()
        except Exception:
            return statuses

        for from_call, status_key, status_label, updated_utc_ts, updated_utc_str, response_code, status_source, status_source_detail in rows:
            call = (from_call or "").strip().upper()
            if not call:
                continue
            key = (status_key or "").strip().lower()
            if key not in {"red", "yellow", "green", "unknown"}:
                key = "unknown"
            updated_ts = self._safe_float(updated_utc_ts, 0.0)
            updated_str = (updated_utc_str or "").strip()
            if not updated_str and updated_ts > 0:
                try:
                    updated_str = datetime.datetime.utcfromtimestamp(updated_ts).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    updated_str = ""
            source = (status_source or "").strip().upper()
            source_summary = {source: key} if source else {}
            source_chips = self._encode_source_chips(source_summary)
            status_label_text = (status_label or "").strip() or self._sitrep_status_label(key)
            source_detail = (status_source_detail or "").strip()
            if not source_detail and source_chips:
                source_detail = source_chips
            statuses[call] = {
                "status_key": key,
                "status_label": status_label_text,
                "updated_utc_ts": updated_ts,
                "updated_utc_str": updated_str,
                "response_code": (response_code or "").strip(),
                "status_source": source_family_label(source),
                "status_source_detail": source_detail,
                "status_source_chips": source_chips,
                "status_conflict": False,
                "status_age": self._sitrep_age_text(updated_ts),
                "status_source_count": len(source_summary),
                "report_group": "",
                "transport_label": "",
                "state_code": "",
                "state_confidence": "",
                "geo_confidence": "",
                "brevity_summary": "",
            }

        sitrep_report_groups: Set[str] = set()
        for (
            callsign,
            effective_status,
            latest_event_ts,
            latest_event_ts_utc,
            latest_report_group,
            latest_transport_mode,
            latest_state_code,
            latest_state_confidence,
            latest_geo_confidence,
            latest_brevity_summary,
            source_summary_json,
        ) in fused_rows:
            call = (callsign or "").strip().upper()
            if not call:
                continue
            key = str(effective_status or "").strip().lower()
            if key not in {"red", "yellow", "green", "unknown", "not_reported"}:
                key = "unknown"
            if key == "not_reported":
                key = "unknown"
            summary = self._decode_source_summary(source_summary_json)
            if not summary:
                summary = {"FUSED": key}
            source_count = len(summary)
            source_chips = self._encode_source_chips(summary)
            source = source_family_label(next(iter(summary.keys()), "FUSED")) if source_count <= 1 else "Mixed"
            updated_ts = self._safe_float(latest_event_ts, 0.0)
            updated_str = (latest_event_ts_utc or "").strip()
            if not updated_str and updated_ts > 0:
                try:
                    updated_str = datetime.datetime.utcfromtimestamp(updated_ts).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    updated_str = ""
            candidate = {
                "status_key": key,
                "status_label": self._sitrep_status_label(key),
                "updated_utc_ts": updated_ts,
                "updated_utc_str": updated_str,
                "response_code": "",
                "status_source": source,
                "status_source_detail": "Unified SitRep",
                "status_source_chips": source_chips,
                "status_conflict": self._sitrep_conflict(summary),
                "status_age": self._sitrep_age_text(updated_ts),
                "status_source_count": source_count,
                "report_group": str(latest_report_group or "").strip().upper(),
                "transport_label": transport_label(latest_transport_mode),
                "state_code": str(latest_state_code or "").strip().upper(),
                "state_confidence": str(latest_state_confidence or "").strip().lower(),
                "geo_confidence": str(latest_geo_confidence or "").strip().lower(),
                "brevity_summary": str(latest_brevity_summary or "").strip(),
            }
            if candidate["report_group"]:
                sitrep_report_groups.add(candidate["report_group"])
            existing = statuses.get(call)
            if existing is None:
                statuses[call] = candidate
                continue
            existing_source = str(existing.get("status_source") or "").strip().upper()
            existing_ts = self._safe_float(existing.get("updated_utc_ts"), 0.0)
            # Preserve manual operator override until newer unified data arrives.
            if existing_source == "MANUAL" and existing_ts >= updated_ts:
                continue
            if updated_ts >= existing_ts:
                statuses[call] = candidate
        report_groups_sorted = sorted(sitrep_report_groups)
        if report_groups_sorted != self._sitrep_report_groups:
            self._sitrep_report_groups = report_groups_sorted
            if hasattr(self, "group_filter_combo"):
                self._refresh_group_filter_options()

        self._query_cache_set(cache_key, dict(statuses))
        return statuses

    def _spotter_form_codes_for_flag(self, flag: str) -> Optional[set[str]]:
        try:
            return form_codes_enabled_for(self.settings, flag=flag)
        except Exception:
            return None

    def _load_spotter_map_activity(self) -> Dict[str, Dict]:
        map_codes = self._spotter_form_codes_for_flag("map")
        if map_codes is not None:
            weather_codes = self._spotter_weather_form_codes_for_map() or set()
            map_codes = set(map_codes) - set(weather_codes)
        if map_codes is not None and not map_codes:
            return {}
        cache_key = ("spotter_map_activity", tuple(sorted(map_codes)) if map_codes is not None else "__legacy__")
        cached = self._query_cache_get(cache_key)
        if isinstance(cached, dict):
            return {str(k): dict(v) if isinstance(v, dict) else v for k, v in cached.items()}
        out: Dict[str, Dict] = {}
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return out
        if not db_path.exists():
            return out
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            base_sql = """
                SELECT from_call, form_id, utc_ts, utc_str, decoded_text
                FROM spotter_traffic
            """
            params: tuple = ()
            if map_codes is not None:
                form_ids = sorted(code[2:] for code in map_codes if code.startswith("F!"))
                placeholders = ",".join(["?"] * len(form_ids))
                base_sql += f" WHERE form_id IN ({placeholders})"
                params = tuple(form_ids)
            else:
                base_sql += " WHERE 1=0"
            cur.execute(base_sql + " ORDER BY COALESCE(utc_ts, 0) DESC, id DESC LIMIT 1000", params)
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("StationsMap: failed to load mapped Spotter form activity: %s", e)
            return out
        for from_call, form_id, utc_ts, utc_str, decoded_text in rows:
            call = (from_call or "").strip().upper()
            if not call or call in out:
                continue
            form = str(form_id or "").strip().upper()
            if form and not form.startswith("F!"):
                form = f"F!{form}"
            first_line = ""
            if decoded_text:
                first_line = str(decoded_text or "").strip().splitlines()[0][:80]
            out[call] = {
                "form_id": form,
                "utc_ts": self._safe_float(utc_ts, 0.0),
                "utc_str": str(utc_str or "").strip(),
                "summary": first_line,
            }
        self._query_cache_set(cache_key, dict(out))
        return out

    def _spotter_weather_form_codes_for_map(self) -> Optional[set[str]]:
        try:
            return forms_enabled_for(self.settings, purpose=PURPOSE_WEATHER, flag="map")
        except Exception:
            return set()

    def _spotter_alert_form_codes_for_map(self) -> set[str]:
        try:
            return forms_enabled_for(self.settings, purpose=PURPOSE_HAZARD, flag="map") or set()
        except Exception:
            return set()

    def _spotter_infrastructure_form_codes_for_map(self) -> set[str]:
        try:
            codes = forms_enabled_for(self.settings, purpose=PURPOSE_INFRASTRUCTURE, flag="map") or set()
            status_codes = forms_enabled_for(self.settings, flag="status") or set()
            codes |= {code for code in status_codes if code in {"F!301", "F!304", "F!306"}}
            return codes
        except Exception:
            return set()

    @staticmethod
    def _classify_weather_text(text: object) -> tuple[str, str]:
        lower = str(text or "").lower()
        severe_terms = (
            "tornado",
            "warning",
            "flash flood",
            "flooding",
            "wildfire",
            "evacuation",
            "damaging wind",
            "hail",
            "severe",
        )
        caution_terms = (
            "watch",
            "thunderstorm",
            "lightning",
            "heavy rain",
            "high wind",
            "ice",
            "freezing",
            "snow",
            "smoke",
            "heat",
        )
        severity = "routine"
        if any(term in lower for term in severe_terms):
            severity = "severe"
        elif any(term in lower for term in caution_terms):
            severity = "caution"

        strong_wind = any(term in lower for term in ("high wind", "damaging wind", "gust", "wind gust"))
        if any(term in lower for term in ("wildfire", "fire", "smoke")):
            icon = "fire"
        elif any(term in lower for term in ("flood", "high water", "washed out")):
            icon = "flood"
        elif any(term in lower for term in ("tornado", "thunderstorm", "lightning", "hail")):
            icon = "storm"
        elif any(term in lower for term in ("snow", "ice", "freezing", "sleet")):
            icon = "snow"
        elif any(term in lower for term in ("heat", "hot", "temperature")):
            icon = "heat"
        elif strong_wind:
            icon = "wind"
        elif any(term in lower for term in ("rain", "precip", "drizzle")):
            icon = "rain"
        elif any(term in lower for term in ("wind", "breeze")):
            icon = "wind"
        else:
            icon = "general"
        return icon, severity

    @staticmethod
    def _summarize_weather_text(text: object, *, max_len: int = 150) -> str:
        lines = [ln.strip() for ln in str(text or "").splitlines() if ln.strip()]
        if not lines:
            return "Weather report received"
        preferred_terms = (
            "weather",
            "storm",
            "rain",
            "wind",
            "snow",
            "ice",
            "flood",
            "fire",
            "smoke",
            "temperature",
            "visibility",
            "remarks",
            "narrative",
            "status",
        )
        picked: List[str] = []
        for line in lines:
            lower = line.lower()
            if "(no response)" in lower or "unknown" == lower:
                continue
            if any(term in lower for term in preferred_terms):
                picked.append(line)
            if len(picked) >= 3:
                break
        if not picked:
            picked = [line for line in lines[:3] if "(no response)" not in line.lower()]
        summary = "; ".join(picked).strip() or "Weather report received"
        return summary[: max_len - 1].rstrip() + "..." if len(summary) > max_len else summary

    def _load_spotter_weather_reports(self) -> List[Dict[str, object]]:
        weather_codes = self._spotter_weather_form_codes_for_map()
        if weather_codes is not None and not weather_codes:
            return []
        cache_key = ("spotter_weather_reports", tuple(sorted(weather_codes)) if weather_codes is not None else "__none__")
        cached = self._query_cache_get(cache_key)
        if isinstance(cached, list):
            return [dict(row) for row in cached if isinstance(row, dict)]
        out: List[Dict[str, object]] = []
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return out
        if not db_path.exists():
            return out
        cutoff = time.time() - WEATHER_REPORT_MAX_AGE_SEC
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            form_ids = sorted(code[2:] for code in (weather_codes or set()) if str(code).startswith("F!"))
            if not form_ids:
                conn.close()
                return out
            placeholders = ",".join(["?"] * len(form_ids))
            cur.execute(
                f"""
                SELECT from_call, form_id, utc_ts, utc_str, decoded_text, raw_text
                FROM spotter_traffic
                WHERE form_id IN ({placeholders})
                  AND COALESCE(utc_ts, 0) >= ?
                ORDER BY COALESCE(utc_ts, 0) DESC, id DESC
                LIMIT 1500
                """,
                tuple(form_ids) + (cutoff,),
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("StationsMap: failed to load Spotter weather reports: %s", e)
            return out
        for from_call, form_id, utc_ts, utc_str, decoded_text, raw_text in rows:
            call = str(from_call or "").strip().upper()
            if not call:
                continue
            text = str(decoded_text or raw_text or "")
            icon, severity = self._classify_weather_text(text)
            form = str(form_id or "").strip().upper()
            if form and not form.startswith("F!"):
                form = f"F!{form}"
            out.append(
                {
                    "callsign": call,
                    "form_id": form,
                    "utc_ts": self._safe_float(utc_ts, 0.0),
                    "utc_str": str(utc_str or "").strip(),
                    "summary": self._summarize_weather_text(text),
                    "icon": icon,
                    "severity": severity,
                }
            )
        self._query_cache_set(cache_key, list(out))
        return out

    @staticmethod
    def _classify_alert_text(text: object) -> tuple[str, str]:
        lower = str(text or "").lower()
        if any(term in lower for term in ("warning", "evacuation", "immediate", "urgent", "emergency", "severe")):
            severity = "severe"
        elif any(term in lower for term in ("watch", "alert", "awareness", "prepare", "activation", "expected")):
            severity = "caution"
        else:
            severity = "routine"
        if any(term in lower for term in ("evacuation", "shelter")):
            icon = "evacuation"
        elif any(term in lower for term in ("rfi", "request for information", "information needed")):
            icon = "rfi"
        elif any(term in lower for term in ("warning", "alert", "awareness", "emergency")):
            icon = "warning"
        else:
            icon = "notice"
        return icon, severity

    @staticmethod
    def _classify_infrastructure_text(text: object) -> tuple[str, str]:
        lower = str(text or "").lower()
        if any(term in lower for term in ("not functioning", "down", "outage", "failed", "unavailable", "not available")):
            severity = "severe"
        elif any(term in lower for term in ("partially", "degraded", "unstable", "intermittent", "limited", "reduced")):
            severity = "caution"
        elif any(term in lower for term in ("functioning", "stable", "normal", "available")):
            severity = "routine"
        else:
            severity = "unknown"
        if any(term in lower for term in ("road", "bridge", "transport", "closure", "highway", "route")):
            icon = "transport"
        elif any(term in lower for term in ("general utility", "local services", "utility status")):
            icon = "utility"
        elif any(term in lower for term in ("water", "public water", "sewage", "waste")):
            icon = "water"
        elif any(term in lower for term in ("internet", "phone", "cell", "communications", "radio", "comms")):
            icon = "comms"
        elif any(term in lower for term in ("power", "grid", "generator", "electric")):
            icon = "power"
        else:
            icon = "utility"
        return icon, severity

    @staticmethod
    def _summarize_operational_text(text: object, *, max_len: int = 160) -> str:
        lines = [ln.strip() for ln in str(text or "").splitlines() if ln.strip()]
        useful = [
            line
            for line in lines
            if "(no response)" not in line.lower()
            and line.lower() not in {"unknown", "n/a"}
        ]
        summary = "; ".join(useful[:3]).strip() or "Report received"
        return summary[: max_len - 1].rstrip() + "..." if len(summary) > max_len else summary

    def _load_spotter_layer_reports(
        self,
        *,
        layer_name: str,
        form_codes: set[str],
        max_age_sec: int,
        classifier,
        summarizer,
    ) -> List[Dict[str, object]]:
        if not form_codes:
            return []
        cache_key = (f"spotter_{layer_name}_reports", tuple(sorted(form_codes)))
        cached = self._query_cache_get(cache_key)
        if isinstance(cached, list):
            return [dict(row) for row in cached if isinstance(row, dict)]
        out: List[Dict[str, object]] = []
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return out
        if not db_path.exists():
            return out
        cutoff = time.time() - max_age_sec
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            form_ids = sorted(code[2:] for code in form_codes if str(code).startswith("F!"))
            if not form_ids:
                conn.close()
                return out
            placeholders = ",".join(["?"] * len(form_ids))
            cur.execute(
                f"""
                SELECT from_call, form_id, utc_ts, utc_str, decoded_text, raw_text
                FROM spotter_traffic
                WHERE form_id IN ({placeholders})
                  AND COALESCE(utc_ts, 0) >= ?
                ORDER BY COALESCE(utc_ts, 0) DESC, id DESC
                LIMIT 1500
                """,
                tuple(form_ids) + (cutoff,),
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("StationsMap: failed to load Spotter %s reports: %s", layer_name, e)
            return out
        for from_call, form_id, utc_ts, utc_str, decoded_text, raw_text in rows:
            call = str(from_call or "").strip().upper()
            if not call:
                continue
            text = str(decoded_text or raw_text or "")
            icon, severity = classifier(text)
            form = str(form_id or "").strip().upper()
            if form and not form.startswith("F!"):
                form = f"F!{form}"
            out.append(
                {
                    "callsign": call,
                    "form_id": form,
                    "utc_ts": self._safe_float(utc_ts, 0.0),
                    "utc_str": str(utc_str or "").strip(),
                    "summary": summarizer(text),
                    "icon": icon,
                    "severity": severity,
                }
            )
        self._query_cache_set(cache_key, list(out))
        return out

    def _load_spotter_alert_reports(self) -> List[Dict[str, object]]:
        return self._load_spotter_layer_reports(
            layer_name="alert",
            form_codes=self._spotter_alert_form_codes_for_map(),
            max_age_sec=ALERT_REPORT_MAX_AGE_SEC,
            classifier=self._classify_alert_text,
            summarizer=self._summarize_operational_text,
        )

    def _load_spotter_infrastructure_reports(self) -> List[Dict[str, object]]:
        return self._load_spotter_layer_reports(
            layer_name="infrastructure",
            form_codes=self._spotter_infrastructure_form_codes_for_map(),
            max_age_sec=INFRASTRUCTURE_REPORT_MAX_AGE_SEC,
            classifier=self._classify_infrastructure_text,
            summarizer=self._summarize_operational_text,
        )

    def _load_observation_operational_reports(
        self,
        *,
        layer_name: str,
        max_age_sec: int,
    ) -> List[Dict[str, object]]:
        """Load read-only observation projection rows for map review layers."""
        if not bool(getattr(self, "_observation_focus_enabled", False)):
            return []
        focus_mode = str(getattr(self, "_observation_focus_mode", "") or "all_reports").strip().lower()
        topic_filter = self._selected_map_topic_filter()
        cache_key = (
            f"observation_{layer_name}_reports",
            int(max_age_sec or 0),
            focus_mode,
            topic_filter,
        )
        cached = self._query_cache_get(cache_key, ttl_sec=6.0)
        if isinstance(cached, list):
            return [dict(row) for row in cached if isinstance(row, dict)]
        out: List[Dict[str, object]] = []
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return out
        if not db_path.exists():
            return out
        since_utc = ""
        if max_age_sec and max_age_sec > 0:
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=max_age_sec)
            since_utc = cutoff.replace(microsecond=0).isoformat()
        try:
            view_rows = map_observation_rows(
                db_path,
                ObservationQuery(since_utc=since_utc, limit=1500),
                layer_enabled=True,
                allow_unconfirmed_local=False,
                exercise_layer=False,
            )
        except Exception as e:
            log.debug("StationsMap: failed to load observation %s reports: %s", layer_name, e)
            return out

        wanted_sources = self._observation_focus_sources(focus_mode)
        for view_row in view_rows:
            obs = view_row.observation
            if obs.source_family not in wanted_sources:
                continue
            source_family = str(obs.source_family or "").strip().lower()
            eligibility = view_row.map_eligibility
            eligibility_allowed = bool(eligibility is not None and eligibility.allowed)
            if (
                not eligibility_allowed
                and source_family == "condition_alert"
                and str(obs.from_call or "").strip()
            ):
                # Condition alerts often carry the sender/target and condition
                # level, not a report grid. Let the event builder place them
                # from the station/roster lookup when available.
                eligibility_allowed = True
            if eligibility is None or not eligibility_allowed:
                continue
            topics = {str(topic).strip() for topic in obs.observed_topics if str(topic).strip()}
            if topic_filter and topic_filter not in topics:
                continue
            if layer_name == "alert" and source_family == "condition_alert":
                include = True
                icon, severity = "warning", "caution"
            elif source_family == "condition_alert":
                include = False
                icon, severity = "warning", "caution"
            elif layer_name == "alert":
                include = bool(
                    topics.intersection({"Fire", "Weather", "Shelter", "Medical", "General Intel"})
                    or str(obs.status or "").strip().upper() in {"WATCH", "PRIORITY", "EMERGENCY", "RED", "YELLOW"}
                )
                classifier = self._classify_alert_text
                text = " ".join(part for part in (obs.subject, obs.summary, " ".join(sorted(topics))) if part)
                icon, severity = classifier(text)
            else:
                include = bool(
                    topics.intersection({"Infrastructure", "Power", "Water", "Comms", "Fuel", "Travel/Roads"})
                )
                classifier = self._classify_infrastructure_text
                text = " ".join(part for part in (obs.subject, obs.summary, " ".join(sorted(topics))) if part)
                icon, severity = classifier(text)
            if not include:
                continue
            form = str((obs.provenance or {}).get("form_name", "") or "").strip()
            out.append(
                {
                    "callsign": str(obs.from_call or "").strip().upper(),
                    "form_id": form,
                    "utc_ts": self._observation_ts(obs.event_utc or obs.received_utc),
                    "utc_str": str(obs.event_utc or obs.received_utc or "").strip(),
                    "summary": str(obs.subject or obs.summary or "Observation received").strip(),
                    "icon": icon,
                    "severity": severity,
                    "lat": obs.lat,
                    "lon": obs.lon,
                    "grid": obs.grid,
                    "source_family": obs.source_family,
                    "source_label": self._map_report_source_label(obs.source_family, obs.source_app),
                    "source_app": obs.source_app,
                    "to_target": obs.to_target,
                    "topics": sorted(topics),
                    "state": obs.state,
                    "location_confidence": obs.location_confidence,
                    "auth_state": obs.auth_state,
                    "trusted_state": obs.trusted_state,
                    "confirmed_state": obs.confirmed_state,
                    "eligibility": eligibility.reason_text,
                }
            )
        self._query_cache_set(cache_key, list(out))
        return out

    @staticmethod
    def _observation_focus_sources(focus_mode: str) -> Set[str]:
        mode = str(focus_mode or "").strip().lower()
        if mode == "hf_reports":
            return {"spotter", "condition_alert"}
        if mode == "local_reports":
            return {"local_report"}
        if mode == "rf_pins":
            return {"rf_pin"}
        return {"spotter", "local_report", "condition_alert", "rf_pin"}

    def _selected_map_topic_filter(self) -> str:
        combo = getattr(self, "_map_topic_filter_combo", None)
        if combo is None:
            return ""
        try:
            value = str(combo.currentText() or "").strip()
        except Exception:
            return ""
        if not value or value == "All Topics":
            return ""
        return value

    @staticmethod
    def _map_report_age_text(ts_value: object, *, now: Optional[float] = None) -> str:
        try:
            ts = float(ts_value or 0.0)
        except Exception:
            ts = 0.0
        if ts <= 0:
            return "unknown age"
        now_ts = time.time() if now is None else float(now)
        seconds = max(0, int(now_ts - ts))
        if seconds < 90:
            return "now" if seconds < 15 else f"{seconds}s ago"
        minutes = seconds // 60
        if minutes < 60:
            return f"{minutes} min ago"
        if minutes < 24 * 60:
            hours = minutes // 60
            rem = minutes % 60
            return f"{hours}:{rem:02d} h ago"
        days = minutes // (24 * 60)
        return f"{days} day{'s' if days != 1 else ''} ago"

    @staticmethod
    def _map_report_source_label(source_family: object, source_app: object = "") -> str:
        source = str(source_family or "").strip().lower()
        app = str(source_app or "").strip()
        if source == "spotter":
            return "HF JS8Spotter"
        if source == "rf_pin":
            return "RF Pin"
        if not source:
            return "HF Report"
        if source == "local_report":
            return "Local Report"
        label = source_family_label(source or app or "report")
        return str(label or "Report").strip()

    @staticmethod
    def _map_report_source_kind(source_family: object) -> str:
        source = str(source_family or "").strip().lower()
        if source == "local_report":
            return "local"
        if source == "rf_pin":
            return "pin"
        if source == "mixed":
            return "mixed"
        return "hf"

    @staticmethod
    def _compact_report_status_line(report: Dict[str, object]) -> str:
        source_family = str(report.get("source_family") or "").strip().lower()
        auth = str(report.get("auth_state") or "").strip()
        trusted = str(report.get("trusted_state") or "").strip()
        confirmed = str(report.get("confirmed_state") or "").strip().upper()
        parts: List[str] = []
        if source_family == "local_report" and confirmed:
            parts.append(f"Local: {confirmed.replace('_', ' ').title()}")
        if auth:
            auth_text = auth.replace("_", " ").title()
            if trusted:
                auth_text = f"{auth_text}, {trusted.replace('_', ' ').title()}"
            parts.append(f"Auth: {auth_text}")
        elif trusted:
            parts.append(f"Trust: {trusted.replace('_', ' ').title()}")
        return " | ".join(parts)

    @staticmethod
    def _map_report_location_line(report: Dict[str, object]) -> str:
        state = str(report.get("state") or "").strip().upper()
        grid = str(report.get("grid") or "").strip().upper()
        confidence = str(report.get("location_confidence") or "").strip().replace("_", " ").title()
        area = " / ".join(part for part in (state, grid) if part)
        if not area:
            area = "Mapped location"
        return f"Area: {area}" + (f" ({confidence})" if confidence else "")

    def _map_report_detail_lines(self, report: Dict[str, object], *, now: Optional[float] = None) -> List[str]:
        source = str(report.get("source_label") or self._map_report_source_label(report.get("source_family"))).strip()
        call = str(report.get("callsign") or "").strip().upper()
        to_target = str(report.get("to_target") or "").strip().lstrip("@")
        form = str(report.get("form_id") or "").strip()
        summary = str(report.get("summary") or "Report received").strip()
        raw_topics = report.get("topics", [])
        topics = (
            [str(t).strip() for t in raw_topics if str(t).strip()]
            if isinstance(raw_topics, (list, tuple, set))
            else []
        )
        age = self._map_report_age_text(report.get("utc_ts"), now=now)
        route = " -> ".join(part for part in (call, to_target) if part)
        heading_parts = [source]
        if form:
            heading_parts.append(form)
        if route:
            heading_parts.append(route)
        lines = [f"{' | '.join(heading_parts)} | {age}"]
        if topics:
            lines.append(f"Topics: {', '.join(topics[:4])}" + ("..." if len(topics) > 4 else ""))
        lines.append(self._map_report_location_line(report))
        status_line = self._compact_report_status_line(report)
        if status_line:
            lines.append(status_line)
        if summary:
            lines.append(f"Summary: {summary}")
        return lines

    @staticmethod
    def _observation_ts(value: object) -> float:
        text = str(value or "").strip()
        if not text:
            return 0.0
        try:
            parsed = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=datetime.timezone.utc)
            return float(parsed.timestamp())
        except Exception:
            return 0.0

    @staticmethod
    def _report_position(
        report: Dict[str, object],
        station_lookup: Dict[str, StationPoint],
    ) -> tuple[Optional[float], Optional[float]]:
        call = str(report.get("callsign") or "").strip().upper()
        pt = station_lookup.get(call)
        if pt is None:
            base = JS8LogLinkIndexer._base_callsign(call)
            pt = station_lookup.get(base) if base else None
        if pt is not None:
            return float(pt.lat or 0.0), float(pt.lon or 0.0)
        lat = report.get("lat")
        lon = report.get("lon")
        try:
            if lat is not None and lon is not None:
                return float(lat), float(lon)
        except Exception:
            pass
        grid = str(report.get("grid") or "").strip().upper()
        if grid:
            ll = maidenhead_to_latlon(grid)
            if ll:
                return float(ll[0]), float(ll[1])
        return None, None

    def _build_weather_map_events(self, station_lookup: Dict[str, StationPoint]) -> List[Dict[str, object]]:
        reports = self._cached_map_value(
            "spotter_weather_reports",
            {},
            self._load_spotter_weather_reports,
            ttl_sec=6.0,
        )
        if not reports:
            return []
        buckets: Dict[tuple[int, int], Dict[str, object]] = {}
        now = time.time()
        for report in reports:
            if not isinstance(report, dict):
                continue
            call = str(report.get("callsign") or "").strip().upper()
            lat, lon = self._report_position(report, station_lookup)
            if lat is None or lon is None:
                continue
            key = (
                int(round(lat / WEATHER_CLUSTER_DEGREES)),
                int(round(lon / WEATHER_CLUSTER_DEGREES)),
            )
            severity = str(report.get("severity") or "unknown").strip().lower()
            icon = str(report.get("icon") or "general").strip().lower()
            rank = WEATHER_SEVERITY_RANK.get(severity, 0)
            ts = self._safe_float(report.get("utc_ts"), 0.0)
            bucket = buckets.setdefault(
                key,
                {
                    "lat_sum": 0.0,
                    "lon_sum": 0.0,
                    "count": 0,
                    "latest_ts": 0.0,
                    "max_rank": 0,
                    "severity": "unknown",
                    "icon": "general",
                    "reports": [],
                    "callsigns": set(),
                },
            )
            bucket["lat_sum"] = float(bucket.get("lat_sum", 0.0)) + lat
            bucket["lon_sum"] = float(bucket.get("lon_sum", 0.0)) + lon
            bucket["count"] = int(bucket.get("count", 0) or 0) + 1
            bucket["callsigns"].add(call)
            if rank > int(bucket.get("max_rank", 0) or 0) or ts >= self._safe_float(bucket.get("latest_ts"), 0.0):
                if rank >= int(bucket.get("max_rank", 0) or 0):
                    bucket["icon"] = icon
                    bucket["severity"] = severity
                    bucket["max_rank"] = rank
            if ts > self._safe_float(bucket.get("latest_ts"), 0.0):
                bucket["latest_ts"] = ts
            bucket["reports"].append(report)

        events: List[Dict[str, object]] = []
        for bucket in buckets.values():
            count = max(1, int(bucket.get("count", 0) or 0))
            reports_sorted = sorted(
                [r for r in bucket.get("reports", []) if isinstance(r, dict)],
                key=lambda r: self._safe_float(r.get("utc_ts"), 0.0),
                reverse=True,
            )
            latest_ts = self._safe_float(bucket.get("latest_ts"), 0.0)
            age_minutes = int(max(0.0, now - latest_ts) // 60) if latest_ts else 0
            age_label = f"{age_minutes}m ago" if age_minutes < 120 else f"{age_minutes // 60}h ago"
            calls = sorted(str(c) for c in bucket.get("callsigns", set()) if str(c))
            detail_lines = [
                f"Weather Reports: {count}",
                f"Newest: {age_label}",
                f"Severity: {str(bucket.get('severity') or 'unknown').title()}",
                f"Sources: {', '.join(calls[:6])}" + ("..." if len(calls) > 6 else ""),
            ]
            for report in reports_sorted[:4]:
                summary = str(report.get("summary") or "Weather report received").strip()
                source = str(report.get("callsign") or "").strip().upper()
                form = str(report.get("form_id") or "").strip()
                detail_lines.append(f"{source} {form}: {summary}".strip())
            events.append(
                {
                    "lat": float(bucket.get("lat_sum", 0.0)) / count,
                    "lon": float(bucket.get("lon_sum", 0.0)) / count,
                    "count": count,
                    "icon": str(bucket.get("icon") or "general"),
                    "severity": str(bucket.get("severity") or "unknown"),
                    "latest_ts": latest_ts,
                    "age": age_label,
                    "tooltip": "<br/>".join(html.escape(line) for line in detail_lines if line),
                }
            )
        return sorted(
            events,
            key=lambda row: (
                WEATHER_SEVERITY_RANK.get(str(row.get("severity") or "unknown"), 0),
                self._safe_float(row.get("latest_ts"), 0.0),
            ),
            reverse=True,
        )

    def _build_spotter_operational_events(
        self,
        station_lookup: Dict[str, StationPoint],
        *,
        layer_name: str,
        display_label: str,
        reports_loader,
    ) -> List[Dict[str, object]]:
        reports = self._cached_map_value(
            f"spotter_{layer_name}_reports",
            {},
            reports_loader,
            ttl_sec=6.0,
        )
        if not reports:
            return []
        buckets: Dict[tuple[int, int], Dict[str, object]] = {}
        now = time.time()
        for report in reports:
            if not isinstance(report, dict):
                continue
            call = str(report.get("callsign") or "").strip().upper()
            lat, lon = self._report_position(report, station_lookup)
            if lat is None or lon is None:
                continue
            key = (
                int(round(lat / WEATHER_CLUSTER_DEGREES)),
                int(round(lon / WEATHER_CLUSTER_DEGREES)),
            )
            severity = str(report.get("severity") or "unknown").strip().lower()
            icon = str(report.get("icon") or "general").strip().lower()
            rank = WEATHER_SEVERITY_RANK.get(severity, 0)
            ts = self._safe_float(report.get("utc_ts"), 0.0)
            bucket = buckets.setdefault(
                key,
                {
                    "lat_sum": 0.0,
                    "lon_sum": 0.0,
                    "count": 0,
                    "latest_ts": 0.0,
                    "max_rank": 0,
                    "severity": "unknown",
                    "icon": icon,
                    "reports": [],
                    "callsigns": set(),
                },
            )
            bucket["lat_sum"] = float(bucket.get("lat_sum", 0.0)) + lat
            bucket["lon_sum"] = float(bucket.get("lon_sum", 0.0)) + lon
            bucket["count"] = int(bucket.get("count", 0) or 0) + 1
            bucket["callsigns"].add(call)
            if rank > int(bucket.get("max_rank", 0) or 0) or ts >= self._safe_float(bucket.get("latest_ts"), 0.0):
                if rank >= int(bucket.get("max_rank", 0) or 0):
                    bucket["icon"] = icon
                    bucket["severity"] = severity
                    bucket["max_rank"] = rank
            if ts > self._safe_float(bucket.get("latest_ts"), 0.0):
                bucket["latest_ts"] = ts
            bucket["reports"].append(report)

        events: List[Dict[str, object]] = []
        for bucket in buckets.values():
            count = max(1, int(bucket.get("count", 0) or 0))
            reports_sorted = sorted(
                [r for r in bucket.get("reports", []) if isinstance(r, dict)],
                key=lambda r: self._safe_float(r.get("utc_ts"), 0.0),
                reverse=True,
            )
            latest_ts = self._safe_float(bucket.get("latest_ts"), 0.0)
            age_minutes = int(max(0.0, now - latest_ts) // 60) if latest_ts else 0
            age_label = self._map_report_age_text(latest_ts, now=now)
            calls = sorted(str(c) for c in bucket.get("callsigns", set()) if str(c))
            source_counts: Dict[str, int] = {}
            source_kinds: Set[str] = set()
            for report in reports_sorted:
                source = str(report.get("source_label") or self._map_report_source_label(report.get("source_family"))).strip()
                source_counts[source] = source_counts.get(source, 0) + 1
                source_kinds.add(self._map_report_source_kind(report.get("source_family")))
            source_text = ", ".join(f"{label} {count}" for label, count in sorted(source_counts.items()))
            if len(source_kinds) > 1:
                source_kind = "mixed"
            else:
                source_kind = next(iter(source_kinds), "hf")
            detail_lines = [
                f"{display_label}: {count}",
                f"Newest: {age_label}",
                f"Severity: {str(bucket.get('severity') or 'unknown').title()}",
                f"Source Type: {source_text}" if source_text else "",
                f"From: {', '.join(calls[:6])}" + ("..." if len(calls) > 6 else ""),
            ]
            for idx, report in enumerate(reports_sorted[:4]):
                if idx:
                    detail_lines.append("")
                detail_lines.extend(self._map_report_detail_lines(report, now=now))
            events.append(
                {
                    "lat": float(bucket.get("lat_sum", 0.0)) / count,
                    "lon": float(bucket.get("lon_sum", 0.0)) / count,
                    "count": count,
                    "icon": str(bucket.get("icon") or "general"),
                    "severity": str(bucket.get("severity") or "unknown"),
                    "latest_ts": latest_ts,
                    "age": age_label,
                    "source_mix": source_text,
                    "source_kind": source_kind,
                    "tooltip": "<br/>".join(html.escape(line) for line in detail_lines if line),
                }
            )
        return sorted(
            events,
            key=lambda row: (
                WEATHER_SEVERITY_RANK.get(str(row.get("severity") or "unknown"), 0),
                self._safe_float(row.get("latest_ts"), 0.0),
            ),
            reverse=True,
        )

    def _load_sitrep_state_rollup(self, report_group: str = "") -> List[Dict[str, object]]:
        report_group_key = str(report_group or "").strip().upper() or "__ALL__"
        cache_key = ("sitrep_state_rollup", report_group_key)
        cached = self._query_cache_get(cache_key)
        if isinstance(cached, list):
            return [dict(row) for row in cached if isinstance(row, dict)]
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return []
        if not db_path.exists():
            return []
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cols = {
                str(row[1] or "").strip()
                for row in conn.execute("PRAGMA table_info(sitrep_state_rollup)").fetchall()
                if len(row) > 1 and str(row[1] or "").strip()
            }
            js8_expr = "js8_count" if "js8_count" in cols else "0 AS js8_count"
            internet_expr = "internet_count" if "internet_count" in cols else "0 AS internet_count"
            mixed_expr = "mixed_transport_count" if "mixed_transport_count" in cols else "0 AS mixed_transport_count"
            latest_expr = "latest_event_ts" if "latest_event_ts" in cols else "0 AS latest_event_ts"
            cur.execute(
                f"""
                SELECT state_code, callsign_count, red_count, yellow_count, green_count, unknown_count,
                       {js8_expr}, {internet_expr}, {mixed_expr}, {latest_expr}
                FROM sitrep_state_rollup
                WHERE report_group=?
                ORDER BY callsign_count DESC, latest_event_ts DESC, state_code
                """,
                (report_group_key,),
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.debug("StationsMap: failed to load sitrep_state_rollup for %s: %s", report_group_key, e)
            return []
        out = [
            {
                "state_code": str(row[0] or "").strip().upper(),
                "callsign_count": int(row[1] or 0),
                "red_count": int(row[2] or 0),
                "yellow_count": int(row[3] or 0),
                "green_count": int(row[4] or 0),
                "unknown_count": int(row[5] or 0),
                "js8_count": int(row[6] or 0),
                "internet_count": int(row[7] or 0),
                "mixed_transport_count": int(row[8] or 0),
                "latest_event_ts": float(row[9] or 0.0),
            }
            for row in rows
            if str(row[0] or "").strip()
        ]
        self._query_cache_set(cache_key, list(out))
        return out

    def _load_recent_calls(self, max_age_sec: Optional[int], band_filter=None) -> Set[str]:
        if not max_age_sec or max_age_sec <= 0:
            return set()
        band_sig = ""
        try:
            band_sig = json.dumps(band_filter or {"type": "all"}, sort_keys=True, default=str)
        except Exception:
            band_sig = str(band_filter or {"type": "all"})
        cache_key = ("recent_calls", int(max_age_sec), band_sig)
        cached = self._query_cache_get(cache_key, ttl_sec=15.0)
        if isinstance(cached, (set, list, tuple)):
            return {str(c) for c in cached}
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as e:
            log.error("StationsMap: failed to resolve DB path for recent calls: %s", e)
            return set()
        if not db_path.exists():
            return set()
        ts_cut = time.time() - max_age_sec
        calls: Set[str] = set()
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT ts, origin, destination, band, freq_hz FROM js8_links WHERE ts >= ?",
                (ts_cut,),
            )
            rows = cur.fetchall()
            cur.execute(
                "SELECT callsign, last_seen_ts, last_band, last_freq_hz FROM varac_callsign_stats WHERE last_seen_ts >= ?",
                (ts_cut,),
            )
            varac_rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.error("StationsMap: failed to load recent js8_links: %s", e)
            return set()

        bf = band_filter or {"type": "all"}
        for ts, o, d, band, freq_hz in rows:
            band_val = (band or "").upper()
            if bf.get("type") == "band":
                if band_val != str(bf.get("value")).upper():
                    continue
            elif bf.get("type") == "freq":
                target_f = bf.get("value")
                try:
                    freq_mhz = float(freq_hz) / 1_000_000.0 if freq_hz is not None else None
                except Exception:
                    freq_mhz = None
                if freq_mhz is None or target_f is None or abs(freq_mhz - target_f) > 0.001:
                    continue
            if o:
                calls.add((o or "").strip().upper())
            if d:
                calls.add((d or "").strip().upper())
        for cs, last_seen_ts, last_band, last_freq_hz in varac_rows:
            band_val = (last_band or "").upper()
            if bf.get("type") == "band":
                if band_val != str(bf.get("value")).upper():
                    continue
            elif bf.get("type") == "freq":
                target_f = bf.get("value")
                try:
                    freq_mhz = float(last_freq_hz) / 1_000_000.0 if last_freq_hz is not None else None
                except Exception:
                    freq_mhz = None
                if freq_mhz is None or target_f is None or abs(freq_mhz - target_f) > 0.001:
                    continue
            if cs:
                calls.add((cs or "").strip().upper())
        self._query_cache_set(cache_key, set(calls))
        return calls

    def _load_recent_calls_by_band(self, max_age_sec: Optional[int]) -> Dict[str, Set[str]]:
        out: Dict[str, Set[str]] = {band: set() for band in PROP_BANDS}
        if not max_age_sec or max_age_sec <= 0:
            return out
        cache_key = ("recent_calls_by_band", int(max_age_sec))
        cached = self._query_cache_get(cache_key, ttl_sec=15.0)
        if isinstance(cached, dict):
            normalized: Dict[str, Set[str]] = {band: set() for band in PROP_BANDS}
            for band, calls in cached.items():
                band_key = str(band or "").strip().upper()
                if band_key not in normalized:
                    continue
                if isinstance(calls, (set, list, tuple)):
                    normalized[band_key] = {str(c).strip().upper() for c in calls if str(c).strip()}
            return normalized
        try:
            from freqinout.core.config_paths import get_config_dir

            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as e:
            log.error("StationsMap: failed to resolve DB path for recent calls by band: %s", e)
            return out
        if not db_path.exists():
            return out
        ts_cut = time.time() - max_age_sec
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT origin, destination, band FROM js8_links WHERE ts >= ?",
                (ts_cut,),
            )
            rows = cur.fetchall()
            cur.execute(
                "SELECT callsign, last_band FROM varac_callsign_stats WHERE last_seen_ts >= ?",
                (ts_cut,),
            )
            varac_rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.error("StationsMap: failed to load recent calls by band: %s", e)
            return out
        for o, d, band in rows:
            band_key = (band or "").strip().upper()
            if band_key not in out:
                continue
            if o:
                out[band_key].add((o or "").strip().upper())
            if d:
                out[band_key].add((d or "").strip().upper())
        for cs, last_band in varac_rows:
            band_key = (last_band or "").strip().upper()
            if band_key not in out:
                continue
            if cs:
                out[band_key].add((cs or "").strip().upper())
        self._query_cache_set(cache_key, {k: set(v) for k, v in out.items()})
        return out

    def _is_usa_canada(self, lat: float, lon: float) -> bool:
        return 7.0 <= lat <= 83.0 and -172.0 <= lon <= -50.0

    def _links_active(self) -> bool:
        combo_mode, _ = self._parse_link_selection(
            self.link_mode_combo.currentData() if hasattr(self, "link_mode_combo") else ("off", "")
        )
        return bool(combo_mode and combo_mode.lower() != "off")

    def _display_links_for_mode(self, links: List[Dict], sitrep_mode: bool) -> List[Dict]:
        if sitrep_mode:
            return []
        return list(links or [])

    def _load_prop_target_operator_callsigns(self) -> list[str]:
        out: list[str] = []
        try:
            for cs in sorted(self.operator_index.keys()):
                call = (cs or "").strip().upper()
                if call:
                    out.append(call)
        except Exception:
            out = []
        if out:
            return out
        db_path = get_config_dir() / "config" / "freqinout_nets.db"
        if not db_path.exists():
            return out
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT IFNULL(callsign, '')
                FROM operator_checkins
                ORDER BY callsign COLLATE NOCASE
                """
            )
            for (callsign,) in cur.fetchall():
                cs = (callsign or "").strip().upper()
                if cs and cs not in out:
                    out.append(cs)
            conn.close()
        except Exception as e:
            log.debug("StationsMap: failed to load propagation operator options: %s", e)
        return out

    def _prop_target_options(self, target_type: str) -> list[str]:
        target_type = (target_type or "REGION").strip().upper()
        if target_type == "STATE":
            return [s for s in LOWER48_STATES if s in STATE_CENTERS]
        if target_type == "OPERATOR":
            return self._load_prop_target_operator_callsigns()
        return ["ALL"] + sorted(FEMA_REGIONS.keys())

    def _set_prop_target_value_options(self, target_type: str, selected_value: str) -> None:
        if self.prop_target_value_combo is None:
            return
        target_type = (target_type or "REGION").strip().upper()
        selected_value = (selected_value or "").strip().upper()
        if target_type == "REGION" and selected_value == "NATIONAL":
            selected_value = "ALL"
        options = self._prop_target_options(target_type)
        self.prop_target_value_combo.blockSignals(True)
        self.prop_target_value_combo.clear()
        for value in options:
            self.prop_target_value_combo.addItem(value)
        if selected_value:
            idx = self.prop_target_value_combo.findText(selected_value, Qt.MatchFixedString)
            if idx >= 0:
                self.prop_target_value_combo.setCurrentIndex(idx)
            else:
                self.prop_target_value_combo.setEditText(selected_value)
        elif self.prop_target_value_combo.count() > 0:
            self.prop_target_value_combo.setCurrentIndex(0)
        else:
            self.prop_target_value_combo.setEditText("")
        self.prop_target_value_combo.setEditable(target_type == "OPERATOR")
        self.prop_target_value_combo.blockSignals(False)

    def _refresh_prop_target_controls(self) -> None:
        if not self.settings or self.prop_target_type_combo is None or self.prop_target_value_combo is None:
            return
        self._prop_target_syncing = True
        try:
            if hasattr(self.settings, "reload"):
                self.settings.reload()
            target_type = (self.settings.get("prop_target_type", "REGION") or "REGION").strip().upper()
            if target_type not in {"REGION", "STATE", "OPERATOR"}:
                target_type = "REGION"
            target_value = (self.settings.get("prop_target_value", "") or "").strip().upper()
            idx = self.prop_target_type_combo.findData(target_type)
            if idx < 0:
                idx = 0
            self.prop_target_type_combo.blockSignals(True)
            self.prop_target_type_combo.setCurrentIndex(idx)
            self.prop_target_type_combo.blockSignals(False)
            self._set_prop_target_value_options(target_type, target_value)
            current_value = (self.prop_target_value_combo.currentText() or "").strip().upper()
            existing_type = (self.settings.get("prop_target_type", "") or "").strip().upper()
            existing_value = (self.settings.get("prop_target_value", "") or "").strip().upper()
            if existing_type != target_type or existing_value != current_value:
                self.settings.set_many(
                    {
                        "prop_target_type": target_type,
                        "prop_target_value": current_value,
                    }
                )
        except Exception as e:
            log.debug("StationsMap: failed to refresh propagation target controls: %s", e)
        finally:
            self._prop_target_syncing = False

    def attach_prop_controls(
        self,
        overlay_chk: QCheckBox | None,
        badge: QLabel | None,
        mode_combo: QComboBox | None = None,
        window_combo: QComboBox | None = None,
    ) -> None:
        # Backward compatibility path for legacy external controls.
        if overlay_chk is not None:
            self.prop_overlay_chk = overlay_chk
        if badge is not None:
            self.prop_badge = badge
        if mode_combo is not None:
            self.prop_mode_combo = mode_combo
        if window_combo is not None:
            self.prop_window_combo = window_combo
        # Sync checkbox state from settings
        if self.prop_overlay_chk is not None:
            self.prop_overlay_chk.blockSignals(True)
            self.prop_overlay_chk.setChecked(bool(self._bool_setting("map_prop_overlay", False)))
            self.prop_overlay_chk.blockSignals(False)
            self.prop_overlay_enabled = self.prop_overlay_chk.isChecked()
        try:
            self.prop_adaptive_enabled = self._bool_setting("map_prop_adaptive", True)
        except Exception:
            pass
        if self.prop_mode_combo is not None:
            try:
                mode = (self.settings.get("map_prop_mode", "blended") or "blended").strip().lower()
            except Exception:
                mode = "blended"
            if mode == "adaptive":
                mode = "actual"
            if mode not in ("model", "actual", "blended"):
                mode = "blended"
            self.prop_mode = mode
            idx = self.prop_mode_combo.findData(mode)
            if idx >= 0:
                self.prop_mode_combo.blockSignals(True)
                self.prop_mode_combo.setCurrentIndex(idx)
                self.prop_mode_combo.blockSignals(False)
        if self.prop_window_combo is not None:
            try:
                hours = int(self.settings.get("map_prop_window_hours", 6) or 6)
            except Exception:
                hours = 6
            self.prop_window_hours = hours
            idx = self.prop_window_combo.findData(hours)
            if idx >= 0:
                self.prop_window_combo.blockSignals(True)
                self.prop_window_combo.setCurrentIndex(idx)
                self.prop_window_combo.blockSignals(False)
        # Update badge immediately with current target.
        target_ctx = self._prop_target_context()
        target_label = str(target_ctx.get("label") or "National")
        if self.prop_overlay_enabled:
            region_scores = self._compute_region_scores("")
            state_scores = self._compute_state_scores()
            best_band, best_score = self._best_band_for_target(target_ctx, region_scores, state_scores)
            self._update_prop_badge(target_label, best_band, best_score)
        else:
            self._update_prop_badge(target_label, "", 0.0)

    def _effective_prop_mode(self) -> str:
        mode = (self.prop_mode or "blended").strip().lower()
        if mode == "adaptive":
            mode = "actual"
        if mode not in ("model", "actual", "blended"):
            mode = "blended"
        return mode

    def _prop_window_seconds(self) -> int:
        try:
            hours = int(self.prop_window_hours or 6)
        except Exception:
            hours = 6
        hours = max(1, min(hours, 168))
        return hours * 3600

    # ------------- Propagation overlay ------------- #
    def _load_prop_profiles(self) -> Dict[str, Dict]:
        return self._prop_service.load_profiles()

    def _load_prop_db_cache(self) -> None:
        self._prop_service.load_climatology_cache()

    def _lookup_db_score(self, band: str, lat: float, lon: float, month: int) -> Optional[float]:
        return self._prop_service.lookup_db_score(band, lat, lon, month)

    def _get_user_latlon(self) -> Optional[tuple[float, float]]:
        if not self.settings:
            return None
        grid = (self.settings.get("operator_grid6", "") or self.settings.get("operator_grid", "") or "").strip().upper()
        if grid:
            ll = maidenhead_to_latlon(grid)
            if ll:
                return ll
        return None

    def _get_origin_grid6(self) -> str:
        if not self.settings:
            return ""
        grid = (self.settings.get("operator_grid6", "") or self.settings.get("operator_grid", "") or "").strip().upper()
        return grid[:6] if len(grid) >= 4 else ""

    def _blend_settings_snapshot(self) -> Dict[str, object]:
        if not self.settings:
            return {}
        return {
            "prop_blend_enabled": self.settings.get("prop_blend_enabled", 1),
            "prop_empirical_alpha": self.settings.get("prop_empirical_alpha", 2.0),
            "prop_empirical_beta": self.settings.get("prop_empirical_beta", 3.0),
            "prop_decay_half_life_days": self.settings.get("prop_decay_half_life_days", 75),
            "prop_blend_gate_attempt_min": self.settings.get("prop_blend_gate_attempt_min", 8.0),
            "prop_blend_gate_unique_days_min": self.settings.get("prop_blend_gate_unique_days_min", 3),
            "prop_blend_max_weight": self.settings.get("prop_blend_max_weight", 0.85),
            "prop_blend_recent_window_days": self.settings.get("prop_blend_recent_window_days", 30),
            "prop_blend_history_cap_days": self.settings.get("prop_blend_history_cap_days", 365),
        }

    @staticmethod
    def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        return PropagationService.haversine_km(lat1, lon1, lat2, lon2)

    def _band_score(self, band: str, distance_km: float, hour_utc: int) -> float:
        return self._prop_service.band_score(band, distance_km, hour_utc)

    def _local_hour_from_lon(self, utc_dt: datetime.datetime, lon: float) -> int:
        return self._prop_service.local_hour_from_lon(utc_dt, lon)

    def _path_band_weight(self, band: str, distance_km: float, hour_local: int) -> float:
        return self._prop_service.path_band_weight(band, distance_km, hour_local)

    def _modeled_band_score(
        self,
        band: str,
        dest_lat: float,
        dest_lon: float,
        now_utc: datetime.datetime,
        distance_km: float,
    ) -> float:
        user_ll = self._get_user_latlon()
        if not user_ll:
            return 0.0
        return self._prop_service.modeled_band_score(
            band=band,
            user_ll=user_ll,
            dest_lat=dest_lat,
            dest_lon=dest_lon,
            now_utc=now_utc,
            distance_km=distance_km,
        )

    def _band_score_db(self, band: str, lat: float, lon: float, month: int) -> Optional[float]:
        return self._prop_service.band_score_db(band, lat, lon, month)

    def _diurnal_weight(self, band: str, hour_local: int) -> float:
        return self._prop_service.diurnal_weight(band, hour_local)

    def _stations_for_region(self, region_id: str) -> List[StationPoint]:
        region_id = (region_id or "").strip().upper()
        if not region_id:
            return []
        out = []
        for pt in self.stations:
            region = self.operator_index.get(pt.callsign, {}).get("region")
            if region == region_id:
                out.append(pt)
        return out

    def _stations_for_state(self, state_abbr: str) -> List[StationPoint]:
        state_abbr = (state_abbr or "").strip().upper()
        if not state_abbr:
            return []
        out = []
        for pt in self.stations:
            state = self.operator_index.get(pt.callsign, {}).get("state")
            if state == state_abbr:
                out.append(pt)
        return out

    def _region_centroid(self, region_id: str) -> Optional[tuple[float, float]]:
        region_id = (region_id or "").strip().upper()
        if not region_id:
            return None
        states = FEMA_REGIONS.get(region_id, [])
        if not states:
            return None
        lat_sum = 0.0
        lon_sum = 0.0
        count = 0
        for st in states:
            center = STATE_CENTERS.get(st)
            if center:
                lat_sum += center[0]
                lon_sum += center[1]
                count += 1
        if count == 0:
            return None
        return (lat_sum / count, lon_sum / count)

    def _state_centroid(self, state_abbr: str) -> Optional[tuple[float, float]]:
        state_abbr = (state_abbr or "").strip().upper()
        if not state_abbr:
            return None
        return STATE_CENTERS.get(state_abbr)

    def _normalize_state_abbr(self, value: str) -> str:
        state = (value or "").strip().upper()
        if not state:
            return ""
        if len(state) <= 2:
            return state
        if state in US_STATE_ABBR_FROM_NAME:
            return US_STATE_ABBR_FROM_NAME[state]
        if state in CANADA_PROV_ABBR_FROM_NAME:
            return CANADA_PROV_ABBR_FROM_NAME[state]
        return ""

    def _points_for_region_lower48(self, region_id: str) -> List[tuple[float, float]]:
        region_id = (region_id or "").strip().upper()
        if not region_id:
            return []
        states = FEMA_REGIONS.get(region_id, [])
        return [STATE_CENTERS[s] for s in states if s in LOWER48_STATES and s in STATE_CENTERS]

    def _operator_target_point(self, callsign: str) -> tuple[Optional[tuple[float, float]], str]:
        callsign = (callsign or "").strip().upper()
        if not callsign:
            return None, ""
        for row in self.operator_rows:
            cs = (row.get("callsign") or "").strip().upper()
            if cs != callsign:
                continue
            grid = (row.get("grid") or "").strip().upper()
            state = self._normalize_state_abbr(row.get("state") or "")
            ll = maidenhead_to_latlon(grid) if grid else None
            if ll:
                return ll, state
            if state and state in STATE_CENTERS and state in LOWER48_STATES:
                return STATE_CENTERS[state], state
            return None, state
        return None, ""

    def _prop_target_context(self) -> Dict[str, object]:
        target_type = "REGION"
        target_value = ""
        if self.settings:
            try:
                target_type = (self.settings.get("prop_target_type", "REGION") or "REGION").strip().upper()
            except Exception:
                target_type = "REGION"
            try:
                target_value = (self.settings.get("prop_target_value", "") or "").strip().upper()
            except Exception:
                target_value = ""
        if target_type not in {"REGION", "STATE", "OPERATOR"}:
            target_type = "REGION"
        if target_type == "STATE":
            target_value = self._normalize_state_abbr(target_value)
        context: Dict[str, object] = {
            "type": target_type,
            "value": target_value,
            "label": "National",
            "region_id": "",
            "state_abbr": "",
            "point": None,
        }

        # Default/fallback target: operator state -> FEMA region.
        operator_state = ""
        if self.settings:
            try:
                operator_state = self._normalize_state_abbr(self.settings.get("operator_state", "") or "")
            except Exception:
                operator_state = ""
        fallback_region = STATE_TO_FEMA_REGION.get(operator_state, "")

        if target_type == "REGION":
            if target_value in {"ALL", "NATIONAL"}:
                context["label"] = "National"
                context["value"] = "ALL"
                return context
            region_id = target_value if target_value in FEMA_REGIONS else fallback_region
            if region_id:
                context["label"] = f"Region {region_id}"
                context["region_id"] = region_id
            return context

        if target_type == "STATE":
            state_abbr = target_value if target_value in STATE_CENTERS and target_value in LOWER48_STATES else ""
            if state_abbr:
                context["label"] = state_abbr
                context["state_abbr"] = state_abbr
                context["region_id"] = STATE_TO_FEMA_REGION.get(state_abbr, "")
            elif fallback_region:
                context["label"] = f"Region {fallback_region}"
                context["region_id"] = fallback_region
            return context

        callsign = target_value
        if callsign:
            context["label"] = callsign
            ll, state_abbr = self._operator_target_point(callsign)
            if ll:
                context["point"] = ll
            if state_abbr:
                context["state_abbr"] = state_abbr
                context["region_id"] = STATE_TO_FEMA_REGION.get(state_abbr, "")
            return context
        if fallback_region:
            context["label"] = f"Region {fallback_region}"
            context["region_id"] = fallback_region
        return context

    def _best_band_for_state(self, state_scores: Dict[str, Dict], state_abbr: str) -> tuple[str, float]:
        state_abbr = (state_abbr or "").strip().upper()
        bands = state_scores.get(state_abbr, {}).get("bands", {})
        if not bands:
            return ("", 0.0)
        best_band = max(bands.items(), key=lambda kv: kv[1])
        return best_band[0], float(best_band[1])

    def _best_band_overall_states(self, state_scores: Dict[str, Dict], states: Optional[List[str]] = None) -> tuple[str, float]:
        if not state_scores:
            return ("", 0.0)
        selected_states = states if states else list(state_scores.keys())
        totals: Dict[str, List[float]] = {b: [] for b in PROP_BANDS}
        for state in selected_states:
            entry = state_scores.get(state, {})
            for band, score in (entry.get("bands") or {}).items():
                if band in totals:
                    totals[band].append(float(score))
        best_band = ""
        best_score = 0.0
        for band, vals in totals.items():
            if not vals:
                continue
            avg = sum(vals) / len(vals)
            if avg > best_score:
                best_score = avg
                best_band = band
        return best_band, best_score

    def _best_band_for_target(
        self,
        target_ctx: Dict[str, object],
        region_scores: Dict[str, Dict],
        state_scores: Dict[str, Dict],
    ) -> tuple[str, float]:
        mode = self._effective_prop_mode()
        user_ll = self._get_user_latlon()
        origin_grid6 = self._get_origin_grid6()
        blend_settings = self._blend_settings_snapshot() if mode == "blended" else None
        target_type = str(target_ctx.get("type") or "REGION").upper()
        target_value = str(target_ctx.get("value") or "").upper()
        region_id = str(target_ctx.get("region_id") or "").upper()
        state_abbr = str(target_ctx.get("state_abbr") or "").upper()
        point = target_ctx.get("point")

        # Modeled/blended parity path: use the same point-set model used by ControlFreq.
        if mode in {"model", "blended"} and user_ll:
            points: List[tuple[float, float]] = []
            target_id = ""
            if target_type == "REGION" and region_id:
                points = self._points_for_region_lower48(region_id)
                target_id = region_id
            elif target_type == "STATE" and state_abbr in STATE_CENTERS and state_abbr in LOWER48_STATES:
                points = [STATE_CENTERS[state_abbr]]
                target_id = state_abbr
            elif target_type == "OPERATOR" and isinstance(point, tuple):
                points = [point]
                target_id = target_value
            if not points:
                points = [STATE_CENTERS[s] for s in LOWER48_STATES if s in STATE_CENTERS]
                target_id = "NATIONAL"
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            top = self._prop_service.top_bands_modeled(
                bands=PROP_BANDS,
                mid_utc=now_utc,
                user_ll=user_ll,
                points=points,
                origin_grid6=origin_grid6,
                target_type=target_type,
                target_id=target_id,
                blend_settings=blend_settings,
                limit=1,
            )
            if top:
                return top[0][0], float(top[0][1])

        if target_type == "REGION" and region_id:
            return self._best_band_for_region(region_scores, region_id)
        if target_type == "STATE" and state_abbr:
            return self._best_band_for_state(state_scores, state_abbr)
        if target_type == "OPERATOR":
            if state_abbr:
                return self._best_band_for_state(state_scores, state_abbr)
            if region_id:
                return self._best_band_for_region(region_scores, region_id)
        return self._best_band_overall_states(state_scores, states=LOWER48_STATES)

    def _adaptive_adjustment(self, band: str, region_id: str) -> float:
        if not self.prop_adaptive_enabled:
            return 0.0
        max_age = self.recency_seconds or 24 * 60 * 60
        recent = self._load_recent_calls_by_band(max_age).get((band or "").strip().upper(), set())
        if not recent:
            return 0.0
        stations = self._stations_for_region(region_id)
        if not stations:
            return 0.0
        hit = sum(1 for s in stations if s.callsign.upper() in recent)
        ratio = hit / max(1, len(stations))
        adj = (ratio - 0.5) * 20.0
        return max(-12.0, min(12.0, adj))

    def _adaptive_adjustment_state(self, band: str, state_abbr: str) -> float:
        if not self.prop_adaptive_enabled:
            return 0.0
        max_age = self.recency_seconds or 24 * 60 * 60
        recent = self._load_recent_calls_by_band(max_age).get((band or "").strip().upper(), set())
        if not recent:
            return 0.0
        stations = self._stations_for_state(state_abbr)
        if not stations:
            return 0.0
        hit = sum(1 for s in stations if s.callsign.upper() in recent)
        ratio = hit / max(1, len(stations))
        adj = (ratio - 0.5) * 20.0
        return max(-12.0, min(12.0, adj))

    def _presence_band_weights(self) -> Dict[str, Dict]:
        now_ts = time.time()
        if self._presence_weights_cache and (now_ts - self._presence_weights_ts) < 10:
            return self._presence_weights_cache

        now_utc = datetime.datetime.now(datetime.timezone.utc)
        schedule_presence = self._load_peer_schedule_presence(now_utc)
        observed_by_band = self._load_observed_band_presence(self._prop_window_seconds())

        sched_region: Dict[str, Dict[str, Set[str]]] = {}
        sched_state: Dict[str, Dict[str, Set[str]]] = {}
        obs_region: Dict[str, Dict[str, Set[str]]] = {}
        obs_state: Dict[str, Dict[str, Set[str]]] = {}

        for entry in schedule_presence:
            cs = (entry.get("callsign") or "").strip().upper()
            band = (entry.get("band") or "").strip().upper()
            if not cs or not band:
                continue
            meta = self.operator_index.get(cs, {})
            region = meta.get("region") or ""
            state = meta.get("state") or ""
            if region:
                sched_region.setdefault(region, {}).setdefault(band, set()).add(cs)
            if state:
                sched_state.setdefault(state, {}).setdefault(band, set()).add(cs)

        for band, calls in observed_by_band.items():
            for cs in calls:
                meta = self.operator_index.get(cs, {})
                region = meta.get("region") or ""
                state = meta.get("state") or ""
                if region:
                    obs_region.setdefault(region, {}).setdefault(band, set()).add(cs)
                if state:
                    obs_state.setdefault(state, {}).setdefault(band, set()).add(cs)

        def _weights(
            obs_map: Dict[str, Dict[str, Set[str]]],
            sched_map: Dict[str, Dict[str, Set[str]]],
        ) -> tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, float]], Set[str]]:
            out: Dict[str, Dict[str, float]] = {}
            ratio_out: Dict[str, Dict[str, float]] = {}
            has_data: Set[str] = set()
            keys = set(obs_map.keys()) | set(sched_map.keys())
            for key in keys:
                max_weighted = 0
                band_weighted: Dict[str, int] = {}
                for band in PROP_BANDS:
                    obs_count = len(obs_map.get(key, {}).get(band, set()))
                    sched_count = len(sched_map.get(key, {}).get(band, set()))
                    weighted = obs_count * 2 + sched_count
                    band_weighted[band] = weighted
                    if weighted > max_weighted:
                        max_weighted = weighted
                if max_weighted > 0:
                    has_data.add(key)
                if max_weighted <= 0:
                    out[key] = {band: 1.0 for band in PROP_BANDS}
                    ratio_out[key] = {band: 0.0 for band in PROP_BANDS}
                else:
                    ratio_out[key] = {
                        band: (band_weighted[band] / max_weighted) if max_weighted else 0.0
                        for band in PROP_BANDS
                    }
                    out[key] = {
                        band: max(0.6, min(1.4, 0.7 + 0.6 * ratio_out[key][band]))
                        for band in PROP_BANDS
                    }
            return out, ratio_out, has_data

        region_weights, region_ratio, region_has = _weights(obs_region, sched_region)
        state_weights, state_ratio, state_has = _weights(obs_state, sched_state)
        weights = {
            "region": region_weights,
            "state": state_weights,
            "region_ratio": region_ratio,
            "state_ratio": state_ratio,
            "region_has_data": region_has,
            "state_has_data": state_has,
        }
        self._presence_weights_cache = weights
        self._presence_weights_ts = now_ts
        return weights

    def _load_observed_band_presence(self, max_age_sec: int) -> Dict[str, Set[str]]:
        observed: Dict[str, Set[str]] = {band: set() for band in PROP_BANDS}
        recent_by_band = self._load_recent_calls_by_band(max_age_sec)
        for band in PROP_BANDS:
            calls = recent_by_band.get(band, set())
            if calls:
                observed[band] = {c.strip().upper() for c in calls if c}
        return observed

    def _load_peer_schedule_presence(self, now_utc: datetime.datetime) -> List[Dict]:
        rows: List[Dict] = []
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
        except Exception:
            return rows
        if not db_path.exists():
            return rows
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            has_effective_view = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='view' AND name='peer_hf_schedule_effective'"
            ).fetchone()
            if has_effective_view:
                cur.execute(
                    """
                    SELECT owner_callsign, day_utc, start_utc, end_utc, band, mode, frequency
                    FROM peer_hf_schedule_effective
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT owner_callsign, day_utc, start_utc, end_utc, band, mode, frequency
                    FROM peer_hf_schedule
                    """
                )
            raw = cur.fetchall()
            conn.close()
        except Exception:
            return rows

        active_by_callsign: Dict[str, Dict] = {}
        for cs, day_utc, start_utc, end_utc, band, mode, freq in raw:
            callsign = self._normalize_peer_callsign(cs)
            if not callsign:
                continue
            start_min = self._parse_hhmm_minutes(start_utc)
            end_min = self._parse_hhmm_minutes(end_utc)
            if start_min is None or end_min is None:
                continue
            minutes_to_end = self._schedule_minutes_to_end(
                str(day_utc or "ALL"),
                now_utc,
                start_min,
                end_min,
            )
            if minutes_to_end is None:
                continue
            existing = active_by_callsign.get(callsign)
            if existing and minutes_to_end >= existing.get("minutes_to_end", 0):
                continue
            active_by_callsign[callsign] = {
                "callsign": callsign,
                "band": (band or "").strip().upper(),
                "mode": (mode or "").strip().upper(),
                "frequency": str(freq or "").strip(),
                "minutes_to_end": minutes_to_end,
            }
        return list(active_by_callsign.values())

    @staticmethod
    def _normalize_peer_callsign(value: object) -> str:
        raw = str(value or "").strip().upper()
        if not raw:
            return ""
        return re.sub(r"/(P|M|MM|QRP|SOTA|ROVER|[A-Z0-9]{1,4})$", "", raw)

    def _parse_hhmm_minutes(self, value: Optional[str]) -> Optional[int]:
        txt = (value or "").strip()
        if not txt:
            return None
        match = re.match(r"^(\d{1,2}):?(\d{2})$", txt)
        if not match:
            return None
        h = int(match.group(1))
        m = int(match.group(2))
        if h < 0 or h > 23 or m < 0 or m > 59:
            return None
        return h * 60 + m

    def _compute_region_scores(self, region_filter: str = "") -> Dict[str, Dict]:
        region_filter = (region_filter or "").strip().upper()
        regions = [region_filter] if region_filter else sorted(FEMA_REGIONS.keys())
        user_ll = self._get_user_latlon()
        if not user_ll:
            return {}
        mode = self._effective_prop_mode()
        presence_stats = self._presence_band_weights()
        presence_ratio = presence_stats.get("region_ratio", {})
        presence_has_data = presence_stats.get("region_has_data", set())
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        month = now_utc.month
        hour_utc = now_utc.hour
        hour_local = hour_utc
        try:
            if self.settings:
                tz_name = self.settings.get("timezone", "UTC") or "UTC"
                tz = get_timezone(tz_name)
                hour_local = now_utc.astimezone(tz).hour
        except Exception:
            hour_local = hour_utc
        scores: Dict[str, Dict] = {}
        for region_id in regions:
            stations = self._stations_for_region(region_id)
            use_station_weight = len(stations) >= 5
            centroid = self._region_centroid(region_id)
            for band in PROP_BANDS:
                if mode == "actual":
                    if region_id not in presence_has_data:
                        continue
                    ratio = presence_ratio.get(region_id, {}).get(band, 0.0)
                    entry = scores.setdefault(region_id, {"bands": {}})
                    entry["bands"][band] = max(0.0, min(100.0, ratio * 100.0))
                    continue
                base_scores = []
                if use_station_weight and stations:
                    for s in stations:
                        dist = self._haversine_km(user_ll[0], user_ll[1], s.lat, s.lon)
                        base_scores.append(self._modeled_band_score(band, s.lat, s.lon, now_utc, dist))
                elif centroid:
                    dist = self._haversine_km(user_ll[0], user_ll[1], centroid[0], centroid[1])
                    base_scores.append(self._modeled_band_score(band, centroid[0], centroid[1], now_utc, dist))
                base = sum(base_scores) / max(1, len(base_scores))
                adj = 0.0 if mode == "model" else self._adaptive_adjustment(band, region_id)
                total = max(0.0, min(100.0, base + adj))
                entry = scores.setdefault(region_id, {"bands": {}})
                entry["bands"][band] = total
        return scores

    def _compute_state_scores(self) -> Dict[str, Dict]:
        user_ll = self._get_user_latlon()
        if not user_ll:
            return {}
        mode = self._effective_prop_mode()
        presence_stats = self._presence_band_weights()
        presence_weights = presence_stats.get("state", {})
        presence_ratio = presence_stats.get("state_ratio", {})
        presence_has_data = presence_stats.get("state_has_data", set())
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        month = now_utc.month
        hour_utc = now_utc.hour
        hour_local = hour_utc
        try:
            if self.settings:
                tz_name = self.settings.get("timezone", "UTC") or "UTC"
                tz = get_timezone(tz_name)
                hour_local = now_utc.astimezone(tz).hour
        except Exception:
            hour_local = hour_utc
        scores: Dict[str, Dict] = {}
        for state_abbr in USA_STATES:
            stations = self._stations_for_state(state_abbr)
            use_station_weight = len(stations) >= 5
            centroid = self._state_centroid(state_abbr)
            for band in PROP_BANDS:
                if mode == "actual":
                    if state_abbr not in presence_has_data:
                        continue
                    ratio = presence_ratio.get(state_abbr, {}).get(band, 0.0)
                    entry = scores.setdefault(state_abbr, {"bands": {}})
                    entry["bands"][band] = max(0.0, min(100.0, ratio * 100.0))
                    continue
                base_scores = []
                if use_station_weight and stations:
                    for s in stations:
                        dist = self._haversine_km(user_ll[0], user_ll[1], s.lat, s.lon)
                        base_scores.append(self._modeled_band_score(band, s.lat, s.lon, now_utc, dist))
                elif centroid:
                    dist = self._haversine_km(user_ll[0], user_ll[1], centroid[0], centroid[1])
                    base_scores.append(self._modeled_band_score(band, centroid[0], centroid[1], now_utc, dist))
                if not base_scores:
                    continue
                base = sum(base_scores) / max(1, len(base_scores))
                adj = 0.0 if mode == "model" else self._adaptive_adjustment_state(band, state_abbr)
                total = max(0.0, min(100.0, base + adj))
                if mode in ("adaptive", "blended") and state_abbr in presence_has_data:
                    band_weight = presence_weights.get(state_abbr, {}).get(band, 1.0)
                    if mode == "adaptive":
                        band_weight = max(0.4, min(1.8, band_weight * band_weight))
                    total = max(0.0, min(100.0, total * band_weight))
                entry = scores.setdefault(state_abbr, {"bands": {}})
                entry["bands"][band] = total
        return scores

    def _best_band_for_region(self, region_scores: Dict[str, Dict], region_id: str) -> tuple[str, float]:
        region_id = (region_id or "").strip().upper()
        bands = region_scores.get(region_id, {}).get("bands", {})
        if not bands:
            return ("", 0.0)
        best_band = max(bands.items(), key=lambda kv: kv[1])
        return best_band[0], float(best_band[1])

    def _best_band_overall(self, region_scores: Dict[str, Dict]) -> tuple[str, float]:
        if not region_scores:
            return ("", 0.0)
        totals: Dict[str, List[float]] = {b: [] for b in PROP_BANDS}
        for entry in region_scores.values():
            for band, score in (entry.get("bands") or {}).items():
                if band in totals:
                    totals[band].append(float(score))
        best_band = ""
        best_score = 0.0
        for band, scores in totals.items():
            if not scores:
                continue
            avg = sum(scores) / len(scores)
            if avg > best_score:
                best_score = avg
                best_band = band
        return best_band, best_score

    def _score_level(self, score: float) -> str:
        if score >= 70:
            return "high"
        if score >= 45:
            return "med"
        return "low"

    def _resolve_prop_band_colors(self) -> Dict[str, str]:
        theme = resolve_theme(self.settings)
        is_dark = theme.get("bg") == "#0F1216"
        palette = BAND_COLORS_DARK if is_dark else BAND_COLORS_LIGHT
        colors: Dict[str, str] = {k.upper(): v for k, v in palette.items()}
        try:
            raw = self.settings.get("band_colors", {}) or {}
        except Exception:
            raw = {}
        if isinstance(raw, dict):
            for k, v in raw.items():
                if not k or not v:
                    continue
                key = str(k).strip().lower()
                band_key = key.upper()
                if band_key.endswith("M"):
                    colors[band_key] = str(v).strip()
                else:
                    colors[f"{band_key}M"] = str(v).strip()
        return {b: colors.get(b, PROP_BAND_COLORS[b]) for b in PROP_BANDS if b in PROP_BAND_COLORS}

    def _freq_to_band(self, freq: Optional[float]) -> str:
        if not freq:
            return ""
        try:
            mhz = float(freq)
        except Exception:
            return ""
        bands = [
            ("80M", 3.5, 4.0),
            ("40M", 7.0, 7.3),
            ("30M", 10.1, 10.15),
            ("20M", 14.0, 14.35),
            ("15M", 21.0, 21.45),
            ("10M", 28.0, 29.7),
        ]
        for name, lo, hi in bands:
            if lo <= mhz <= hi:
                return name
        return ""

    def _update_prop_badge(
        self,
        target_label: str,
        best_band: str,
        best_score: float,
        theme: Optional[Dict[str, str]] = None,
    ) -> None:
        if self.prop_badge is None:
            return
        if theme is None:
            theme = self._theme_snapshot()
        scheduled = self._freq_to_band(current_scheduler_freq(self.window()))
        level = self._score_level(best_score)
        display_label = (target_label or "National").strip()
        if not best_band:
            self.prop_badge.setText("Best Band: --")
            self.prop_badge.setStyleSheet(f"font-weight: bold; color: {theme.get('text_muted', '#666666')};")
            return
        text_lines = [
            f"Target: {display_label}",
            f"Best Now: {best_band} ({level.upper()})",
        ]
        if scheduled and scheduled != best_band:
            text_lines.append(f"Schedule: {scheduled}")
        text = "<br/>".join(text_lines)
        if scheduled and scheduled != best_band:
            self.prop_badge.setStyleSheet(f"font-weight: bold; color: {theme.get('warning', '#FB8C00')};")
        else:
            self.prop_badge.setStyleSheet(
                f"font-weight: bold; color: {theme.get('info', theme.get('accent', '#1E88E5'))};"
            )
        self.prop_badge.setText(text)

    def _write_map_html(self, html: str) -> Optional[Path]:
        try:
            self._map_cache_dir.mkdir(parents=True, exist_ok=True)
            self._managed_map_file.write_text(html, encoding="utf-8")
            return self._managed_map_file
        except Exception as e:
            log.error("StationsMap: failed writing map html: %s", e)
            return None

    def _load_web_map_file(self, path: Path) -> bool:
        if self.web is None:
            return False
        try:
            url = QUrl.fromLocalFile(str(path))
            # Cache-bust while reusing the same local file path to avoid temp-file growth.
            url.setQuery(f"v={int(time.time() * 1000)}")
            self._map_page_loading = True
            self._map_load_ok = False
            self._set_map_runtime_state("loading", "Loading the map surface.")
            self._emit_map_event("page_load_started", source="file")
            self.web.setUrl(url)
            return True
        except Exception as e:
            log.error("StationsMap: failed loading map html in webview: %s", e)
            self._map_page_loading = False
            self._enter_map_degraded("Map file load failed before the preview was ready.", reason="file_load", exc=e)
            return False

    def _load_map_html_into_webview(self, html: str, path: Optional[Path] = None) -> bool:
        if self.web is None:
            return False
        if path is not None and self._load_web_map_file(path):
            return True
        try:
            self._map_page_loading = True
            self._map_load_ok = False
            self._set_map_runtime_state("loading", "Loading the map surface.")
            self._emit_map_event("page_load_started", source="inline")
            self.web.setHtml(html)
            return True
        except Exception as e:
            log.error("StationsMap: failed loading inline map html in webview: %s", e)
            self._map_page_loading = False
            self._enter_map_degraded("Inline map preview load failed before the preview was ready.", reason="inline_load", exc=e)
            return False

    def _ensure_web_view(self) -> bool:
        """
        Lazily create the WebEngine view so startup avoids eager WebEngine native
        view/process initialization. The tab shell and loading placeholder are
        created during __init__.
        """
        if self.web is not None:
            return True
        if self._map_stack is None:
            return False
        if not _ensure_webengine_imported() or QWebEngineView is None:
            return False
        try:
            web = QWebEngineView(self._map_stack)
            web.loadFinished.connect(self._on_map_load_finished)
            self.web = web
            self._map_stack.addWidget(web)
            return True
        except Exception as e:
            log.error("StationsMap: failed creating WebEngine view lazily: %s", e)
            self.web = None
            if self._map_loading_label is not None:
                self._map_loading_label.setText("Map preview unavailable.")
            return False

    def prepare_webview_for_first_show(self) -> bool:
        """
        Create the map webview only when the tab is visible and the app is
        active. This avoids hidden-tab WebEngine churn during wake/sleep and
        help-dialog teardown, while keeping the normal first visible load.
        """
        if not self._app_active or not self._map_visible:
            self._map_dirty = True
            self._emit_map_event("webview_prepare_deferred", reason="inactive_or_hidden")
            return False
        return self._ensure_web_view()

    # ------------- Map rendering ------------- #
    def _render_map(self, preserve_view: bool = True):
        if not self._map_visible or not self._app_active:
            self._map_dirty = True
            return
        if self._map_page_loading:
            self._map_dirty = True
            self._render_requested_during_load = True
            return
        self._map_dirty = False
        theme_key = ""
        try:
            if hasattr(self.settings, "reload"):
                self.settings.reload()
            theme_key = str(self.settings.get("ui_theme", "") or "").strip().lower()
        except Exception:
            theme_key = ""
        config_sig = (
            bool(self.show_callsigns),
            bool(self.show_states),
            bool(self.show_cities),
            bool(self.show_grids),
            bool(self.show_grid_labels),
            bool(self.show_regions),
            int(self.city_pop_min),
            bool(self.prop_overlay_enabled),
            bool(self.prop_adaptive_enabled),
            str(self._effective_prop_mode()),
            theme_key,
            int(self.prop_window_hours or 6),
        )
        force_reload = self._map_initialized and self._last_map_config and config_sig != self._last_map_config

        view_state = None
        if isinstance(preserve_view, dict):
            view_state = preserve_view or self._last_map_view
            if view_state:
                self._last_map_view = view_state
        elif preserve_view:
            view_state = self._last_map_view
        if view_state is None and self._last_map_view:
            view_state = self._last_map_view

        if not self.stations:
            self._map_marker_count = 0
            self._map_link_count = 0
            html = "<html><body><h3>No station data to display.</h3></body></html>"
            if self.web is not None:
                self._map_initialized = False
                if self._map_stack is not None:
                    self._map_stack.setCurrentIndex(0)
                if self._map_loading_label is not None:
                    self._map_loading_label.setText("Preparing map...")
                path = self._write_map_html(html)
                if path is not None:
                    self._map_file = path
                    self._load_map_html_into_webview(html, path)
                else:
                    self._load_map_html_into_webview(html)
            else:
                path = self._write_map_html(html)
                if path is not None:
                    self._map_file = path
                    log.info("StationsMap: map written to %s (open in browser).", path)
            self._last_map_view = view_state or self._last_map_view or {"lat": 45, "lon": -97, "zoom": 3}
            return

        self.show_city_labels = self.show_cities

        def _fmt_ts(ts_val):
            try:
                if ts_val:
                    return datetime.datetime.utcfromtimestamp(ts_val).strftime("%Y-%m-%d %H:%M:%S UTC")
            except Exception:
                pass
            return ""

        def _timed_map_call(name: str, fn, *, meta: Optional[Dict[str, object]] = None):
            start = time.perf_counter()
            try:
                return fn()
            finally:
                emit_span(
                    name,
                    (time.perf_counter() - start) * 1000.0,
                    settings=self.settings,
                    meta=meta,
                    min_ms=5.0,
                )

        selection = self._parse_link_selection(
            self.link_mode_combo.currentData() if hasattr(self, "link_mode_combo") else ("off", "")
        )
        group_filter = ""
        region_filter = ""
        if hasattr(self, "group_filter_combo"):
            group_filter = self.group_filter_combo.currentData() or ""
        if hasattr(self, "region_filter_combo"):
            region_filter = self.region_filter_combo.currentData() or ""
        target_ctx = self._prop_target_context()
        target_label = str(target_ctx.get("label") or "National")
        prop_region_scores: Dict[str, Dict] = {}
        prop_state_scores: Dict[str, Dict] = {}
        if self.prop_overlay_enabled:
            # Keep overlay data broad; link filters are independent from propagation target.
            prop_region_scores = self._cached_map_value(
                "prop_region_scores",
                {"region_filter": "", "window_hours": int(self.prop_window_hours or 6), "mode": str(self._effective_prop_mode())},
                lambda: _timed_map_call(
                    "map.compute_region_scores",
                    lambda: self._compute_region_scores(""),
                ),
                ttl_sec=20.0,
                force=bool(force_reload),
            )
            prop_state_scores = self._cached_map_value(
                "prop_state_scores",
                {"window_hours": int(self.prop_window_hours or 6), "mode": str(self._effective_prop_mode())},
                lambda: _timed_map_call(
                    "map.compute_state_scores",
                    self._compute_state_scores,
                ),
                ttl_sec=20.0,
                force=bool(force_reload),
            )
            best_band, best_score = self._best_band_for_target(target_ctx, prop_region_scores, prop_state_scores)
            self._update_prop_badge(target_label, best_band, best_score)
            target_sig = f"{target_ctx.get('type','')}:{target_ctx.get('value','')}"
            if target_sig != self._last_prop_region_filter:
                force_reload = True
        else:
            self._update_prop_badge(target_label, "", 0.0)
            target_sig = f"{target_ctx.get('type','')}:{target_ctx.get('value','')}"
        self._last_prop_region_filter = target_sig
        sitrep_mode = bool(self._sitrep_status_only_enabled)
        band_filter = self.band_combo.currentData() if hasattr(self, "band_combo") else {"type": "all"}
        my_call = ""
        try:
            my_call = (self.settings.get("operator_callsign", "") or "").upper()
        except Exception:
            my_call = ""

        map_input_sig = (
            config_sig,
            bool(self._links_active()),
            bool(sitrep_mode),
            tuple(sorted(selection.items())) if isinstance(selection, dict) else str(selection),
            str(group_filter or "").strip().upper(),
            str(region_filter or "").strip().upper(),
            self._map_band_filter_signature(band_filter),
            int(self.recency_seconds or 0),
            str(my_call or "").strip().upper(),
            bool(self._now_reachable_enabled),
            bool(self.show_station_markers),
            bool(self.show_link_paths),
            bool(self.show_weather_reports),
            bool(self.show_alert_reports),
            bool(self.show_infrastructure_reports),
            len(self._now_reachable_callsigns),
            hash("|".join(sorted(self._now_reachable_callsigns))) if self._now_reachable_callsigns else 0,
            str(target_sig or ""),
            self._nets_db_fingerprint(),
            int(self._stations_revision or 0),
        )
        if (
            not force_reload
            and self.web is not None
            and self._map_initialized
            and bool(self._last_map_payload_sig)
            and map_input_sig == self._last_map_render_input_sig
        ):
            self._emit_map_event("render_skipped_unchanged", reason="input_signature_match")
            self._last_map_view = view_state or self._last_map_view or {"lat": 45, "lon": -97, "zoom": 3}
            return

        # init stats and links
        stats_lookup: Dict[str, Dict] = {}
        links: List[Dict] = []
        if self._links_active():
            relay_target = (self.relay_target or "").strip().upper()
            reachable_filter = self._now_reachable_callsigns if self._now_reachable_enabled else None

            links, stats_lookup = _timed_map_call(
                "map.load_js8_links",
                lambda: self._load_js8_links(
                    band_filter=band_filter,
                    my_call=my_call,
                    link_selection=selection,
                    relay_target=relay_target or None,
                    group_filter=group_filter,
                    region_filter=region_filter,
                    reachable_callsigns=reachable_filter,
                    max_age_sec=self.recency_seconds,
                ),
                meta={"sitrep_mode": sitrep_mode},
            )
            links.extend(
                _timed_map_call(
                    "map.load_varac_links",
                    lambda: self._load_varac_links(
                        band_filter=band_filter,
                        my_call=my_call,
                        link_selection=selection,
                        group_filter=group_filter,
                        region_filter=region_filter,
                        reachable_callsigns=reachable_filter,
                        max_age_sec=self.recency_seconds,
                    ),
                    meta={"sitrep_mode": sitrep_mode},
                )
            )
            if view_state:
                self._last_map_view = view_state

        varac_stats = self._cached_map_value(
            "varac_stats_recent",
            {"max_age_sec": self.recency_seconds},
            lambda: _timed_map_call(
                "map.load_varac_stats_recent",
                lambda: self._load_varac_stats(max_age_sec=self.recency_seconds),
            ),
            ttl_sec=8.0,
        )
        varac_all = self._cached_map_value(
            "varac_stats_all",
            {"max_age_sec": None},
            lambda: _timed_map_call("map.load_varac_stats_all", lambda: self._load_varac_stats(max_age_sec=None)),
            ttl_sec=12.0,
        )
        activity_lookup = self._cached_map_value(
            "operator_activity_summary",
            {"recency_seconds": self.recency_seconds},
            lambda: _timed_map_call("map.load_operator_activity_summary", self._load_operator_activity_summary),
            ttl_sec=8.0,
        )
        direct_contact_lookup = self._cached_map_value(
            "js8_direct_contact_summary",
            {"my_call": my_call},
            lambda: _timed_map_call(
                "map.load_js8_direct_contact_summary",
                lambda: self._load_js8_direct_contact_summary(my_call),
            ),
            ttl_sec=8.0,
        )
        js8_all = self._cached_map_value(
            "js8_presence",
            {"recency_seconds": self.recency_seconds},
            lambda: _timed_map_call("map.load_js8_presence", self._load_js8_presence),
            ttl_sec=8.0,
        )
        fldigi_calls = self._cached_map_value(
            "fldigi_presence",
            {"recency_seconds": self.recency_seconds},
            lambda: _timed_map_call("map.load_fldigi_presence", self._load_fldigi_presence),
            ttl_sec=8.0,
        )
        spotter_status_lookup = self._cached_map_value(
            "spotter_station_status",
            {"group_filter": str(group_filter or "").strip().upper(), "region_filter": str(region_filter or "").strip().upper()},
            lambda: _timed_map_call("map.load_spotter_station_status", self._load_spotter_station_status),
            ttl_sec=6.0,
        )
        spotter_map_activity = self._cached_map_value(
            "spotter_map_activity",
            {},
            lambda: _timed_map_call("map.load_spotter_map_activity", self._load_spotter_map_activity),
            ttl_sec=6.0,
        )
        sitrep_state_summary: List[Dict[str, object]] = []
        sitrep_summary_group = ""
        if sitrep_mode:
            sitrep_summary_group = str(group_filter or "").strip().upper()
            sitrep_state_summary = self._cached_map_value(
                "sitrep_state_rollup",
                {"group": sitrep_summary_group},
                lambda: _timed_map_call(
                    "map.load_sitrep_state_rollup",
                    lambda: self._load_sitrep_state_rollup(sitrep_summary_group),
                ),
                ttl_sec=6.0,
            )
        links = self._display_links_for_mode(links, sitrep_mode)

        # Spread overlapping stations with the same base lat/lon
        markers = []
        weather_station_lookup: Dict[str, StationPoint] = {}
        base_map: Dict[tuple[float, float], List[StationPoint]] = {}
        my_call = (self.settings.get("operator_callsign", "") or "").strip().upper()
        traffic_calls = {cs.upper() for cs in stats_lookup.keys()}
        for link in links:
            origin = (link.get("origin") or "").strip().upper()
            dest = (link.get("destination") or "").strip().upper()
            if origin:
                traffic_calls.add(origin)
                base_origin = JS8LogLinkIndexer._base_callsign(origin)
                if base_origin:
                    traffic_calls.add(base_origin)
            if dest:
                traffic_calls.add(dest)
                base_dest = JS8LogLinkIndexer._base_callsign(dest)
                if base_dest:
                    traffic_calls.add(base_dest)
        for cs in varac_stats.keys():
            if cs:
                traffic_calls.add(cs)
        for cs in spotter_map_activity.keys():
            if cs:
                traffic_calls.add(str(cs).upper())
        if self._now_reachable_enabled:
            traffic_calls.update({c for c in self._now_reachable_callsigns if c})
        show_all_stations = (not self._links_active()) or sitrep_mode
        recent_calls: Set[str] = set()
        if show_all_stations and self.recency_seconds and not sitrep_mode:
            band_filter = self.band_combo.currentData() if hasattr(self, "band_combo") else {"type": "all"}
            recent_calls = self._cached_map_value(
                "recent_calls",
                {"recency_seconds": self.recency_seconds, "band_filter": band_filter},
                lambda: _timed_map_call(
                    "map.load_recent_calls",
                    lambda: self._load_recent_calls(self.recency_seconds, band_filter=band_filter),
                ),
                ttl_sec=6.0,
            )
        for pt in self.stations:
            cs_upper = pt.callsign.upper()
            if self._marker_station_matches_filters(
                cs_upper,
                group_filter="" if sitrep_mode else group_filter,
                region_filter=region_filter,
                my_call=my_call,
                allow_self=True,
            ):
                weather_station_lookup[cs_upper] = pt
                base_cs = JS8LogLinkIndexer._base_callsign(cs_upper)
                if base_cs:
                    weather_station_lookup.setdefault(base_cs, pt)
            if not self._marker_station_matches_filters(
                cs_upper,
                group_filter="" if sitrep_mode else group_filter,
                region_filter=region_filter,
                my_call=my_call,
                allow_self=bool(self._links_active() or self._now_reachable_enabled),
            ):
                continue
            if self._now_reachable_enabled:
                # Peer Sched Now is a strict station filter: only peers whose
                # schedule alignment is in the computed reachable set (plus me).
                if cs_upper != my_call and cs_upper not in self._now_reachable_callsigns:
                    continue
            if sitrep_mode:
                status_data = spotter_status_lookup.get(cs_upper, {})
                if (status_data.get("status_key") or "").strip().lower() not in {"red", "yellow", "green"}:
                    continue
                if group_filter:
                    status_group = str(status_data.get("report_group") or "").strip().upper()
                    if status_group != str(group_filter or "").strip().upper():
                        continue
            if not show_all_stations:
                if cs_upper not in traffic_calls and cs_upper != my_call:
                    continue
            elif recent_calls:
                if cs_upper not in recent_calls and cs_upper != my_call:
                    continue
            key = (round(pt.lat, 4), round(pt.lon, 4))
            base_map.setdefault(key, []).append(pt)

        weather_events = self._build_weather_map_events(weather_station_lookup) if self.show_weather_reports else []
        include_legacy_spotter_reports = self._include_legacy_spotter_report_layers()
        alert_events = (
            self._build_spotter_operational_events(
                weather_station_lookup,
                layer_name="alert",
                display_label="Alerts",
                reports_loader=self._load_spotter_alert_reports,
            )
            if self.show_alert_reports and include_legacy_spotter_reports
            else []
        )
        if self.show_alert_reports and bool(getattr(self, "_observation_focus_enabled", False)):
            alert_events.extend(
                self._build_spotter_operational_events(
                    weather_station_lookup,
                    layer_name="alert",
                    display_label="Observation Alerts",
                    reports_loader=lambda: self._load_observation_operational_reports(
                        layer_name="alert",
                        max_age_sec=ALERT_REPORT_MAX_AGE_SEC,
                    ),
                )
            )
        infrastructure_events = (
            self._build_spotter_operational_events(
                weather_station_lookup,
                layer_name="infrastructure",
                display_label="Infrastructure Reports",
                reports_loader=self._load_spotter_infrastructure_reports,
            )
            if self.show_infrastructure_reports and include_legacy_spotter_reports
            else []
        )
        if self.show_infrastructure_reports and bool(getattr(self, "_observation_focus_enabled", False)):
            infrastructure_events.extend(
                self._build_spotter_operational_events(
                    weather_station_lookup,
                    layer_name="infrastructure",
                    display_label="Observation Infrastructure",
                    reports_loader=lambda: self._load_observation_operational_reports(
                        layer_name="infrastructure",
                        max_age_sec=INFRASTRUCTURE_REPORT_MAX_AGE_SEC,
                    ),
                )
            )

        def offset_positions(base_lat: float, base_lon: float, items: List[StationPoint]):
            if len(items) == 1:
                return [(base_lat, base_lon)]
            coords = []
            radius = 0.25  # degrees, modest spread
            for idx, _ in enumerate(items):
                angle = (idx / len(items)) * 6.28318530718  # 2*pi
                lat_off = base_lat + radius * math.cos(angle)
                lon_off = base_lon + (radius * math.sin(angle) / max(0.1, math.cos(math.radians(base_lat))))
                coords.append((lat_off, lon_off))
            return coords

        for (base_lat, base_lon), items in base_map.items():
            positions = offset_positions(base_lat, base_lon, items)
            for pt, (lat_off, lon_off) in zip(items, positions):
                cs_upper = pt.callsign.upper()
                stats = stats_lookup.get(cs_upper, {})
                vstats = varac_stats.get(cs_upper, {})
                activity = activity_lookup.get(cs_upper, {})
                direct_contact = direct_contact_lookup.get(cs_upper, {})
                modes: List[str] = []
                if cs_upper in js8_all:
                    modes.append("JS8")
                if cs_upper in varac_all:
                    modes.append("VarAC")
                if cs_upper in fldigi_calls:
                    modes.append("FLDigi")

                detail_lines = [
                    f"{pt.callsign}",
                    f"Name: {pt.name}" if pt.name else "",
                    f"State: {pt.state}" if pt.state else "",
                    f"Grid: {pt.grid}" if pt.grid else "",
                    f"Group: {pt.group}" if pt.group else "",
                    f"Modes: {', '.join(modes)}" if modes else "",
                ]
                reach_meta = self._now_reachable_meta.get(cs_upper, {}) if self._now_reachable_enabled else {}
                qsy_text = (reach_meta.get("qsy_text") or "").strip() if isinstance(reach_meta, dict) else ""
                qsy_soon = bool(reach_meta.get("qsy_soon")) if isinstance(reach_meta, dict) else False
                spotter_data = spotter_status_lookup.get(cs_upper, {})
                spotter_status_key = str(spotter_data.get("status_key") or "unknown").strip().lower()
                if spotter_status_key not in {"red", "yellow", "green", "unknown"}:
                    spotter_status_key = "unknown"
                spotter_status_label = str(spotter_data.get("status_label") or "").strip() or self._sitrep_status_label(spotter_status_key)
                spotter_status_ts = _fmt_ts(spotter_data.get("updated_utc_ts", 0))
                spotter_status_source = str(spotter_data.get("status_source") or "").strip()
                spotter_status_source_detail = str(spotter_data.get("status_source_detail") or "").strip()
                spotter_status_source_chips = str(spotter_data.get("status_source_chips") or "").strip()
                spotter_status_conflict = bool(spotter_data.get("status_conflict"))
                spotter_status_age = str(spotter_data.get("status_age") or "").strip()
                spotter_status_group = str(spotter_data.get("report_group") or "").strip()
                spotter_status_transport = str(spotter_data.get("transport_label") or "").strip()
                spotter_status_state = str(spotter_data.get("state_code") or "").strip()
                spotter_status_state_conf = str(spotter_data.get("state_confidence") or "").strip()
                spotter_status_geo_conf = str(spotter_data.get("geo_confidence") or "").strip()
                spotter_status_brevity = str(spotter_data.get("brevity_summary") or "").strip()
                spotter_map_data = spotter_map_activity.get(cs_upper, {})
                spotter_map_form = str(spotter_map_data.get("form_id") or "").strip()
                spotter_map_ts = _fmt_ts(spotter_map_data.get("utc_ts", 0))
                spotter_map_summary = str(spotter_map_data.get("summary") or "").strip()
                if qsy_text:
                    detail_lines.append(f"Schedule: {qsy_text}")
                if spotter_map_form:
                    detail_lines.append(f"Spotter Form: {spotter_map_form}" + (f" at {spotter_map_ts}" if spotter_map_ts else ""))
                    if spotter_map_summary:
                        detail_lines.append(f"Spotter Summary: {spotter_map_summary}")
                # Filter empty lines
                detail_lines = [d for d in detail_lines if d]
                title = "\n".join(detail_lines)
                tooltip_html = "<br/>".join(detail_lines)

                markers.append(
                    {
                        "lat": lat_off,
                        "lon": lon_off,
                        "title": title,
                        "tooltip": tooltip_html,
                        "label": pt.callsign if self.show_callsigns else "",
                        "last_seen": _fmt_ts(activity.get("overall_last_seen_ts", 0)),
                        "last_spotter": _fmt_ts(stats.get("last_spotter", 0)),
                        "direct_snr": stats.get("direct_snr"),
                        "avg_snr_excl_my": stats.get("avg_snr_excl_my"),
                        "direct_count": stats.get("direct_count", 0),
                        "avg_snr_count": stats.get("avg_snr_count", 0),
                        "last_band": activity.get("overall_last_band", "") or "",
                        "last_contact": _fmt_ts(direct_contact.get("last_contact_ts", 0)),
                        "last_contact_band": direct_contact.get("last_contact_band", "") or "",
                        "last_contact_snr": direct_contact.get("last_contact_snr"),
                        "varac_last_seen": _fmt_ts(vstats.get("last_seen_ts", 0)),
                        "varac_last_band": vstats.get("last_band", ""),
                        "varac_avg_snr": vstats.get("avg_snr"),
                        "qsy_soon": qsy_soon,
                        "qsy_text": qsy_text,
                        "spotter_status_key": spotter_status_key,
                        "spotter_status_label": spotter_status_label,
                        "spotter_status_ts": spotter_status_ts,
                        "spotter_status_source": spotter_status_source,
                        "spotter_status_source_detail": spotter_status_source_detail,
                        "spotter_status_source_chips": spotter_status_source_chips,
                        "spotter_status_conflict": spotter_status_conflict,
                        "spotter_status_age": spotter_status_age,
                        "spotter_status_group": spotter_status_group,
                        "spotter_status_transport": spotter_status_transport,
                        "spotter_status_state": spotter_status_state,
                        "spotter_status_state_conf": spotter_status_state_conf,
                        "spotter_status_geo_conf": spotter_status_geo_conf,
                        "spotter_status_brevity": spotter_status_brevity,
                    }
                )

        display_markers = markers if self.show_station_markers else []
        display_links = links if self.show_link_paths else []
        self._map_marker_count = len(display_markers)
        self._map_link_count = len(display_links)
        self._last_map_render_input_sig = map_input_sig

        if self.web is not None and self._map_initialized and self._map_file and not force_reload:
            self._push_map_payload(
                display_markers,
                display_links,
                weather_events=weather_events,
                alert_events=alert_events,
                infrastructure_events=infrastructure_events,
                sitrep_state_summary=sitrep_state_summary,
                sitrep_summary_group=sitrep_summary_group,
            )
            self._last_map_view = view_state or self._last_map_view or {"lat": 45, "lon": -97, "zoom": 3}
            return

        leaflet_js, leaflet_css = self._ensure_leaflet_assets()
        geojson_us = self._ensure_geojson(
            self._geojson_path,
            "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json",
        )
        geojson_ca = self._ensure_geojson(
            self._geojson_canada,
            "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/canada.geojson",
        )
        geojson_mx = self._ensure_geojson(
            self._geojson_mexico,
            "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/mexico.geojson",
        )
        fema_geojson = self._ensure_fema_geojson()
        cities_geojson = self._ensure_cities_geojson()
        geojson_urls = [u for u in (geojson_us, geojson_ca, geojson_mx, fema_geojson) if u]
        # For webview reloads, keep bootstrap HTML lightweight and push live data
        # after loadFinished to avoid serializing the same payload twice.
        bootstrap_markers = display_markers if self.web is None else []
        bootstrap_links = display_links if self.web is None else []
        bootstrap_weather_events = weather_events if self.web is None else []
        bootstrap_alert_events = alert_events if self.web is None else []
        bootstrap_infrastructure_events = infrastructure_events if self.web is None else []
        html = self._build_leaflet_html(
            bootstrap_markers,
            links=bootstrap_links,
            weather_events=bootstrap_weather_events,
            alert_events=bootstrap_alert_events,
            infrastructure_events=bootstrap_infrastructure_events,
            max_zoom=12,
            leaflet_js=leaflet_js,
            leaflet_css=leaflet_css,
            geojson_urls=geojson_urls,
            cities_geojson=cities_geojson,
            city_min_pop=self.city_pop_min,
            show_city_labels=self.show_city_labels,
            initial_view=view_state or self._last_map_view,
            prop_overlay_enabled=self.prop_overlay_enabled,
            prop_region_scores=prop_region_scores,
            prop_state_scores=prop_state_scores,
            sitrep_state_summary=sitrep_state_summary,
            sitrep_summary_group=sitrep_summary_group,
        )

        if self.web is not None:
            self._last_map_config = config_sig
            had_visible_map = bool(self._map_initialized and self._map_load_ok)
            self._map_initialized = False
            if self._map_stack is not None:
                # Keep the existing map visible for config/layer reloads to avoid
                # a disruptive blank/loading flash between updates.
                if had_visible_map:
                    self._map_stack.setCurrentIndex(1)
                else:
                    self._map_stack.setCurrentIndex(0)
                    if self._map_loading_label is not None:
                        self._map_loading_label.setText("Loading map...")
            # New page context: force first payload push even if content hash matches
            # the prior page's payload.
            self._last_map_payload_sig = None
            self._last_map_render_input_sig = None
            self._pending_map_payload = {
                "markers": display_markers,
                "links": display_links,
                "weather_events": weather_events,
                "alert_events": alert_events,
                "infrastructure_events": infrastructure_events,
                "now_reachable_enabled": bool(self._now_reachable_enabled),
                "sitrep_state_summary": sitrep_state_summary,
                "sitrep_summary_group": sitrep_summary_group,
            }
            path = self._write_map_html(html)
            if path is not None:
                self._map_file = path
                self._load_map_html_into_webview(html, path)
            else:
                self._load_map_html_into_webview(html)
        else:
            path = self._write_map_html(html)
            if path is not None:
                self._map_file = path
                log.info("StationsMap: map written to %s (open in browser).", path)
        self._last_map_view = view_state or self._last_map_view or {"lat": 45, "lon": -97, "zoom": 3}

    def _on_map_load_finished(self, ok: bool) -> None:
        self._map_page_loading = False
        self._map_initialized = bool(ok)
        self._map_load_ok = bool(ok)
        self._emit_map_event("page_load_finished", ok=bool(ok))
        if self._map_stack is not None:
            if ok:
                self._map_stack.setCurrentIndex(1)
            else:
                self._map_stack.setCurrentIndex(0)
                if self._map_loading_label is not None:
                    self._map_loading_label.setText("Map failed to load.")
        if not ok:
            self._enter_map_degraded("Map preview did not load successfully. You can retry without restarting FIO.", reason="load_finished")
        else:
            self._set_map_runtime_state(
                "ready",
                (
                    f"Map is ready with {int(getattr(self, '_map_marker_count', 0) or 0)} station markers "
                    f"and {int(getattr(self, '_map_link_count', 0) or 0)} links."
                ),
            )
        if not ok or self.web is None:
            return
        self._maybe_start_map_ingest()
        if self._pending_map_payload:
            payload = self._pending_map_payload
            self._pending_map_payload = None
            # Ensure payload is applied to the freshly loaded page, even when
            # marker/link data is identical to the previous render.
            self._last_map_payload_sig = None
            self._push_map_payload(
                payload.get("markers", []),
                payload.get("links", []),
                weather_events=payload.get("weather_events", []),
                alert_events=payload.get("alert_events", []),
                infrastructure_events=payload.get("infrastructure_events", []),
                now_reachable_enabled=payload.get("now_reachable_enabled"),
                sitrep_state_summary=payload.get("sitrep_state_summary", []),
                sitrep_summary_group=payload.get("sitrep_summary_group", ""),
            )
        if getattr(self, "_map_visible", False) and (
            getattr(self, "_map_dirty", False) or getattr(self, "_render_requested_during_load", False)
        ):
            self._render_requested_during_load = False
            self._map_dirty = False
            queued_level = self._refresh_level_name(
                max(int(getattr(self, "_render_requested_during_load_level", 0) or 0), 2)
            )
            self._render_requested_during_load_level = 0
            self._request_map_refresh(level=queued_level, reason="post_load", preserve_view=True)

    def _on_map_visible_deferred(self) -> None:
        if not self._map_visible or self._is_shutting_down:
            return
        if not self._app_active:
            self._map_dirty = True
            self._set_map_runtime_state("warming", "Preparing the map view.")
            return
        self._ensure_initial_data_loaded()
        if not self._ensure_web_view():
            self._enter_map_degraded("Qt WebEngine is not available for the embedded map preview.", reason="webengine_missing")
            return
        if not self._map_initialized:
            # First visible render: build/load the map HTML before waiting on loadFinished.
            # Clear dirty before first render to avoid an immediate duplicate render in
            # _on_map_load_finished(). Any real updates during load will set dirty again.
            self._map_dirty = False
            self._request_map_refresh(level="full", reason="visible_init", preserve_view=True)
            return
        if self._map_dirty:
            self._map_dirty = False
            self._request_map_refresh(level="medium", reason="visible_dirty", preserve_view=True)

    def _push_map_payload(
        self,
        markers: List[Dict],
        links: List[Dict],
        weather_events: Optional[List[Dict[str, object]]] = None,
        alert_events: Optional[List[Dict[str, object]]] = None,
        infrastructure_events: Optional[List[Dict[str, object]]] = None,
        now_reachable_enabled: Optional[bool] = None,
        sitrep_state_summary: Optional[List[Dict[str, object]]] = None,
        sitrep_summary_group: str = "",
    ) -> None:
        if getattr(self, "web", None) is None:
            return
        if not getattr(self, "_map_visible", False) or not getattr(self, "_app_active", True):
            self._map_dirty = True
            return
        if getattr(self, "_map_page_loading", False) or not getattr(self, "_map_initialized", False):
            self._pending_map_payload = {
                "markers": list(markers),
                "links": list(links),
                "weather_events": list(weather_events or []),
                "alert_events": list(alert_events or []),
                "infrastructure_events": list(infrastructure_events or []),
                "now_reachable_enabled": (
                    bool(self._now_reachable_enabled)
                    if now_reachable_enabled is None
                    else bool(now_reachable_enabled)
                ),
                "sitrep_state_summary": list(sitrep_state_summary or []),
                "sitrep_summary_group": str(sitrep_summary_group or ""),
            }
            return
        now_reachable_flag = (
            bool(self._now_reachable_enabled)
            if now_reachable_enabled is None
            else bool(now_reachable_enabled)
        )
        try:
            payload = json.dumps(
                {
                    "markers": markers,
                    "links": links,
                    "weather_events": list(weather_events or []),
                    "alert_events": list(alert_events or []),
                    "infrastructure_events": list(infrastructure_events or []),
                    "now_reachable_enabled": now_reachable_flag,
                    "sitrep_state_summary": list(sitrep_state_summary or []),
                    "sitrep_summary_group": str(sitrep_summary_group or ""),
                }
            )
        except Exception:
            payload = (
                '{"markers": [], "links": [], "weather_events": [], "alert_events": [], "infrastructure_events": [], "sitrep_state_summary": [], "sitrep_summary_group": "", '
                f'"now_reachable_enabled": {str(now_reachable_flag).lower()}}}'
            )
        sig = str(hash(payload))
        if sig == self._last_map_payload_sig:
            return
        self._last_map_payload_sig = sig
        js = f"if (window.updateMapData) {{ window.updateMapData({payload}); }}"
        try:
            self.web.page().runJavaScript(js)
        except Exception:
            pass

    def _parse_view_state(self, js_result) -> Dict[str, float]:
        """
        Convert JS callback output into a view state dict.
        Accepts JSON string or dict-like values.
        """
        if isinstance(js_result, dict):
            lat = js_result.get("lat")
            lon = js_result.get("lon")
            zoom = js_result.get("zoom")
        else:
            try:
                data = json.loads(js_result) if js_result else {}
            except Exception:
                data = {}
            lat = data.get("lat")
            lon = data.get("lon")
            zoom = data.get("zoom")
        if lat is None or lon is None or zoom is None:
            return self._last_map_view or {"lat": 45, "lon": -97, "zoom": 3}
        return {"lat": float(lat), "lon": float(lon), "zoom": float(zoom)}

    def _build_leaflet_html(
        self,
        markers: List[Dict],
        links: List[Dict],
        max_zoom: int,
        leaflet_js: str,
        leaflet_css: str,
        geojson_urls: List[str],
        cities_geojson: Optional[str],
        city_min_pop: int,
        show_city_labels: bool,
        weather_events: Optional[List[Dict[str, object]]] = None,
        alert_events: Optional[List[Dict[str, object]]] = None,
        infrastructure_events: Optional[List[Dict[str, object]]] = None,
        initial_view: Optional[Dict[str, float]] = None,
        prop_overlay_enabled: bool = False,
        prop_region_scores: Optional[Dict[str, Dict]] = None,
        prop_state_scores: Optional[Dict[str, Dict]] = None,
        sitrep_state_summary: Optional[List[Dict[str, object]]] = None,
        sitrep_summary_group: str = "",
    ) -> str:
        theme = resolve_theme(self.settings)
        try:
            ui_theme = str(self.settings.get("ui_theme", "") or "").strip().lower()
        except Exception:
            ui_theme = ""
        is_dark = theme.get("bg") == "#0F1216" or ui_theme == "dark"
        grid_color = "#5F6B7A" if is_dark else "#666"
        grid_opacity = "0.3" if is_dark else "0.3"
        now_reachable_enabled = str(bool(self._now_reachable_enabled)).lower()
        markers_json = json.dumps(markers)
        links_json = json.dumps(links)
        weather_events_json = json.dumps(weather_events or [])
        alert_events_json = json.dumps(alert_events or [])
        infrastructure_events_json = json.dumps(infrastructure_events or [])
        sitrep_state_summary_json = json.dumps(sitrep_state_summary or [])
        sitrep_summary_group_json = json.dumps(str(sitrep_summary_group or "").strip().upper())
        init_lat = initial_view.get("lat") if initial_view else 45
        init_lon = initial_view.get("lon") if initial_view else -97
        init_zoom = initial_view.get("zoom") if initial_view else 3
        tile_layer = "L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 18, maxNativeZoom: 18, attribution: '&copy; OpenStreetMap contributors' }).addTo(map);"
        grid_layer = (
            """
const gridLayer = L.layerGroup();
const gridLabelLayer = L.layerGroup();
let gridUpdating = false;
let gridUpdateTimer = null;
function maidenFromLatLon(lat, lon, level) {
      // level: 2,4,6 chars
      let adjLon = lon + 180.0;
      let adjLat = lat + 90.0;
      let fieldLon = Math.floor(adjLon / 20);
      let fieldLat = Math.floor(adjLat / 10);
      let out = String.fromCharCode(65 + fieldLon) + String.fromCharCode(65 + fieldLat);
      if (level >= 4) {
        let squareLon = Math.floor((adjLon % 20) / 2);
        let squareLat = Math.floor((adjLat % 10) / 1);
        out += squareLon.toString() + squareLat.toString();
      }
      if (level >= 6) {
        let subsLon = Math.floor(((adjLon % 2) / 2) * 24);
        let subsLat = Math.floor(((adjLat % 1) / 1) * 24);
        out += String.fromCharCode(65 + subsLon) + String.fromCharCode(65 + subsLat);
      }
      return out;
    }
function addGrid(res, maxCells) {
  const stepLon = res;
  const stepLat = res/2;
  const bounds = map.getBounds();
  const west = Math.max(-180, bounds.getWest() - stepLon);
  const east = Math.min(180, bounds.getEast() + stepLon);
  const south = Math.max(-90, bounds.getSouth() - stepLat);
  const north = Math.min(90, bounds.getNorth() + stepLat);
  let lonCount = Math.ceil((east - west) / stepLon);
  let latCount = Math.ceil((north - south) / stepLat);
  if (lonCount * latCount > maxCells) return false;
  for (let lon = Math.floor(west / stepLon) * stepLon; lon <= east; lon += stepLon) {
    gridLayer.addLayer(L.polyline([[ south, lon ], [ north, lon ]], {color:'{grid_color}', weight:0.5, opacity:{grid_opacity}}));
  }
  for (let lat = Math.floor(south / stepLat) * stepLat; lat <= north; lat += stepLat) {
    gridLayer.addLayer(L.polyline([[ lat, west ], [ lat, east ]], {color:'{grid_color}', weight:0.5, opacity:{grid_opacity}}));
  }
  return true;
}
function scheduleGridUpdate() {
  if (gridUpdateTimer) {
    clearTimeout(gridUpdateTimer);
  }
  gridUpdateTimer = setTimeout(updateGrid, 80);
}
function updateGrid() {
  if (gridUpdating) return;
  gridUpdating = true;
  gridLayer.clearLayers();
  const z = map.getZoom();
  const bounds = map.getBounds();
  const size = map.getSize();
  const maxCells = Math.max(1200, Math.floor((size.x * size.y) / 900));
  const maxLabels = Math.max(400, Math.floor((size.x * size.y) / 2000));
  // Maidenhead grid sizes: 2-char ~20x10 deg, 4-char ~2x1 deg, 6-char ~5x2.5 arcmin (~0.0833x0.0417 deg)
  if (""" + str(self.show_grids).lower() + """) {
    let resVal = 0;
    let level = 0;
    if (z < 5) {
      resVal = 20; level = 2;
    } else if (z < 9) {
      resVal = 2; level = 4;
    } else {
      resVal = 0.083333; level = 6;
    }
    if (resVal > 0 && addGrid(resVal, maxCells)) {
      gridLayer.addTo(map);
    } else {
      map.removeLayer(gridLayer);
    }
    if (""" + str(self.show_grid_labels).lower() + """) {
      const showLabels = (level === 2 && z >= 4) || (level === 4 && z >= 6) || (level === 6 && z >= 10);
      if (showLabels) {
        addGridLabels(resVal, level, bounds, maxLabels);
      } else {
        map.removeLayer(gridLabelLayer);
      }
    } else {
      map.removeLayer(gridLabelLayer);
    }
  } else {
    map.removeLayer(gridLayer);
    map.removeLayer(gridLabelLayer);
  }
  gridUpdating = false;
}

function addGridLabels(res, level, bounds, maxLabels) {
  gridLabelLayer.clearLayers();
  if (res <= 0) return;
      const stepLon = res;
      const stepLat = res/2;
      const west = Math.max(-180, bounds.getWest() - stepLon);
      const east = Math.min(180, bounds.getEast() + stepLon);
  const south = Math.max(-90, bounds.getSouth() - stepLat);
  const north = Math.min(90, bounds.getNorth() + stepLat);
  let count = 0;
  for (let lat = Math.floor(south / stepLat) * stepLat + stepLat/2; lat < north; lat += stepLat) {
    for (let lon = Math.floor(west / stepLon) * stepLon + stepLon/2; lon < east; lon += stepLon) {
      const label = maidenFromLatLon(lat, lon, level);
      const icon = L.divIcon({className:'label-text no-border', html: label});
      gridLabelLayer.addLayer(L.marker([lat, lon], {icon}));
      count++;
      if (count > maxLabels) break;
    }
    if (count > maxLabels) break;
  }
  map.addLayer(gridLabelLayer);
}

    map.on('zoomend', scheduleGridUpdate);
    map.on('moveend', scheduleGridUpdate);
    updateGrid();
            """
            if self.show_grids
            else ""
        )
        if grid_layer:
            # Replace style placeholders without converting the full JS block
            # into an f-string (which would require escaping many braces).
            grid_layer = (
                grid_layer.replace("{grid_color}", str(grid_color))
                .replace("{grid_opacity}", str(grid_opacity))
            )
        road_fetch = ""
        prop_region_best: Dict[str, Dict] = {}
        if prop_region_scores:
            for region_id, data in prop_region_scores.items():
                bands = (data or {}).get("bands", {})
                if not bands:
                    continue
                best_band, best_score = max(bands.items(), key=lambda kv: kv[1])
                level = "low"
                if best_score >= 70:
                    level = "high"
                elif best_score >= 45:
                    level = "med"
                prop_region_best[region_id] = {
                    "band": best_band,
                    "score": round(float(best_score), 1),
                    "level": level,
                }
        prop_state_best: Dict[str, Dict] = {}
        if prop_state_scores:
            for state_abbr, data in prop_state_scores.items():
                bands = (data or {}).get("bands", {})
                if not bands:
                    continue
                best_band, best_score = max(bands.items(), key=lambda kv: kv[1])
                level = "low"
                if best_score >= 70:
                    level = "high"
                elif best_score >= 45:
                    level = "med"
                prop_state_best[state_abbr] = {
                    "band": best_band,
                    "score": round(float(best_score), 1),
                    "level": level,
                }
        prop_colors = self._resolve_prop_band_colors()
        label_color = theme.get("text", "#E6E8EE" if is_dark else "#1C1F21")
        state_label_color = theme.get("text_muted", "#A3ACB8" if is_dark else "#5B6570")
        region_label_color = theme.get("info", theme.get("accent", "#B8C7FF" if is_dark else "#1E88E5"))
        callsign_label_color = theme.get("text", label_color)
        region_band_label_color = theme.get("text", label_color)
        label_halo = (
            "0 1px 2px rgba(0,0,0,0.88), 0 0 3px rgba(0,0,0,0.72)"
            if is_dark
            else "0 1px 2px rgba(255,255,255,0.92), 0 0 3px rgba(255,255,255,0.82)"
        )
        to_rgba = getattr(self, "_hex_to_rgba", StationsMapTab._hex_to_rgba)
        callsign_chip_bg = to_rgba(theme.get("surface", "#171B21" if is_dark else "#F0F2F4"), 0.78 if is_dark else 0.84)
        callsign_chip_border = to_rgba(theme.get("border", "#2A313A" if is_dark else "#D3D7DD"), 0.88 if is_dark else 0.80)
        tooltip_bg = "#1A1F26" if is_dark else "#fff"
        tooltip_text = "#E6E8EE" if is_dark else "#000"
        tooltip_border = "#3A4452" if is_dark else "#444"
        legend_bg = "rgba(26,31,38,0.92)" if is_dark else "rgba(255,255,255,0.92)"
        legend_text = "#C6CBD4" if is_dark else "#000"
        state_border = "#8A93A6" if is_dark else "#666"
        state_border_opacity = "0.7" if is_dark else "0.5"
        region_fill_opacity = "0.05" if is_dark else "0.08"
        geojson_fetches = "\n".join(
            [
                f"""
    fetch('{u}')
      .then(r => r.json())
      .then(data => {{
        const regionCenters = {{}};
        L.geoJSON(data, {{
          style: function() {{
            const props = arguments[0].properties || {{}};
            const fullName = (props.STATE_NAME || props.name || props.state || '').toUpperCase();
            let stateAbbr = (props.state_abbrev || props.state || '').toUpperCase();
            if (!stateAbbr && fullName && window.STATE_ABBR_FROM_NAME && window.STATE_ABBR_FROM_NAME[fullName]) {{
              stateAbbr = window.STATE_ABBR_FROM_NAME[fullName];
            }}
            let reg = props.fema_region;
            if (!reg && stateAbbr && window.FEMA_LOOKUP_ABBR && window.FEMA_LOOKUP_ABBR[stateAbbr]) {{
              reg = window.FEMA_LOOKUP_ABBR[stateAbbr];
            }}
            if (!reg && fullName && window.FEMA_LOOKUP_NAME && window.FEMA_LOOKUP_NAME[fullName]) {{
              reg = window.FEMA_LOOKUP_NAME[fullName];
            }}
            if (window.propOverlayEnabled && !{str(self.show_regions).lower()}) {{
              const st = stateAbbr || '';
              const stEntry = st && window.propStateScores[st];
              if (stEntry) {{
                const bandColor = window.propBandColors[stEntry.band] || '#6D4C41';
                const opacity = stEntry.level === 'high' ? 0.28 : (stEntry.level === 'med' ? 0.2 : 0.12);
                return {{color: bandColor, weight: 1, opacity: 0.9, fillOpacity: opacity, fillColor: bandColor}};
              }}
            }}
            if ({str(self.show_regions).lower()} && reg) {{
              const color = regionColors[(parseInt(reg, 10) - 1) % regionColors.length];
              return {{color: color, weight: 1, opacity: 0.8, fillOpacity: {region_fill_opacity}, fillColor: color}};
            }} else {{
              return {{color: '{state_border}', weight: 1, opacity: {state_border_opacity}, fillOpacity: 0}};
            }}
          }},
          onEachFeature: function (feature, layer) {{
            const props = feature.properties || {{}};
            const fullName = (props.STATE_NAME || props.name || props.state || '').toUpperCase();
            let stateAbbr = (props.state_abbrev || props.state || '').toUpperCase();
            if (!stateAbbr && fullName && window.STATE_ABBR_FROM_NAME && window.STATE_ABBR_FROM_NAME[fullName]) {{
              stateAbbr = window.STATE_ABBR_FROM_NAME[fullName];
            }}
            const displayLabel = stateAbbr || (props.name || props.STATE_NAME || props.state);
            if ({str(self.show_states).lower()} && displayLabel) {{
              const tooltip = L.tooltip({{direction:'center', permanent:true, className:'label-text no-border state-label'}});
              tooltip.setContent(displayLabel);
              layer.bindTooltip(tooltip);
            }}
            if ({str(self.show_states).lower()} && window.propOverlayEnabled) {{
              const st = stateAbbr || '';
              const stEntry = st && window.propStateScores[st];
              if (stEntry) {{
                const tip = stEntry.band + ' (' + stEntry.level.toUpperCase() + ')';
                layer.on('mouseover', function() {{
                  this.bindTooltip(tip, {{direction:'top', sticky:true}});
                  this.openTooltip();
                }});
                layer.on('mouseout', function() {{
                  this.closeTooltip();
                }});
              }}
            }}
            // FEMA region tooltip from state
            if ({str(self.show_regions).lower()}) {{
              const abbrev = (props.state_abbrev || props.state || props.name || '').toUpperCase();
              const fullName = (props.STATE_NAME || props.name || props.state || '').toUpperCase();
              let reg = null;
              if (abbrev && window.FEMA_LOOKUP_ABBR && window.FEMA_LOOKUP_ABBR[abbrev]) {{
                reg = window.FEMA_LOOKUP_ABBR[abbrev];
              }} else if (fullName && window.FEMA_LOOKUP_NAME && window.FEMA_LOOKUP_NAME[fullName]) {{
                reg = window.FEMA_LOOKUP_NAME[fullName];
              }}
              if (reg) {{
                const labelTxt = 'R' + reg.toString().padStart(2,'0');
                if (window.propOverlayEnabled) {{
                  const st = stateAbbr || '';
                  const stEntry = st && window.propStateScores[st];
                  if (stEntry) {{
                    const tip = stEntry.band + ' (' + stEntry.level.toUpperCase() + ')';
                    layer.bindTooltip(tip);
                  }} else if (window.propRegionScores[labelTxt]) {{
                    const entry = window.propRegionScores[labelTxt];
                    const tip = entry.band + ' (' + entry.level.toUpperCase() + ')';
                    layer.bindTooltip(tip);
                  }}
                }}
                // accumulate center per region
                const c = layer.getBounds().getCenter();
                const key = labelTxt;
                if (!regionCenters[key]) {{
                  regionCenters[key] = {{lat:0, lon:0, count:0}};
                }}
                regionCenters[key].lat += c.lat;
                regionCenters[key].lon += c.lng;
                regionCenters[key].count += 1;
              }}
            }}
          }}
        }}).addTo(map);
        // Add a single label per region using averaged centers
        if ({str(self.show_regions).lower()}) {{
          // Force specific placements for clarity
          regionCenters['R09'] = {{lat: 37.0, lon: -119.0, count: 1}}; // California
          regionCenters['R10'] = {{lat: 47.5, lon: -121.5, count: 1}}; // Washington
          Object.keys(regionCenters).forEach(k => {{
            const entry = regionCenters[k];
            const lat = entry.lat / entry.count;
            const lon = entry.lon / entry.count;
            const icon = L.divIcon({{className:'label-text no-border region-label', html: k, iconAnchor:[0,0]}});
            const marker = L.marker([lat, lon], {{icon}});
            if (window.propOverlayEnabled && window.propRegionScores[k]) {{
              const entry = window.propRegionScores[k];
              const tip = '<span style="white-space:nowrap;">' + entry.band + ' (' + entry.level.toUpperCase() + ')</span>';
              marker.on('mouseover', function() {{
                this.bindTooltip(tip, {{direction:'top', sticky:true}});
                this.openTooltip();
              }});
              marker.on('mouseout', function() {{
                this.closeTooltip();
              }});
              const bandIcon = L.divIcon({{className:'label-text no-border region-band-label', html: tip, iconAnchor:[0,-14]}});
              regionLabelLayer.addLayer(L.marker([lat, lon], {{icon: bandIcon}}));
            }}
            regionLabelLayer.addLayer(marker);
          }});
          regionLabelLayer.addTo(map);
        }}
      }}).catch(err => console.error('GeoJSON load failed', err));
                """
                for u in geojson_urls
            ]
        )
        show_cities_flag = str(self.show_cities).lower()
        show_city_labels_flag = str(show_city_labels).lower()
        min_pop_val = int(city_min_pop)
        fallback_cities = [{"name": n, "lat": la, "lon": lo, "pop": p} for n, la, lo, p in CITIES]
        city_source = f"'{cities_geojson}'" if cities_geojson else "null"
        city_js = f"""
    const cityLayer = L.layerGroup();
    const showCities = {show_cities_flag};
    const showCityLabels = {show_city_labels_flag};
    const minPop = {min_pop_val};
    const citySourceUrl = {city_source};
    const fallbackCities = {json.dumps(fallback_cities)};

    function addCityMarker(name, lat, lon) {{
      const marker = L.circleMarker([lat, lon], {{radius: 4, color: '#1b4f72', weight: 1, fillColor: '#1b4f72', fillOpacity: 0.9}});
      if (showCityLabels && name) {{
        marker.bindTooltip(name, {{direction:'right'}});
      }}
      cityLayer.addLayer(marker);
    }}

    function loadCities() {{
      if (cityLayer._loaded) return;
      cityLayer._loaded = true;
      if (citySourceUrl) {{
        fetch(citySourceUrl)
          .then(r => r.json())
          .then(data => {{
            const layer = L.geoJSON(data, {{
              filter: function(f) {{
                const p = f.properties || {{}};
                const pop = p.pop || p.population || p.POPULATION || p.pop_max || p.pop_min || p.POP;
                if (pop === undefined) return false;
                return Number(pop) >= minPop;
              }},
              pointToLayer: function(feature, latlng) {{
                return L.circleMarker(latlng, {{radius: 4, color: '#1b4f72', weight: 1, fillColor: '#1b4f72', fillOpacity: 0.9}});
              }},
              onEachFeature: function(feature, layer) {{
                const props = feature.properties || {{}};
                const name = props.name || props.NAME || props.city || props.town || '';
                if (name && showCityLabels) {{
                  layer.bindTooltip(name, {{direction:'right'}});
                }}
              }}
            }});
            cityLayer.addLayer(layer);
            updateCityVisibility();
          }})
          .catch(err => console.error('City load failed', err));
      }} else {{
        fallbackCities.forEach(c => {{
          if (c.pop >= minPop) {{
            addCityMarker(c.name, c.lat, c.lon);
          }}
        }});
      }}
    }}

    function updateCityVisibility() {{
      if (!showCities) {{
        map.removeLayer(cityLayer);
        return;
      }}
      if (map.getZoom() >= 5) {{
        loadCities();
        map.addLayer(cityLayer);
      }} else {{
        map.removeLayer(cityLayer);
      }}
    }}
    map.on('zoomend', updateCityVisibility);
    updateCityVisibility();
            """
        dark_map_filter = "filter: brightness(0.75) saturate(0.85) contrast(1.05);" if is_dark else ""
        ui_text_scale = resolve_ui_text_scale(self.settings)
        label_font_px = max(10.0, 10.0 * float(ui_text_scale))
        state_label_font_px = max(10.0, 10.0 * float(ui_text_scale))
        callsign_label_font_px = max(11.0, 11.0 * float(ui_text_scale))
        region_label_font_px = max(12.0, 12.0 * float(ui_text_scale))
        region_band_label_font_px = max(10.0, 10.0 * float(ui_text_scale))
        panel_font_px = max(11.0, 11.0 * float(ui_text_scale))
        legend_font_px = max(12.0, 12.0 * float(ui_text_scale))
        return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Stations Map</title>
  <link rel="stylesheet" href="{leaflet_css}" />
  <style>
    html, body {{ height: 100%; margin: 0; padding: 0; }}
    body {{ min-height: 100%; background: {theme.get("bg", legend_bg)}; }}
    #map-shell {{ height: 100%; display: flex; flex-direction: column; }}
    #map-wrap {{ position: relative; flex: 1 1 auto; min-height: 0; }}
    #map {{ height: 100%; {dark_map_filter} }}
    #legendDock {{ flex: 0 0 auto; display: flex; justify-content: center; padding: 6px 10px 10px; }}
    .label-text {{ font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; font-size: {label_font_px:.1f}px; line-height: 1; letter-spacing: 0; color: {label_color}; background: transparent; padding: 0; border: none; box-shadow: none; pointer-events: none; text-shadow: {label_halo}; white-space: nowrap; text-rendering: optimizeLegibility; -webkit-font-smoothing: antialiased; }}
    .label-text.no-border {{ background: transparent; border: none; box-shadow: none; pointer-events: none; }}
    .state-label {{ color: {state_label_color}; font-size: {state_label_font_px:.1f}px; font-weight: 600; opacity: 0.88; text-transform: uppercase; }}
    .region-label {{ color: {region_label_color}; font-size: {region_label_font_px:.1f}px; font-weight: 800; pointer-events: auto; }}
    .callsign-label {{ color: {callsign_label_color}; font-size: {callsign_label_font_px:.1f}px; font-weight: 700; padding: 1px 4px; border: 1px solid {callsign_chip_border}; border-radius: 3px; background: {callsign_chip_bg}; box-shadow: 0 1px 2px rgba(0,0,0,0.18); pointer-events: auto; }}
    .region-band-label {{ color: {region_band_label_color}; font-size: {region_band_label_font_px:.1f}px; font-weight: 600; pointer-events: none; }}
    .cs-tooltip {{ background: {tooltip_bg}; color: {tooltip_text}; border: 1px solid {tooltip_border}; padding: 5px 7px; border-radius: 4px; box-shadow: 0 1px 3px rgba(0,0,0,0.4); z-index: 10000; }}
    .leaflet-tooltip.cs-tooltip {{ z-index: 10000; pointer-events: none; }}
    .leaflet-popup.cs-tooltip {{ z-index: 10001; }}
    .detail-panel {{ background: {legend_bg}; color: {legend_text}; padding: 6px 8px; border: 1px solid {tooltip_border}; border-radius: 4px; width: 260px; max-width: calc(100vw - 34px); box-sizing: border-box; font-size: {panel_font_px:.1f}px; line-height: 1.35; white-space: normal; overflow-wrap: anywhere; word-break: normal; }}
    .zoom-display {{ padding: 4px 8px; font-size: {panel_font_px:.1f}px; background: {legend_bg}; color: {legend_text}; border: 1px solid {tooltip_border}; }}
    .legend-box {{ background: {legend_bg}; color: {legend_text}; padding: 8px 12px; border: 1px solid {tooltip_border}; border-radius: 4px; font-size: {legend_font_px:.1f}px; line-height: 1.35; max-width: min(100%, 860px); box-sizing: border-box; }}
    .summary-panel {{ background: {legend_bg}; color: {legend_text}; padding: 6px 8px; border: 1px solid {tooltip_border}; border-radius: 4px; font-size: {panel_font_px:.1f}px; line-height: 1.35; min-width: 180px; max-width: 240px; }}
    .summary-region {{ margin-top: 6px; }}
    .summary-region:first-of-type {{ margin-top: 4px; }}
    .summary-region-header {{ font-weight: 700; color: {legend_text}; opacity: 0.95; margin-bottom: 3px; }}
    .summary-row {{ display: flex; justify-content: space-between; gap: 8px; align-items: flex-start; }}
    .summary-row + .summary-row {{ margin-top: 3px; }}
    .summary-state {{ font-weight: 700; }}
    .summary-counts {{ color: {legend_text}; opacity: 0.9; text-align: right; }}
    .legend-rows {{ display: flex; flex-direction: column; align-items: center; gap: 8px; }}
    .legend-row {{ display: inline-flex; flex-wrap: wrap; justify-content: center; align-items: center; gap: 14px; max-width: 100%; }}
    .legend-label {{ font-weight: 700; white-space: nowrap; }}
    .legend-sep {{ display: inline-block; width: 0; height: 12px; border-left: 1px solid {tooltip_border}; opacity: 0.55; }}
    .legend-item {{ display: inline-flex; align-items: center; justify-content: center; gap: 5px; white-space: nowrap; }}
    .legend-swatch {{ display: inline-block; min-width: 12px; text-align: center; }}
    .wx-marker {{ width: 34px; height: 34px; border-radius: 50%; display: flex; align-items: center; justify-content: center; border: 2px solid #455A64; background: #ECEFF1; box-shadow: 0 2px 6px rgba(0,0,0,0.35); position: relative; box-sizing: border-box; }}
    .wx-marker svg {{ width: 21px; height: 21px; display: block; }}
    .wx-severe {{ border-color: #B71C1C; }}
    .wx-caution {{ border-color: #E65100; }}
    .wx-routine {{ border-color: #1565C0; }}
    .wx-unknown {{ border-color: #546E7A; }}
    .wx-kind-general {{ background: #ECEFF1; color: #455A64; }}
    .wx-kind-rain {{ background: #E3F2FD; color: #1565C0; }}
    .wx-kind-storm {{ background: #F3E5F5; color: #6A1B9A; }}
    .wx-kind-wind {{ background: #E0F7FA; color: #00838F; }}
    .wx-kind-snow {{ background: #E1F5FE; color: #0277BD; }}
    .wx-kind-flood {{ background: #E0F2F1; color: #00695C; }}
    .wx-kind-fire {{ background: #FFF3E0; color: #E65100; }}
    .wx-kind-heat {{ background: #FFEBEE; color: #C62828; }}
    .wx-count {{ position: absolute; right: -7px; top: -7px; min-width: 16px; height: 16px; padding: 0 4px; border-radius: 8px; background: #263238; color: white; font-size: 10px; line-height: 16px; text-align: center; font-weight: 700; border: 1px solid rgba(255,255,255,0.85); box-sizing: border-box; }}
    .op-marker {{ width: 34px; height: 34px; border-radius: 7px; display: flex; align-items: center; justify-content: center; border: 2px solid #455A64; background: #ECEFF1; box-shadow: 0 2px 6px rgba(0,0,0,0.35); position: relative; box-sizing: border-box; }}
    .op-marker svg {{ width: 21px; height: 21px; display: block; }}
    .op-source-hf {{ border-radius: 7px; outline: 2px solid rgba(0,105,92,0.28); }}
    .op-source-local {{ border-radius: 50% 50% 50% 8px; outline: 2px solid rgba(94,53,177,0.32); transform: rotate(-45deg); }}
    .op-source-local svg, .op-source-local .wx-count {{ transform: rotate(45deg); }}
    .op-source-pin {{ border-radius: 50% 50% 50% 6px; outline: 2px solid rgba(245,127,23,0.38); background: #FFF8E1; }}
    .op-source-mixed {{ border-radius: 50%; outline: 2px solid rgba(69,90,100,0.35); }}
    .op-severe {{ border-color: #B71C1C; }}
    .op-caution {{ border-color: #E65100; }}
    .op-routine {{ border-color: #1565C0; }}
    .op-unknown {{ border-color: #546E7A; }}
    .op-layer-alert {{ background: #FFF8E1; color: #F57F17; }}
    .op-layer-infrastructure {{ background: #E8F5E9; color: #2E7D32; }}
    .op-kind-power {{ background: #FFFDE7; color: #F9A825; }}
    .op-kind-water {{ background: #E3F2FD; color: #1565C0; }}
    .op-kind-comms {{ background: #E0F7FA; color: #00838F; }}
    .op-kind-transport {{ background: #EFEBE9; color: #5D4037; }}
    .op-kind-warning {{ background: #FFF8E1; color: #F57F17; }}
    .op-kind-evacuation {{ background: #FFEBEE; color: #C62828; }}
    .op-kind-rfi {{ background: #EDE7F6; color: #5E35B1; }}
  </style>
</head>
<body>
  <div id="map-shell">
    <div id="map-wrap">
      <div id="map"></div>
    </div>
    <div id="legendDock">
      <div class="legend-box" id="legendBox"></div>
    </div>
  </div>
  <script src="{leaflet_js}"></script>
  <script>
    window.FEMA_LOOKUP = {json.dumps({s:r[1:] for r,states in FEMA_REGIONS.items() for s in states})};
    const regionColors = ['#1E88E5','#43A047','#FB8C00','#8E24AA','#00ACC1','#F4511E','#3949AB','#FB8C00','#6D4C41','#00897B'];
    window.propOverlayEnabled = {str(bool(prop_overlay_enabled)).lower()};
    window.propRegionScores = {json.dumps(prop_region_best)};
    window.propStateScores = {json.dumps(prop_state_best)};
    window.propBandColors = {json.dumps(prop_colors)};
    const markers = {markers_json};
    const links = {links_json};
    const weatherEvents = {weather_events_json};
    const alertEvents = {alert_events_json};
    const infrastructureEvents = {infrastructure_events_json};
    let sitrepStateSummary = {sitrep_state_summary_json};
    let sitrepSummaryGroup = {sitrep_summary_group_json};
    window.FEMA_LOOKUP_ABBR = {json.dumps({s:r[1:] for r,states in FEMA_REGIONS.items() for s in states})};
    window.FEMA_LOOKUP_NAME = {json.dumps({US_STATE_NAMES[s]:r[1:] for r,states in FEMA_REGIONS.items() for s in states if s in US_STATE_NAMES})};
    window.STATE_ABBR_FROM_NAME = {json.dumps({**US_STATE_ABBR_FROM_NAME, **CANADA_PROV_ABBR_FROM_NAME})};
    if (typeof L === 'undefined') {{
      document.getElementById('map').innerHTML = '<h3>Leaflet failed to load.</h3>';
    }} else {{
    const map = L.map('map', {{maxZoom: {max_zoom}}}).setView([{init_lat}, {init_lon}], {init_zoom});
    // Dedicated pane for stations to keep them above overlays
    map.createPane('stationsPane');
    map.getPane('stationsPane').style.zIndex = 650;
    map.getPane('stationsPane').style.pointerEvents = 'auto';
    window._leafletMap = map;
    window._lastView = {{lat: {init_lat}, lon: {init_lon}, zoom: {init_zoom}}};
    {tile_layer}
    const regionLabelLayer = L.layerGroup();
    if ({str(self.show_regions).lower()}) {{
      regionLabelLayer.addTo(map);
    }}
    // Zoom display control
    const ZoomDisplay = L.Control.extend({{
      options: {{ position: 'topright' }},
      onAdd: function() {{
        const div = L.DomUtil.create('div', 'leaflet-bar zoom-display');
        div.innerHTML = 'Zoom: 0%';
        return div;
      }}
    }});
    const zoomDisplay = new ZoomDisplay();
    map.addControl(zoomDisplay);
    function updateZoomDisplay() {{
      const pct = Math.round((map.getZoom() / map.getMaxZoom()) * 100);
      const el = document.querySelector('.zoom-display');
      if (el) {{
        el.innerHTML = 'Zoom: ' + pct + '%';
      }}
      const c = map.getCenter();
      window._lastView = {{lat: c.lat, lon: c.lng, zoom: map.getZoom()}};
    }}
    map.on('zoomend', updateZoomDisplay);
    map.on('moveend', updateZoomDisplay);
    updateZoomDisplay();
    {geojson_fetches}
    {road_fetch}
    {grid_layer}
    L.control.zoom({{position:'topright'}}).addTo(map);
    // USA outline frame
    const frame = [[{USA_FRAME[0][0]}, {USA_FRAME[0][1]}], [{USA_FRAME[1][0]}, {USA_FRAME[1][1]}]];
    L.rectangle(frame, {{color: '#444', weight: 1, fillOpacity: 0}}).addTo(map);

    // Cities/towns overlay (pop filter)
    {city_js}

    // Detail panel (top right)
    const detailPanel = L.control({{position: 'topright'}});
    detailPanel.onAdd = function() {{
      this._div = L.DomUtil.create('div', 'detail-panel');
      this._div.innerHTML = '<b>Station Detail</b><br/>Hover Over Stations to Display.';
      return this._div;
    }};
    detailPanel.addTo(map);
    function showDetail(html) {{
      const el = document.querySelector('.detail-panel');
      if (el) el.innerHTML = html;
    }}

    function buildSitrepSummaryHtml(rows, groupName) {{
      if (!rows || !rows.length) {{
        return '<b>SitRep State Summary</b><br/>No current state rollups.';
      }}
      const header = '<b>SitRep State Summary</b><br/>' + (groupName ? ('Group: ' + groupName + '<br/>') : 'All Groups<br/>');
      const regionBuckets = new Map();
      (rows || []).forEach(r => {{
        const stateCode = String(r.state_code || '').toUpperCase();
        const regionCode = window.FEMA_LOOKUP[stateCode] ? ('R' + window.FEMA_LOOKUP[stateCode]) : 'OTHER';
        if (!regionBuckets.has(regionCode)) {{
          regionBuckets.set(regionCode, []);
        }}
        regionBuckets.get(regionCode).push(r);
      }});
      const orderedRegions = Array.from(regionBuckets.keys()).sort((a, b) => {{
        if (a === 'OTHER') return 1;
        if (b === 'OTHER') return -1;
        return a.localeCompare(b);
      }});
      const body = orderedRegions.map(regionCode => {{
        const label = regionCode === 'OTHER' ? 'Other / Non-FEMA' : ('Region ' + regionCode.replace(/^R/, ''));
        const totals = (regionBuckets.get(regionCode) || []).reduce((acc, r) => {{
          acc.callsign_count += (r.callsign_count || 0);
          acc.red_count += (r.red_count || 0);
          acc.yellow_count += (r.yellow_count || 0);
          acc.green_count += (r.green_count || 0);
          acc.unknown_count += (r.unknown_count || 0);
          acc.js8_count += (r.js8_count || 0);
          acc.internet_count += (r.internet_count || 0);
          acc.mixed_transport_count += (r.mixed_transport_count || 0);
          return acc;
        }}, {{
          callsign_count: 0,
          red_count: 0,
          yellow_count: 0,
          green_count: 0,
          unknown_count: 0,
          js8_count: 0,
          internet_count: 0,
          mixed_transport_count: 0
        }});
        const counts = [
          'R' + totals.red_count,
          'Y' + totals.yellow_count,
          'G' + totals.green_count,
          'U' + totals.unknown_count
        ].join(' ');
        const receipt = [
          totals.js8_count ? ('JS8 ' + totals.js8_count) : '',
          totals.internet_count ? ('Net ' + totals.internet_count) : '',
          totals.mixed_transport_count ? ('Mix ' + totals.mixed_transport_count) : ''
        ].filter(Boolean).join(' | ');
        return '<div class="summary-region">' +
          '<div class="summary-region-header">' + label + '</div>' +
          '<div class="summary-row">' +
          '<span class="summary-state">' + totals.callsign_count + ' reporting</span>' +
          '<span class="summary-counts">' + counts + (receipt ? ('<br/>' + receipt) : '') + '</span>' +
          '</div>' +
          '</div>';
      }}).join('');
      return header + body;
    }}

    const sitrepSummaryPanel = L.control({{position: 'bottomleft'}});
    sitrepSummaryPanel.onAdd = function() {{
      this._div = L.DomUtil.create('div', 'summary-panel');
      this._div.innerHTML = buildSitrepSummaryHtml(sitrepStateSummary, sitrepSummaryGroup);
      this._div.style.display = (sitrepStateSummary && sitrepStateSummary.length) ? 'block' : 'none';
      return this._div;
    }};
    sitrepSummaryPanel.addTo(map);
    function updateSitrepSummaryPanel(rows, groupName) {{
      const el = document.querySelector('.summary-panel');
      if (el) {{
        el.style.display = (rows && rows.length) ? 'block' : 'none';
        el.innerHTML = buildSitrepSummaryHtml(rows || [], groupName || '');
      }}
    }}

    // Legend for link colors
    function linkColor(val) {{
      if (val === null || val === undefined || isNaN(val)) return '#607d8b';
      if (val >= 5) return '#1b5e20';
      if (val >= 0) return '#2e7d32';
      if (val >= -5) return '#fbc02d';
      if (val >= -10) return '#f57c00';
      return '#c62828';
    }}
    function legendItem(color, symbol, label) {{
      return '<div class="legend-item"><span class="legend-swatch" style="color:' + color + ';">' + symbol + '</span><span>' + label + '</span></div>';
    }}
    function legendRow(label, items) {{
      const body = items.map(function(item, idx) {{
        return (idx ? '<span class="legend-sep"></span>' : '') + item;
      }}).join('');
      return '<div class="legend-row"><span class="legend-label">' + label + '</span>' + body + '</div>';
    }}
    let nowReachableEnabled = {now_reachable_enabled};
    const propOverlayLegendEnabled = {'true' if prop_overlay_enabled else 'false'};
    function buildLegendHtml(showPeerSchedNow) {{
      const rows = [];
      rows.push(legendRow('Link SNR:', [
        legendItem(linkColor(5), '&#9632;', '&gt;= 5'),
        legendItem(linkColor(0), '&#9632;', '0 to &lt;5'),
        legendItem(linkColor(-5), '&#9632;', '-5 to &lt;0'),
        legendItem(linkColor(-6), '&#9632;', '-10 to &lt;-5'),
        legendItem(linkColor(-11), '&#9632;', '&lt; -10')
      ]));
      rows.push(legendRow('SitRep Status:', [
        legendItem('#43A047', '&#9679;', 'Functioning'),
        legendItem('#FBC02D', '&#9679;', 'Partially Functioning'),
        legendItem('#D32F2F', '&#9679;', 'Not Functioning'),
        legendItem('#4FC3F7', '&#9679;', 'Unknown / No Report')
      ]));
      if (showPeerSchedNow) {{
        rows.push(legendRow('Peer Sched Now:', [
          legendItem('#2E7D32', '&#9679;', 'NOW'),
          legendItem('#1E88E5', '&#9679;', 'Later Today'),
          legendItem('#7E57C2', '&#9679;', 'QSY &lt;10m')
        ]));
      }}
      rows.push(legendRow('Report Source:', [
        legendItem('#00695C', '&#9632;', 'HF'),
        legendItem('#5E35B1', '&#9670;', 'Local'),
        legendItem('#455A64', '&#9679;', 'Mixed')
      ]));
      if (propOverlayLegendEnabled) {{
        rows.push(legendRow(
          'Best Band Now:',
          Object.keys(window.propBandColors).map(k => legendItem(window.propBandColors[k], '&#9632;', k))
        ));
      }}
      return '<div class="legend-rows">' + rows.join('') + '</div>';
    }}
    function updateLegend() {{
      const legendEl = document.getElementById('legendBox');
      if (legendEl) {{
        legendEl.innerHTML = buildLegendHtml(nowReachableEnabled);
      }}
    }}
    updateLegend();

    const stationsLayer = L.layerGroup().addTo(map);
    const linksLayer = L.layerGroup().addTo(map);
    const weatherLayer = L.layerGroup().addTo(map);
    const alertLayer = L.layerGroup().addTo(map);
    const infrastructureLayer = L.layerGroup().addTo(map);

    function weatherSvg(kind) {{
      const common = "fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'";
      if (kind === 'storm') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M7 18a4 4 0 1 1 .9-7.9A6 6 0 0 1 19 12.5 3.5 3.5 0 0 1 18 19h-2"/><path ${{common}} d="M13 13l-3 5h4l-2 4"/></svg>`;
      if (kind === 'rain') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M7 17a4 4 0 1 1 .9-7.9A6 6 0 0 1 19 11.5 3.5 3.5 0 0 1 18 18H8"/><path ${{common}} d="M8 21l1-2M13 21l1-2M18 21l1-2"/></svg>`;
      if (kind === 'wind') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M3 8h12a3 3 0 1 0-3-3"/><path ${{common}} d="M3 13h16a3 3 0 1 1-3 3"/><path ${{common}} d="M3 18h8"/></svg>`;
      if (kind === 'snow') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M12 2v20M4.9 4.9l14.2 14.2M2 12h20M4.9 19.1L19.1 4.9"/></svg>`;
      if (kind === 'fire') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M12 22c4 0 7-3 7-7 0-3-2-5-4-7 .2 2-.8 3.2-2 4-1-4-4-6-4-9-3 2-5 6-5 10 0 5 3.5 9 8 9z"/></svg>`;
      if (kind === 'flood') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M3 16c2 0 2-1 4-1s2 1 4 1 2-1 4-1 2 1 4 1 2-1 2-1"/><path ${{common}} d="M3 20c2 0 2-1 4-1s2 1 4 1 2-1 4-1 2 1 4 1 2-1 2-1"/><path ${{common}} d="M12 3l5 8H7l5-8z"/></svg>`;
      if (kind === 'heat') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M14 14.8V5a2 2 0 0 0-4 0v9.8a4 4 0 1 0 4 0z"/><path ${{common}} d="M12 9v8"/></svg>`;
      return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M7 18a4 4 0 1 1 .9-7.9A6 6 0 0 1 19 12.5 3.5 3.5 0 0 1 18 19H8"/></svg>`;
    }}

    function weatherIcon(event) {{
      const severity = (event.severity || 'unknown').toLowerCase();
      const kind = (event.icon || 'general').toLowerCase();
      const count = Number(event.count || 0);
      const badge = count > 1 ? `<span class="wx-count">${{count > 99 ? '99+' : count}}</span>` : '';
      return L.divIcon({{
        className: '',
        html: `<div class="wx-marker wx-${{severity}} wx-kind-${{kind}}">${{weatherSvg(kind)}}${{badge}}</div>`,
        iconSize: [34, 34],
        iconAnchor: [17, 17]
      }});
    }}

    function renderWeatherEvents(list) {{
      weatherLayer.clearLayers();
      (list || []).forEach(event => {{
        if (event.lat === undefined || event.lon === undefined) return;
        const marker = L.marker([event.lat, event.lon], {{icon: weatherIcon(event), pane: 'stationsPane'}});
        const tipText = event.tooltip || 'Weather report received';
        marker.on('mouseover', function() {{ showDetail(tipText); }});
        marker.on('click', function() {{ showDetail(tipText); }});
        weatherLayer.addLayer(marker);
      }});
    }}

    function operationalSvg(kind, layerType) {{
      const common = "fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'";
      if (kind === 'power') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M13 2L5 14h6l-1 8 8-12h-6l1-8z"/></svg>`;
      if (kind === 'water') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M12 3s6 6.4 6 11a6 6 0 0 1-12 0c0-4.6 6-11 6-11z"/></svg>`;
      if (kind === 'comms') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M5 12.5a10 10 0 0 1 14 0"/><path ${{common}} d="M8.5 16a5 5 0 0 1 7 0"/><path ${{common}} d="M12 20h.01"/></svg>`;
      if (kind === 'transport') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M6 19L10 3h4l4 16"/><path ${{common}} d="M8 11h8M7 15h10"/></svg>`;
      if (kind === 'evacuation') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M12 3l9 18H3L12 3z"/><path ${{common}} d="M12 9v5M12 17h.01"/></svg>`;
      if (kind === 'rfi') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M9 9a3 3 0 1 1 4.5 2.6c-1 .6-1.5 1.2-1.5 2.4"/><path ${{common}} d="M12 18h.01"/><circle ${{common}} cx="12" cy="12" r="10"/></svg>`;
      if (kind === 'warning' || layerType === 'alert') return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M12 3l9 18H3L12 3z"/><path ${{common}} d="M12 9v5M12 17h.01"/></svg>`;
      return `<svg viewBox="0 0 24 24" aria-hidden="true"><path ${{common}} d="M4 8h16M4 16h16M8 4v16M16 4v16"/></svg>`;
    }}

    function operationalIcon(event, layerType) {{
      const severity = (event.severity || 'unknown').toLowerCase();
      const kind = (event.icon || 'general').toLowerCase();
      const sourceKind = (event.source_kind || 'hf').toLowerCase();
      const count = Number(event.count || 0);
      const badge = count > 1 ? `<span class="wx-count">${{count > 99 ? '99+' : count}}</span>` : '';
      return L.divIcon({{
        className: '',
        html: `<div class="op-marker op-${{severity}} op-layer-${{layerType}} op-kind-${{kind}} op-source-${{sourceKind}}">${{operationalSvg(kind, layerType)}}${{badge}}</div>`,
        iconSize: [34, 34],
        iconAnchor: [17, 17]
      }});
    }}

    function renderOperationalEvents(layer, list, layerType) {{
      layer.clearLayers();
      (list || []).forEach(event => {{
        if (event.lat === undefined || event.lon === undefined) return;
        const marker = L.marker([event.lat, event.lon], {{icon: operationalIcon(event, layerType), pane: 'stationsPane'}});
        const tipText = event.tooltip || 'Report received';
        marker.on('mouseover', function() {{ showDetail(tipText); }});
        marker.on('click', function() {{ showDetail(tipText); }});
        layer.addLayer(marker);
      }});
    }}

    function renderMarkers(list) {{
      stationsLayer.clearLayers();
      list.forEach(m => {{
        const qsySoon = !!m.qsy_soon;
        const qsyText = String(m.qsy_text || '').toLowerCase();
        const scheduleState = qsySoon
          ? 'qsy_soon'
          : (qsyText.startsWith('stable') ? 'now' : 'later_today');
        const scheduleStrokeByState = {{
          now: '#2E7D32',
          later_today: '#1E88E5',
          qsy_soon: '#7E57C2'
        }};
        const statusKey = (m.spotter_status_key || 'unknown').toLowerCase();
        const markerFillByStatus = {{
          red: '#D32F2F',
          yellow: '#FBC02D',
          green: '#43A047',
          unknown: '#4FC3F7'
        }};
        const markerStrokeByStatus = {{
          red: '#8E0000',
          yellow: '#8D6E00',
          green: '#1B5E20',
          unknown: '#1976D2'
        }};
        const baseStroke = markerStrokeByStatus[statusKey] || markerStrokeByStatus.unknown;
        const markerStroke = nowReachableEnabled
          ? (scheduleStrokeByState[scheduleState] || baseStroke)
          : (qsySoon ? '#5E35B1' : baseStroke);
        const markerFill = markerFillByStatus[statusKey] || markerFillByStatus.unknown;
        const circle = L.circleMarker([m.lat, m.lon], {{
          radius: qsySoon ? 7 : 6,
          color: markerStroke,
          weight: 1,
          fillColor: markerFill,
          fillOpacity: 0.8,
          pane: 'stationsPane'
        }});
        stationsLayer.addLayer(circle);
        const hasJS8 = m.last_seen || m.last_band || m.last_contact || m.last_contact_band || m.direct_snr !== undefined || m.avg_snr_excl_my !== undefined;
        const hasVarAC = m.varac_last_seen || m.varac_last_band || m.varac_avg_snr !== undefined;
        const tipText = (m.tooltip || m.title || '') +
          (hasJS8 || hasVarAC ? '<br/>Activity' : '') +
          (m.last_seen ? '<br/>Last Seen: ' + m.last_seen : '') +
          (m.last_band ? '<br/>Last Band: ' + m.last_band : '') +
          (m.last_contact ? '<br/>Last Contact: ' + m.last_contact : '') +
          (m.last_contact_band ? '<br/>Last Contact Band: ' + m.last_contact_band : '') +
          (m.last_contact_snr !== undefined && m.last_contact_snr !== null ? '<br/>Last Contact SNR: ' + m.last_contact_snr.toFixed(1) : '') +
          (m.direct_snr !== undefined && m.direct_snr !== null ? '<br/>Direct SNR Avg: ' + m.direct_snr.toFixed(1) : '') +
          (m.avg_snr_excl_my !== undefined && m.avg_snr_excl_my !== null ? '<br/>Direct SNR Avg (Excl My): ' + m.avg_snr_excl_my.toFixed(1) : '') +
          (m.varac_last_seen ? '<br/>VarAC Last Seen: ' + m.varac_last_seen : '') +
          (m.varac_last_band ? '<br/>VarAC Last Band: ' + m.varac_last_band : '') +
          (m.varac_avg_snr !== undefined && m.varac_avg_snr !== null ? '<br/>VarAC Avg SNR: ' + m.varac_avg_snr.toFixed(1) : '') +
          (m.spotter_status_label ? '<br/>SitRep: ' + m.spotter_status_label : '') +
          (m.spotter_status_group ? '<br/>Report Group: ' + m.spotter_status_group : '') +
          (m.spotter_status_transport ? '<br/>Receipt: ' + m.spotter_status_transport : '') +
          (m.spotter_status_state ? '<br/>State: ' + m.spotter_status_state : '') +
          (m.spotter_status_state_conf ? '<br/>State Confidence: ' + m.spotter_status_state_conf : '') +
          (m.spotter_status_geo_conf ? '<br/>Geo Confidence: ' + m.spotter_status_geo_conf : '') +
          (m.spotter_status_brevity ? '<br/>Brevity: ' + m.spotter_status_brevity : '') +
          (m.spotter_status_source ? '<br/>Source: ' + m.spotter_status_source + (m.spotter_status_source_detail ? ' (' + m.spotter_status_source_detail + ')' : '') : '') +
          (m.spotter_status_source_chips ? '<br/>Sources: ' + m.spotter_status_source_chips : '') +
          (m.spotter_status_conflict ? '<br/>Conflict: sources disagree' : '') +
          (m.spotter_status_ts ? '<br/>SitRep Updated: ' + m.spotter_status_ts : '') +
          (m.spotter_status_age ? '<br/>SitRep Age: ' + m.spotter_status_age : '') +
          (m.qsy_text ? '<br/>Schedule: ' + m.qsy_text : '');
        circle.on('mouseover', function() {{
          this.bringToFront();
          showDetail(tipText);
        }});
        circle.on('click', function() {{
          this.bringToFront();
          showDetail(tipText);
        }});
        // Permanent label only when show_callsigns is on
        if (m.label) {{
          const icon = L.divIcon({{
            className: 'label-text callsign-label',
            html: m.label
          }});
          const labelMarker = L.marker([m.lat, m.lon], {{icon, pane:'stationsPane'}});
          stationsLayer.addLayer(labelMarker);
          labelMarker.on('mouseover', function() {{
            showDetail(tipText);
          }});
          labelMarker.on('click', function() {{
            showDetail(tipText);
          }});
        }}
      }});
    }}
    // JS8 links
    function renderLinks(list) {{
      linksLayer.clearLayers();
      list.forEach(l => {{
        const line = L.polyline([[l.lat1, l.lon1], [l.lat2, l.lon2]], {{color: linkColor(l.snr), weight: 2.5, opacity: 0.8}});
        linksLayer.addLayer(line);
      }});
    }}

    window.updateMapData = function(payload) {{
      if (!payload) return;
      if (payload.markers) renderMarkers(payload.markers);
      if (payload.links) renderLinks(payload.links);
      if (payload.weather_events) renderWeatherEvents(payload.weather_events);
      if (payload.alert_events) renderOperationalEvents(alertLayer, payload.alert_events, 'alert');
      if (payload.infrastructure_events) renderOperationalEvents(infrastructureLayer, payload.infrastructure_events, 'infrastructure');
      if (Object.prototype.hasOwnProperty.call(payload, 'now_reachable_enabled')) {{
        nowReachableEnabled = !!payload.now_reachable_enabled;
        updateLegend();
      }}
      if (Object.prototype.hasOwnProperty.call(payload, 'sitrep_state_summary')) {{
        sitrepStateSummary = payload.sitrep_state_summary || [];
      }}
      if (Object.prototype.hasOwnProperty.call(payload, 'sitrep_summary_group')) {{
        sitrepSummaryGroup = payload.sitrep_summary_group || '';
      }}
      updateSitrepSummaryPanel(sitrepStateSummary, sitrepSummaryGroup);
    }};
    window.updateMapData({{
      markers: markers,
      links: links,
      weather_events: weatherEvents,
      alert_events: alertEvents,
      infrastructure_events: infrastructureEvents,
      sitrep_state_summary: sitrepStateSummary,
      sitrep_summary_group: sitrepSummaryGroup
    }});
    window._mapReady = true;
    }}
    </script>
</body>
</html>
        """

    # ------------- UI handlers ------------- #
    def _on_show_calls_changed(self, state):
        self.show_callsigns = bool(state)
        self._save_display_preferences()
        self._request_map_refresh(level="light", reason="toggle_callsigns")

    def _on_show_states_changed(self, state):
        self.show_states = bool(state)
        self._sync_city_pop_enabled()
        self._save_display_preferences()
        self._request_map_refresh(level="light", reason="toggle_states")

    def _on_show_cities_changed(self, state):
        self.show_cities = bool(state)
        self._sync_city_pop_enabled()
        self.show_city_labels = self.show_cities
        self._save_display_preferences()
        self._request_map_refresh(level="light", reason="toggle_cities")

    def _on_show_grid_labels_changed(self, state):
        # Single toggle now controls both grid lines and labels
        enabled = bool(state)
        self.show_grids = enabled
        self.show_grid_labels = enabled
        self._save_display_preferences()
        self._request_map_refresh(level="light", reason="toggle_grids")

    def _on_map_stations_changed(self, state):
        self.show_station_markers = bool(state)
        self._save_display_preferences()
        self._request_map_refresh(level="medium", reason="toggle_station_markers")

    def _on_map_links_changed(self, state):
        self.show_link_paths = bool(state)
        self._save_display_preferences()
        self._request_map_refresh(level="medium", reason="toggle_link_paths")

    def _on_map_weather_changed(self, state):
        self.show_weather_reports = bool(state)
        self._save_display_preferences()
        self._request_map_refresh(level="medium", reason="toggle_weather")

    def _on_map_alerts_changed(self, state):
        self.show_alert_reports = bool(state)
        self._save_display_preferences()
        self._request_map_refresh(level="medium", reason="toggle_alerts")

    def _on_map_infrastructure_changed(self, state):
        self.show_infrastructure_reports = bool(state)
        self._save_display_preferences()
        self._request_map_refresh(level="medium", reason="toggle_infrastructure")

    def _on_show_regions_changed(self, state):
        self.show_regions = bool(state)
        self._save_display_preferences()
        self._request_map_refresh(level="light", reason="toggle_regions")

    def _on_city_pop_changed(self, idx: int):
        try:
            val = int(self.city_pop_combo.itemData(idx))
        except Exception:
            val = 100000
        self.city_pop_min = val
        if self.show_cities or self.show_states:
            self._request_map_refresh(level="light", reason="city_population")
        self._save_display_preferences()

    def _on_link_mode_changed(self, idx: int):
        data = self.link_mode_combo.itemData(idx) if hasattr(self, "link_mode_combo") else ("off", "")
        self.link_mode, self.link_value = self._parse_link_selection(data)
        if self._now_reachable_enabled and (self.link_mode or "").lower() == "off":
            try:
                self.link_mode_combo.blockSignals(True)
                self.link_mode_combo.setCurrentText("My Station")
                self.link_mode_combo.blockSignals(False)
                self.link_mode, self.link_value = self._parse_link_selection(self.link_mode_combo.currentData())
            except Exception:
                pass
        if (self.link_mode or "").lower() == "off":
            self.relay_target = ""
            try:
                self.relay_target_combo.blockSignals(True)
                self.relay_target_combo.setCurrentIndex(0)
                self.relay_target_combo.setEditText("")
            except Exception:
                pass
            finally:
                try:
                    self.relay_target_combo.blockSignals(False)
                except Exception:
                    pass
        self._request_map_refresh(level="medium", reason="link_mode")

    def _on_group_filter_changed(self, idx: int):
        self._request_map_refresh(level="medium", reason="group_filter")

    def _on_region_filter_changed(self, idx: int):
        self._request_map_refresh(level="medium", reason="region_filter")

    def _on_band_changed(self, idx: int):
        self.selected_band = self.band_combo.itemText(idx)
        self._request_map_refresh(level="medium", reason="band_filter")

    def _on_recency_changed(self, idx: int):
        val = self.recency_combo.itemText(idx)
        mapping = {
            "Any": None,
            "15m": 15 * 60,
            "30m": 30 * 60,
            "1h": 60 * 60,
            "3h": 3 * 60 * 60,
            "6h": 6 * 60 * 60,
            "12h": 12 * 60 * 60,
            "24h": 24 * 60 * 60,
            "7d": 7 * 24 * 60 * 60,
        }
        self.recency_seconds = mapping.get(val, None)
        self._request_map_refresh(level="medium", reason="recency_filter")

    def _on_map_topic_filter_changed(self, _idx: int):
        self._clear_report_query_caches()
        self._request_map_refresh(level="medium", reason="topic_filter")

    def _on_relay_target_changed(self, text: str):
        normalized = self._relay_target_callsign_from_text(text)
        if normalized == self.relay_target:
            return
        self.relay_target = normalized
        self._request_map_refresh(level="medium", reason="relay_target")

    def _on_prop_overlay_changed(self, state):
        self.prop_overlay_enabled = bool(state)
        self._save_display_preferences()
        self._request_map_refresh(level="full", reason="prop_overlay")

    def _on_prop_mode_changed(self, _idx: int) -> None:
        if self.prop_mode_combo is None:
            return
        mode = self.prop_mode_combo.currentData() or self.prop_mode_combo.currentText()
        mode = str(mode or "blended").strip().lower()
        if mode == "adaptive":
            mode = "actual"
        if mode not in {"model", "actual", "blended"}:
            mode = "blended"
        self.prop_mode = mode
        self._save_display_preferences()
        self._request_map_refresh(level="full", reason="prop_mode")

    def _on_prop_window_changed(self, _idx: int) -> None:
        if self.prop_window_combo is None:
            return
        try:
            hours = int(self.prop_window_combo.currentData())
        except Exception:
            hours = 6
        self.prop_window_hours = hours
        self._save_display_preferences()
        self._request_map_refresh(level="full", reason="prop_window")

    def _on_prop_target_type_changed(self, _idx: int) -> None:
        if self._prop_target_syncing or self.prop_target_type_combo is None or not self.settings:
            return
        target_type = (self.prop_target_type_combo.currentData() or "REGION").strip().upper()
        self._prop_target_syncing = True
        try:
            self._set_prop_target_value_options(target_type, "")
            value = (self.prop_target_value_combo.currentText() if self.prop_target_value_combo is not None else "").strip().upper()
            self.settings.set_many(
                {
                    "prop_target_type": target_type,
                    "prop_target_value": value,
                }
            )
        except Exception as e:
            log.debug("StationsMap: propagation target type change failed: %s", e)
        finally:
            self._prop_target_syncing = False
        self._request_map_refresh(level="full", reason="prop_target_type")

    def _on_prop_target_value_changed(self, text: str) -> None:
        if self._prop_target_syncing or self.prop_target_type_combo is None or not self.settings:
            return
        target_type = (self.prop_target_type_combo.currentData() or "REGION").strip().upper()
        value = (text or "").strip().upper()
        if target_type == "REGION" and value == "NATIONAL":
            value = "ALL"
        try:
            self.settings.set_many(
                {
                    "prop_target_type": target_type,
                    "prop_target_value": value,
                }
            )
        except Exception as e:
            log.debug("StationsMap: propagation target value change failed: %s", e)
        self._request_map_refresh(level="full", reason="prop_target_value")

    def _on_prop_adaptive_changed(self, state):
        self.prop_adaptive_enabled = bool(state)
        self._save_display_preferences()
        self._request_map_refresh(level="full", reason="prop_adaptive")
    def _ensure_leaflet_assets(self) -> tuple[str, str]:
        """
        Resolve Leaflet asset URLs without blocking the UI thread.
        Prefer local bundled assets; fall back to CDN when unavailable.
        Returns (js_url, css_url).
        """
        js_file = self._asset_dir / "leaflet.js"
        css_file = self._asset_dir / "leaflet.css"
        self._asset_dir.mkdir(parents=True, exist_ok=True)

        js_url = QUrl.fromLocalFile(str(js_file)).toString() if js_file.exists() else "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
        css_url = QUrl.fromLocalFile(str(css_file)).toString() if css_file.exists() else "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
        return js_url, css_url

    def _ensure_geojson(self, dest: Path, url: str) -> Optional[str]:
        """
        Resolve GeoJSON URL without blocking the UI thread.
        Prefer local file; fall back to remote URL when unavailable.
        """
        if dest.exists() and dest.stat().st_size > 0:
            return QUrl.fromLocalFile(str(dest)).toString()
        return url

    def _ensure_cities_geojson(self) -> Optional[str]:
        """
        Return a local GeoJSON URL for cities/towns (pop >= 1000) if available.
        Users can drop a pre-filtered file at config/leaflet/cities_na_1k.geojson.
        """
        try:
            if self._cities_geojson.exists() and self._cities_geojson.stat().st_size > 0:
                return QUrl.fromLocalFile(str(self._cities_geojson)).toString()
            # fallback to Natural Earth populated places if downloaded
            ne_places = self._asset_dir / "ne_populated_places.geojson"
            if ne_places.exists() and ne_places.stat().st_size > 0:
                return QUrl.fromLocalFile(str(ne_places)).toString()
        except Exception as e:
            log.warning("StationsMap: failed to load cities geojson: %s", e)
        return None

    def _ensure_fema_geojson(self) -> Optional[str]:
        """
        Build a simple GeoJSON for FEMA regions from the state outline data if available.
        """
        # If we already built it, reuse
        fema_path = self._asset_dir / "fema_regions.geojson"
        if fema_path.exists() and fema_path.stat().st_size > 0:
            return QUrl.fromLocalFile(str(fema_path)).toString()

        # Try to derive from US states GeoJSON
        us_path = self._geojson_path
        if not us_path.exists():
            return None
        try:
            import json as _json
            data = _json.loads(us_path.read_text(encoding="utf-8"))
            features = []
            for feat in data.get("features", []):
                props = feat.get("properties", {})
                name = props.get("name") or props.get("STATE_NAME") or props.get("state")
                if not name:
                    continue
                abbrev = props.get("state_abbrev") or props.get("state") or ""
                if not abbrev:
                    upper_name = str(name).upper()
                    if upper_name in US_STATE_ABBR_FROM_NAME:
                        abbrev = US_STATE_ABBR_FROM_NAME[upper_name]
                    elif upper_name in CANADA_PROV_ABBR_FROM_NAME:
                        abbrev = CANADA_PROV_ABBR_FROM_NAME[upper_name]
                abbrev = (abbrev or "").upper()
                region = None
                for r, states in FEMA_REGIONS.items():
                    if abbrev in states:
                        region = r[1:]  # numeric
                        break
                if region:
                    # attach region label
                    new_props = dict(props)
                    new_props["fema_region"] = region
                    features.append({"type": "Feature", "geometry": feat.get("geometry"), "properties": new_props})
            if features:
                out = {"type": "FeatureCollection", "features": features}
                fema_path.write_text(_json.dumps(out), encoding="utf-8")
                return QUrl.fromLocalFile(str(fema_path)).toString()
        except Exception as e:
            log.warning("StationsMap: failed to build FEMA geojson: %s", e)
        return None
