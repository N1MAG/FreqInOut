from __future__ import annotations

import datetime
import json
import shutil
import sqlite3
import tempfile
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional, Set
import math
import time

from PySide6.QtCore import QUrl, Qt
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QCheckBox,
    QComboBox,
)

try:
    from PySide6.QtWebEngineWidgets import QWebEngineView
except Exception:  # pragma: no cover - optional dependency
    QWebEngineView = None

from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager


USA_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

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


class StationsMapTab(QWidget):
    """
    Displays JS8Call-heard stations on an OSM-based map with a Maidenhead overlay.
    USA/Canada stations are shown; map tiles are streamed from OSM (requires network).
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.show_callsigns = False
        self.stations: List[StationPoint] = []
        self._map_file: Optional[Path] = None
        self._asset_dir = Path(__file__).resolve().parents[2] / "config" / "leaflet"
        self._geojson_path = self._asset_dir / "us_states.geojson"
        self._geojson_canada = self._asset_dir / "canada_provinces.geojson"
        self._geojson_mexico = self._asset_dir / "mexico_states.geojson"
        self._cities_geojson = self._asset_dir / "cities_na_1k.geojson"

        self.show_callsigns = False
        self.show_cities = False
        self.show_states = False
        self.show_grids = False
        self.show_grid_labels = False  # driven by the "Show grids" toggle
        self.show_regions = False
        self.show_city_labels = False
        self.city_pop_min = 100000
        self.link_mode = "off"
        self.link_value = ""
        self.relay_target = ""
        self.selected_band = "All"
        self.recency_seconds: Optional[int] = None
        self.operator_rows: List[Dict] = []
        self.operator_index: Dict[str, Dict] = {}
        self._operator_groups: List[str] = []
        self._last_map_view: Optional[Dict[str, float]] = None

        # Settings handle so SettingsTab import works; JS8 indexer may be added later
        try:
            self.settings = SettingsManager()
        except Exception:
            self.settings = None

        self._build_ui()
        self._load_operator_history()
        self._refresh_band_options()
        self._render_map()

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

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # Top controls
        ctrl_row = QHBoxLayout()
        self.show_calls_chk = QCheckBox("Show callsigns")
        self.show_calls_chk.setChecked(False)
        self.show_calls_chk.stateChanged.connect(self._on_show_calls_changed)
        ctrl_row.addWidget(self.show_calls_chk)

        self.show_regions_chk = QCheckBox("Show regions")
        self.show_regions_chk.setChecked(False)
        self.show_regions_chk.stateChanged.connect(self._on_show_regions_changed)
        ctrl_row.addWidget(self.show_regions_chk)

        self.show_states_chk = QCheckBox("Show states")
        self.show_states_chk.setChecked(False)
        self.show_states_chk.stateChanged.connect(self._on_show_states_changed)
        ctrl_row.addWidget(self.show_states_chk)

        self.show_cities_chk = QCheckBox("Show cities")
        self.show_cities_chk.setChecked(False)
        self.show_cities_chk.stateChanged.connect(self._on_show_cities_changed)
        ctrl_row.addWidget(self.show_cities_chk)

        self.city_pop_combo = QComboBox()
        self._city_pop_options = [
            ("≥1M", 1_000_000),
            ("≥750k", 750_000),
            ("≥500k", 500_000),
            ("≥250k", 250_000),
            ("≥100k", 100_000),
            ("≥75k", 75_000),
            ("≥50k", 50_000),
            ("≥25k", 25_000),
            ("≥10k", 10_000),
            ("≥5k", 5_000),
            ("<5k", 0),
        ]
        for label, val in self._city_pop_options:
            self.city_pop_combo.addItem(label, val)
        self.city_pop_combo.setCurrentIndex(4)  # default ≥100k
        self.city_pop_combo.setEnabled(False)
        self.city_pop_combo.currentIndexChanged.connect(self._on_city_pop_changed)
        ctrl_row.addWidget(self.city_pop_combo)

        self.show_grid_labels_chk = QCheckBox("Show grids")
        self.show_grid_labels_chk.setChecked(False)
        self.show_grid_labels_chk.stateChanged.connect(self._on_show_grid_labels_changed)
        ctrl_row.addWidget(self.show_grid_labels_chk)

        # JS8 link controls
        ctrl_row.addWidget(QLabel("Show Paths"))
        self.link_mode_combo = QComboBox()
        ctrl_row.addWidget(self.link_mode_combo)

        self.band_combo = QComboBox()
        ctrl_row.addWidget(QLabel("Band/Freq"))
        ctrl_row.addWidget(self.band_combo)

        ctrl_row.addWidget(QLabel("Link recency"))
        self.recency_combo = QComboBox()
        self.recency_combo.addItems(
            ["Any", "15m", "30m", "1h", "3h", "6h", "12h", "24h", "7d"]
        )
        ctrl_row.addWidget(self.recency_combo)

        ctrl_row.addWidget(QLabel("Show Paths to:"))
        self.relay_target_combo = QComboBox()
        self.relay_target_combo.setEditable(True)
        self.relay_target_combo.setInsertPolicy(QComboBox.NoInsert)
        self.relay_target_combo.setDuplicatesEnabled(False)
        completer = self.relay_target_combo.completer()
        if completer:
            completer.setFilterMode(Qt.MatchContains)
            completer.setCaseSensitivity(Qt.CaseInsensitive)
        ctrl_row.addWidget(self.relay_target_combo)
        ctrl_row.addWidget(self.show_regions_chk)

        ctrl_row.addStretch()
        layout.addLayout(ctrl_row)

        # Map view
        if QWebEngineView is not None:
            self.web = QWebEngineView()
            layout.addWidget(self.web)
        else:
            self.web = None
            layout.addWidget(QLabel("Qt WebEngine is not available. Map preview disabled."))

        # extra signals
        self.link_mode_combo.currentIndexChanged.connect(self._on_link_mode_changed)
        self.band_combo.currentIndexChanged.connect(self._on_band_changed)
        self.recency_combo.currentIndexChanged.connect(self._on_recency_changed)
        self.relay_target_combo.currentTextChanged.connect(self._on_relay_target_changed)

    # ------------- Data helpers ------------- #
    def _load_operator_history(self):
        """
        Load operator_checkins (callsign, name, state, grid, group1-3) and plot as stations.
        Grid is preferred; if missing, fall back to state centroid when available.
        """
        pts: List[StationPoint] = []
        try:
            root = Path(__file__).resolve().parents[2]
            db_path = root / "config" / "freqinout_nets.db"
        except Exception as e:
            log.error("StationsMap: failed to resolve DB path: %s", e)
            self.stations = pts
            return
        if not db_path.exists():
            self.stations = pts
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
                    COALESCE(trusted,1)
                FROM operator_checkins
                ORDER BY callsign COLLATE NOCASE
                """
            )
            rows = cur.fetchall()
            conn.close()
        except Exception as e:
            log.error("StationsMap: failed to load operator history: %s", e)
            self.stations = pts
            return

        for cs, name, state, grid, g1, g2, g3, gj, trusted in rows:
            cs = (cs or "").strip().upper()
            if not cs:
                continue
            grid = (grid or "").strip().upper()
            state = (state or "").strip().upper()
            latlon = None
            if grid:
                latlon = maidenhead_to_latlon(grid)
            if not latlon and state:
                latlon = STATE_CENTERS.get(state)
            if not latlon:
                continue
            lat, lon = latlon
            if not self._is_usa_canada(lat, lon):
                continue
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
                    state=state,
                    group=(group or "").strip(),
                    groups=groups,
                    trusted=bool(trusted),
                    lat=lat,
                    lon=lon,
                )
            )

        self.stations = pts
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
        if hasattr(self, "relay_target_combo"):
            self._refresh_relay_targets()

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
        self.stations = pts
        self._render_map()

    def _daily_schedule_freqs(self) -> List[float]:
        """
        Return unique list of frequencies (MHz) from daily_schedule_tab if present.
        """
        freqs: List[float] = []
        try:
            root = Path(__file__).resolve().parents[2]
            db_path = root / "config" / "freqinout_nets.db"
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
        bands = ["All", "160M", "80M", "60M", "40M", "30M", "20M", "17M", "15M", "12M", "10M", "6M", "2M"]
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
            group_set = {g.strip() for g in (r.get("group1") or "", r.get("group2") or "", r.get("group3") or "") if g.strip()}
            idx[cs] = {"state": state_abbr, "region": region, "groups": group_set}
            groups.update(group_set)
        self.operator_index = idx
        self._operator_groups = sorted(groups)

    def _refresh_link_mode_options(self):
        current_data = self.link_mode_combo.currentData() if hasattr(self, "link_mode_combo") else None
        self.link_mode_combo.blockSignals(True)
        self.link_mode_combo.clear()
        self.link_mode_combo.addItem("Off", ("off", ""))
        self.link_mode_combo.addItem("My Station", ("my_station", ""))
        self.link_mode_combo.addItem("All", ("all", ""))
        for reg in sorted(FEMA_REGIONS.keys()):
            self.link_mode_combo.addItem(f"Region {reg}", ("region", reg))
        for g in self._operator_groups:
            self.link_mode_combo.addItem(f"Group: {g}", ("group", g))
        restore_idx = self.link_mode_combo.findData(current_data) if current_data else -1
        if restore_idx >= 0:
            self.link_mode_combo.setCurrentIndex(restore_idx)
        else:
            self.link_mode_combo.setCurrentIndex(0)
        self.link_mode_combo.blockSignals(False)
        self.link_mode, self.link_value = self._parse_link_selection(self.link_mode_combo.currentData())

    def _refresh_relay_targets(self):
        calls = sorted({r.get("callsign", "") for r in self.operator_rows if r.get("callsign")})
        current_text = self.relay_target_combo.currentText() if hasattr(self, "relay_target_combo") else ""
        self.relay_target_combo.blockSignals(True)
        self.relay_target_combo.clear()
        self.relay_target_combo.addItem("")
        for cs in calls:
            self.relay_target_combo.addItem(cs)
        if current_text:
            idx = self.relay_target_combo.findText(current_text, Qt.MatchFixedString)
            if idx >= 0:
                self.relay_target_combo.setCurrentIndex(idx)
            else:
                self.relay_target_combo.setEditText(current_text)
        self.relay_target_combo.blockSignals(False)

    def _load_js8_links(
        self,
        band_filter=None,
        my_call: str = "",
        link_selection: Optional[tuple[str, str]] = None,
        relay_target: Optional[str] = None,
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
        if mode == "off" and not relay_target:
            return links, {}

        try:
            root = Path(__file__).resolve().parents[2]
            db_path = root / "config" / "freqinout_nets.db"
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
            if not include:
                continue

            key = tuple(sorted((o, d)))
            try:
                snr_val = float(snr)
            except Exception:
                snr_val = None

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

            def _record_station(cs: str, ts_val, snr_value, spotted: bool):
                s = stat.setdefault(cs, {"last_seen": 0, "last_spotter": 0, "snrs": []})
                if ts_val and ts_val > s["last_seen"]:
                    s["last_seen"] = ts_val
                if spotted and ts_val and ts_val > s["last_spotter"]:
                    s["last_spotter"] = ts_val
                if snr_value is not None:
                    s["snrs"].append(snr_value)

            _record_station(o, ts, snr_val, bool(is_spotter))
            _record_station(d, ts, snr_val, bool(is_spotter))

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
            }
        return links, stats_out

    def _is_usa_canada(self, lat: float, lon: float) -> bool:
        return 7.0 <= lat <= 83.0 and -172.0 <= lon <= -50.0

    def _links_active(self) -> bool:
        combo_mode, _ = self._parse_link_selection(
            self.link_mode_combo.currentData() if hasattr(self, "link_mode_combo") else ("off", "")
        )
        return (combo_mode and combo_mode.lower() != "off") or bool((self.relay_target or "").strip())

    # ------------- Map rendering ------------- #
    def _render_map(self, preserve_view: bool = True):
        if preserve_view is True and getattr(self, "web", None) is not None and self._map_file:
            try:
                self.web.page().runJavaScript(
                    "(() => { if (window._leafletMap) { const c = window._leafletMap.getCenter(); return JSON.stringify({lat:c.lat, lon:c.lng, zoom: window._leafletMap.getZoom()}); } if (window._lastView) { return JSON.stringify(window._lastView); } return null; })();",
                    lambda res: self._render_map(self._parse_view_state(res)),
                )
                return
            except Exception:
                pass
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
            html = "<html><body><h3>No station data to display.</h3></body></html>"
        else:
            self.show_city_labels = self.show_cities

            def _fmt_ts(ts_val):
                try:
                    if ts_val:
                        return datetime.datetime.utcfromtimestamp(ts_val).strftime("%Y-%m-%d %H:%M:%S UTC")
                except Exception:
                    pass
                return ""

            # init stats and links
            stats_lookup: Dict[str, Dict] = {}
            links: List[Dict] = []
            if self._links_active():
                band_filter = self.band_combo.currentData() if hasattr(self, "band_combo") else {"type": "all"}
                selection = self._parse_link_selection(
                    self.link_mode_combo.currentData() if hasattr(self, "link_mode_combo") else ("off", "")
                )
                my_call = ""
                try:
                    my_call = (self.settings.get("operator_callsign", "") or "").upper()
                except Exception:
                    my_call = ""
                relay_target = (self.relay_target or "").strip().upper()

                links, stats_lookup = self._load_js8_links(
                    band_filter=band_filter,
                    my_call=my_call,
                    link_selection=selection,
                    relay_target=relay_target or None,
                    max_age_sec=self.recency_seconds,
                )
                if view_state:
                    self._last_map_view = view_state

            # Spread overlapping stations with the same base lat/lon
            markers = []
            base_map: Dict[tuple[float, float], List[StationPoint]] = {}
            for pt in self.stations:
                key = (round(pt.lat, 4), round(pt.lon, 4))
                base_map.setdefault(key, []).append(pt)

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
                    stats = stats_lookup.get(pt.callsign.upper(), {})

                    detail_lines = [
                        f"{pt.callsign}",
                        f"Name: {pt.name}" if pt.name else "",
                        f"State: {pt.state}" if pt.state else "",
                        f"Grid: {pt.grid}" if pt.grid else "",
                        f"Group: {pt.group}" if pt.group else "",
                    ]
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
                            "last_seen": _fmt_ts(stats.get("last_seen", 0)),
                            "last_spotter": _fmt_ts(stats.get("last_spotter", 0)),
                            "avg_snr": stats.get("avg_snr"),
                            "max_snr": stats.get("max_snr"),
                        }
                    )
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
            html = self._build_leaflet_html(
                markers,
                links=links,
                max_zoom=12,
                leaflet_js=leaflet_js,
                leaflet_css=leaflet_css,
                geojson_urls=geojson_urls,
                cities_geojson=cities_geojson,
                city_min_pop=self.city_pop_min,
                show_city_labels=self.show_city_labels,
                initial_view=view_state or self._last_map_view,
            )

        if self.web is not None:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as f:
                f.write(html.encode("utf-8"))
            self._map_file = Path(f.name)
            self.web.setUrl(QUrl.fromLocalFile(str(self._map_file)))
        else:
            tmp = Path(tempfile.gettempdir()) / "freqinout_map.html"
            tmp.write_text(html, encoding="utf-8")
            self._map_file = tmp
            log.info("StationsMap: map written to %s (open in browser).", tmp)
        self._last_map_view = view_state or self._last_map_view or {"lat": 45, "lon": -97, "zoom": 3}

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
        initial_view: Optional[Dict[str, float]] = None,
    ) -> str:
        markers_json = json.dumps(markers)
        links_json = json.dumps(links)
        init_lat = initial_view.get("lat") if initial_view else 45
        init_lon = initial_view.get("lon") if initial_view else -97
        init_zoom = initial_view.get("zoom") if initial_view else 3
        tile_layer = "L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 18, maxNativeZoom: 18, attribution: '&copy; OpenStreetMap contributors' }).addTo(map);"
        grid_layer = (
            """
const gridLayer = L.layerGroup();
const gridLabelLayer = L.layerGroup();
let gridUpdating = false;
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
function addGrid(res) {
  const stepLon = res;
  const stepLat = res/2;
  const bounds = map.getBounds();
  const west = Math.max(-180, bounds.getWest() - stepLon);
  const east = Math.min(180, bounds.getEast() + stepLon);
  const south = Math.max(-90, bounds.getSouth() - stepLat);
  const north = Math.min(90, bounds.getNorth() + stepLat);
  let lonCount = Math.ceil((east - west) / stepLon);
  let latCount = Math.ceil((north - south) / stepLat);
  if (lonCount * latCount > 1500) return;
  for (let lon = Math.floor(west / stepLon) * stepLon; lon <= east; lon += stepLon) {
    gridLayer.addLayer(L.polyline([[ south, lon ], [ north, lon ]], {color:'#666', weight:0.5, opacity:0.3}));
  }
  for (let lat = Math.floor(south / stepLat) * stepLat; lat <= north; lat += stepLat) {
    gridLayer.addLayer(L.polyline([[ lat, west ], [ lat, east ]], {color:'#666', weight:0.5, opacity:0.3}));
  }
}
function updateGrid() {
  if (gridUpdating) return;
  gridUpdating = true;
  gridLayer.clearLayers();
  const z = map.getZoom();
  const bounds = map.getBounds();
  // Maidenhead grid sizes: 2-char ~20x10 deg, 4-char ~2x1 deg, 6-char ~5x2.5 arcmin (~0.0833x0.0417 deg)
  if (""" + str(self.show_grids).lower() + """) {
        let resVal = 20;
        let level = 2;
        if (z < 4) {
          resVal = 20; level = 2;
        } else if (z < 7) {
          resVal = 2; level = 4;
    } else {
      resVal = 0.083333; level = 6;
    }
    // If extremely dense, fall back to coarser grid
    const west = Math.max(-180, bounds.getWest());
    const east = Math.min(180, bounds.getEast());
    const south = Math.max(-90, bounds.getSouth());
    const north = Math.min(90, bounds.getNorth());
    let lonCount = Math.ceil((east - west) / resVal);
    let latCount = Math.ceil((north - south) / (resVal/2));
    if (lonCount * latCount > 1500 && resVal < 2) {{
      resVal = 2; level = 4;
    }}
    if (lonCount * latCount > 1500 && resVal >= 2) {{
      resVal = 20; level = 2;
    }}
    addGrid(resVal);
    gridLayer.addTo(map);
    if (""" + str(self.show_grid_labels).lower() + """) {
      addGridLabels(resVal, level, bounds);
    } else {
      map.removeLayer(gridLabelLayer);
        }
    } else {
      map.removeLayer(gridLayer);
      map.removeLayer(gridLabelLayer);
    }
  gridUpdating = false;
}

function addGridLabels(res, level, bounds) {
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
      if (count > 600) break;
    }
    if (count > 600) break;
  }
  map.addLayer(gridLabelLayer);
}

    map.on('zoomend', updateGrid);
    updateGrid();
            """
            if self.show_grids
            else ""
        )
        road_fetch = ""
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
            const abbrev = (props.state_abbrev || props.state || props.name || '').toUpperCase();
            const fullName = (props.STATE_NAME || props.name || props.state || '').toUpperCase();
            let reg = props.fema_region;
            if (!reg && abbrev && window.FEMA_LOOKUP_ABBR && window.FEMA_LOOKUP_ABBR[abbrev]) {{
              reg = window.FEMA_LOOKUP_ABBR[abbrev];
            }}
            if (!reg && fullName && window.FEMA_LOOKUP_NAME && window.FEMA_LOOKUP_NAME[fullName]) {{
              reg = window.FEMA_LOOKUP_NAME[fullName];
            }}
            if ({str(self.show_regions).lower()} && reg) {{
              const color = regionColors[(parseInt(reg, 10) - 1) % regionColors.length];
              return {{color: color, weight: 1, opacity: 0.8, fillOpacity: 0.08, fillColor: color}};
            }} else {{
              return {{color: '#666', weight: 1, opacity: 0.5, fillOpacity: 0}};
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
              const tooltip = L.tooltip({{direction:'center', permanent:true, className:'label-text no-border'}});
              tooltip.setContent(displayLabel);
              layer.bindTooltip(tooltip);
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
            const icon = L.divIcon({{className:'label-text no-border region-label', html: k}});
            regionLabelLayer.addLayer(L.marker([lat, lon], {{icon}}));
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
        return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Stations Map</title>
  <link rel="stylesheet" href="{leaflet_css}" />
  <style>
    html, body, #map {{ height: 100%; margin: 0; padding: 0; }}
    .label-text {{ font-size: 10px; color: #000; background: transparent; padding: 0; border: none; box-shadow: none; pointer-events: none; }}
    .label-text.no-border {{ background: transparent; border: none; box-shadow: none; pointer-events: none; }}
    .region-label {{ color: #1E88E5; font-weight: 700; }}
    .cs-tooltip {{ background: #fff; color: #000; border: 1px solid #444; padding: 4px 6px; border-radius: 3px; box-shadow: 0 1px 3px rgba(0,0,0,0.2); z-index: 10000; }}
    .leaflet-tooltip.cs-tooltip {{ z-index: 10000; pointer-events: none; }}
    .leaflet-popup.cs-tooltip {{ z-index: 10001; }}
    .detail-panel {{ background: rgba(255,255,255,0.92); padding: 6px 8px; border: 1px solid #666; border-radius: 4px; min-width: 150px; max-width: 220px; font-size: 11px; }}
    .zoom-display {{ padding: 4px 8px; font-size: 11px; background: rgba(255,255,255,0.85); }}
    .legend-box {{ background: rgba(255,255,255,0.92); padding: 6px 8px; border: 1px solid #666; border-radius: 4px; font-size: 11px; line-height: 1.3; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <script src="{leaflet_js}"></script>
  <script>
    window.FEMA_LOOKUP = {json.dumps({s:r[1:] for r,states in FEMA_REGIONS.items() for s in states})};
    const regionColors = ['#1E88E5','#43A047','#FB8C00','#8E24AA','#00ACC1','#F4511E','#3949AB','#FB8C00','#6D4C41','#00897B'];
    const markers = {markers_json};
    const links = {links_json};
    window.FEMA_LOOKUP_ABBR = {json.dumps({s:r[1:] for r,states in FEMA_REGIONS.items() for s in states})};
    window.FEMA_LOOKUP_NAME = {json.dumps({US_STATE_NAMES[s]:r[1:] for r,states in FEMA_REGIONS.items() for s in states if s in US_STATE_NAMES})};
    window.STATE_ABBR_FROM_NAME = {json.dumps({**US_STATE_ABBR_FROM_NAME, **CANADA_PROV_ABBR_FROM_NAME})};
    if (typeof L === 'undefined') {{
      document.getElementById('map').innerHTML = '<h3>Leaflet failed to load.</h3>';
    }} else {{
    const map = L.map('map', {{maxZoom: {max_zoom}}}).setView([{init_lat}, {init_lon}], {init_zoom});
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
      this._div.innerHTML = '<b>Station Detail</b><br/>Enable Show Callsigns to Display.';
      return this._div;
    }};
    detailPanel.addTo(map);
    function showDetail(html) {{
      const el = document.querySelector('.detail-panel');
      if (el) el.innerHTML = html;
    }}

    // Legend for link colors
    const legend = L.control({{position:'bottomright'}});
    legend.onAdd = function() {{
      const div = L.DomUtil.create('div', 'legend-box');
      div.innerHTML = '<b>Link SNR</b><br/>' +
        '<span style="color:#1b5e20;">&#9632;</span> >= 5<br/>' +
        '<span style="color:#2e7d32;">&#9632;</span> 0 to &lt;5<br/>' +
        '<span style="color:#fbc02d;">&#9632;</span> -5 to &lt;0<br/>' +
        '<span style=\"color:#f57c00;\">&#9632;</span> -10 to &lt;-5<br/>' +
        '<span style=\"color:#c62828;\">&#9632;</span> &lt; -10';
      return div;
    }};
    legend.addTo(map);

    markers.forEach(m => {{
      const circle = L.circleMarker([m.lat, m.lon], {{
        radius: 6,
        color: '#1976d2',
        weight: 1,
        fillColor: '#4FC3F7',
        fillOpacity: 0.8
      }}).addTo(map);
      const tipText = (m.tooltip || m.title || '') +
        (m.last_seen ? '<br/><b>Last seen:</b> ' + m.last_seen : '') +
        (m.last_spotter ? '<br/><b>Last spotter:</b> ' + m.last_spotter : '') +
        (m.avg_snr !== undefined && m.avg_snr !== null ? '<br/><b>Avg SNR:</b> ' + m.avg_snr.toFixed(1) : '') +
        (m.max_snr !== undefined && m.max_snr !== null ? '<br/><b>Max SNR:</b> ' + m.max_snr.toFixed(1) : '');
      circle.on('mouseover', function() {{
        this.bringToFront();
        if ({str(self.show_callsigns).lower()}) {{ showDetail(tipText); }}
      }});
      circle.on('click', function() {{
        this.bringToFront();
        if ({str(self.show_callsigns).lower()}) {{ showDetail(tipText); }}
      }});
      // Permanent label only when show_callsigns is on
      if (m.label) {{
        const icon = L.divIcon({{
          className: 'label-text',
          html: m.label
        }});
        const labelMarker = L.marker([m.lat, m.lon], {{icon}}).addTo(map);
        labelMarker.on('mouseover', function() {{
          if ({str(self.show_callsigns).lower()}) {{ showDetail(tipText); }}
        }});
        labelMarker.on('click', function() {{
          if ({str(self.show_callsigns).lower()}) {{ showDetail(tipText); }}
        }});
      }}
    }});
    // JS8 links
    function linkColor(val) {{
      if (val === null || val === undefined || isNaN(val)) return '#607d8b';
      if (val >= 5) return '#1b5e20';
      if (val >= 0) return '#2e7d32';
      if (val >= -5) return '#fbc02d';
      if (val >= -10) return '#f57c00';
      return '#c62828';
    }}
    links.forEach(l => {{
      const line = L.polyline([[l.lat1, l.lon1], [l.lat2, l.lon2]], {{color: linkColor(l.snr), weight: 2.5, opacity: 0.8}});
      line.addTo(map);
    }});
    }}
    </script>
</body>
</html>
        """

    # ------------- UI handlers ------------- #
    def _on_show_calls_changed(self, state):
        self.show_callsigns = bool(state)
        self._render_map()

    def _on_show_states_changed(self, state):
        self.show_states = bool(state)
        self._render_map()

    def _on_show_cities_changed(self, state):
        self.show_cities = bool(state)
        self.city_pop_combo.setEnabled(self.show_cities)
        self.show_city_labels = self.show_cities
        self._render_map()

    def _on_show_grid_labels_changed(self, state):
        # Single toggle now controls both grid lines and labels
        enabled = bool(state)
        self.show_grids = enabled
        self.show_grid_labels = enabled
        self._render_map()

    def _on_show_regions_changed(self, state):
        self.show_regions = bool(state)
        self._render_map()

    def _on_city_pop_changed(self, idx: int):
        try:
            val = int(self.city_pop_combo.itemData(idx))
        except Exception:
            val = 100000
        self.city_pop_min = val
        if self.show_cities:
            self._render_map()

    def _on_link_mode_changed(self, idx: int):
        data = self.link_mode_combo.itemData(idx) if hasattr(self, "link_mode_combo") else ("off", "")
        self.link_mode, self.link_value = self._parse_link_selection(data)
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
        self._render_map()

    def _on_band_changed(self, idx: int):
        self.selected_band = self.band_combo.itemText(idx)
        self._render_map()

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
        self._render_map()

    def _on_relay_target_changed(self, text: str):
        self.relay_target = (text or "").strip().upper()
        self._render_map()
    def _ensure_leaflet_assets(self) -> tuple[str, str]:
        """
        Ensure leaflet.js/css are available locally; otherwise use CDN.
        Returns (js_url, css_url).
        """
        js_file = self._asset_dir / "leaflet.js"
        css_file = self._asset_dir / "leaflet.css"
        self._asset_dir.mkdir(parents=True, exist_ok=True)

        def download(url: str, dest: Path):
            import urllib.request
            try:
                urllib.request.urlretrieve(url, dest)
                return True
            except Exception as e:
                log.warning("StationsMap: failed to download %s: %s", url, e)
                return False

        if not js_file.exists() or js_file.stat().st_size == 0:
            download("https://unpkg.com/leaflet@1.9.4/dist/leaflet.js", js_file)
        if not css_file.exists() or css_file.stat().st_size == 0:
            download("https://unpkg.com/leaflet@1.9.4/dist/leaflet.css", css_file)

        js_url = QUrl.fromLocalFile(str(js_file)).toString() if js_file.exists() else "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
        css_url = QUrl.fromLocalFile(str(css_file)).toString() if css_file.exists() else "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
        return js_url, css_url

    def _ensure_geojson(self, dest: Path, url: str) -> Optional[str]:
        """
        Ensure a GeoJSON file is available locally; otherwise try to download.
        """
        try:
            if not dest.exists() or dest.stat().st_size == 0:
                dest.parent.mkdir(parents=True, exist_ok=True)
                urllib.request.urlretrieve(url, dest)
        except Exception as e:
            log.warning("StationsMap: failed to fetch GeoJSON %s: %s", url, e)
        if dest.exists():
            return QUrl.fromLocalFile(str(dest)).toString()
        return None

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


# JS8 log ingestion for SettingsTab "Load JS8 Traffic"
class JS8LogLinkIndexer:
    """
    Parses JS8Call DIRECTED.TXT and ALL.TXT to populate js8_links table.
    Only ALL.TXT lines containing "Transmitting" are ingested.
    """

    def __init__(self, settings: SettingsManager, db_path: Path):
        self.settings = settings
        self.db_path = db_path

    # -------- parsing helpers -------- #
    def _parse_directed_line(self, line: str) -> Optional[tuple]:
        """
        DIRECTED.TXT format (tab separated):
        2025-12-09 03:30:55\t3.588000\t1950\t+05\tKE7CIU: KJ5CRF HEARTBEAT SNR -12
        """
        parts = [p for p in line.strip().split("\t") if p]
        if len(parts) < 5:
            return None
        dt_str, freq_txt, _shift, snr_txt, msg = parts[0], parts[1], parts[2], parts[3], parts[4]
        origin, dest = self._extract_origin_dest(msg)
        if not origin or not dest:
            return None
        try:
            ts = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp()
        except Exception:
            return None
        try:
            freq_hz = float(freq_txt) * 1_000_000.0
        except Exception:
            freq_hz = None
        try:
            snr = float(snr_txt)
        except Exception:
            snr = None
        return (ts, origin, dest, snr, freq_hz)

    def _parse_all_line(self, line: str) -> Optional[tuple]:
        """
        ALL.TXT lines of interest contain "Transmitting":
        2025-12-06 20:17:15  Transmitting 14.11 MHz  JS8:  N1MAG: W3BFO SNR -01
        """
        if "Transmitting" not in line:
            return None
        try:
            dt_str = line[:19]
            ts = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp()
        except Exception:
            return None
        freq_hz = None
        try:
            mhz_part = line.split("Transmitting", 1)[1]
            mhz_tok = [tok for tok in mhz_part.split() if tok.replace(".", "", 1).isdigit()]
            if mhz_tok:
                freq_hz = float(mhz_tok[0]) * 1_000_000.0
        except Exception:
            freq_hz = None
        msg_part = ""
        if ":" in line:
            msg_part = line.split(":", 1)[1]
            if ":" in msg_part:
                msg_part = msg_part.split(":", 1)[1]
        origin, dest = self._extract_origin_dest(msg_part)
        if not origin or not dest:
            return None
        snr = None
        for tok in reversed(msg_part.split()):
            try:
                snr = float(tok)
                break
            except Exception:
                continue
        return (ts, origin, dest, snr, freq_hz)

    def _extract_origin_dest(self, msg: str) -> tuple[str, str]:
        if ":" not in msg:
            return "", ""
        origin, rest = msg.split(":", 1)
        origin = origin.strip().upper()
        first = (rest.strip().split() or [""])[0]
        dest = first.strip().strip(",").strip().upper()
        return origin, dest

    # -------- DB helpers -------- #
    def _ensure_table(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS js8_links (
                ts REAL,
                origin TEXT,
                destination TEXT,
                snr REAL,
                band TEXT,
                freq_hz REAL,
                is_relay INTEGER DEFAULT 0,
                relay_via TEXT,
                is_spotter INTEGER DEFAULT 0,
                last_seen_utc TEXT
            )
            """
        )
        # Add last_seen_utc if created earlier without it
        try:
            cur = conn.execute("PRAGMA table_info(js8_links)")
            cols = {row[1] for row in cur.fetchall()}
            if "last_seen_utc" not in cols:
                conn.execute("ALTER TABLE js8_links ADD COLUMN last_seen_utc TEXT")
        except Exception:
            pass
        conn.commit()

    def _clear_table(self, conn: sqlite3.Connection) -> None:
        conn.execute("DELETE FROM js8_links")
        conn.commit()

    # -------- public API -------- #
    def update(self) -> int:
        """
        Rebuild js8_links from DIRECTED.TXT and ALL.TXT.
        Returns number of rows inserted.
        """
        directed_path = self._resolve_directed_path()
        all_path = directed_path.parent / "ALL.TXT" if directed_path else None

        rows: List[tuple] = []
        if directed_path and directed_path.exists():
            for line in directed_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                parsed = self._parse_directed_line(line)
                if parsed:
                    rows.append(parsed)

        if all_path and all_path.exists():
            for line in all_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                parsed = self._parse_all_line(line)
                if parsed:
                    rows.append(parsed)

        if not rows:
            return 0

        def _freq_to_band(freq_hz: Optional[float]) -> Optional[str]:
            if freq_hz is None:
                return None
            try:
                mhz = float(freq_hz) / 1_000_000.0
            except Exception:
                return None
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
            return None

        # De-duplicate by station pair + band, averaging SNR and keeping the newest timestamp/frequency.
        agg: Dict[tuple, Dict] = {}
        for ts, origin, dest, snr, freq_hz in rows:
            a = (origin or "").strip().upper()
            b = (dest or "").strip().upper()
            if not a or not b:
                continue
            band = _freq_to_band(freq_hz)
            key = (tuple(sorted((a, b))), band)
            entry = agg.setdefault(key, {"last_ts": ts, "snr_sum": 0.0, "snr_count": 0, "freq_hz": freq_hz})
            if ts and (entry["last_ts"] is None or ts > entry["last_ts"]):
                entry["last_ts"] = ts
                if freq_hz is not None:
                    entry["freq_hz"] = freq_hz
            try:
                if snr is not None:
                    entry["snr_sum"] += float(snr)
                    entry["snr_count"] += 1
            except Exception:
                pass
            if entry["freq_hz"] is None and freq_hz is not None:
                entry["freq_hz"] = freq_hz

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        try:
            self._ensure_table(conn)
            self._clear_table(conn)
            payload = []
            for key, entry in agg.items():
                pair, band = key
                origin, dest = pair
                avg_snr = entry["snr_sum"] / entry["snr_count"] if entry["snr_count"] else None
                payload.append(
                    (
                        entry["last_ts"],
                        origin,
                        dest,
                        avg_snr,
                        band,
                        entry.get("freq_hz"),
                        0,
                        None,
                        0,
                    )
                )
            conn.executemany(
                """
                INSERT INTO js8_links
                    (ts, origin, destination, snr, band, freq_hz, is_relay, relay_via, is_spotter)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            conn.commit()
            return len(payload)
        finally:
            conn.close()

    def query_links(self, *args, **kwargs):
        return []

    def _resolve_directed_path(self) -> Optional[Path]:
        path_txt = (self.settings.get("js8_directed_path", "") or "").strip()
        if not path_txt:
            return None
        p = Path(path_txt)
        return p if p.exists() else None
