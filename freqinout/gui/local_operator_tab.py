from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QComboBox,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QFileDialog,
    QMessageBox,
    QDialog,
    QFormLayout,
    QTextEdit,
    QCheckBox,
)

from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.local_ops_store import get_all_operators, upsert_operator, delete_operators
from freqinout.gui.theme import resolve_theme, button_style


LOCAL_CATEGORIES = ["VHF", "UHF", "GMRS", "MURS", "FRS", "Other"]


class LocalOperatorTab(QWidget):
    """
    Local operator roster for ad hoc VHF/UHF/GMRS/MURS/FRS net operations.
    """

    local_operator_updated = Signal()

    COL_SELECT = 0
    COL_CALLSIGN = 1
    COL_FIRST_NAME = 2
    COL_LAST_NAME = 3
    COL_CITY = 4
    COL_STATE = 5
    COL_CATEGORY = 6
    COL_FIRST_SEEN = 7
    COL_LAST_SEEN = 8
    COL_COUNT = 9
    COL_SITREP = 10
    COL_NOTES = 11

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._rows: List[Dict[str, Any]] = []
        self._build_ui()
        self._load_data()
        self.apply_theme()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>Local Operators</h3>"))
        header.addStretch()
        layout.addLayout(header)

        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Search:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Callsign, first/last name, city, state, category, sitrep, notes")
        filter_row.addWidget(self.search_edit, stretch=1)
        filter_row.addWidget(QLabel("Category:"))
        self.category_filter = QComboBox()
        self.category_filter.addItem("All")
        filter_row.addWidget(self.category_filter)
        layout.addLayout(filter_row)

        actions = QHBoxLayout()
        self.refresh_btn = QPushButton("Refresh")
        self.add_btn = QPushButton("Add")
        self.edit_btn = QPushButton("Edit Selected")
        self.delete_btn = QPushButton("Delete Selected")
        self.import_btn = QPushButton("Import CSV")
        self.export_btn = QPushButton("Export CSV")
        actions.addWidget(self.refresh_btn)
        actions.addWidget(self.add_btn)
        actions.addWidget(self.edit_btn)
        actions.addWidget(self.delete_btn)
        actions.addWidget(self.import_btn)
        actions.addWidget(self.export_btn)
        actions.addStretch()
        layout.addLayout(actions)

        self.table = QTableWidget(0, 12)
        self.table.setHorizontalHeaderLabels(
            [
                "Selected",
                "Callsign",
                "First Name",
                "Last Name",
                "City",
                "State",
                "Category",
                "First Seen",
                "Last Seen",
                "Check-ins",
                "SitRep",
                "Notes",
            ]
        )
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSortingEnabled(True)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(self.COL_SELECT, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_CALLSIGN, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_FIRST_NAME, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_LAST_NAME, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_CITY, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_STATE, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_CATEGORY, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_FIRST_SEEN, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_LAST_SEEN, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_COUNT, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_SITREP, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(self.COL_NOTES, QHeaderView.Stretch)
        layout.addWidget(self.table)

        self.refresh_btn.clicked.connect(self._load_data)
        self.add_btn.clicked.connect(self._add_operator)
        self.edit_btn.clicked.connect(self._edit_selected)
        self.delete_btn.clicked.connect(self._delete_selected)
        self.import_btn.clicked.connect(self._import_csv)
        self.export_btn.clicked.connect(self._export_csv)
        self.search_edit.textChanged.connect(self._apply_filters)
        self.category_filter.currentIndexChanged.connect(self._apply_filters)

    def apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        self.refresh_btn.setStyleSheet(button_style("muted", theme))
        self.add_btn.setStyleSheet(button_style("eligible_success", theme))
        role = "eligible_info" if self._selected_callsigns() else "muted"
        self.edit_btn.setStyleSheet(button_style(role, theme))
        self.delete_btn.setStyleSheet(button_style("eligible_danger" if self._selected_callsigns() else "muted", theme))
        self.import_btn.setStyleSheet(button_style("muted", theme))
        self.export_btn.setStyleSheet(button_style("muted", theme))

    def on_settings_saved(self) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        self.apply_theme()

    def _load_data(self) -> None:
        try:
            self._rows = get_all_operators()
        except Exception as e:
            log.error("LocalOperatorTab: load failed: %s", e)
            self._rows = []
        self._refresh_category_filter()
        self._apply_filters()
        self.local_operator_updated.emit()

    def _refresh_category_filter(self) -> None:
        cur = self.category_filter.currentText()
        categories = sorted(
            {str(r.get("category", "")).strip() for r in self._rows if str(r.get("category", "")).strip()},
            key=str.upper,
        )
        self.category_filter.blockSignals(True)
        self.category_filter.clear()
        self.category_filter.addItem("All")
        for cat in categories:
            self.category_filter.addItem(cat)
        idx = self.category_filter.findText(cur)
        self.category_filter.setCurrentIndex(idx if idx >= 0 else 0)
        self.category_filter.blockSignals(False)

    def _row_matches(self, row: Dict[str, Any], query: str, category: str) -> bool:
        if category and category.upper() != "ALL":
            if str(row.get("category", "")).strip().upper() != category.upper():
                return False
        if not query:
            return True
        hay = " ".join(
            [
                str(row.get("callsign", "")),
                str(row.get("first_name", "")),
                str(row.get("last_name", "")),
                str(row.get("name", "")),
                str(row.get("city", "")),
                str(row.get("state", "")),
                str(row.get("category", "")),
                str(row.get("first_seen_utc", "")),
                str(row.get("last_seen_utc", "")),
                str(row.get("checkin_count", "")),
                str(row.get("sitrep_status", "")),
                str(row.get("notes", "")),
            ]
        ).upper()
        return query.upper() in hay

    def _apply_filters(self) -> None:
        query = self.search_edit.text().strip()
        category = self.category_filter.currentText().strip()
        filtered = [r for r in self._rows if self._row_matches(r, query, category)]
        self._populate_table(filtered)
        self.apply_theme()

    def _populate_table(self, rows: List[Dict[str, Any]]) -> None:
        sorting_enabled = self.table.isSortingEnabled()
        self.table.setSortingEnabled(False)
        try:
            self.table.setRowCount(0)
            for row in rows:
                r = self.table.rowCount()
                self.table.insertRow(r)
                sel_chk = QCheckBox()
                sel_chk.stateChanged.connect(self.apply_theme)
                sel_wrap = QWidget()
                sel_layout = QHBoxLayout(sel_wrap)
                sel_layout.setContentsMargins(0, 0, 0, 0)
                sel_layout.setAlignment(Qt.AlignCenter)
                sel_layout.addWidget(sel_chk)
                self.table.setCellWidget(r, self.COL_SELECT, sel_wrap)

                vals = [
                    str(row.get("callsign", "")).upper(),
                    str(row.get("first_name", "")),
                    str(row.get("last_name", "")),
                    str(row.get("city", "")),
                    str(row.get("state", "")).upper(),
                    str(row.get("category", "")),
                    str(row.get("first_seen_utc", "")),
                    str(row.get("last_seen_utc", "")),
                    str(int(row.get("checkin_count", 0) or 0)),
                    str(row.get("sitrep_status", "GREEN")).upper(),
                    str(row.get("notes", "")),
                ]
                for idx, value in enumerate(vals, start=1):
                    item = QTableWidgetItem(value)
                    self.table.setItem(r, idx, item)
                    if idx == self.COL_SITREP:
                        self._apply_sitrep_item_style(item, value)
                    if idx == self.COL_NOTES:
                        item.setToolTip(value)
        finally:
            self.table.setSortingEnabled(sorting_enabled)

    def _apply_sitrep_item_style(self, item: QTableWidgetItem, status: str) -> None:
        key = (status or "").strip().upper()
        if key == "RED":
            item.setBackground(Qt.red)
            item.setForeground(Qt.white)
        elif key == "YELLOW":
            item.setBackground(Qt.yellow)
            item.setForeground(Qt.black)
        else:
            item.setBackground(Qt.darkGreen)
            item.setForeground(Qt.white)

    def _selected_callsigns(self) -> List[str]:
        out: List[str] = []
        for r in range(self.table.rowCount()):
            w = self.table.cellWidget(r, self.COL_SELECT)
            chk = w.findChild(QCheckBox) if isinstance(w, QWidget) else None
            if chk is None or not chk.isChecked():
                continue
            cs_item = self.table.item(r, self.COL_CALLSIGN)
            cs = cs_item.text().strip().upper() if cs_item else ""
            if cs:
                out.append(cs)
        return out

    def _dialog_profile(self, existing: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        dlg = QDialog(self)
        dlg.setWindowTitle("Edit Local Operator" if existing else "Add Local Operator")
        form = QFormLayout(dlg)

        callsign_edit = QLineEdit(str((existing or {}).get("callsign", "")))
        first_name_edit = QLineEdit(str((existing or {}).get("first_name", "")))
        last_name_edit = QLineEdit(str((existing or {}).get("last_name", "")))
        city_edit = QLineEdit(str((existing or {}).get("city", "")))
        state_edit = QLineEdit(str((existing or {}).get("state", "")))
        category_combo = QComboBox()
        category_combo.setEditable(True)
        category_combo.addItems(LOCAL_CATEGORIES)
        if existing and str((existing or {}).get("category", "")):
            category_combo.setCurrentText(str((existing or {}).get("category", "")))
        notes_edit = QTextEdit(str((existing or {}).get("notes", "")))
        notes_edit.setMinimumHeight(90)
        sitrep_combo = QComboBox()
        sitrep_combo.addItems(["GREEN", "YELLOW", "RED"])
        sitrep_combo.setCurrentText(str((existing or {}).get("sitrep_status", "GREEN")).strip().upper() or "GREEN")

        form.addRow("Callsign:", callsign_edit)
        form.addRow("First Name:", first_name_edit)
        form.addRow("Last Name:", last_name_edit)
        form.addRow("City:", city_edit)
        form.addRow("State:", state_edit)
        form.addRow("Category:", category_combo)
        form.addRow("SitRep:", sitrep_combo)
        form.addRow("Notes:", notes_edit)

        btn_row = QHBoxLayout()
        save_btn = QPushButton("Save")
        cancel_btn = QPushButton("Cancel")
        btn_row.addStretch()
        btn_row.addWidget(save_btn)
        btn_row.addWidget(cancel_btn)
        form.addRow(btn_row)

        out: Dict[str, Any] = {}

        def _save() -> None:
            cs = callsign_edit.text().strip().upper()
            if not cs:
                QMessageBox.warning(self, "Validation", "Callsign is required.")
                return
            out.update(
                {
                    "callsign": cs,
                    "first_name": first_name_edit.text().strip(),
                    "last_name": last_name_edit.text().strip(),
                    "city": city_edit.text().strip(),
                    "state": state_edit.text().strip().upper(),
                    "category": category_combo.currentText().strip(),
                    "sitrep_status": sitrep_combo.currentText().strip().upper(),
                    "notes": notes_edit.toPlainText().strip(),
                }
            )
            dlg.accept()

        save_btn.clicked.connect(_save)
        cancel_btn.clicked.connect(dlg.reject)
        if dlg.exec() != QDialog.Accepted:
            return None
        return out if out else None

    def _find_row_by_callsign(self, callsign: str) -> Optional[Dict[str, Any]]:
        cs = (callsign or "").strip().upper()
        for row in self._rows:
            if str(row.get("callsign", "")).strip().upper() == cs:
                return row
        return None

    def _add_operator(self) -> None:
        row = self._dialog_profile(None)
        if not row:
            return
        upsert_operator(
            row["callsign"],
            first_name=row.get("first_name", ""),
            last_name=row.get("last_name", ""),
            city=row.get("city", ""),
            state=row.get("state", ""),
            category=row.get("category", ""),
            sitrep_status=row.get("sitrep_status", "GREEN"),
            notes=row.get("notes", ""),
            touch_seen=False,
            increment_checkins=False,
        )
        self._load_data()

    def _edit_selected(self) -> None:
        selected = self._selected_callsigns()
        if not selected:
            QMessageBox.information(self, "Edit Local Operator", "Select one operator to edit.")
            return
        if len(selected) > 1:
            QMessageBox.warning(self, "Edit Local Operator", "Select only one operator to edit.")
            return
        existing = self._find_row_by_callsign(selected[0])
        if not existing:
            return
        updated = self._dialog_profile(existing)
        if not updated:
            return
        old_cs = str(existing.get("callsign", "")).strip().upper()
        new_cs = str(updated.get("callsign", "")).strip().upper()
        if old_cs and new_cs and old_cs != new_cs:
            delete_operators([old_cs])
        upsert_operator(
            new_cs,
            first_name=updated.get("first_name", ""),
            last_name=updated.get("last_name", ""),
            city=updated.get("city", ""),
            state=updated.get("state", ""),
            category=updated.get("category", ""),
            sitrep_status=updated.get("sitrep_status", existing.get("sitrep_status", "GREEN")),
            notes=updated.get("notes", ""),
            touch_seen=False,
            increment_checkins=False,
            first_seen_utc=existing.get("first_seen_utc", ""),
            last_seen_utc=existing.get("last_seen_utc", ""),
            checkin_count=existing.get("checkin_count", 0),
        )
        self._load_data()

    def _delete_selected(self) -> None:
        selected = self._selected_callsigns()
        if not selected:
            QMessageBox.information(self, "Delete Local Operators", "Select one or more operators to delete.")
            return
        resp = QMessageBox.question(
            self,
            "Delete Local Operators",
            f"Delete {len(selected)} selected local operator(s)?",
        )
        if resp != QMessageBox.Yes:
            return
        delete_operators(selected)
        self._load_data()

    @staticmethod
    def _csv_pick(row: Dict[str, Any], keys: List[str]) -> str:
        low_map = {str(k).strip().lower(): v for k, v in row.items()}
        for key in keys:
            val = low_map.get(key.lower())
            if val is None:
                continue
            txt = str(val).strip()
            if txt:
                return txt
        return ""

    @staticmethod
    def _split_name(value: str) -> tuple[str, str]:
        txt = (value or "").strip()
        if not txt:
            return "", ""
        parts = [p for p in txt.split() if p]
        if len(parts) <= 1:
            return (parts[0] if parts else ""), ""
        return parts[0], " ".join(parts[1:])

    def _import_csv(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Import Local Operators", "", "CSV Files (*.csv)")
        if not path:
            return
        imported = 0
        try:
            with Path(path).open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cs = self._csv_pick(row, ["callsign", "call", "operator_callsign"]).upper()
                    if not cs:
                        continue
                    first_name = self._csv_pick(row, ["first_name", "firstname", "fname"])
                    last_name = self._csv_pick(row, ["last_name", "lastname", "lname"])
                    full_name = self._csv_pick(row, ["name", "operator_name"])
                    if not first_name and not last_name and full_name:
                        first_name, last_name = self._split_name(full_name)
                    city = self._csv_pick(row, ["city"])
                    state = self._csv_pick(row, ["state", "st"]).upper()
                    category = self._csv_pick(row, ["category", "service", "type"])
                    notes = self._csv_pick(row, ["notes", "note", "comment"])
                    first_seen = self._csv_pick(row, ["first_seen_utc", "first_seen"])
                    last_seen = self._csv_pick(row, ["last_seen_utc", "last_seen"])
                    status = self._csv_pick(row, ["sitrep_status", "status"])
                    count_txt = self._csv_pick(row, ["checkins", "checkin_count", "count"])
                    count_val = None
                    if count_txt:
                        try:
                            count_val = int(float(count_txt))
                        except Exception:
                            count_val = None
                    upsert_operator(
                        cs,
                        first_name=first_name,
                        last_name=last_name,
                        name=full_name,
                        city=city,
                        state=state,
                        category=category,
                        notes=notes if notes else None,
                        sitrep_status=status if status else None,
                        first_seen_utc=first_seen if first_seen else None,
                        last_seen_utc=last_seen if last_seen else None,
                        checkin_count=count_val,
                        touch_seen=False,
                        increment_checkins=False,
                    )
                    imported += 1
            self._load_data()
            QMessageBox.information(self, "Import Local Operators", f"Imported/updated {imported} row(s).")
        except Exception as e:
            QMessageBox.warning(self, "Import Local Operators", f"Import failed:\n{e}")

    def _export_csv(self) -> None:
        out, _ = QFileDialog.getSaveFileName(
            self,
            "Export Local Operators",
            "local_operators.csv",
            "CSV Files (*.csv)",
        )
        if not out:
            return
        rows = self._rows
        try:
            with Path(out).open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "callsign",
                        "first_name",
                        "last_name",
                        "name",
                        "city",
                        "state",
                        "category",
                        "first_seen_utc",
                        "last_seen_utc",
                        "checkin_count",
                        "notes",
                        "sitrep_status",
                    ],
                )
                writer.writeheader()
                for row in rows:
                    writer.writerow(
                        {
                            "callsign": row.get("callsign", ""),
                            "first_name": row.get("first_name", ""),
                            "last_name": row.get("last_name", ""),
                            "name": row.get("name", ""),
                            "city": row.get("city", ""),
                            "state": row.get("state", ""),
                            "category": row.get("category", ""),
                            "first_seen_utc": row.get("first_seen_utc", ""),
                            "last_seen_utc": row.get("last_seen_utc", ""),
                            "checkin_count": row.get("checkin_count", 0),
                            "notes": row.get("notes", ""),
                            "sitrep_status": row.get("sitrep_status", "GREEN"),
                        }
                    )
            QMessageBox.information(self, "Export Local Operators", f"Exported {len(rows)} row(s).")
        except Exception as e:
            QMessageBox.warning(self, "Export Local Operators", f"Export failed:\n{e}")
