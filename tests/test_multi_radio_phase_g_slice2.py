from __future__ import annotations

import os
import sqlite3
import sys
import types

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

if "reportlab" not in sys.modules:
    reportlab = types.ModuleType("reportlab")
    lib = types.ModuleType("reportlab.lib")
    pagesizes = types.ModuleType("reportlab.lib.pagesizes")
    pagesizes.letter = (612.0, 792.0)
    pdfgen = types.ModuleType("reportlab.pdfgen")
    canvas_mod = types.ModuleType("reportlab.pdfgen.canvas")
    canvas_mod.Canvas = object
    reportlab.lib = lib
    reportlab.pdfgen = pdfgen
    lib.pagesizes = pagesizes
    pdfgen.canvas = canvas_mod
    sys.modules["reportlab"] = reportlab
    sys.modules["reportlab.lib"] = lib
    sys.modules["reportlab.lib.pagesizes"] = pagesizes
    sys.modules["reportlab.pdfgen"] = pdfgen
    sys.modules["reportlab.pdfgen.canvas"] = canvas_mod

from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import StationRuntimeManager
from freqinout.core.varac_ingest import ingest_varac, resolve_varac_ingest_sources
from freqinout.gui.controlfreq_tab import ControlFreqTab
from freqinout.gui.message_viewer_tab import VarACMessage, _RowsBuildWorker
import freqinout.core.station_runtime_manager as station_runtime_manager_mod


def _create_varac_db(path, *, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE vmail (
                id INTEGER PRIMARY KEY,
                guid TEXT,
                creation_time TEXT,
                sent_time TEXT,
                received_time TEXT,
                folder_id INTEGER,
                vmail_to TEXT,
                vmail_from TEXT,
                delivery_band TEXT,
                delivery_snr REAL,
                subject TEXT,
                msg TEXT,
                read_status INTEGER,
                is_deleted INTEGER,
                frequency REAL,
                vmail_via TEXT,
                urgent INTEGER,
                has_attachment INTEGER
            )
            """
        )
        for row in rows:
            conn.execute(
                """
                INSERT INTO vmail (
                    id, guid, creation_time, sent_time, received_time, folder_id,
                    vmail_to, vmail_from, delivery_band, delivery_snr, subject, msg,
                    read_status, is_deleted, frequency, vmail_via, urgent, has_attachment
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(row.get("id", 0) or 0),
                    str(row.get("guid", "") or ""),
                    str(row.get("creation_time", "2026-03-15 00:00:00") or ""),
                    str(row.get("sent_time", "2026-03-15 00:00:00") or ""),
                    str(row.get("received_time", "2026-03-15 00:00:00") or ""),
                    int(row.get("folder_id", 1) or 1),
                    str(row.get("to_call", "") or ""),
                    str(row.get("from_call", "") or ""),
                    str(row.get("band", "20M") or "20M"),
                    float(row.get("snr", -5.0) or -5.0),
                    str(row.get("subject", "") or ""),
                    str(row.get("body", "") or ""),
                    int(row.get("read_status", 0) or 0),
                    int(row.get("is_deleted", 0) or 0),
                    float(row.get("frequency", 14_078_000.0) or 14_078_000.0),
                    str(row.get("via", "") or ""),
                    int(row.get("urgent", 0) or 0),
                    int(row.get("has_attachment", 0) or 0),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _configure_varac_device(store: MultiRadioStore, profile: dict[str, object], db_path: str) -> dict[str, object]:
    return store.save_device_profile(
        {
            "id": profile["id"],
            "name": profile["name"],
            "varac_install_path": os.path.dirname(db_path),
            "varac_db_path": db_path,
            "varac_ini_path": db_path.replace(".db", ".ini"),
            "launch_cmd": db_path.replace(".db", ".exe"),
        }
    )


def _status_snapshot(*_args, **_kwargs):
    return {
        "JS8Call_API": {"state": "idle", "tooltip": "JS8 idle"},
        "JS8Call": {"state": "idle", "tooltip": "JS8 idle"},
        "FLRig": {"state": "ok", "tooltip": "FLRig reachable"},
        "RigCtlD": {"state": "idle", "tooltip": "RigCtlD idle"},
        "FLDigi": {"state": "idle", "tooltip": "FLDigi idle"},
        "FLMsg": {"state": "idle", "tooltip": "FLMsg idle"},
        "FLAmp": {"state": "idle", "tooltip": "FLAmp idle"},
        "VarAC": {"state": "idle", "tooltip": "VarAC idle"},
        "JS8Spotter": {"state": "idle", "tooltip": "JS8Spotter idle"},
        "CommStat": {"state": "idle", "tooltip": "CommStat idle"},
    }


class _StubControlBroker(QObject):
    control_snapshot_ready = Signal(object)
    control_snapshot_failed = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self.requests: list[dict[str, object]] = []

    def request_control_snapshot(self, *, settings: object, request: dict[str, object] | None = None) -> None:
        self.requests.append({"settings": settings, "request": dict(request or {})})


def test_ingest_varac_deduplicates_shared_cluster_sources_and_records_metadata(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None

    shared_db = tmp_path / "shared" / "VarAC.db"
    _create_varac_db(
        shared_db,
        rows=[
            {
                "id": 1,
                "guid": "cluster-vmail-1",
                "from_call": "K1OPS",
                "to_call": "N0CALL",
                "subject": "Cluster Traffic",
                "body": "Shared cluster message",
            }
        ],
    )

    primary = _configure_varac_device(store, primary, str(tmp_path / "nodes" / "primary" / "VarAC.db"))
    secondary = store.save_device_profile(
        {
            "name": "Backup VarAC",
            "control_backend": "flrig",
            "flrig_host": "127.0.0.2",
            "flrig_port": 22345,
            "varac_install_path": str(tmp_path / "nodes" / "secondary"),
            "varac_db_path": str(tmp_path / "nodes" / "secondary" / "VarAC.db"),
            "varac_ini_path": str(tmp_path / "nodes" / "secondary" / "VarAC.ini"),
        }
    )
    store.set_device_profile_runtime_active(int(secondary["id"]), True)

    cluster = store.save_varac_cluster(
        {
            "name": "Ops Cluster",
            "cluster_id": "OPS-A",
            "shared_db_path": str(shared_db),
        }
    )
    store.set_varac_cluster_member(int(cluster["id"]), int(primary["id"]), instance_number=1, enabled=True)
    store.set_varac_cluster_member(int(cluster["id"]), int(secondary["id"]), instance_number=2, enabled=True)

    sources = resolve_varac_ingest_sources(settings)
    assert [source.ingest_source_key for source in sources] == [f"cluster:{int(cluster['id'])}"]

    assert ingest_varac(settings, force=True) is True

    nets_db = cfg_root / "config" / "freqinout_nets.db"
    conn = sqlite3.connect(nets_db)
    try:
        rows = conn.execute(
            """
            SELECT source, table_name, ingest_source_key, ingest_scope, ingest_source_label, cluster_name, cluster_public_id
              FROM varac_messages
             WHERE msg_type='VMAIL'
            """
        ).fetchall()
        assert len(rows) == 1
        source, table_name, ingest_source_key, ingest_scope, ingest_source_label, cluster_name, cluster_public_id = rows[0]
        assert source == f"cluster:{int(cluster['id'])}|vmail"
        assert table_name == "vmail"
        assert ingest_source_key == f"cluster:{int(cluster['id'])}"
        assert ingest_scope == "cluster"
        assert ingest_source_label == "Ops Cluster [OPS-A]"
        assert cluster_name == "Ops Cluster"
        assert cluster_public_id == "OPS-A"
        assert conn.execute(
            "SELECT COUNT(*) FROM varac_ingest_state_v2 WHERE ingest_source_key=? AND table_name='vmail'",
            (f"cluster:{int(cluster['id'])}",),
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_ingest_varac_keeps_cluster_and_standalone_sources_distinct(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None

    shared_db = tmp_path / "shared" / "VarAC.db"
    standalone_db = tmp_path / "standalone" / "VarAC.db"
    _create_varac_db(
        shared_db,
        rows=[{"id": 1, "guid": "cluster-one", "from_call": "K1OPS", "to_call": "N0CALL", "subject": "Cluster"}],
    )
    _create_varac_db(
        standalone_db,
        rows=[{"id": 1, "guid": "device-one", "from_call": "K9NODE", "to_call": "N0CALL", "subject": "Standalone"}],
    )

    primary = _configure_varac_device(store, primary, str(tmp_path / "nodes" / "primary" / "VarAC.db"))
    cluster = store.save_varac_cluster(
        {
            "name": "Ops Cluster",
            "cluster_id": "OPS-A",
            "shared_db_path": str(shared_db),
        }
    )
    store.set_varac_cluster_member(int(cluster["id"]), int(primary["id"]), instance_number=1, enabled=True)

    standalone = store.save_device_profile(
        {
            "name": "Field Node",
            "control_backend": "flrig",
            "flrig_host": "127.0.0.3",
            "flrig_port": 32345,
            "varac_install_path": str(standalone_db.parent),
            "varac_db_path": str(standalone_db),
            "varac_ini_path": str(standalone_db).replace(".db", ".ini"),
        }
    )
    store.set_device_profile_runtime_active(int(standalone["id"]), True)

    assert ingest_varac(settings, force=True) is True

    nets_db = cfg_root / "config" / "freqinout_nets.db"
    conn = sqlite3.connect(nets_db)
    try:
        rows = conn.execute(
            "SELECT ingest_source_key, ingest_source_label FROM varac_messages WHERE msg_type='VMAIL' ORDER BY ingest_source_key ASC"
        ).fetchall()
        assert rows == [
            (f"cluster:{int(cluster['id'])}", "Ops Cluster [OPS-A]"),
            (f"device:{int(standalone['id'])}", "Field Node"),
        ]
    finally:
        conn.close()


def test_varac_rows_worker_surfaces_cluster_identity():
    captured = {}
    worker = _RowsBuildWorker(
        js8_messages=[],
        spotter_messages=[],
        varac_messages=[
            VarACMessage(
                msg_id=1,
                guid="cluster-vmail-1",
                source="cluster:9|vmail",
                msg_type="VMAIL",
                from_call="K1OPS",
                to_call="N0CALL",
                subject="Cluster Mail",
                body="Shared cluster body",
                ts=1_742_000_000.0,
                band="20M",
                freq_hz=14_078_000.0,
                snr=-4.0,
                read_status=0,
                folder="1",
                vmail_guid="cluster-vmail-1",
                ingest_source_key="cluster:9",
                ingest_scope="cluster",
                ingest_source_label="Ops Cluster [OPS-A]",
                cluster_name="Ops Cluster",
                cluster_public_id="OPS-A",
                ingest_db_path="C:/VarAC/Shared/VarAC.db",
            )
        ],
        sitrep_messages=[],
        files={},
        read_state_map={},
        signature_state_map={},
        sender_cache_seed={},
        form_titles={},
        show_local_time=False,
        tz_name="UTC",
        sitrep_dedupe_enabled=True,
        sitrep_show_raw_duplicates=False,
        force=False,
        generation=1,
    )
    worker.finished.connect(lambda payload: captured.setdefault("payload", payload))
    worker.run()

    rows = captured["payload"]["rows"]
    assert len(rows) == 1
    assert "Ops Cluster [OPS-A]" in rows[0].title


def test_station_runtime_and_controlfreq_include_cluster_ingest_context(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None

    shared_db = tmp_path / "shared" / "VarAC.db"
    _create_varac_db(
        shared_db,
        rows=[{"id": 1, "guid": "cluster-one", "from_call": "K1OPS", "to_call": "N0CALL", "subject": "Cluster"}],
    )

    primary = _configure_varac_device(store, primary, str(tmp_path / "nodes" / "primary" / "VarAC.db"))
    cluster = store.save_varac_cluster(
        {
            "name": "Ops Cluster",
            "cluster_id": "OPS-A",
            "shared_db_path": str(shared_db),
            "counters_refresh_sec": 15,
        }
    )
    store.set_varac_cluster_member(int(cluster["id"]), int(primary["id"]), instance_number=1, enabled=True)
    store.set_varac_cluster_gateway_handler(int(cluster["id"]), int(primary["id"]))

    assert ingest_varac(settings, force=True) is True

    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _status_snapshot)
    monkeypatch.setattr(SoftwareStatusService, "program_is_running", lambda self, name: name == "VarAC")
    monkeypatch.setattr(
        station_runtime_manager_mod.VarACStatusClient,
        "get_status",
        lambda self: {"busy": False, "waiting_for_frequency": False, "reason": None},
    )

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    snapshots = manager.get_runtime_snapshots(force=True)
    primary_snapshot = next(snapshot for snapshot in snapshots if snapshot.runtime_primary)
    assert "Last ingest" in primary_snapshot.varac_cluster_summary
    assert "Last ingest" in primary_snapshot.service_states["VarAC Cluster"]["tooltip"]

    tab = ControlFreqTab()
    try:
        rows = tab._collect_inbox_rows("")
        varac_row = next(row for row in rows if row[0] == "VarAC")
        assert varac_row[1] == "1"
        assert "Ops Cluster" in varac_row[2]
    finally:
        tab.deleteLater()
        app.processEvents()


def test_controlfreq_uses_async_broker_for_running_status(monkeypatch):
    app = QApplication.instance() or QApplication([])
    monkeypatch.setattr(SoftwareStatusService, "status_snapshot", _status_snapshot)

    tab = ControlFreqTab()
    broker = _StubControlBroker()
    try:
        monkeypatch.setattr(
            tab._status_service,
            "status_snapshot",
            lambda: (_ for _ in ()).throw(AssertionError("status_snapshot should not run on the UI thread when a broker is present.")),
        )
        tab.set_status_broker(broker)
        tab._refresh_running_status()

        assert len(broker.requests) == 1

        broker.control_snapshot_ready.emit(
            {
                "FLRig": {"state": "ok", "tooltip": "FLRig reachable"},
                "JS8Call": {"state": "idle", "tooltip": "JS8 idle"},
            }
        )
        app.processEvents()
        assert tab._last_status_snapshot["FLRig"]["state"] == "ok"
    finally:
        tab.deleteLater()
        app.processEvents()
