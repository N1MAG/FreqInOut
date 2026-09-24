from pathlib import Path
import sqlite3

import pytest

from tools.create_readme_demo_profile import (
    CallsignShiftSanitizer,
    _configure_primary_js8_runtime,
    _demo_runtime_binding,
    _sanitize_database,
    _shift_ascii,
    _sqlite_backup,
    _rotate_existing_output,
    _verify_callsign_transform,
)


def _sanitizer() -> CallsignShiftSanitizer:
    return CallsignShiftSanitizer(
        Path("/source"),
        Path("/output"),
        letter_shift=3,
        digit_shift=1,
        preserve_callsigns=("N1MAG",),
    )


def test_shift_ascii_wraps_letters_and_digits() -> None:
    assert _shift_ascii("W8UFO", letter_shift=3, digit_shift=1) == "Z9XIR"
    assert _shift_ascii("Z9XYZ", letter_shift=3, digit_shift=1) == "C0ABC"


def test_callsign_mask_preserves_n1mag_and_suffixes() -> None:
    sanitizer = _sanitizer()

    assert sanitizer.text("N1MAG N1MAG/P n1mag") == "N1MAG N1MAG/P n1mag"
    assert sanitizer.text("W8UFO W8UFO/P") == "Z9XIR Z9XIR/P"


def test_callsign_mask_leaves_names_grids_and_ordinary_text_unchanged() -> None:
    sanitizer = _sanitizer()
    source = "Bill and Scott scheduled JS8Call from grid EM12JV."

    assert sanitizer.text(source) == source


def test_callsign_mask_supports_digit_prefix_international_callsigns() -> None:
    sanitizer = _sanitizer()

    assert sanitizer.text("4X1ABC 2E0AAA 3D2AA") == "5A2DEF 3H1DDD 4G3DD"


def test_callsign_mask_updates_nested_json_without_changing_other_fields() -> None:
    sanitizer = _sanitizer()
    source = '{"from":"W8UFO","operator":"Bill","grid":"EM12JV","owner":"N1MAG"}'

    assert sanitizer.value("messages", "payload_json", 1, source) == (
        '{"from":"Z9XIR","operator":"Bill","grid":"EM12JV","owner":"N1MAG"}'
    )


def test_callsign_mask_rejects_collision_with_preserved_callsign() -> None:
    sanitizer = _sanitizer()

    with pytest.raises(ValueError, match="would become preserved callsign N1MAG"):
        sanitizer.register_call("K0JXD")


def test_database_transform_is_exact_and_preserves_other_content(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    destination = tmp_path / "destination.db"
    connection = sqlite3.connect(source)
    try:
        connection.execute(
            "CREATE TABLE messages(id INTEGER PRIMARY KEY, callsign TEXT UNIQUE, body TEXT)"
        )
        connection.execute("CREATE TABLE dirty(message_id INTEGER UNIQUE)")
        connection.execute(
            "CREATE TRIGGER mark_dirty AFTER UPDATE ON messages "
            "BEGIN INSERT OR IGNORE INTO dirty(message_id) VALUES(NEW.id); END"
        )
        connection.execute(
            "INSERT INTO messages(callsign, body) VALUES(?, ?)",
            ("W8UFO", "Bill at EM12JV heard W8UFO/P and N1MAG"),
        )
        connection.commit()
    finally:
        connection.close()
    sanitizer = CallsignShiftSanitizer(
        source.parent,
        destination.parent,
        preserve_callsigns=("N1MAG",),
    )
    sanitizer.register_call("W8UFO")
    _sqlite_backup(source, destination)

    assert _sanitize_database(destination, sanitizer) == 1
    _verify_callsign_transform(source, destination, sanitizer)

    connection = sqlite3.connect(destination)
    try:
        assert connection.execute("SELECT callsign, body FROM messages").fetchone() == (
            "Z9XIR",
            "Bill at EM12JV heard Z9XIR/P and N1MAG",
        )
        assert connection.execute("SELECT COUNT(*) FROM dirty").fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='mark_dirty'"
        ).fetchone() == (1,)
    finally:
        connection.close()


def test_replace_output_rotates_existing_profile(tmp_path: Path) -> None:
    output = tmp_path / "demo"
    output.mkdir()
    (output / "sentinel.txt").write_text("previous", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--replace-output"):
        _rotate_existing_output(output, replace_output=False)

    previous = _rotate_existing_output(output, replace_output=True)

    assert previous is not None
    assert not output.exists()
    assert (previous / "sentinel.txt").read_text(encoding="utf-8") == "previous"


def test_replace_output_rejects_open_sqlite_profile(tmp_path: Path) -> None:
    output = tmp_path / "demo"
    (output / "config").mkdir(parents=True)
    database = output / "config" / "freqinout.db"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY)")
    connection.commit()

    try:
        with pytest.raises(RuntimeError, match="appears to be open in FreqInOut"):
            _rotate_existing_output(output, replace_output=True)
    finally:
        connection.close()

    assert output.exists()


def test_gui_lab_binding_matches_launcher_contract(tmp_path: Path) -> None:
    binding = _demo_runtime_binding(
        1,
        tmp_path / "demo",
        tmp_path / "radio-tools",
        tmp_path / "gui-lab",
    )

    assert binding["profile"] == "b"
    assert binding["rigctld_port"] == 4533
    assert binding["flrig_port"] == 12346
    assert binding["fldigi_port"] == 7363
    assert binding["js8_port"] == 2243
    assert binding["js8_save_dir"] == (
        tmp_path / "gui-lab" / "tool-homes" / "js8call" / "fio-b" / "save"
    )
    assert str(binding["js8_data_root"]).endswith(
        "/Library/Application Support/JS8Call - fio-b"
    )
    assert str(binding["js8_path"]).endswith(
        "/Applications/RadioApps/JS8Call 2.app/Contents/MacOS/JS8Call"
    )


def test_primary_js8_compatibility_settings_follow_primary_lab_suite(tmp_path: Path) -> None:
    database = tmp_path / "settings.db"
    connection = sqlite3.connect(database)
    try:
        connection.execute("CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT)")
        _configure_primary_js8_runtime(
            connection,
            host="127.0.0.1",
            port=2242,
            offset_hz=2125,
            save_dir=tmp_path / "lab" / "save",
            data_root=tmp_path / "Library" / "Application Support" / "JS8Call - fio-a",
        )
        values = dict(connection.execute("SELECT key, value FROM kv"))
    finally:
        connection.close()

    assert values == {
        "js8_directed_path": str(
            tmp_path / "Library" / "Application Support" / "JS8Call - fio-a" / "DIRECTED.TXT"
        ),
        "js8_forms_path": str(
            tmp_path / "Library" / "Application Support" / "JS8Call - fio-a" / "forms"
        ),
        "js8_host": "127.0.0.1",
        "js8_offset_hz": "2125",
        "js8_port": "2242",
        "js8_profile_path": str(tmp_path / "lab" / "save"),
    }
