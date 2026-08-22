from freqinout.core.rf_pins import delete_rf_pins, list_rf_pins, save_rf_pin


def test_rf_pin_helpers_round_trip_without_authorizing_routing(tmp_path) -> None:
    db_path = tmp_path / "fio.db"

    saved = save_rf_pin(
        db_path,
        {
            "pin_id": "manual:shelter-net",
            "label": "Shelter net",
            "callsign": "K0PRA",
            "group": "MAGNET",
            "topics": ("Shelter", "Comms"),
            "grid": "DM79",
        },
    )

    rows = list_rf_pins(db_path)
    assert len(rows) == 1
    assert rows[0].observation_id == saved.observation_id
    assert rows[0].source_ref == "manual:shelter-net"
    assert rows[0].route_eligible is False
    assert rows[0].publish_authorized is False

    assert delete_rf_pins(db_path, ["manual:shelter-net"]) == 1
    assert list_rf_pins(db_path) == ()
