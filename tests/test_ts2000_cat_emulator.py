from tools.ts2000_cat_emulator import RadioState, response_for


def test_ts2000_identity_frequency_and_mode_queries() -> None:
    state = RadioState(freq_a=14_115_000, freq_b=7_078_000)

    assert response_for("ID;", state) == "ID019;"
    assert response_for("PS;", state) == "PS1;"
    assert response_for("FA;", state) == "FA00014115000;"
    assert response_for("FB;", state) == "FB00007078000;"
    assert response_for("MD;", state) == "MD2;"


def test_ts2000_setters_update_only_the_selected_state() -> None:
    state = RadioState()

    assert response_for("FA00007102000;", state) is None
    assert response_for("MD1;", state) is None
    assert response_for("FR1;", state) is None
    assert response_for("FT1;", state) is None

    assert response_for("FA;", state) == "FA00007102000;"
    assert response_for("MD;", state) == "MD1;"
    assert response_for("FR;", state) == "FR1;"
    assert response_for("FT;", state) == "FT1;"


def test_ts2000_flrig_control_queries_are_stable() -> None:
    state = RadioState()

    assert response_for("PC;", state) == "PC050;"
    assert response_for("SM0;", state) == "SM00035;"
    assert response_for("FW;", state) == "FW2400;"
    assert response_for("EX0120000;", state) == "EX01200000;"
    assert response_for("ZZ;", state) == "?;"
