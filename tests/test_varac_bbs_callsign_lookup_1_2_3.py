from freqinout.core.varac_bbs_config import format_callsign_list, parse_callsign_list


def test_format_callsign_list_accepts_iterables() -> None:
    assert format_callsign_list(["k7rie", " KC1VXQ "]) == "K7RIE, KC1VXQ"


def test_parse_callsign_list_normalizes_manual_tokens() -> None:
    assert parse_callsign_list("k7rie, kc1vxq;, w8ufo ") == ["K7RIE", "KC1VXQ", "W8UFO"]
