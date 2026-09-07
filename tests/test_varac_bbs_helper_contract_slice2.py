from __future__ import annotations

from freqinout.core.varac_bbs_vault import (
    ASYNC_REFRESH_NOTICE,
    VaultLocation,
    _is_fio_bbs_generated_listing,
    _quick_refresh_notice_entry,
    _read_first_entry,
    _root_location_helper_entry,
    root_location_helper_filename_preview,
)


def test_logical_location_helper_label_is_extensionless_but_disk_name_is_compatible() -> None:
    location = VaultLocation(
        id="intel",
        name="Intel",
        source_dir="/tmp/intel",
        alias="INTEL",
        description="Latest reports",
    )

    logical_label = root_location_helper_filename_preview(
        location,
        default_location_id="default",
        global_code_policy="Allow public locations",
        order=20,
    )
    entry = _root_location_helper_entry(
        location,
        default_location_id="default",
        global_code_policy="Allow public locations",
        order=20,
    )

    assert logical_label == "20 type INTEL - open Intel - Latest reports"
    assert not logical_label.lower().endswith(".txt")
    assert entry.name == logical_label
    assert not entry.name.lower().endswith(".txt")
    assert entry.content == logical_label + "\n"


def test_helper_refresh_language_is_asynchronous_and_has_no_fixed_wait() -> None:
    read_first = _read_first_entry()
    notice = _quick_refresh_notice_entry("LIST Q")

    assert "wait" not in read_first.name.lower()
    assert "wait" not in read_first.content.lower()
    assert "wait" not in notice.name.lower()
    assert "wait" not in notice.content.lower()
    assert ASYNC_REFRESH_NOTICE in notice.content
    assert notice.content.startswith("00 NOTICE - LIST Q received.")


def test_legacy_generated_listing_names_remain_recognized() -> None:
    assert _is_fio_bbs_generated_listing(
        "00 READ FIRST - type command, wait 10 sec, refresh BBS.txt"
    )
    assert _is_fio_bbs_generated_listing(
        "00 NOTICE - LIST BLKS 1AD1 received; wait 10 sec, refresh again.txt"
    )
    assert _is_fio_bbs_generated_listing("01 COMMANDS - type one command below.txt")
    assert _is_fio_bbs_generated_listing(
        "00 READ FIRST - type command, then refresh BBS.txt"
    )
