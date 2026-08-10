from pathlib import Path

from freqinout.core.js8_expect_dispatcher import dispatch_expect_auto_reply, list_expect_dispatch_audit
from freqinout.core.js8_expect_store import ExpectEvaluationResult
from freqinout.core.js8_msg_auth import MsgAuthKey, verify_js8_text
from freqinout.core.js8_msg_auth_store import MSG_AUTH_SCOPE_SIGNING, save_msg_auth_key
from freqinout.radio_interface.js8_api_client import JS8ApiClient
from tests.test_js8_send_service import _safe_server, _response


def _ready_eval(**overrides) -> ExpectEvaluationResult:
    values = {
        "decision": "reply-ready",
        "reason": "Matched enabled Expect entry.",
        "expect_entry_id": 7,
        "expect_key": "F!304",
        "response_text": "@MAGNET F!304 OK",
        "reply_radio_id": "radio-a",
        "reply_js8_instance_id": "fio-a",
        "auto_reply_enabled": True,
        "unattended_auto_reply_enabled": True,
    }
    values.update(overrides)
    return ExpectEvaluationResult(**values)


def test_expect_dispatch_holds_when_runtime_unattended_disabled(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    server = _safe_server()
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        result = dispatch_expect_auto_reply(
            evaluation=_ready_eval(),
            client=client,
            runtime_unattended_enabled=False,
            event_id="evt-1",
            source_radio_id="radio-a",
            source_js8_instance_id="fio-a",
            requesting_callsign="N0CALL",
            target_group="@MAGNET",
            db_path=db_path,
            timeout_s=0.4,
        )

        assert result.sent is False
        assert result.decision == "held"
        assert "TX.SEND_MESSAGE" not in [row["type"] for row in server.received]
        audit = list_expect_dispatch_audit(db_path=db_path)
        assert audit[0]["decision"] == "held"
        assert audit[0]["event_id"] == "evt-1"
        assert audit[0]["expect_key"] == "F!304"
    finally:
        client.stop()
        server.stop()


def test_expect_dispatch_sends_only_when_runtime_and_entry_are_unattended_enabled(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    server = _safe_server()
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        result = dispatch_expect_auto_reply(
            evaluation=_ready_eval(),
            client=client,
            runtime_unattended_enabled=True,
            event_id="evt-2",
            source_radio_id="radio-a",
            source_js8_instance_id="fio-a",
            requesting_callsign="N0CALL",
            target_group="@MAGNET",
            db_path=db_path,
            timeout_s=0.4,
        )

        assert result.sent is True
        assert result.decision == "sent"
        assert result.transmitted_text == "@MAGNET F!304 OK"
        assert server.received[-1]["type"] == "TX.SEND_MESSAGE"
        audit = list_expect_dispatch_audit(db_path=db_path)
        assert audit[0]["decision"] == "sent"
        assert audit[0]["transmitted_text"] == "@MAGNET F!304 OK"
    finally:
        client.stop()
        server.stop()


def test_expect_dispatch_uses_guarded_send_and_audits_blocks(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    server = _safe_server(
        **{"TX.GET_QUEUE_DEPTH": lambda req: _response("TX.QUEUE_DEPTH", req, {"DEPTH": 1})}
    )
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        result = dispatch_expect_auto_reply(
            evaluation=_ready_eval(),
            client=client,
            runtime_unattended_enabled=True,
            event_id="evt-3",
            db_path=db_path,
            timeout_s=0.4,
        )

        assert result.sent is False
        assert result.decision == "blocked"
        assert "tx_queue_not_empty" in [issue.code for issue in result.send_result.preflight.issues]
        assert "TX.SEND_MESSAGE" not in [row["type"] for row in server.received]
        assert list_expect_dispatch_audit(db_path=db_path)[0]["decision"] == "blocked"
    finally:
        client.stop()
        server.stop()


def test_expect_dispatch_signs_saved_payload_for_actual_target(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    shared_key = "MAGNET-MR08-DIRECT-SHARED"
    for target in ("MAGNET", "MR08", "K1ABC"):
        save_msg_auth_key(
            {
                "group_name": target,
                "callsign": "N1MAG",
                "key_scope": MSG_AUTH_SCOPE_SIGNING,
                "label": "Active",
                "key_text": shared_key,
            },
            db_path=db_path,
        )
    transmitted: list[str] = []
    for target in ("@MAGNET", "@MR08", "K1ABC"):
        server = _safe_server()
        client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
        try:
            result = dispatch_expect_auto_reply(
                evaluation=_ready_eval(
                    response_text="F!304 OK",
                    msg_auth_sign_enabled=True,
                    msg_auth_sign_callsign="N1MAG",
                    msg_auth_include_datecode=True,
                    msg_auth_datecode="#HHJL",
                ),
                client=client,
                runtime_unattended_enabled=True,
                target_group=target,
                requesting_callsign="K1ABC",
                db_path=db_path,
                timeout_s=0.4,
            )
            assert result.sent is True
            transmitted.append(result.transmitted_text)
            payload = result.transmitted_text.split(" ", 1)[1]
            assert "#HHJL" in payload
            assert verify_js8_text("N1MAG", target, payload, keys=[MsgAuthKey(label="shared", key=shared_key)]).state == "verified"
        finally:
            client.stop()
            server.stop()

    assert len(set(text.rsplit(" ", 1)[-1] for text in transmitted)) == 3


def test_expect_dispatch_sends_unsigned_when_signing_key_is_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    server = _safe_server()
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    try:
        result = dispatch_expect_auto_reply(
            evaluation=_ready_eval(
                response_text="F!304 OK",
                msg_auth_sign_enabled=True,
                msg_auth_sign_callsign="N1MAG",
            ),
            client=client,
            runtime_unattended_enabled=True,
            target_group="@MR08",
            requesting_callsign="K1ABC",
            db_path=db_path,
            timeout_s=0.4,
        )

        assert result.sent is True
        assert result.transmitted_text == "@MR08 F!304 OK"
        assert "sent unsigned" in result.reason
    finally:
        client.stop()
        server.stop()
