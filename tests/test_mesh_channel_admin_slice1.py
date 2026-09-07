import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QSplitter

from freqinout.core.mesh import MeshChannel, MeshChannelCapabilities, MeshChannelPolicy
from freqinout.gui.mesh_channel_admin import MeshChannelAdminWidget


@pytest.fixture(scope="module")
def app():
    return QApplication.instance() or QApplication([])


def make_widget(app, capabilities=None):
    widget = MeshChannelAdminWidget()
    widget.set_adapter_context("adapter-1")
    widget.set_channels([MeshChannel("adapter-1", "meshcore", 0, "Field", channel_id="chan-0", psk_hint="SECRET")])
    widget.set_policies([MeshChannelPolicy("adapter-1", "meshcore", "chan-0", "Field", review_state="pending", key_hint="SECRET")])
    widget.set_capabilities(capabilities or MeshChannelCapabilities())
    widget.show()
    app.processEvents()
    return widget


def test_device_remove_requires_explicit_confirmation(app):
    widget = make_widget(app, MeshChannelCapabilities(can_remove_from_device=True))
    requested, confirmations = [], []
    widget.remove_from_device_requested.connect(lambda *args: requested.append(args))
    widget.remove_from_device_confirmation_requested.connect(lambda *args: confirmations.append(args))
    widget.remove_device_button.click()
    assert confirmations == [("adapter-1", "chan-0")]
    assert requested == []
    widget.confirm_remove_from_device()
    assert requested == [("adapter-1", "chan-0")]


def test_unsupported_device_actions_are_disabled(app):
    widget = make_widget(app)
    assert not widget.configure_button.isEnabled()
    assert not widget.remove_device_button.isEnabled()
    assert widget.guidance_label.text()


def test_compact_layout_uses_vertical_splitter_and_single_column_actions(app):
    widget = make_widget(app, MeshChannelCapabilities(can_configure=True, can_remove_from_device=True))
    widget.resize(500, 600)
    app.processEvents()
    splitter = widget.findChild(QSplitter)
    assert splitter.orientation() == Qt.Vertical
    assert widget._actions_layout.getItemPosition(widget._actions_layout.indexOf(widget.ignore_button))[:2] == (1, 0)
    widget.resize(900, 600)
    app.processEvents()
    assert splitter.orientation() == Qt.Horizontal
    assert widget._actions_layout.getItemPosition(widget._actions_layout.indexOf(widget.ignore_button))[:2] == (0, 1)


def test_secret_material_is_not_rendered(app):
    widget = make_widget(app)
    visible_text = " ".join(label.text() for label in widget.findChildren(type(widget.detail_title)))
    assert "SECRET" not in visible_text


def test_refresh_changes_to_cancel_during_sync(app):
    widget = make_widget(app)
    refreshes, cancels = [], []
    widget.refresh_requested.connect(refreshes.append)
    widget.cancel_requested.connect(lambda *args: cancels.append(args))
    widget.refresh_button.click()
    assert refreshes == ["adapter-1"]
    widget.set_sync_state("Syncing channels · 2 found")
    assert widget.refresh_button.text() == "Cancel"
    widget.refresh_button.click()
    assert cancels == [("adapter-1", "channel-sync")]


def test_refresh_summary_updates_do_not_overwrite_live_cancelling_state(app):
    widget = make_widget(app)
    refreshes = []
    widget.refresh_requested.connect(refreshes.append)

    widget.refresh_button.click()
    assert refreshes == ["adapter-1"]
    assert widget.refresh_button.text() == "Cancel"

    widget.set_sync_summary("Syncing channels · 2 found")
    assert widget.refresh_button.text() == "Cancel"

    widget.set_sync_state("Cancelling channel refresh…")
    assert widget.refresh_button.text() == "Cancel"
    assert not widget.refresh_button.isEnabled()

    widget.set_sync_summary("Channel sync complete · 2 found")
    assert widget.refresh_button.text() == "Cancel"
    assert not widget.refresh_button.isEnabled()


def test_policy_signal_preserves_scopes_groups_and_retention(app):
    widget = make_widget(app)
    updates = []
    widget.policy_update_requested.connect(lambda *args: updates.append(args))
    widget.groups_edit.setText("MR08, magnet")
    widget.map_check.setChecked(False)
    widget.retention_combo.setCurrentText("30d")
    widget.accept_button.click()

    assert updates[0][0:2] == ("adapter-1", "chan-0")
    assert updates[0][2]["mapped_groups"] == ("MR08", "MAGNET")
    assert updates[0][2]["map_enabled"] is False
    assert updates[0][2]["retention_window"] == "30d"


def test_device_channels_are_listed_before_staged_channels_and_naturally_sorted(app):
    widget = MeshChannelAdminWidget()
    widget.set_adapter_context("adapter-1")
    widget.set_device_channel_ids({"10", "2"})
    widget.set_channels(
        [
            MeshChannel("adapter-1", "meshcore", -1, "Channel 10", channel_id="10"),
            MeshChannel("adapter-1", "meshcore", -1, "Channel 2", channel_id="2"),
            MeshChannel("adapter-1", "meshcore", -1, "Channel 11", channel_id="11"),
            MeshChannel("adapter-1", "meshcore", -1, "Channel 3", channel_id="3"),
        ]
    )
    ids = [widget.channel_list.item(row).data(32) for row in range(widget.channel_list.count())]
    assert ids == ["2", "10", "3", "11"]
