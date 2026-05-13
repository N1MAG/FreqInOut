from __future__ import annotations

import sys

from freqinout.core import secret_store


class _FakeKeyring:
    def __init__(self) -> None:
        self.values: dict[tuple[str, str], str] = {}

    def get_keyring(self):
        return "fake-keyring"

    def set_password(self, service: str, account: str, password: str) -> None:
        self.values[(service, account)] = password

    def get_password(self, service: str, account: str):
        return self.values.get((service, account))

    def delete_password(self, service: str, account: str) -> None:
        self.values.pop((service, account))


def test_gpg_passphrase_uses_normalized_fingerprint_account(monkeypatch) -> None:
    fake = _FakeKeyring()
    monkeypatch.setitem(sys.modules, "keyring", fake)

    fpr = "1234 5678 90ab cdef 1234 5678 90ab cdef 1234 5678"
    ok, msg = secret_store.store_gpg_signing_passphrase(fpr, "secret")
    loaded, err = secret_store.load_gpg_signing_passphrase(fpr)
    has_secret, has_err = secret_store.has_gpg_signing_passphrase(fpr)

    assert ok, msg
    assert loaded == "secret"
    assert err == ""
    assert has_secret
    assert has_err == ""
    assert ("FreqInOut GPG Signing", "gpg-compose:1234567890ABCDEF1234567890ABCDEF12345678") in fake.values


def test_gpg_passphrase_can_be_deleted(monkeypatch) -> None:
    fake = _FakeKeyring()
    monkeypatch.setitem(sys.modules, "keyring", fake)
    fpr = "1234567890ABCDEF1234567890ABCDEF12345678"

    ok, msg = secret_store.store_gpg_signing_passphrase(fpr, "secret")
    assert ok, msg
    ok, msg = secret_store.delete_gpg_signing_passphrase(fpr)
    loaded, err = secret_store.load_gpg_signing_passphrase(fpr)

    assert ok, msg
    assert loaded is None
    assert err == ""
