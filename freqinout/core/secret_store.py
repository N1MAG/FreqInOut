from __future__ import annotations

from typing import Optional, Tuple

from freqinout.core.gpg_tools import normalize_fingerprint


GPG_SIGNING_PASSPHRASE_SERVICE = "FreqInOut GPG Signing"


def _keyring_module():
    try:
        import keyring  # type: ignore
    except Exception as exc:
        return None, f"Secure credential storage is unavailable: {exc}"
    return keyring, ""


def gpg_signing_passphrase_account(fingerprint: str) -> str:
    fpr = normalize_fingerprint(fingerprint)
    return f"gpg-compose:{fpr}" if fpr else ""


def credential_store_available() -> Tuple[bool, str]:
    keyring, err = _keyring_module()
    if keyring is None:
        return False, err
    try:
        backend = keyring.get_keyring()
    except Exception as exc:
        return False, f"Secure credential storage is unavailable: {exc}"
    backend_name = f"{backend.__class__.__module__}.{backend.__class__.__name__}"
    lowered = backend_name.lower()
    if any(token in lowered for token in ("fail", "null", "plaintext")):
        return False, f"No secure OS credential store is available for keyring backend {backend_name}."
    return True, f"Secure credential storage: {backend}"


def load_gpg_signing_passphrase(fingerprint: str) -> Tuple[Optional[str], str]:
    account = gpg_signing_passphrase_account(fingerprint)
    if not account:
        return None, "Missing signing key fingerprint."
    keyring, err = _keyring_module()
    if keyring is None:
        return None, err
    try:
        return keyring.get_password(GPG_SIGNING_PASSPHRASE_SERVICE, account), ""
    except Exception as exc:
        return None, f"Could not read saved signing passphrase: {exc}"


def store_gpg_signing_passphrase(fingerprint: str, passphrase: str) -> Tuple[bool, str]:
    account = gpg_signing_passphrase_account(fingerprint)
    if not account:
        return False, "Missing signing key fingerprint."
    if passphrase == "":
        return False, "Passphrase is empty."
    keyring, err = _keyring_module()
    if keyring is None:
        return False, err
    try:
        keyring.set_password(GPG_SIGNING_PASSPHRASE_SERVICE, account, passphrase)
    except Exception as exc:
        return False, f"Could not save signing passphrase: {exc}"
    return True, "Signing passphrase saved in the OS credential store."


def delete_gpg_signing_passphrase(fingerprint: str) -> Tuple[bool, str]:
    account = gpg_signing_passphrase_account(fingerprint)
    if not account:
        return False, "Missing signing key fingerprint."
    keyring, err = _keyring_module()
    if keyring is None:
        return False, err
    try:
        keyring.delete_password(GPG_SIGNING_PASSPHRASE_SERVICE, account)
    except Exception as exc:
        text = str(exc)
        if "not found" in text.lower():
            return True, "No saved signing passphrase was present."
        return False, f"Could not clear saved signing passphrase: {exc}"
    return True, "Saved signing passphrase cleared."


def has_gpg_signing_passphrase(fingerprint: str) -> Tuple[bool, str]:
    passphrase, err = load_gpg_signing_passphrase(fingerprint)
    if err:
        return False, err
    return passphrase is not None, ""
