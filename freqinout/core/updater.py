
"""Simple placeholder updater.

You MUST set UPDATE_INFO_URL to a real JSON endpoint before using.
"""
from __future__ import annotations

import sys
import zipfile
import shutil
import tempfile
from pathlib import Path
from typing import Optional, Tuple

import requests

from freqinout.core.logger import log
from freqinout import __version__ as LOCAL_VERSION

UPDATE_INFO_URL = "https://example.com/freqinout/update.json"
DOWNLOAD_DIR = Path.home() / ".config" / "freqinout" / "downloads"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR_NAME = "backup_prev_version"

def parse_version(v: str):
    parts = v.strip().split(".")
    parts = (parts + ["0","0","0"])[:3]
    try:
        return tuple(int(p) for p in parts)
    except ValueError:
        return (0,0,0)

def is_remote_newer(local: str, remote: str) -> bool:
    return parse_version(remote) > parse_version(local)

def fetch_update_info(timeout: int = 10) -> Optional[dict]:
    try:
        log.info(f"Checking for updates at: {UPDATE_INFO_URL}")
        r = requests.get(UPDATE_INFO_URL, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        if "version" not in data or "download_url" not in data:
            log.error("Update JSON missing keys.")
            return None
        return data
    except Exception as e:
        log.error(f"Failed to fetch update info: {e}")
        return None

def download_release(url: str) -> Optional[Path]:
    filename = url.split("/")[-1] or "freqinout_update.zip"
    dest = DOWNLOAD_DIR / filename
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        log.info(f"Downloaded update to {dest}")
        return dest
    except Exception as e:
        log.error(f"Download failed: {e}")
        return None

def backup_current_install(install_dir: Path) -> Optional[Path]:
    backup_dir = install_dir.parent / BACKUP_DIR_NAME
    if backup_dir.exists():
        shutil.rmtree(backup_dir, ignore_errors=True)
    try:
        shutil.copytree(install_dir, backup_dir)
        log.info(f"Backup created at {backup_dir}")
        return backup_dir
    except Exception as e:
        log.error(f"Backup failed: {e}")
        return None

def apply_update_archive(archive: Path, install_dir: Path) -> bool:
    if not archive.exists():
        log.error("Archive missing.")
        return False
    backup = backup_current_install(install_dir)
    if not backup:
        return False
    tmp = Path(tempfile.mkdtemp(prefix="freqinout_update_"))
    try:
        with zipfile.ZipFile(archive, "r") as zf:
            zf.extractall(tmp)
        for item in tmp.iterdir():
            dest = install_dir / item.name
            if dest.exists():
                if dest.is_dir():
                    shutil.rmtree(dest, ignore_errors=True)
                else:
                    dest.unlink()
            if item.is_dir():
                shutil.copytree(item, dest)
            else:
                shutil.copy2(item, dest)
        log.info("Update applied successfully.")
        return True
    except Exception as e:
        log.error(f"Failed to apply update: {e}")
        return False
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

def check_for_update() -> Tuple[bool, Optional[str], Optional[str]]:
    info = fetch_update_info()
    if not info:
        return False, None, None
    remote = info.get("version")
    changelog = info.get("changelog","")
    if remote and is_remote_newer(LOCAL_VERSION, remote):
        return True, remote, changelog
    return False, remote, changelog

def run_interactive_update() -> None:
    print(f"FreqInOut current version: {LOCAL_VERSION}")
    available, remote, changelog = check_for_update()
    if not available:
        print("No update available.")
        return
    print(f"New version available: {remote}")
    if changelog:
        print("Changelog:\n" + changelog)
    ans = input("Apply this update now? [y/N]: ").strip().lower()
    if ans != "y":
        print("Cancelled.")
        return
    info = fetch_update_info()
    if not info:
        print("Failed to re-fetch update info.")
        return
    archive = download_release(info["download_url"])
    if not archive:
        print("Download failed.")
        return
    install_dir = Path(__file__).resolve().parents[2]
    if apply_update_archive(archive, install_dir):
        print("Update applied. Restart FreqInOut.")
    else:
        print("Update failed. See log.")
