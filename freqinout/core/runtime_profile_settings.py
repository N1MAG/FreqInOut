from __future__ import annotations

from typing import Any, Mapping

from freqinout.core.settings_manager import SettingsManager


class RuntimeProfileSettings:
    def __init__(self, profile: Mapping[str, Any], fallback_settings: SettingsManager) -> None:
        self.profile = dict(profile or {})
        self.fallback_settings = fallback_settings

    def get(self, key: str, default=None):
        if key in self.profile:
            value = self.profile.get(key)
            if value not in (None, ""):
                return value
        return self.fallback_settings.get(key, default)

    def set(self, key: str, value) -> None:
        try:
            self.fallback_settings.set(key, value)
        except Exception:
            pass

    def save(self) -> None:
        try:
            if hasattr(self.fallback_settings, "save"):
                self.fallback_settings.save()
        except Exception:
            pass
