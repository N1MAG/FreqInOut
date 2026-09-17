# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

import PySide6


ROOT = Path(SPECPATH)
QT_ROOT = Path(PySide6.__file__).resolve().parent / "Qt"


def qml_module_files(module_name):
    """Collect only the root files of a Qt QML module unless it is tiny."""
    module_dir = QT_ROOT / "qml" / module_name
    if not module_dir.exists():
        return []
    if module_name in {"QtLocation", "QtPositioning", "QtQuick/Controls", "QtQuick/Templates"}:
        return [(str(module_dir), f"PySide6/Qt/qml/{module_name}")]
    return [
        (str(path), f"PySide6/Qt/qml/{module_name}")
        for path in module_dir.iterdir()
        if path.is_file()
    ]


qt_qml_datas = []
for qml_module in (
    "QtLocation",
    "QtPositioning",
    "QtQml",
    "QtQuick",
    "QtQuick/Controls",
    "QtQuick/Templates",
):
    qt_qml_datas.extend(qml_module_files(qml_module))

qt_plugin_binaries = []
for plugin_group in ("geoservices", "position"):
    plugin_dir = QT_ROOT / "plugins" / plugin_group
    if plugin_dir.exists():
        plugin_files = [path for path in plugin_dir.iterdir() if path.is_file()]
        if plugin_group == "geoservices":
            # The Map contract is provider-free. Ship only Qt's coordinate
            # item-overlay backend, never an online tile-provider fallback.
            plugin_files = [path for path in plugin_files if "itemsoverlay" in path.name.lower()]
        qt_plugin_binaries.extend(
            (str(path), f"PySide6/Qt/plugins/{plugin_group}")
            for path in plugin_files
        )


a = Analysis(
    ['freqinout/main.py'],
    pathex=['.'],
    binaries=qt_plugin_binaries,
    datas=[
        ('freqinout/gui/qml', 'freqinout/gui/qml'),
        ('freqinout/resources/spotter_forms', 'freqinout/resources/spotter_forms'),
        ('config/leaflet', 'config/leaflet'),
        ('config/shortwave/eibi', 'config/shortwave/eibi'),
        ('assets', 'assets'),
        ('docs/guide.html', 'docs'),
        ('third_party/js8net', 'third_party/js8net'),
    ] + qt_qml_datas,
    hiddenimports=[
        "PySide6.QtLocation",
        "PySide6.QtPositioning",
        "PySide6.QtQml",
        "PySide6.QtQuick",
        "PySide6.QtQuickControls2",
        "PySide6.QtQuickWidgets",
        "tzdata",
        "keyring.backends.chainer",
        "keyring.backends.fail",
        "keyring.backends.kwallet",
        "keyring.backends.macOS",
        "keyring.backends.SecretService",
        "keyring.backends.Windows",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=['packaging/pyinstaller_runtime_qt.py'],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='FreqInOut',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='assets/FreqInOut.ico',
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='FreqInOut',
)
