# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['freqinout\\main.py'],
    pathex=['.'],
    binaries=[],
    datas=[
        ('config\\leaflet', 'config\\leaflet'),
        ('assets', 'assets'),
        ('docs\\guide.html', 'docs'),
        ('third_party\\js8net', 'third_party\\js8net'),
    ],
    hiddenimports=["tzdata"],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
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
    icon='assets\\FreqInOut.ico',
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
