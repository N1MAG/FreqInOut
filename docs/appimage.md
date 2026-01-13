# AppImage (Debian/Mint/Ubuntu)

This guide targets Linux Mint 22.1+ (Ubuntu 24.04 base). It bundles Python 3.11
inside the AppImage and uses the entrypoint `python3.11 -m freqinout.main`.

## Recipe file

Create `appimage-builder.yml` at the repo root:

```yaml
version: 1

AppDir:
  path: ./AppDir
  app_info:
    id: com.freqinout.FreqInOut
    name: FreqInOut
    version: 1.0.0
    icon: freqinout
    exec: usr/bin/python3.11
    exec_args: -m freqinout.main $@

  apt:
    arch: amd64
    sources:
      - sourceline: "deb http://archive.ubuntu.com/ubuntu/ noble main universe"
      - sourceline: "deb http://archive.ubuntu.com/ubuntu/ noble-updates main universe"
      - sourceline: "deb http://archive.ubuntu.com/ubuntu/ noble-security main universe"
    packages:
      - python3.11
      - python3.11-venv
      - python3.11-distutils
      - python3-pip
      - libglib2.0-0
      - libnss3
      - libnspr4
      - libx11-6
      - libxkbcommon0
      - libxkbcommon-x11-0
      - libxcomposite1
      - libxdamage1
      - libxrandr2
      - libxss1
      - libxtst6
      - libxrender1
      - libxext6
      - libxcb1
      - libxcb-icccm4
      - libxcb-image0
      - libxcb-keysyms1
      - libxcb-randr0
      - libxcb-render-util0
      - libxcb-shape0
      - libxcb-xfixes0
      - libxcb-xinerama0
      - libxcb-util1
      - libdrm2
      - libgbm1
      - libasound2
      - libfontconfig1
      - libfreetype6
      - libjpeg8
      - libpng16-16
      - libdbus-1-3
      - libexpat1
      - libgl1
      - libgtk-3-0
      - libatk-bridge2.0-0
      - libatspi2.0-0
      - libcups2
      - libpango-1.0-0
      - libpangocairo-1.0-0
      - fonts-dejavu-core

  files:
    include:
      - freqinout
      - docs/guide.html

  runtime:
    env:
      PYTHONNOUSERSITE: "1"
      PYTHONPATH: "${APPDIR}/usr/lib/python3.11/site-packages:${APPDIR}/usr/lib/freqinout"

  python:
    version: "3.11"
    packages:
      - -r requirements.txt

AppImage:
  arch: x86_64
```

## Prep steps for icon and desktop entry

Some appimage-builder versions do not support `source/destination` mapping for
files. In that case, prepare the AppDir tree before building:

```bash
rm -rf AppDir
mkdir -p AppDir/usr/share/icons/hicolor/256x256/apps
mkdir -p AppDir/usr/share/applications

cp assets/FreqInOut_logo.png \
  AppDir/usr/share/icons/hicolor/256x256/apps/freqinout.png

cat > AppDir/usr/share/applications/freqinout.desktop <<'EOF'
[Desktop Entry]
Type=Application
Name=FreqInOut
Comment=JS8Call monitoring, nets, and automation tools
Exec=FreqInOut
Icon=freqinout
Terminal=false
Categories=Utility;Network;
EOF
```

## Build

```bash
appimage-builder --recipe appimage-builder.yml
```

