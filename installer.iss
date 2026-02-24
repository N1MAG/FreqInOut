; installer.iss
#define MyAppName "FreqInOut"
#define MyAppVersion "1.2.0"
#define MyAppExeName "FreqInOut.exe"

[Setup]
AppName={#MyAppName}
AppVersion={#MyAppVersion}
DefaultDirName={autopf}\FreqInOut
DisableDirPage=yes
DisableProgramGroupPage=yes
OutputBaseFilename=FreqInOut-Setup
Compression=lzma
SolidCompression=yes

; Use a custom installer icon
SetupIconFile="assets\FreqInOut.ico"

[Files]
; Install the built app folder; exclude git/venv/logs/db/config files
Source: "dist\FreqInOut\*"; DestDir: "{app}"; Flags: recursesubdirs; Excludes: ".git*;*.pdb;*.obj;__pycache__\*;venv\*;.venv\*;*.log;*.db;config\config.json;freqinout\logs\*"
Source: "assets\FreqInOut.ico"; DestDir: "{app}"

[Icons]
Name: "{autoprograms}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{commondesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Launch {#MyAppName}"; Flags: nowait postinstall skipifsilent
