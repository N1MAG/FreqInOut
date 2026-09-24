@echo off
setlocal

set "SCRIPT_WORKTREE=%~dp0"
if defined FREQINOUT_INSTALL_DIR (
  set "WORKTREE=%FREQINOUT_INSTALL_DIR%"
) else (
  set "WORKTREE=%SCRIPT_WORKTREE%"
)

if exist "%WORKTREE%\.venv\Scripts\python.exe" (
  set "PYTHON=%WORKTREE%\.venv\Scripts\python.exe"
) else if exist "%WORKTREE%\venv\Scripts\python.exe" (
  set "PYTHON=%WORKTREE%\venv\Scripts\python.exe"
) else (
  >&2 echo Missing virtual environment at "%WORKTREE%\.venv" or "%WORKTREE%\venv"
  exit /b 1
)

rem FIO owns the Windows default profile location under LOCALAPPDATA/APPDATA.
rem Preserve explicit isolated profiles without creating a parallel default.
if defined FREQINOUT_RUNTIME_ROOT if not defined FREQINOUT_CONFIG_DIR (
  set "FREQINOUT_CONFIG_DIR=%FREQINOUT_RUNTIME_ROOT%"
  >&2 echo Warning: FREQINOUT_RUNTIME_ROOT is deprecated; use FREQINOUT_CONFIG_DIR.
)

pushd "%WORKTREE%" >nul || exit /b 1
"%PYTHON%" -m freqinout.main %*
set "EXIT_CODE=%ERRORLEVEL%"
popd
exit /b %EXIT_CODE%
