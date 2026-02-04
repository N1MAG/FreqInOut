Param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Args
)

$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
$WrapperPy = Join-Path $PSScriptRoot "freqinout_db.py"

$PythonExe = $null
$PythonExtraArgs = @()
$VenvPy = Join-Path $RootDir "venv\Scripts\python.exe"
if (Test-Path $VenvPy) {
    $PythonExe = $VenvPy
}
elseif (Get-Command py -ErrorAction SilentlyContinue) {
    $PythonExe = "py"
    $PythonExtraArgs = @("-3")
}
elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $PythonExe = "python"
}
else {
    Write-Error "Python not found."
    exit 1
}

Push-Location $RootDir
try {
    & $PythonExe @PythonExtraArgs $WrapperPy @Args
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
