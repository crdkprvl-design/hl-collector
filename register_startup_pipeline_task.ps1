Param(
    [string]$TaskName = "HL_Startup_Sync_And_Summary"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $root) {
    $root = Get-Location
}

$scriptPath = Join-Path $root "startup_sync_and_summary.ps1"
if (-not (Test-Path -LiteralPath $scriptPath)) {
    throw "startup_sync_and_summary.ps1 not found: $scriptPath"
}

$wrapperPath = Join-Path $env:USERPROFILE "hl_startup_sync.ps1"
$wrapperBody = @(
    '$ErrorActionPreference = "Stop"'
    "& '$scriptPath'"
) -join [Environment]::NewLine
Set-Content -LiteralPath $wrapperPath -Value $wrapperBody -Encoding UTF8

$registrationMode = $null
$actionArgs = @(
    "/Create"
    "/TN", $TaskName
    "/SC", "ONLOGON"
    "/TR", "powershell.exe -NoProfile -ExecutionPolicy Bypass -File $wrapperPath"
    "/F"
)
$proc = Start-Process -FilePath "schtasks.exe" -ArgumentList $actionArgs -Wait -PassThru -NoNewWindow -ErrorAction SilentlyContinue
if ($proc -and $proc.ExitCode -eq 0) {
    $registrationMode = "task_scheduler"
}
else {
    $startupDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\Startup"
    $startupLauncher = Join-Path $startupDir "$TaskName.cmd"
    $startupBody = @(
        '@echo off'
        "powershell.exe -NoProfile -ExecutionPolicy Bypass -File ""$wrapperPath"""
    ) -join [Environment]::NewLine
    Set-Content -LiteralPath $startupLauncher -Value $startupBody -Encoding ASCII
    if (-not (Test-Path -LiteralPath $startupLauncher)) {
        throw "Failed to register startup launcher: $startupLauncher"
    }
    $registrationMode = "startup_folder"
}

Write-Output "registered_task=$TaskName"
Write-Output "action=$wrapperPath"
Write-Output "wrapper=$wrapperPath"
Write-Output "registration_mode=$registrationMode"
