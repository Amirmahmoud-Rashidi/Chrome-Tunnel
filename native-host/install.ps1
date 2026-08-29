<#
.SYNOPSIS
  Installs the chrometunnel native messaging host for Chrome.

.DESCRIPTION
  1. Fills in local.chrometunnel.host.json with:
       - the absolute path to host.bat in THIS folder
       - the Chrome extension ID you provide
  2. Writes that resolved manifest path into the Windows registry so Chrome
     can find it:
       HKCU\Software\Google\Chrome\NativeMessagingHosts\local.chrometunnel.host

.PARAMETER ExtensionId
  The ID Chrome assigned when you loaded the unpacked extension
  (chrome://extensions, shown under the extension's name/card).
  Required — the manifest's allowed_origins must match it exactly or
  Chrome will refuse the native messaging connection.

.EXAMPLE
  .\install.ps1 -ExtensionId "abcdefghijklmnopqrstuvwxyzabcdef"
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$ExtensionId
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$manifestPath = Join-Path $scriptDir "local.chrometunnel.host.json"
$hostBatPath = Join-Path $scriptDir "host.bat"

if (-not (Test-Path $manifestPath)) {
    throw "Could not find $manifestPath. Run this script from inside the native-host folder."
}
if (-not (Test-Path $hostBatPath)) {
    throw "Could not find $hostBatPath. Make sure host.bat exists next to this script."
}

# Basic sanity check on the extension ID (Chrome IDs are 32 lowercase a-p letters).
if ($ExtensionId -notmatch '^[a-p]{32}$') {
    Write-Warning "ExtensionId '$ExtensionId' doesn't look like a standard 32-character Chrome extension ID. Continuing anyway, but double-check it against chrome://extensions."
}

Write-Host "Using native host folder: $scriptDir"
Write-Host "Extension ID: $ExtensionId"

# --- Step 1: rewrite the manifest with real values ---------------------------

$manifestObj = Get-Content $manifestPath -Raw | ConvertFrom-Json

# host.bat needs backslashes in JSON escaped; Node/PowerShell handle this
# automatically when we set a .NET string property and re-serialize.
$manifestObj.path = $hostBatPath
$manifestObj.allowed_origins = @("chrome-extension://$ExtensionId/")

# ConvertTo-Json escapes backslashes correctly for us.
$manifestObj | ConvertTo-Json -Depth 5 | Set-Content -Path $manifestPath -Encoding UTF8

Write-Host "Updated manifest: $manifestPath"
Write-Host "  path            = $($manifestObj.path)"
Write-Host "  allowed_origins = $($manifestObj.allowed_origins)"

# --- Step 2: register in the Windows registry --------------------------------

$registryKeyPath = "HKCU:\Software\Google\Chrome\NativeMessagingHosts\local.chrometunnel.host"

if (-not (Test-Path $registryKeyPath)) {
    New-Item -Path $registryKeyPath -Force | Out-Null
}

Set-ItemProperty -Path $registryKeyPath -Name "(Default)" -Value $manifestPath

Write-Host "Registered native messaging host in registry at:"
Write-Host "  $registryKeyPath"
Write-Host "  (Default) = $manifestPath"

Write-Host ""
Write-Host "Done. Restart Chrome fully (close all windows) for the registry change to take effect."
