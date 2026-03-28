$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$kafkaRuntimeDir = Join-Path $repoRoot "runtime\kafka"
$brokerPidFile = Join-Path $kafkaRuntimeDir "broker.pid"

if (-not (Test-Path $brokerPidFile)) {
    Write-Host "No broker PID file found. Kafka may already be stopped."
    return
}

$brokerPid = (Get-Content $brokerPidFile | Select-Object -First 1).Trim()
if (-not $brokerPid) {
    Remove-Item -LiteralPath $brokerPidFile -Force
    Write-Host "Broker PID file was empty and has been cleared."
    return
}

$process = Get-Process -Id $brokerPid -ErrorAction SilentlyContinue
if ($process) {
    Write-Host "Stopping Kafka broker PID $brokerPid..."
    Stop-Process -Id $brokerPid -Force
} else {
    Write-Host "Kafka broker PID $brokerPid is not running."
}

Remove-Item -LiteralPath $brokerPidFile -Force
Write-Host "Kafka broker stopped."
