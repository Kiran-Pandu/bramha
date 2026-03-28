$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$kafkaRuntimeDir = Join-Path $repoRoot "runtime\kafka"
$consumerPidFile = Join-Path $kafkaRuntimeDir "consumer.pid"

if (-not (Test-Path $consumerPidFile)) {
    Write-Host "No consumer PID file found. Kafka consumer may already be stopped."
    return
}

$consumerPid = (Get-Content $consumerPidFile | Select-Object -First 1).Trim()
if (-not $consumerPid) {
    Remove-Item -LiteralPath $consumerPidFile -Force
    Write-Host "Consumer PID file was empty and has been cleared."
    return
}

$consumerProcess = Get-Process -Id $consumerPid -ErrorAction SilentlyContinue
if ($consumerProcess) {
    Write-Host "Stopping Kafka consumer PID $consumerPid..."
    Stop-Process -Id $consumerPid -Force
} else {
    Write-Host "Kafka consumer PID $consumerPid is not running."
}

Remove-Item -LiteralPath $consumerPidFile -Force
Write-Host "Kafka consumer stopped."
