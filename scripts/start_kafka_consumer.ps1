$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$kafkaRuntimeDir = Join-Path $repoRoot "runtime\kafka"
$logsDir = Join-Path $kafkaRuntimeDir "logs"
$consumerPidFile = Join-Path $kafkaRuntimeDir "consumer.pid"
$consumerLog = Join-Path $logsDir "consumer.log"
$consumerErrorLog = Join-Path $logsDir "consumer-error.log"
$pythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $pythonExe)) {
    throw "Python virtual environment not found at $pythonExe. Create it and install requirements first."
}

New-Item -ItemType Directory -Force -Path $kafkaRuntimeDir, $logsDir | Out-Null

if (Test-Path $consumerPidFile) {
    $existingProcessId = (Get-Content $consumerPidFile | Select-Object -First 1).Trim()
    if ($existingProcessId) {
        $existingProcess = Get-Process -Id $existingProcessId -ErrorAction SilentlyContinue
        if ($existingProcess) {
            Write-Host "Kafka consumer is already running with PID $existingProcessId"
            Write-Host "Consumer log: $consumerLog"
            return
        }
    }
    Remove-Item -LiteralPath $consumerPidFile -Force
}

Write-Host "Starting Kafka consumer..."
$consumerProcess = Start-Process `
    -FilePath $pythonExe `
    -ArgumentList "-u", "-m", "scripts.kafka_log_consumer" `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $consumerLog `
    -RedirectStandardError $consumerErrorLog `
    -PassThru

Set-Content -Path $consumerPidFile -Value $consumerProcess.Id -Encoding ASCII
Start-Sleep -Seconds 3

$runningProcess = Get-Process -Id $consumerProcess.Id -ErrorAction SilentlyContinue
if (-not $runningProcess) {
    $logTail = @()
    if (Test-Path $consumerLog) {
        $logTail += Get-Content $consumerLog -Tail 30
    }
    if (Test-Path $consumerErrorLog) {
        $logTail += Get-Content $consumerErrorLog -Tail 30
    }
    if (-not $logTail) {
        $logTail = @("No consumer log found.")
    }
    throw "Kafka consumer failed to start.`n$($logTail -join [Environment]::NewLine)"
}

Write-Host "Kafka consumer is running."
Write-Host "Consumer PID: $($consumerProcess.Id)"
Write-Host "Consumer log: $consumerLog"
Write-Host "Consumer error log: $consumerErrorLog"
