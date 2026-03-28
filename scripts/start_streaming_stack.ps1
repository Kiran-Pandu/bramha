$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot

Write-Host "Starting streaming stack..."
& (Join-Path $PSScriptRoot "start_kafka_stack.ps1")
Start-Sleep -Seconds 2
& (Join-Path $PSScriptRoot "start_kafka_consumer.ps1")

Write-Host ""
Write-Host "Streaming stack is ready."
Write-Host "Broker: localhost:9092"
Write-Host "Consumer: running in background"
Write-Host "Live logs UI: http://127.0.0.1:8000"
Write-Host ""
Write-Host "To send test logs:"
Write-Host '$env:KAFKA_MAX_LINES="300"'
Write-Host '$env:KAFKA_PRODUCER_DELAY_MS="1"'
Write-Host '.\.venv\Scripts\python.exe scripts\kafka_linux_producer.py'
