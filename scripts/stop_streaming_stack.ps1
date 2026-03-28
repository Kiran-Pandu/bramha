$ErrorActionPreference = "Stop"

Write-Host "Stopping streaming stack..."
& (Join-Path $PSScriptRoot "stop_kafka_consumer.ps1")
& (Join-Path $PSScriptRoot "stop_kafka_stack.ps1")
Write-Host "Streaming stack stopped."
