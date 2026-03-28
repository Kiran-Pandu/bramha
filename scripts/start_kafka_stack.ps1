$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$toolsDir = Join-Path $repoRoot "tools"
$runtimeDir = Join-Path $repoRoot "runtime"
$kafkaRuntimeDir = Join-Path $runtimeDir "kafka"
$dataDir = Join-Path $kafkaRuntimeDir "data"
$logsDir = Join-Path $kafkaRuntimeDir "logs"
$brokerLog = Join-Path $logsDir "broker.log"
$brokerErrorLog = Join-Path $logsDir "broker-error.log"
$brokerPidFile = Join-Path $kafkaRuntimeDir "broker.pid"
$clusterIdFile = Join-Path $kafkaRuntimeDir "cluster.id"
$configPath = Join-Path $kafkaRuntimeDir "kraft-server.properties"
$kafkaLogDirProperty = $logsDir -replace "\\", "/"

$javaHome = Join-Path $toolsDir "openjdk17\jdk-17.0.18+8"
$javaBin = Join-Path $javaHome "bin"
$javaExe = Join-Path $javaBin "java.exe"
$kafkaHome = Join-Path $toolsDir "kafka_2.13-3.9.1"
$kafkaLibs = Join-Path $kafkaHome "libs\*"
$log4jConfig = Join-Path $kafkaHome "config\log4j.properties"

if (-not (Test-Path $javaHome)) {
    throw "Portable Java not found at $javaHome. Download or extract it into tools\\openjdk17 first."
}

if (-not (Test-Path $kafkaHome)) {
    throw "Kafka not found at $kafkaHome. Download or extract it into tools\\kafka_2.13-3.9.1 first."
}

if (-not (Test-Path $javaExe)) {
    throw "Java executable not found at $javaExe."
}

New-Item -ItemType Directory -Force -Path $runtimeDir, $kafkaRuntimeDir, $dataDir, $logsDir | Out-Null

$env:JAVA_HOME = $javaHome
$env:Path = "$javaBin;$env:Path"

$config = @"
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=$($dataDir -replace '\\','/')
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
"@
Set-Content -Path $configPath -Value $config -Encoding ASCII

$metadataPath = Join-Path $dataDir "meta.properties"
if (-not (Test-Path $metadataPath)) {
    Write-Host "Formatting Kafka storage..."
    if (-not (Test-Path $clusterIdFile)) {
        $clusterId = (& $javaExe -cp $kafkaLibs kafka.tools.StorageTool random-uuid).Trim()
        Set-Content -Path $clusterIdFile -Value $clusterId -Encoding ASCII
    } else {
        $clusterId = (Get-Content $clusterIdFile | Select-Object -First 1).Trim()
    }

    & $javaExe -cp $kafkaLibs kafka.tools.StorageTool format -t $clusterId -c $configPath | Out-Null
}

if (Test-Path $brokerPidFile) {
    $existingPid = (Get-Content $brokerPidFile | Select-Object -First 1).Trim()
    if ($existingPid) {
        $existingProcess = Get-Process -Id $existingPid -ErrorAction SilentlyContinue
        if ($existingProcess) {
            Write-Host "Kafka broker is already running with PID $existingPid"
            Write-Host "Broker: localhost:9092"
            Write-Host "Log file: $brokerLog"
            return
        }
    }
    Remove-Item -LiteralPath $brokerPidFile -Force
}

Write-Host "Starting local Kafka broker..."
$process = Start-Process `
    -FilePath $javaExe `
    -ArgumentList "-cp", $kafkaLibs, "-Dlog4j.configuration=file:$log4jConfig", "-Dkafka.logs.dir=$kafkaLogDirProperty", "-Xms512M", "-Xmx512M", "kafka.Kafka", $configPath `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $brokerLog `
    -RedirectStandardError $brokerErrorLog `
    -PassThru

Set-Content -Path $brokerPidFile -Value $process.Id -Encoding ASCII

Start-Sleep -Seconds 8

$runningProcess = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
if (-not $runningProcess) {
    $logTail = @()
    if (Test-Path $brokerLog) {
        $logTail += Get-Content $brokerLog -Tail 30
    }
    if (Test-Path $brokerErrorLog) {
        $logTail += Get-Content $brokerErrorLog -Tail 30
    }
    if (-not $logTail) {
        $logTail = @("No broker log found.")
    }
    throw "Kafka broker failed to start.`n$($logTail -join [Environment]::NewLine)"
}

$portReady = (Test-NetConnection -ComputerName localhost -Port 9092 -WarningAction SilentlyContinue).TcpTestSucceeded
if (-not $portReady) {
    Write-Host "Kafka process is running, but port 9092 is not ready yet. Check $brokerLog if startup is still in progress."
} else {
Write-Host "Kafka broker is ready on localhost:9092"
}

Write-Host "Broker PID: $($process.Id)"
Write-Host "Broker log: $brokerLog"
Write-Host "Broker error log: $brokerErrorLog"
