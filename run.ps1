#!/usr/bin/env pwsh


param(
    [long]   $Count     = 50000000,
    [string] $Bootstrap = "localhost:29092",
    [string] $DataDir   = "data"
)

$JAR = "target\kafka-pipeline-v2-1.0.0.jar"

if (-not (Test-Path $JAR)) {
    Write-Host "Building..." -ForegroundColor Yellow
    mvn -B clean package -q -DskipTests
    if ($LASTEXITCODE -ne 0) { exit 1 }
}

$JVM = @(
    "-Xms256m", "-Xmx768m",
    "-XX:+UseG1GC",
    "-XX:MaxGCPauseMillis=200",
    "-XX:G1HeapRegionSize=16m",
    "-XX:+UseStringDeduplication",
    "-XX:+TieredCompilation",
    "-XX:ConcGCThreads=2",
    "-Dfile.encoding=UTF-8",
    "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"
)

$APP = @("--count", $Count, "--bootstrap", $Bootstrap, "--dataDir", $DataDir)
$CMD = @("java") + $JVM + @("-jar", $JAR) + $APP

Write-Host ""
Write-Host "=========================================="
Write-Host " Kafka Pipeline" 
Write-Host "==========================================" 
Write-Host "  count     : $("{0:N0}" -f $Count)"
Write-Host "  bootstrap : $Bootstrap"
Write-Host "  heap      : 768MB"
Write-Host ""

& $CMD[0] $CMD[1..($CMD.Length - 1)]
exit $LASTEXITCODE
