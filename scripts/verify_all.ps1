<#
.SYNOPSIS
  One-command verification (BRD 12.3) for Windows PowerShell 5.1+ and Docker Desktop.

.DESCRIPTION
  1. Builds the Spark 3.5.9 / Java 17 image (with the Kafka connector) unless -SkipBuild.
  2. Runs every pytest suite inside that image (repo mounted READ-ONLY; tests write only to /tmp).
  3. Starts an ISOLATED compose project "stockbatch-verify" (own volumes, no published ports),
     runs the HDFS smoke and the Kafka smoke, then removes ONLY that project (down -v).
  The dev stack (project stock-batch-layer, volumes *_namenode_data/_datanode_data) is never touched.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File .\scripts\verify_all.ps1
  powershell -ExecutionPolicy Bypass -File .\scripts\verify_all.ps1 -SkipBuild -KeepStack
#>
param(
    [switch]$SkipBuild,
    [switch]$SkipTests,
    [switch]$SkipSmoke,
    [switch]$KeepStack,
    [string]$Image = 'stock-batch-layer-spark-job:latest'
)
$ErrorActionPreference = 'Stop'
$Repo = Split-Path -Parent $PSScriptRoot
$RepoMount = ($Repo -replace '\\', '/')
$Project = 'stockbatch-verify'
$Compose = Join-Path $Repo 'docker\verify\docker-compose.verify.yml'
$Stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$LogDir = Join-Path $Repo 'reports\runtime'
New-Item -ItemType Directory -Force $LogDir | Out-Null
$Log = Join-Path $LogDir "verify_all_$Stamp.log"
$Results = [ordered]@{}
$PyPath = '/opt/project/src:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip'

function Invoke-Native {
    # Windows PowerShell 5.1 turns native stderr into errors under 'Stop'; run with 'Continue'.
    param([string]$Exe, [string[]]$Arguments)
    $old = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $Exe @Arguments 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $Log -Append | Out-Host
        return $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $old
    }
}

function Invoke-Quiet {
    param([string]$Exe, [string[]]$Arguments)
    $old = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $out = & $Exe @Arguments 2>&1 | ForEach-Object { "$_" }
        return @{ Code = $LASTEXITCODE; Output = ($out -join "`n") }
    } finally {
        $ErrorActionPreference = $old
    }
}

function Set-Result([string]$Name, [bool]$Ok, [string]$Detail = '') {
    $Results[$Name] = @{ Status = $(if ($Ok) { 'PASS' } else { 'FAIL' }); Detail = $Detail }
    Write-Host ("[{0}] {1} {2}" -f $Results[$Name].Status, $Name, $Detail)
}

function Compose([string[]]$Arguments) {
    if ($Project -ne 'stockbatch-verify') { throw 'refusing to operate on a non-verification compose project' }
    return Invoke-Native 'docker' (@('compose', '-p', $Project, '-f', $Compose) + $Arguments)
}

function Wait-Until([string]$What, [scriptblock]$Probe, [int]$Tries = 60) {
    for ($i = 0; $i -lt $Tries; $i++) {
        if (& $Probe) { Write-Host "ready: $What"; return $true }
        Start-Sleep -Seconds 5
    }
    return $false
}

Write-Host "Log: $Log"
try {
    if (-not $SkipBuild) {
        $code = Invoke-Native 'docker' @('build', '-f', (Join-Path $Repo 'docker\images\Dockerfile'), '-t', $Image, $Repo)
        Set-Result 'build_image' ($code -eq 0) $Image
        if ($code -ne 0) { throw 'image build failed' }
    }

    if (-not $SkipTests) {
        $testArgs = @('run', '--rm', '--user', '185', '-e', 'HOME=/tmp', '-e', 'SPARK_LOCAL_IP=127.0.0.1',
                      '-e', 'PYTHONDONTWRITEBYTECODE=1', '-e', "PYTHONPATH=$PyPath",
                      '-v', "${RepoMount}:/opt/project:ro", '-w', '/opt/project', '--entrypoint', 'python3', $Image,
                      '-m', 'pytest', '-q', '-p', 'no:cacheprovider', 'tests', '--tb=short')
        $code = Invoke-Native 'docker' $testArgs
        $summary = (Select-String -Path $Log -Pattern '(\d+ passed.*)' | Select-Object -Last 1).Matches.Value
        Set-Result 'pytest_all_suites' ($code -eq 0) $summary
    }

    if (-not $SkipSmoke) {
        $env:SPARK_IMAGE = $Image
        # Idempotency: an interrupted earlier run may have skipped the `finally` teardown and left
        # containers/volumes (incl. Kafka topics with old messages) behind. Start from a clean stack.
        $leftover = Invoke-Quiet 'docker' @('ps', '-a', '--filter', "label=com.docker.compose.project=$Project",
                                            '--format', '{{.Names}}')
        $leftoverVolumes = Invoke-Quiet 'docker' @('volume', 'ls', '--filter', "label=com.docker.compose.project=$Project",
                                                   '--format', '{{.Name}}')
        $null = Compose @('down', '-v', '--remove-orphans')
        if ($leftover.Output.Trim() -or $leftoverVolumes.Output.Trim()) {
            $msg = "removed leftover isolated stack: containers=[$($leftover.Output.Trim() -replace "`n", ', ')] " +
                   "volumes=[$($leftoverVolumes.Output.Trim() -replace "`n", ', ')]"
            Write-Host $msg
            $msg | Out-File -FilePath $Log -Append   # same default encoding as Tee-Object (log is UTF-16 on PS 5.1)
        }
        $code = Compose @('up', '-d', 'namenode', 'datanode', 'kafka')
        Set-Result 'verify_stack_up' ($code -eq 0) $Project
        if ($code -ne 0) { throw 'isolated stack failed to start' }
        $hdfsReady = Wait-Until 'HDFS (1 live datanode, safe mode off)' {
            $r = Invoke-Quiet 'docker' @('compose', '-p', $Project, '-f', $Compose, 'exec', '-T', 'namenode',
                                         'hdfs', 'dfsadmin', '-report')
            ($r.Code -eq 0) -and ($r.Output -match 'Live datanodes \(1\)') -and
                ((Invoke-Quiet 'docker' @('compose', '-p', $Project, '-f', $Compose, 'exec', '-T', 'namenode',
                                          'hdfs', 'dfsadmin', '-safemode', 'get')).Output -match 'OFF')
        }
        $kafkaReady = Wait-Until 'Kafka broker' {
            (Invoke-Quiet 'docker' @('compose', '-p', $Project, '-f', $Compose, 'exec', '-T', 'kafka',
                                     '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092', '--list')).Code -eq 0
        }
        Set-Result 'verify_stack_ready' ($hdfsReady -and $kafkaReady) "hdfs=$hdfsReady kafka=$kafkaReady"
        if (-not ($hdfsReady -and $kafkaReady)) { throw 'isolated stack not ready' }
        foreach ($topic in @('stock-price-topic', 'news-topic')) {
            $null = Compose @('exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092',
                              '--create', '--if-not-exists', '--topic', $topic, '--partitions', '2', '--replication-factor', '1')
        }
        foreach ($smoke in @('smoke_hdfs', 'smoke_kafka')) {
            $code = Compose @('run', '--rm', 'spark', "/opt/project/scripts/smoke/$smoke.py")
            $line = (Select-String -Path $Log -Pattern ("{0}_SUMMARY=(.*)" -f $smoke.ToUpper()) | Select-Object -Last 1)
            $detail = ''
            if ($line) {
                $obj = $line.Matches[0].Groups[1].Value | ConvertFrom-Json
                $detail = "checks passed=$($obj.passed) failed=$($obj.failed)"
            }
            Set-Result $smoke ($code -eq 0) $detail
        }
    }
} catch {
    Set-Result 'verify_all_runner' $false $_.Exception.Message
} finally {
    if (-not $SkipSmoke -and -not $KeepStack) {
        $null = Compose @('down', '-v', '--remove-orphans')
        Write-Host "isolated project $Project removed (its own volumes only)"
    }
}

Write-Host ''
Write-Host '==================== verify_all summary ===================='
$failed = 0
foreach ($k in $Results.Keys) {
    Write-Host ("{0,-24} {1,-5} {2}" -f $k, $Results[$k].Status, $Results[$k].Detail)
    if ($Results[$k].Status -ne 'PASS') { $failed++ }
}
Write-Host "log: $Log"
if ($failed -gt 0) { exit 1 } else { exit 0 }
