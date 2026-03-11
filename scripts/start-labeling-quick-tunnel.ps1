param(
    [string]$ApiHost = "127.0.0.1",
    [int]$ApiPort = 8000,
    [string]$StorageBackend = "postgres",
    [string]$PostgresDsn = "",
    [switch]$RestartApi
)

$ErrorActionPreference = "Stop"

function Get-DefaultPostgresDsn {
    $candidates = @()
    $candidates += $env:LABELING_POSTGRES_DSN
    $dsnFile = "$HOME\.labeling_postgres_dsn.txt"
    if (Test-Path $dsnFile) {
        $candidates += (Get-Content $dsnFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    }
    foreach ($c in $candidates) {
        if ($c -and $c.Trim()) { return $c.Trim() }
    }
    return ""
}

function Find-Cloudflared {
    $candidates = @(
        "C:\Program Files (x86)\cloudflared\cloudflared.exe",
        "C:\Program Files\cloudflared\cloudflared.exe"
    )
    foreach ($p in $candidates) {
        if (Test-Path -LiteralPath $p) { return $p }
    }
    try {
        return (Get-Command cloudflared -ErrorAction Stop).Source
    } catch {
        throw "cloudflared.exe not found. Install it first (winget install Cloudflare.cloudflared)."
    }
}

function Test-Api([string]$TargetHost, [int]$Port) {
    try {
        $resp = Invoke-WebRequest -Uri ("http://{0}:{1}/labeling/api/queue" -f $TargetHost, $Port) -UseBasicParsing -TimeoutSec 5
        if ($resp.StatusCode -eq 200) { return $true }
    } catch { }
    return $false
}

function Stop-UvicornOnPort([int]$Port) {
    $pids = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($procId in ($pids | Where-Object { $_ })) {
        try { Stop-Process -Id $procId -Force -ErrorAction Stop } catch { }
    }
}

function Start-Uvicorn([string]$TargetHost, [int]$Port, [string]$Backend, [string]$Dsn, [string]$RepoRoot) {
    $logDir = Join-Path $RepoRoot "data"
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $logOut = Join-Path $logDir "uvicorn_quick.log"
    $logErr = Join-Path $logDir "uvicorn_quick.err.log"

    if (-not $Dsn) { $Dsn = Get-DefaultPostgresDsn }
    if ($Backend -eq "postgres" -and -not $Dsn) {
        throw "Postgres backend requested but no DSN provided. Pass -PostgresDsn or set LABELING_POSTGRES_DSN."
    }

    $scriptLines = @()
    $scriptLines += ('$env:LABELING_STORAGE_BACKEND = ''{0}''' -f ($Backend.Replace("'", "''")))
    if ($Backend -eq "postgres") {
        $escapedDsn = $Dsn.Replace("'", "''")
        $scriptLines += ('$env:LABELING_POSTGRES_DSN = ''{0}''' -f $escapedDsn)
    }
    $scriptLines += ("& python -m uvicorn 'api.main:app' --host '{0}' --port {1}" -f $TargetHost, $Port)
    $psScript = ($scriptLines -join "; ")
    $debugScriptPath = Join-Path $RepoRoot "data\runtime\last_start_uvicorn_quick.ps1"
    New-Item -ItemType Directory -Force -Path (Split-Path $debugScriptPath) | Out-Null
    Set-Content -Path $debugScriptPath -Encoding ascii -Value $psScript
    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($psScript))

    $proc = Start-Process -FilePath powershell `
        -ArgumentList @("-NoProfile", "-EncodedCommand", $encoded) `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $logOut `
        -RedirectStandardError $logErr `
        -WindowStyle Hidden `
        -PassThru
    return $proc
}

function Stop-QuickCloudflared {
    $procs = Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -match "cloudflared" -and
            $_.CommandLine -match "tunnel\s+--url\s+http://(localhost|127\.0\.0\.1):8000"
        }
    foreach ($p in $procs) {
        try { Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop } catch { }
    }
}

function Start-QuickCloudflared([string]$CloudflaredPath, [string]$RepoRoot, [int]$Port) {
    $dataDir = Join-Path $RepoRoot "data"
    $runtimeDir = Join-Path $RepoRoot "data\runtime"
    New-Item -ItemType Directory -Force -Path $dataDir | Out-Null
    New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null
    $logPath = Join-Path $dataDir "cloudflared_quick.log"
    $quickConfigPath = Join-Path $runtimeDir "cloudflared_quick_empty.yml"
    if (-not (Test-Path $quickConfigPath)) {
        Set-Content -Path $quickConfigPath -Encoding ascii -Value "# quick-tunnel isolated config"
    }
    if (Test-Path $logPath) {
        try {
            Clear-Content -Path $logPath -ErrorAction Stop
        } catch {
            $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
            $archive = Join-Path $dataDir ("cloudflared_quick_{0}.log" -f $stamp)
            try { Move-Item -Path $logPath -Destination $archive -Force -ErrorAction Stop } catch { }
        }
    }

    $proc = Start-Process -FilePath $CloudflaredPath `
        -ArgumentList @("--config", $quickConfigPath, "tunnel", "--url", ("http://localhost:{0}" -f $Port), "--logfile", $logPath) `
        -WorkingDirectory $RepoRoot `
        -WindowStyle Hidden `
        -PassThru

    $url = $null
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 1
        if (-not (Test-Path $logPath)) { continue }
        $match = Get-Content $logPath -ErrorAction SilentlyContinue |
            Select-String -Pattern 'https://[-a-z0-9]+\.trycloudflare\.com' |
            Select-Object -First 1
        if ($match) {
            $url = $match.Matches[0].Value
            break
        }
    }

    return @{
        Process = $proc
        Url = $url
        Log = $logPath
    }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runtimeDir = Join-Path $repoRoot "data\runtime"
New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null

$cloudflared = Find-Cloudflared

if ($RestartApi) {
    Stop-UvicornOnPort -Port $ApiPort
    Start-Sleep -Seconds 2
}

$apiWasRunning = Test-Api -TargetHost $ApiHost -Port $ApiPort
$uvicornProc = $null
if (-not $apiWasRunning) {
    $uvicornProc = Start-Uvicorn -TargetHost $ApiHost -Port $ApiPort -Backend $StorageBackend -Dsn $PostgresDsn -RepoRoot $repoRoot
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 1
        if (Test-Api -TargetHost $ApiHost -Port $ApiPort) { break }
    }
}

if (-not (Test-Api -TargetHost $ApiHost -Port $ApiPort)) {
    throw "API is not reachable at http://$ApiHost`:$ApiPort. Check data\\uvicorn_quick.err.log."
}

Stop-QuickCloudflared
$tunnel = Start-QuickCloudflared -CloudflaredPath $cloudflared -RepoRoot $repoRoot -Port $ApiPort

if (-not $tunnel.Url) {
    throw "Quick tunnel URL not found in $($tunnel.Log)."
}

$urlFile = Join-Path $repoRoot "data\cloudflared_quick_url.txt"
Set-Content -Path $urlFile -Encoding ascii -Value ($tunnel.Url + "/labeling")

if ($uvicornProc) {
    Set-Content -Path (Join-Path $runtimeDir "quick_labeling_uvicorn.pid") -Encoding ascii -Value $uvicornProc.Id
}
Set-Content -Path (Join-Path $runtimeDir "quick_labeling_cloudflared.pid") -Encoding ascii -Value $tunnel.Process.Id

Write-Output ("Local API:   http://{0}:{1}/labeling" -f $ApiHost, $ApiPort)
Write-Output ("Remote URL:  {0}/labeling" -f $tunnel.Url)
Write-Output ("Saved URL:   {0}" -f $urlFile)
Write-Output ("Backend:     {0}" -f $StorageBackend)
if ($StorageBackend -eq "postgres") {
    Write-Output "Storage DB:  Postgres"
}
if ($uvicornProc) {
    Write-Output ("Uvicorn PID: {0}" -f $uvicornProc.Id)
} else {
    Write-Output "Uvicorn PID: (reused existing process)"
}
Write-Output ("Tunnel PID:  {0}" -f $tunnel.Process.Id)
Write-Output ""
Write-Output "Note: trycloudflare URL is temporary and changes when the tunnel restarts."
