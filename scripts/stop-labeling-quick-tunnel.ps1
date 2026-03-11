$ErrorActionPreference = "SilentlyContinue"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runtimeDir = Join-Path $repoRoot "data\runtime"

function Stop-IfExists([string]$PidFile) {
    if (-not (Test-Path $PidFile)) { return }
    $pidText = (Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($pidText -and $pidText -match '^\d+$') {
        try { Stop-Process -Id ([int]$pidText) -Force -ErrorAction Stop } catch { }
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

Stop-IfExists (Join-Path $runtimeDir "quick_labeling_cloudflared.pid")

# Also stop any lingering quick cloudflared processes by command-line match.
Get-CimInstance Win32_Process |
    Where-Object {
        $_.Name -match "cloudflared" -and
        $_.CommandLine -match "tunnel\s+--url\s+http://(localhost|127\.0\.0\.1):8000"
    } |
    ForEach-Object {
        try { Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop } catch { }
    }

# Optional: stop uvicorn started by the quick launcher (only if PID file exists).
Stop-IfExists (Join-Path $runtimeDir "quick_labeling_uvicorn.pid")

Write-Output "Quick labeling tunnel stopped (and launcher-started uvicorn if present)."
