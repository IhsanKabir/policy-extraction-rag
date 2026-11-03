# Start a blind labelling session on its own database (2026-10-29).
#   -Session b        second annotator (IAA): 50 records -> data/labeling_iaa_b.db
#   -Session relabel  annotator A's blind self-relabel: 27 records -> data/labeling_relabel_a.db
# Then open http://127.0.0.1:8000/labeling
# MANUAL_GOLD_PATH points at a file that does not exist, so an empty database is
# never pre-filled from an existing gold file.
param(
    [Parameter(Mandatory = $true)][ValidateSet("b", "relabel")][string]$Session,
    [int]$Port = 8000
)

$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

if ($Session -eq "b") {
    $env:LABELING_QUEUE_PATH = "data/queue_iaa_b.jsonl"
    $env:LABELING_DB_PATH = "data/labeling_iaa_b.db"
} else {
    $env:LABELING_QUEUE_PATH = "data/queue_relabel_a.jsonl"
    $env:LABELING_DB_PATH = "data/labeling_relabel_a.db"
}
$env:MANUAL_GOLD_PATH = "data/NO_GOLD_IMPORT.jsonl"
$env:LABELING_STORAGE_BACKEND = "sqlite"

if (-not (Test-Path $env:LABELING_QUEUE_PATH)) {
    Write-Error "Queue $($env:LABELING_QUEUE_PATH) not found. Build it first (scripts/build_iaa_sample.py or build_relabel_sample.py)."
    exit 1
}
if (Test-Path $env:MANUAL_GOLD_PATH) {
    Write-Error "$($env:MANUAL_GOLD_PATH) exists; remove it so the blind database cannot be pre-filled."
    exit 1
}

Write-Host "Session '$Session': queue $($env:LABELING_QUEUE_PATH), saving to $($env:LABELING_DB_PATH)"
Write-Host "Open http://127.0.0.1:$Port/labeling"
python -m uvicorn api.main:app --port $Port
