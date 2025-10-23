# Copy MLrun-generated PNGs to a target folder and open it in Explorer
param(
    [string]$runId = "5a809261076a47de9f9056db61d9f5b4",
    [string]$mlrunsRoot = "$PSScriptRoot\..\mlruns",
    [string]$outDir = "$PSScriptRoot\..\downloaded_artifacts"
)

$mlrunsRoot = (Resolve-Path -Path $mlrunsRoot).Path
$outDir = (Resolve-Path -Path $outDir -ErrorAction SilentlyContinue).Path
if (-not $outDir) {
    New-Item -ItemType Directory -Path "$PSScriptRoot\..\downloaded_artifacts" | Out-Null
    $outDir = (Resolve-Path -Path "$PSScriptRoot\..\downloaded_artifacts").Path
}
Write-Host "MLruns root: $mlrunsRoot"

# Find run folder
$runFolder = Get-ChildItem -Path $mlrunsRoot -Recurse -Directory | Where-Object { $_.Name -eq $runId } | Select-Object -First 1
if (-not $runFolder) {
    Write-Error "Run folder for ID $runId not found under $mlrunsRoot"
    exit 1
}
Write-Host "Found run folder: $($runFolder.FullName)"

# Search for png files under the run folder or associated models folder
$pngs = Get-ChildItem -Path $runFolder.FullName -Recurse -Include *.png -File -ErrorAction SilentlyContinue
if (-not $pngs) {
    # Also look into models/*/artifacts
    $modelPngs = Get-ChildItem -Path $mlrunsRoot -Recurse -Include *.png -File | Where-Object { $_.FullName -like "*$runId*" }
    if ($modelPngs) { $pngs = $modelPngs }
}

if (-not $pngs) {
    Write-Host "No PNG files found for run $runId. These may not have been generated or were saved to a different outputs folder."
    Write-Host "Check the project 'outputs' directory or rerun the training script to (re)generate PNGs."
    exit 0
}

foreach ($f in $pngs) {
    Copy-Item -Path $f.FullName -Destination $outDir -Force
}

Write-Host "Copied $($pngs.Count) PNG(s) to $outDir"
Start-Process explorer.exe $outDir
