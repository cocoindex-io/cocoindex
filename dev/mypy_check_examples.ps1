$ErrorActionPreference = 'Stop'

# Resolve python in local venv
$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $python)) {
  $python = 'python'
}

# Ensure mypy can resolve local cocoindex package sources
$env:MYPYPATH = Join-Path $repoRoot 'python'

# Collect example entry files
$examples = Join-Path $repoRoot 'examples'
$files = Get-ChildItem -Path $examples -Recurse -File |
  Where-Object { $_.Name -in @('main.py','colpali_main.py') } |
  Sort-Object FullName

$failed = @()
foreach ($f in $files) {
  Write-Host (">>> Checking " + $f.FullName)
  & $python -m mypy --ignore-missing-imports --follow-imports=silent $f.FullName
  if ($LASTEXITCODE -ne 0) {
    $failed += $f.FullName
  }
}

if ($failed.Count -gt 0) {
  Write-Host "\nFailures:"
  $failed | ForEach-Object { Write-Host $_ }
  exit 1
} else {
  Write-Host "\nAll example entry files passed mypy."
}
