# AegisEdge Verification Script
$ErrorActionPreference = "Stop"

Write-Host "==> Checking Go environment..." -ForegroundColor Cyan
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    if (Test-Path "C:\Program Files\Go\bin\go.exe") {
        $env:PATH = "C:\Program Files\Go\bin;" + $env:PATH
    } else {
        Write-Error "Go executable not found in PATH or 'C:\Program Files\Go\bin'."
    }
}
go version

Write-Host "`n==> Running gofmt..." -ForegroundColor Cyan
$unformatted = gofmt -l shared edge services
if ($unformatted) {
    Write-Warning "Unformatted files detected: $unformatted"
    gofmt -w shared edge services
} else {
    Write-Host "All Go files correctly formatted." -ForegroundColor Green
}

Write-Host "`n==> Running go vet across workspace modules..." -ForegroundColor Cyan
go vet ./shared/types/... ./edge/agent/... ./services/control-plane/...
Write-Host "go vet passed." -ForegroundColor Green

Write-Host "`n==> Running go test across all modules..." -ForegroundColor Cyan
go test -v ./shared/types/... ./edge/agent/... ./services/control-plane/...
Write-Host "All tests passed successfully!" -ForegroundColor Green
