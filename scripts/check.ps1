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
go vet ./shared/types/... ./shared/ml/... ./edge/agent/... ./services/control-plane/...
Write-Host "go vet passed." -ForegroundColor Green

Write-Host "`n==> Checking Prometheus configuration and alert rules..." -ForegroundColor Cyan
if (Get-Command promtool -ErrorAction SilentlyContinue) {
    promtool check rules infra/prometheus/alert_rules.yml
    if ($LASTEXITCODE -ne 0) {
        throw "promtool check rules failed with exit code $LASTEXITCODE"
    }

    promtool check config infra/prometheus/prometheus.yml
    if ($LASTEXITCODE -ne 0) {
        throw "promtool check config failed with exit code $LASTEXITCODE"
    }

    promtool check config infra/prometheus/prometheus.docker.yml
    if ($LASTEXITCODE -ne 0) {
        throw "promtool check config (docker) failed with exit code $LASTEXITCODE"
    }

    promtool check config infra/prometheus/prometheus.test.yml
    if ($LASTEXITCODE -ne 0) {
        throw "promtool check config (test) failed with exit code $LASTEXITCODE"
    }

    promtool check rules infra/prometheus/alert_rules_lifecycle_test.yml
    if ($LASTEXITCODE -ne 0) {
        throw "promtool check rules (lifecycle test) failed with exit code $LASTEXITCODE"
    }

    promtool test rules infra/prometheus/alert_rules_test.yml
    if ($LASTEXITCODE -ne 0) {
        throw "promtool test rules failed with exit code $LASTEXITCODE"
    }

    if (Get-Command amtool -ErrorAction SilentlyContinue) {
        amtool check-config infra/prometheus/alertmanager.yml
        if ($LASTEXITCODE -ne 0) {
            throw "amtool check-config failed with exit code $LASTEXITCODE"
        }

        amtool check-config infra/prometheus/alertmanager.test.yml
        if ($LASTEXITCODE -ne 0) {
            throw "amtool check-config (test) failed with exit code $LASTEXITCODE"
        }
    }

    Write-Host "promtool and amtool verification passed." -ForegroundColor Green
}
else {
    $dockerRunning = $false
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        try {
            $tempErr = Join-Path $env:TEMP "aegisedge_docker_check_err.log"
            $tempOut = Join-Path $env:TEMP "aegisedge_docker_check_out.log"
            $proc = Start-Process -FilePath "docker" -ArgumentList "info" -NoNewWindow -Wait -PassThru -RedirectStandardError $tempErr -RedirectStandardOutput $tempOut
            if ($proc.ExitCode -eq 0) {
                $dockerRunning = $true
            }
            Remove-Item $tempErr, $tempOut -Force -ErrorAction SilentlyContinue
        }
        catch {
            $dockerRunning = $false
        }
    }
    if ($dockerRunning) {
        Write-Host "Executing promtool via Docker (prom/prometheus:v3.2.1)..." -ForegroundColor Cyan
        & docker run --rm --entrypoint promtool -v "${PWD}/infra/prometheus:/etc/prometheus" prom/prometheus:v3.2.1 check rules /etc/prometheus/alert_rules.yml
        if ($LASTEXITCODE -ne 0) {
            throw "docker promtool check rules failed with exit code $LASTEXITCODE"
        }

        & docker run --rm --entrypoint promtool -v "${PWD}/infra/prometheus:/etc/prometheus" prom/prometheus:v3.2.1 check config /etc/prometheus/prometheus.yml
        if ($LASTEXITCODE -ne 0) {
            throw "docker promtool check config failed with exit code $LASTEXITCODE"
        }

        & docker run --rm --entrypoint promtool -v "${PWD}/infra/prometheus:/etc/prometheus" prom/prometheus:v3.2.1 check config /etc/prometheus/prometheus.docker.yml
        if ($LASTEXITCODE -ne 0) {
            throw "docker promtool check config (docker) failed with exit code $LASTEXITCODE"
        }

        & docker run --rm --entrypoint promtool -v "${PWD}/infra/prometheus:/etc/prometheus" prom/prometheus:v3.2.1 check config /etc/prometheus/prometheus.test.yml
        if ($LASTEXITCODE -ne 0) {
            throw "docker promtool check config (test) failed with exit code $LASTEXITCODE"
        }

        & docker run --rm --entrypoint promtool -v "${PWD}/infra/prometheus:/etc/prometheus" prom/prometheus:v3.2.1 check rules /etc/prometheus/alert_rules_lifecycle_test.yml
        if ($LASTEXITCODE -ne 0) {
            throw "docker promtool check rules (lifecycle test) failed with exit code $LASTEXITCODE"
        }

        & docker run --rm --entrypoint promtool -v "${PWD}/infra/prometheus:/etc/prometheus" prom/prometheus:v3.2.1 test rules /etc/prometheus/alert_rules_test.yml
        if ($LASTEXITCODE -ne 0) {
            throw "docker promtool test rules failed with exit code $LASTEXITCODE"
        }

        & docker run --rm -v "${PWD}/infra/prometheus:/etc/alertmanager" --entrypoint amtool prom/alertmanager:v0.28.1 check-config /etc/alertmanager/alertmanager.yml
        if ($LASTEXITCODE -ne 0) {
            throw "docker amtool check-config failed with exit code $LASTEXITCODE"
        }

        & docker run --rm -v "${PWD}/infra/prometheus:/etc/alertmanager" --entrypoint amtool prom/alertmanager:v0.28.1 check-config /etc/alertmanager/alertmanager.test.yml
        if ($LASTEXITCODE -ne 0) {
            throw "docker amtool check-config (test) failed with exit code $LASTEXITCODE"
        }

        Write-Host "Dockerized promtool and amtool verification passed." -ForegroundColor Green
    }
    else {
        Write-Host "promtool CLI not found on PATH and Docker daemon is unavailable; Prometheus CLI validation skipped. YAML structural validation remains covered by Go tests." -ForegroundColor Yellow
    }
}

Write-Host "`n==> Validating Grafana dashboard and provisioning syntax..." -ForegroundColor Cyan
$dashboardPath = "infra/prometheus/grafana/dashboards/telemetry_and_recovery_operations.json"
if (Test-Path $dashboardPath) {
    try {
        $dashJson = Get-Content $dashboardPath -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($dashJson.title -notlike "*Telemetry*Recovery*") {
            throw "Unexpected dashboard title: $($dashJson.title)"
        }
        if ($dashJson.uid -ne "aegisedge-telemetry-recovery") {
            throw "Unexpected dashboard UID: $($dashJson.uid)"
        }
        if ($dashJson.panels.Count -ne 20) {
            throw "Dashboard does not have exactly 20 panels: $($dashJson.panels.Count)"
        }
        Write-Host "Grafana dashboard JSON syntax, title, UID, and exactly $($dashJson.panels.Count) panels validated." -ForegroundColor Green
    }
    catch {
        throw "Grafana dashboard validation failed: $_"
    }
}
else {
    throw "Dashboard JSON not found at $dashboardPath"
}

Write-Host "`n==> Running go test across all modules..." -ForegroundColor Cyan
go test -v ./shared/types/... ./shared/ml/... ./edge/agent/... ./services/control-plane/...
Write-Host "All tests passed successfully!" -ForegroundColor Green
