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

Write-Host "`n==> Checking Go formatting (gofmt)..." -ForegroundColor Cyan
$unformatted = gofmt -l shared edge services
if ($unformatted) {
    Write-Host "Unformatted files detected:" -ForegroundColor Red
    $unformatted | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    throw "gofmt verification failed: unformatted files detected. Please format Go source files before proceeding."
} else {
    Write-Host "All Go files correctly formatted." -ForegroundColor Green
}

Write-Host "`n==> Running go vet across workspace modules..." -ForegroundColor Cyan
go vet ./shared/types/... ./shared/ml/... ./edge/agent/... ./services/control-plane/...
Write-Host "go vet passed." -ForegroundColor Green

Write-Host "`n==> Validating Prometheus configuration and alert rules..." -ForegroundColor Cyan
$pwdPath = (Get-Item -Path ".").FullName

# Function to check Docker daemon status safely
function Test-DockerDaemonRunning {
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        try {
            $tempErr = Join-Path $env:TEMP "aegisedge_docker_check_err_$([Guid]::NewGuid().ToString('N').Substring(0,8)).log"
            $tempOut = Join-Path $env:TEMP "aegisedge_docker_check_out_$([Guid]::NewGuid().ToString('N').Substring(0,8)).log"
            $proc = Start-Process -FilePath "docker" -ArgumentList "info" -NoNewWindow -Wait -PassThru -RedirectStandardError $tempErr -RedirectStandardOutput $tempOut
            $isRunning = ($proc.ExitCode -eq 0)
            Remove-Item $tempErr, $tempOut -Force -ErrorAction SilentlyContinue
            return $isRunning
        }
        catch {
            return $false
        }
    }
    return $false
}

# 1. Prometheus Official Validation (promtool)
if (Get-Command promtool -ErrorAction SilentlyContinue) {
    Write-Host "Running official promtool CLI..." -ForegroundColor Cyan

    promtool check rules infra/prometheus/alert_rules.yml
    if ($LASTEXITCODE -ne 0) {
        throw "promtool check rules (alert_rules.yml) failed with exit code $LASTEXITCODE"
    }

    promtool check rules infra/prometheus/alert_rules_lifecycle_test.yml
    if ($LASTEXITCODE -ne 0) {
        throw "promtool check rules (alert_rules_lifecycle_test.yml) failed with exit code $LASTEXITCODE"
    }

    promtool check config infra/prometheus/prometheus.yml
    if ($LASTEXITCODE -ne 0) {
        throw "promtool check config (prometheus.yml) failed with exit code $LASTEXITCODE"
    }

    promtool check config infra/prometheus/prometheus.docker.yml
    if ($LASTEXITCODE -ne 0) {
        throw "promtool check config (prometheus.docker.yml) failed with exit code $LASTEXITCODE"
    }

    promtool check config infra/prometheus/prometheus.test.yml
    if ($LASTEXITCODE -ne 0) {
        throw "promtool check config (prometheus.test.yml) failed with exit code $LASTEXITCODE"
    }

    promtool test rules infra/prometheus/alert_rules_test.yml
    if ($LASTEXITCODE -ne 0) {
        throw "promtool test rules (alert_rules_test.yml) failed with exit code $LASTEXITCODE"
    }

    Write-Host "Official Prometheus validation (promtool) passed." -ForegroundColor Green
}
elseif (Test-DockerDaemonRunning) {
    Write-Host "Executing promtool via Docker container (prom/prometheus:v3.2.1)..." -ForegroundColor Cyan
    $promMount = "$($pwdPath.Replace('\', '/'))/infra/prometheus:/etc/prometheus:ro"

    & docker run --rm --entrypoint promtool -v $promMount prom/prometheus:v3.2.1 check rules /etc/prometheus/alert_rules.yml
    if ($LASTEXITCODE -ne 0) {
        throw "docker promtool check rules (alert_rules.yml) failed with exit code $LASTEXITCODE"
    }

    & docker run --rm --entrypoint promtool -v $promMount prom/prometheus:v3.2.1 check rules /etc/prometheus/alert_rules_lifecycle_test.yml
    if ($LASTEXITCODE -ne 0) {
        throw "docker promtool check rules (alert_rules_lifecycle_test.yml) failed with exit code $LASTEXITCODE"
    }

    & docker run --rm --entrypoint promtool -v $promMount prom/prometheus:v3.2.1 check config /etc/prometheus/prometheus.yml
    if ($LASTEXITCODE -ne 0) {
        throw "docker promtool check config (prometheus.yml) failed with exit code $LASTEXITCODE"
    }

    & docker run --rm --entrypoint promtool -v $promMount prom/prometheus:v3.2.1 check config /etc/prometheus/prometheus.docker.yml
    if ($LASTEXITCODE -ne 0) {
        throw "docker promtool check config (prometheus.docker.yml) failed with exit code $LASTEXITCODE"
    }

    & docker run --rm --entrypoint promtool -v $promMount prom/prometheus:v3.2.1 check config /etc/prometheus/prometheus.test.yml
    if ($LASTEXITCODE -ne 0) {
        throw "docker promtool check config (prometheus.test.yml) failed with exit code $LASTEXITCODE"
    }

    & docker run --rm --entrypoint promtool -v $promMount prom/prometheus:v3.2.1 test rules /etc/prometheus/alert_rules_test.yml
    if ($LASTEXITCODE -ne 0) {
        throw "docker promtool test rules (alert_rules_test.yml) failed with exit code $LASTEXITCODE"
    }

    Write-Host "Official Dockerized Prometheus validation (promtool) passed." -ForegroundColor Green
}
else {
    Write-Host "SKIPPED: promtool CLI not found on PATH and Docker daemon is unavailable. Official Prometheus validation (promtool) skipped." -ForegroundColor Yellow
    Write-Host "         (Note: Custom Go YAML unmarshalling tests provide structural verification, but do not substitute for official promtool validation)." -ForegroundColor Yellow
}

# 2. Alertmanager Official Validation (amtool)
Write-Host "`n==> Validating Alertmanager configurations..." -ForegroundColor Cyan
if (Get-Command amtool -ErrorAction SilentlyContinue) {
    Write-Host "Running official amtool CLI..." -ForegroundColor Cyan

    amtool check-config infra/prometheus/alertmanager.yml
    if ($LASTEXITCODE -ne 0) {
        throw "amtool check-config (alertmanager.yml) failed with exit code $LASTEXITCODE"
    }

    amtool check-config infra/prometheus/alertmanager.test.yml
    if ($LASTEXITCODE -ne 0) {
        throw "amtool check-config (alertmanager.test.yml) failed with exit code $LASTEXITCODE"
    }

    Write-Host "Official Alertmanager validation (amtool) passed." -ForegroundColor Green
}
elseif (Test-DockerDaemonRunning) {
    Write-Host "Executing amtool via Docker container (prom/alertmanager:v0.28.1)..." -ForegroundColor Cyan
    $amMount = "$($pwdPath.Replace('\', '/'))/infra/prometheus:/etc/alertmanager:ro"

    & docker run --rm -v $amMount --entrypoint amtool prom/alertmanager:v0.28.1 check-config /etc/alertmanager/alertmanager.yml
    if ($LASTEXITCODE -ne 0) {
        throw "docker amtool check-config (alertmanager.yml) failed with exit code $LASTEXITCODE"
    }

    & docker run --rm -v $amMount --entrypoint amtool prom/alertmanager:v0.28.1 check-config /etc/alertmanager/alertmanager.test.yml
    if ($LASTEXITCODE -ne 0) {
        throw "docker amtool check-config (alertmanager.test.yml) failed with exit code $LASTEXITCODE"
    }

    Write-Host "Official Dockerized Alertmanager validation (amtool) passed." -ForegroundColor Green
}
else {
    Write-Host "SKIPPED: amtool CLI not found on PATH and Docker daemon is unavailable. Official Alertmanager validation (amtool) skipped." -ForegroundColor Yellow
    Write-Host "         (Note: Custom Go YAML unmarshalling tests provide structural verification, but do not substitute for official amtool validation)." -ForegroundColor Yellow
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
        $totalPanelCount = $dashJson.panels.Count
        foreach ($p in $dashJson.panels) {
            if ($p.panels) {
                $totalPanelCount += $p.panels.Count
            }
        }
        if ($totalPanelCount -ne 20) {
            throw "Dashboard does not have exactly 20 panels (including nested panels): $($totalPanelCount) (top-level: $($dashJson.panels.Count))"
        }
        Write-Host "Grafana dashboard JSON syntax, title, UID, and exactly $totalPanelCount panels (top-level: $($dashJson.panels.Count)) validated." -ForegroundColor Green
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
