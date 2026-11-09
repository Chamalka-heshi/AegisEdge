# AegisEdge End-to-End Alertmanager Lifecycle Integration Test
# Verifies: Prometheus -> Alertmanager -> Webhook Receiver complete lifecycle
# Requirements: Docker Desktop running, Compose network prometheus_default

param(
    [int]$TimeoutSeconds = 60
)

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "AegisEdge: End-to-End Alert Lifecycle Verification" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Check Docker daemon
Write-Host "`n==> 1. Checking Docker daemon..." -ForegroundColor Yellow
$dockerCheck = docker info 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker daemon is unavailable. This test requires Docker Desktop." -ForegroundColor Red
    exit 1
}
Write-Host "Docker daemon is active." -ForegroundColor Green

# 2. Build Linux webhook-receiver binary
Write-Host "`n==> 2. Building webhook-receiver Linux binary..." -ForegroundColor Yellow
if (-not (Test-Path "bin")) {
    New-Item -ItemType Directory -Path "bin" -Force | Out-Null
}
cmd.exe /c "set CGO_ENABLED=0&& set GOOS=linux&& set GOARCH=amd64&& go build -o bin/webhook-receiver ./services/control-plane/cmd/webhook-receiver"
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to compile webhook-receiver binary." -ForegroundColor Red
    exit 1
}
Write-Host "webhook-receiver binary ready." -ForegroundColor Green

# Unique execution run IDs for scenario isolation
$MainRunId = "run-" + [Guid]::NewGuid().ToString("N").Substring(0, 8)
$RestartRunId = "rst-" + [Guid]::NewGuid().ToString("N").Substring(0, 8)
Write-Host "Execution Run IDs: Main=$MainRunId, Restart=$RestartRunId" -ForegroundColor Cyan

# Resource tracking flags for cleanup
$receiverContainerStarted = $false
$composeOverrideStarted = $false
$testError = $null

function Invoke-CleanupResources {
    Write-Host "`n==> Teardown: Restoring production Compose stack & cleaning test resources..." -ForegroundColor Cyan
    $cleanupErrors = @()

    # 1. Stop and remove temporary webhook-receiver container
    try {
        cmd.exe /c "docker stop webhook-receiver >nul 2>&1"
        cmd.exe /c "docker rm -f webhook-receiver >nul 2>&1"
        # Verify container is genuinely gone
        $remaining = cmd.exe /c "docker ps -a --filter name=^/webhook-receiver$ --format {{.Names}}"
        if ($remaining -match "webhook-receiver") {
            $cleanupErrors += "Failed to remove temporary webhook-receiver container"
        } else {
            Write-Host "  Temporary webhook-receiver container removed." -ForegroundColor Green
        }
    } catch {
        $cleanupErrors += "Exception during webhook-receiver cleanup: $_"
    }

    # 2. Restore production compose deployment (removes test configuration and test rule mounts)
    try {
        docker compose -f infra/prometheus/docker-compose.yml up -d
        if ($LASTEXITCODE -ne 0) {
            $cleanupErrors += "docker compose up -d failed with exit code ${LASTEXITCODE}"
        } else {
            Write-Host "  Production Compose stack restored (test mounts removed)." -ForegroundColor Green
        }
    } catch {
        $cleanupErrors += "Exception during production Compose restoration: $_"
    }

    # 3. Verify production Prometheus state
    try {
        $promHealthy = $false
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        while ($sw.Elapsed.TotalSeconds -lt 30) {
            try {
                $readyResp = Invoke-WebRequest -Uri "http://127.0.0.1:9090/-/ready" -UseBasicParsing -TimeoutSec 2
                if ($readyResp.StatusCode -eq 200) {
                    $promHealthy = $true
                    break
                }
            } catch {
                Start-Sleep -Milliseconds 500
            }
        }

        if (-not $promHealthy) {
            $cleanupErrors += "Production Prometheus failed to become ready within 30s during teardown"
        } else {
            Write-Host "  Production Prometheus is healthy." -ForegroundColor Green

            # Check rule groups: must contain 0 test rule groups
            $rules = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/rules" -TimeoutSec 5
            $foundTestGroup = $false
            foreach ($g in $rules.data.groups) {
                if ($g.name -eq "aegisedge-lifecycle-test") {
                    $foundTestGroup = $true
                    $cleanupErrors += "Test rule group 'aegisedge-lifecycle-test' still present in production Prometheus"
                }
            }
            if (-not $foundTestGroup) {
                Write-Host "  Production Prometheus has 0 test rule groups (clean)." -ForegroundColor Green
            }

            # Check targets: must contain 0 test targets, and aegisedge-control-plane present
            $targets = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/targets" -TimeoutSec 5
            $foundTestTarget = $false
            $foundControlPlane = $false
            foreach ($t in $targets.data.activeTargets) {
                if ($t.scrapePool -eq "aegisedge-lifecycle-test" -or $t.labels.job -eq "aegisedge-lifecycle-test") {
                    $foundTestTarget = $true
                    $cleanupErrors += "Test scrape target 'aegisedge-lifecycle-test' still present in production Prometheus"
                }
                if ($t.scrapePool -eq "aegisedge-control-plane" -or $t.labels.job -eq "aegisedge-control-plane") {
                    $foundControlPlane = $true
                }
            }
            if (-not $foundTestTarget) {
                Write-Host "  Production Prometheus has 0 test scrape targets (clean)." -ForegroundColor Green
            }
            if ($foundControlPlane) {
                Write-Host "  Production scrape target 'aegisedge-control-plane' is verified active." -ForegroundColor Green
            }
        }
    } catch {
        $cleanupErrors += "Exception verifying production Prometheus state: $_"
    }

    if ($cleanupErrors.Count -eq 0) {
        Write-Host "  Production restoration fully verified (0 test rules, 0 test targets)." -ForegroundColor Green
    } else {
        foreach ($ce in $cleanupErrors) {
            Write-Host "  Cleanup Error: $ce" -ForegroundColor Red
        }
    }

    return $cleanupErrors
}

# Enter protected lifecycle covering all Docker resources
try {
    # 3. Start Compose stack with isolated test configuration override
    Write-Host "`n==> 3. Starting Prometheus & Alertmanager with isolated test configuration..." -ForegroundColor Yellow
    docker compose -f infra/prometheus/docker-compose.yml -f infra/prometheus/docker-compose.test.yml up -d
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to start docker-compose stack with test override."
    }
    $composeOverrideStarted = $true

    # Flush Alertmanager to clear any residual active alerts from prior aborted runs
    docker restart aegisedge-alertmanager | Out-Null

    # 4. Start temporary webhook-receiver container on Compose network
    Write-Host "`n==> 4. Starting temporary webhook-receiver container on prometheus_default..." -ForegroundColor Yellow
    cmd.exe /c "docker rm -f webhook-receiver >nul 2>&1"
    $pwdPath = (Get-Item -Path ".").FullName
    docker run -d --name webhook-receiver `
        --network prometheus_default `
        -p 127.0.0.1:9095:9095 `
        -v "${pwdPath}\bin\webhook-receiver:/webhook-receiver:ro" `
        --entrypoint /webhook-receiver `
        prom/alertmanager:v0.28.1

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to launch webhook-receiver container."
    }
    $receiverContainerStarted = $true

    # 5. Wait for readiness
    Write-Host "`n==> 5. Waiting for services readiness..." -ForegroundColor Yellow
    $ready = $false
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    while ($stopwatch.Elapsed.TotalSeconds -lt 30) {
        try {
            $promResp = Invoke-WebRequest -Uri "http://127.0.0.1:9090/-/ready" -UseBasicParsing -TimeoutSec 2
            $amResp = Invoke-WebRequest -Uri "http://127.0.0.1:9093/-/ready" -UseBasicParsing -TimeoutSec 2
            $recvResp = Invoke-WebRequest -Uri "http://127.0.0.1:9095/health" -UseBasicParsing -TimeoutSec 2
            if ($promResp.StatusCode -eq 200 -and $amResp.StatusCode -eq 200 -and $recvResp.StatusCode -eq 200) {
                $ready = $true
                break
            }
        } catch {
            Start-Sleep -Milliseconds 500
        }
    }
    if (-not $ready) {
        throw "Services failed to become ready within 30s."
    }
    Write-Host "Prometheus, Alertmanager, and Webhook Receiver are healthy and ready." -ForegroundColor Green

    # 6. Verify Prometheus active Alertmanager discovery
    Write-Host "`n==> 6. Verifying Prometheus active Alertmanager discovery..." -ForegroundColor Yellow
    $amDiscovered = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt 20) {
        try {
            $amResp = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/alertmanagers"
            foreach ($am in $amResp.data.activeAlertmanagers) {
                if ($am.url -like "*9093*") {
                    $amDiscovered = $true
                    Write-Host "  Active Alertmanager URL: $($am.url)" -ForegroundColor Green
                    break
                }
            }
            if ($amDiscovered) { break }
        } catch {}
        Start-Sleep -Milliseconds 500
    }
    if (-not $amDiscovered) {
        throw "Prometheus failed to discover active Alertmanager target."
    }

    # 7. Verify Prometheus test scrape target
    Write-Host "`n==> 7. Verifying test scrape target in isolated test config..." -ForegroundColor Yellow
    $scrapeOk = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt 20) {
        try {
            $targets = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/targets"
            foreach ($t in $targets.data.activeTargets) {
                if (($t.labels.job -eq "aegisedge-lifecycle-test" -or $t.scrapePool -eq "aegisedge-lifecycle-test") -and $t.health -eq "up") {
                    $scrapeOk = $true
                    break
                }
            }
            if ($scrapeOk) { break }
        } catch {}
        Start-Sleep -Milliseconds 500
    }
    if (-not $scrapeOk) {
        throw "Scrape target 'aegisedge-lifecycle-test' failed to become 'up'."
    }
    Write-Host "Scrape target 'aegisedge-lifecycle-test' is UP." -ForegroundColor Green

    # =========================================================================
    # Scenario A: Warning Alert Lifecycle (Firing -> Resolved + Strict Correlation)
    # =========================================================================
    Write-Host "`n==> 8. Testing Warning Alert Lifecycle (Firing -> Resolved)..." -ForegroundColor Yellow
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events" -Method Delete | Out-Null
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/trigger?val=1&run_id=$MainRunId" -Method Post | Out-Null

    Write-Host "  Waiting for Prometheus rule AegisEdgeE2ELifecycleWarning to fire..." -ForegroundColor Cyan
    $ruleFiring = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $rulesResp = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/rules"
            foreach ($group in $rulesResp.data.groups) {
                if ($group.name -eq "aegisedge-lifecycle-test") {
                    foreach ($rule in $group.rules) {
                        if ($rule.name -eq "AegisEdgeE2ELifecycleWarning" -and $rule.state -eq "firing") {
                            $ruleFiring = $true
                            break
                        }
                    }
                }
            }
            if ($ruleFiring) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $ruleFiring) {
        throw "Alert AegisEdgeE2ELifecycleWarning failed to fire in Prometheus."
    }
    Write-Host "  Prometheus rule transitioned to FIRING." -ForegroundColor Green

    Write-Host "  Waiting for routed webhook delivery on /alerts/warning..." -ForegroundColor Cyan
    $warnFiringEvent = $null
    $firingFingerprint = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
            foreach ($ev in $events) {
                if ($ev.route -eq "warning" -and $ev.payload.status -eq "firing" -and $ev.payload.receiver -eq "warning-receiver") {
                    foreach ($alert in $ev.payload.alerts) {
                        if ($alert.status -eq "firing" -and
                            $alert.labels.alertname -eq "AegisEdgeE2ELifecycleWarning" -and
                            $alert.labels.severity -eq "warning" -and
                            $alert.labels.tier -eq "integration-test" -and
                            $alert.labels.test_run_id -eq $MainRunId) {
                            $warnFiringEvent = $ev
                            $firingFingerprint = $alert.fingerprint
                            break
                        }
                    }
                }
                if ($warnFiringEvent) { break }
            }
            if ($warnFiringEvent) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $warnFiringEvent) {
        $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
        Write-Host "  Events recorded in receiver at timeout ($($events.Count)):" -ForegroundColor Red
        $events | ForEach-Object { Write-Host "    - route: $($_.route), receiver: $($_.payload.receiver), status: $($_.payload.status), alerts: $($_.payload.alerts.Count)" -ForegroundColor Red }
        throw "Webhook receiver did not receive firing warning alert payload on /alerts/warning for run_id $MainRunId."
    }

    # Strict Assertions on Firing Event
    $fAlert = $null
    foreach ($a in $warnFiringEvent.payload.alerts) {
        if ($a.fingerprint -eq $firingFingerprint) {
            $fAlert = $a
            break
        }
    }
    if ($null -eq $fAlert) {
        throw "Could not find firing alert with fingerprint $firingFingerprint in payload."
    }
    if ($fAlert.annotations.summary -ne "E2E lifecycle test warning alert") {
        throw "Expected summary annotation 'E2E lifecycle test warning alert', got '$($fAlert.annotations.summary)'"
    }
    if ($fAlert.annotations.description -ne "Integration test alert for Prometheus -> Alertmanager -> webhook delivery") {
        throw "Expected description annotation 'Integration test alert for Prometheus -> Alertmanager -> webhook delivery', got '$($fAlert.annotations.description)'"
    }
    $fStartsAt = [DateTime]::Parse($fAlert.startsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
    if ($fStartsAt.Year -lt 2024) {
        throw "Invalid startsAt timestamp on firing alert: $($fAlert.startsAt)"
    }
    if ([string]::IsNullOrWhiteSpace($firingFingerprint)) {
        throw "Firing alert has empty fingerprint."
    }

    Write-Host "  Firing notification verified (RunID: $MainRunId, Fingerprint: $firingFingerprint, Receiver: $($warnFiringEvent.payload.receiver))." -ForegroundColor Green

    # Trigger Warning Alert Resolution
    Write-Host "  Triggering Warning Alert Resolution (val=0, run_id=$MainRunId)..." -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/trigger?val=0&run_id=$MainRunId" -Method Post | Out-Null

    Write-Host "  Waiting for resolved webhook delivery correlated with firing fingerprint $firingFingerprint..." -ForegroundColor Cyan
    $warnResolvedEvent = $null
    $rAlert = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
            foreach ($ev in $events) {
                # Strict Event Selection (Audit Finding 1):
                # Validate route, receiver, message-level status during selection
                if ($ev.route -eq "warning" -and
                    $ev.payload.status -eq "resolved" -and
                    $ev.payload.receiver -eq "warning-receiver") {
                    
                    # Search specifically for the exact firing fingerprint in this event's alerts
                    foreach ($candAlert in $ev.payload.alerts) {
                        if ($candAlert.fingerprint -eq $firingFingerprint -and
                            $candAlert.status -eq "resolved" -and
                            $candAlert.labels.alertname -eq "AegisEdgeE2ELifecycleWarning" -and
                            $candAlert.labels.severity -eq "warning" -and
                            $candAlert.labels.tier -eq "integration-test" -and
                            $candAlert.labels.test_run_id -eq $MainRunId -and
                            $candAlert.annotations.summary -eq "E2E lifecycle test warning alert" -and
                            $candAlert.annotations.description -eq "Integration test alert for Prometheus -> Alertmanager -> webhook delivery") {

                            # Timestamp validation: endsAt > startsAt
                            $candStartsAt = [DateTime]::Parse($candAlert.startsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                            $candEndsAt = [DateTime]::Parse($candAlert.endsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                            if ($candEndsAt.Year -ge 2024 -and $candEndsAt -gt $candStartsAt) {
                                $warnResolvedEvent = $ev
                                $rAlert = $candAlert
                                break
                            }
                        }
                    }
                }
                if ($warnResolvedEvent) { break }
            }
            if ($warnResolvedEvent) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $warnResolvedEvent) {
        $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
        Write-Host "  Events recorded in receiver at timeout ($($events.Count)):" -ForegroundColor Red
        $events | ForEach-Object { Write-Host "    - route: $($_.route), receiver: $($_.payload.receiver), status: $($_.payload.status), alerts: $($_.payload.alerts.Count)" -ForegroundColor Red }
        throw "Webhook receiver did not receive strictly correlated resolved warning alert payload for fingerprint $firingFingerprint (run_id $MainRunId)."
    }

    Write-Host "  Correlated resolved notification verified (Fingerprint: $($rAlert.fingerprint), EndsAt ($($rAlert.endsAt)) > StartsAt ($($rAlert.startsAt)))." -ForegroundColor Green

    # =========================================================================
    # Scenario B: Critical Alert Routing (Severity Matching) & Resolution
    # =========================================================================
    Write-Host "`n==> 9. Testing Critical Alert Routing (/alerts/critical)..." -ForegroundColor Yellow
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events" -Method Delete | Out-Null
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/trigger?val=2&run_id=$MainRunId" -Method Post | Out-Null

    Write-Host "  Waiting for Prometheus rule AegisEdgeE2ELifecycleCritical to fire..." -ForegroundColor Cyan
    $critFiring = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $rulesResp = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/rules"
            foreach ($group in $rulesResp.data.groups) {
                if ($group.name -eq "aegisedge-lifecycle-test") {
                    foreach ($rule in $group.rules) {
                        if ($rule.name -eq "AegisEdgeE2ELifecycleCritical" -and $rule.state -eq "firing") {
                            $critFiring = $true
                            break
                        }
                    }
                }
            }
            if ($critFiring) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $critFiring) {
        throw "Alert AegisEdgeE2ELifecycleCritical failed to fire in Prometheus."
    }

    Write-Host "  Waiting for routed webhook delivery on /alerts/critical..." -ForegroundColor Cyan
    $critEvent = $null
    $critFingerprint = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
            foreach ($ev in $events) {
                if ($ev.route -eq "critical" -and $ev.payload.status -eq "firing" -and $ev.payload.receiver -eq "critical-receiver") {
                    foreach ($alert in $ev.payload.alerts) {
                        if ($alert.labels.alertname -eq "AegisEdgeE2ELifecycleCritical" -and
                            $alert.labels.severity -eq "critical" -and
                            $alert.labels.tier -eq "integration-test" -and
                            $alert.labels.test_run_id -eq $MainRunId) {
                            $critEvent = $ev
                            $critFingerprint = $alert.fingerprint
                            break
                        }
                    }
                }
                if ($critEvent) { break }
            }
            if ($critEvent) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $critEvent) {
        throw "Webhook receiver did not receive firing critical alert payload on /alerts/critical."
    }
    Write-Host "  Critical alert correctly routed to receiver: $($critEvent.payload.receiver) (Fingerprint: $critFingerprint)" -ForegroundColor Green

    # Resolve critical alert and wait for resolution webhook
    Write-Host "  Triggering Critical Alert Resolution (val=0, run_id=$MainRunId)..." -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/trigger?val=0&run_id=$MainRunId" -Method Post | Out-Null

    Write-Host "  Waiting for resolved webhook delivery on /alerts/critical..." -ForegroundColor Cyan
    $critResolvedEvent = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
            foreach ($ev in $events) {
                if ($ev.route -eq "critical" -and $ev.payload.status -eq "resolved" -and $ev.payload.receiver -eq "critical-receiver") {
                    foreach ($alert in $ev.payload.alerts) {
                        if ($alert.fingerprint -eq $critFingerprint -and
                            $alert.labels.alertname -eq "AegisEdgeE2ELifecycleCritical" -and
                            $alert.status -eq "resolved") {
                            $rStartsAt = [DateTime]::Parse($alert.startsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                            $rEndsAt = [DateTime]::Parse($alert.endsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                            if ($rEndsAt.Year -ge 2024 -and $rEndsAt -gt $rStartsAt) {
                                $critResolvedEvent = $ev
                                break
                            }
                        }
                    }
                }
                if ($critResolvedEvent) { break }
            }
            if ($critResolvedEvent) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $critResolvedEvent) {
        throw "Webhook receiver did not receive resolved critical alert payload on /alerts/critical for fingerprint $critFingerprint."
    }
    Write-Host "  Critical alert resolution verified (Fingerprint: $critFingerprint)." -ForegroundColor Green

    # =========================================================================
    # Scenario C: Default Receiver Fallback (Unmatched Severity) & Resolution
    # =========================================================================
    Write-Host "`n==> 10. Testing Default Receiver Fallback for Unmatched Severity (/alerts/default)..." -ForegroundColor Yellow
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events" -Method Delete | Out-Null
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/trigger?val=3&run_id=$MainRunId" -Method Post | Out-Null

    Write-Host "  Waiting for rule AegisEdgeE2ELifecycleDefaultFallback to fire..." -ForegroundColor Cyan
    $fallbackFiring = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $rulesResp = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/rules"
            foreach ($group in $rulesResp.data.groups) {
                if ($group.name -eq "aegisedge-lifecycle-test") {
                    foreach ($rule in $group.rules) {
                        if ($rule.name -eq "AegisEdgeE2ELifecycleDefaultFallback" -and $rule.state -eq "firing") {
                            $fallbackFiring = $true
                            break
                        }
                    }
                }
            }
            if ($fallbackFiring) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $fallbackFiring) {
        throw "Alert AegisEdgeE2ELifecycleDefaultFallback failed to fire in Prometheus."
    }

    Write-Host "  Waiting for routed webhook delivery on /alerts/default..." -ForegroundColor Cyan
    $defaultEvent = $null
    $defaultFingerprint = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
            foreach ($ev in $events) {
                if ($ev.route -eq "default" -and $ev.payload.status -eq "firing" -and $ev.payload.receiver -eq "default-receiver") {
                    foreach ($alert in $ev.payload.alerts) {
                        if ($alert.labels.alertname -eq "AegisEdgeE2ELifecycleDefaultFallback" -and
                            $alert.labels.test_run_id -eq $MainRunId) {
                            $defaultEvent = $ev
                            $defaultFingerprint = $alert.fingerprint
                            break
                        }
                    }
                }
                if ($defaultEvent) { break }
            }
            if ($defaultEvent) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $defaultEvent) {
        $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
        Write-Host "  Events recorded in receiver at timeout ($($events.Count)):" -ForegroundColor Red
        $events | ForEach-Object { Write-Host "    - route: $($_.route), receiver: $($_.payload.receiver), status: $($_.payload.status), alerts: $($_.payload.alerts.Count)" -ForegroundColor Red }
        throw "Webhook receiver did not receive unmatched severity alert on /alerts/default."
    }
    Write-Host "  Unmatched severity alert fell back to receiver: $($defaultEvent.payload.receiver) (Fingerprint: $defaultFingerprint)" -ForegroundColor Green

    # Resolve fallback alert
    Write-Host "  Triggering fallback alert resolution (val=0, run_id=$MainRunId)..." -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/trigger?val=0&run_id=$MainRunId" -Method Post | Out-Null

    Write-Host "  Waiting for resolved webhook delivery on /alerts/default..." -ForegroundColor Cyan
    $defaultResolvedEvent = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
            foreach ($ev in $events) {
                if ($ev.route -eq "default" -and $ev.payload.status -eq "resolved" -and $ev.payload.receiver -eq "default-receiver") {
                    foreach ($alert in $ev.payload.alerts) {
                        if ($alert.fingerprint -eq $defaultFingerprint -and
                            $alert.labels.alertname -eq "AegisEdgeE2ELifecycleDefaultFallback" -and
                            $alert.status -eq "resolved") {
                            $rStartsAt = [DateTime]::Parse($alert.startsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                            $rEndsAt = [DateTime]::Parse($alert.endsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                            if ($rEndsAt.Year -ge 2024 -and $rEndsAt -gt $rStartsAt) {
                                $defaultResolvedEvent = $ev
                                break
                            }
                        }
                    }
                }
                if ($defaultResolvedEvent) { break }
            }
            if ($defaultResolvedEvent) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $defaultResolvedEvent) {
        throw "Webhook receiver did not receive resolved fallback alert payload on /alerts/default for fingerprint $defaultFingerprint."
    }
    Write-Host "  Default fallback alert resolution verified (Fingerprint: $defaultFingerprint)." -ForegroundColor Green

    # =========================================================================
    # Scenario D: Strict Alert Grouping & Grouped Resolution Verification (Audit Finding 2)
    # =========================================================================
    Write-Host "`n==> 11. Testing Alert Grouping Behavior (Multi-instance Alert Batching)..." -ForegroundColor Yellow
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events" -Method Delete | Out-Null
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/trigger?val=1&instances=2&run_id=$MainRunId" -Method Post | Out-Null

    Write-Host "  Waiting for multi-instance grouped webhook delivery on /alerts/warning..." -ForegroundColor Cyan
    $groupedEvent = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
            foreach ($ev in $events) {
                if ($ev.route -eq "warning" -and $ev.payload.status -eq "firing" -and $ev.payload.receiver -eq "warning-receiver") {
                    if ($ev.payload.alerts.Count -eq 2) {
                        $allMatchRun = $true
                        foreach ($a in $ev.payload.alerts) {
                            if ($a.labels.test_run_id -ne $MainRunId) {
                                $allMatchRun = $false
                                break
                            }
                        }
                        if ($allMatchRun) {
                            $groupedEvent = $ev
                            break
                        }
                    }
                }
            }
            if ($groupedEvent) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $groupedEvent) {
        throw "Webhook receiver did not receive grouped alert payload with exactly 2 alerts for run_id $MainRunId."
    }

    # Strict Grouping Assertions
    if ($groupedEvent.route -ne "warning") {
        throw "Expected grouped event route 'warning', got '$($groupedEvent.route)'"
    }
    if ($groupedEvent.payload.receiver -ne "warning-receiver") {
        throw "Expected grouped message receiver 'warning-receiver', got '$($groupedEvent.payload.receiver)'"
    }
    if ($groupedEvent.payload.alerts.Count -ne 2) {
        throw "Expected exactly 2 alert instances in grouped payload, got $($groupedEvent.payload.alerts.Count)"
    }

    # Validate group labels matching alertmanager.yml group_by: ["alertname", "severity", "job"]
    $gl = $groupedEvent.payload.groupLabels
    if ($null -eq $gl) {
        throw "Grouped payload is missing 'groupLabels'."
    }
    if ($gl.alertname -ne "AegisEdgeE2ELifecycleWarning") {
        throw "Expected groupLabel 'alertname' to be 'AegisEdgeE2ELifecycleWarning', got '$($gl.alertname)'"
    }
    if ($gl.severity -ne "warning") {
        throw "Expected groupLabel 'severity' to be 'warning', got '$($gl.severity)'"
    }
    if ($gl.job -ne "aegisedge-lifecycle-test") {
        throw "Expected groupLabel 'job' to be 'aegisedge-lifecycle-test', got '$($gl.job)'"
    }

    # Validate both firing alert instances and capture fingerprints by node
    $firingFingerprintsByNode = @{}
    $nodes = @()
    foreach ($a in $groupedEvent.payload.alerts) {
        if ($a.labels.alertname -ne "AegisEdgeE2ELifecycleWarning") {
            throw "Unexpected alertname in group: $($a.labels.alertname)"
        }
        if ($a.labels.severity -ne "warning") {
            throw "Unexpected severity in group: $($a.labels.severity)"
        }
        if ($a.labels.tier -ne "integration-test") {
            throw "Unexpected tier in group: $($a.labels.tier)"
        }
        if ($a.labels.test_run_id -ne $MainRunId) {
            throw "Unexpected test_run_id in group: $($a.labels.test_run_id)"
        }
        if ([string]::IsNullOrWhiteSpace($a.labels.node)) {
            throw "Grouped alert instance is missing required 'node' label."
        }
        $nodes += $a.labels.node
        $firingFingerprintsByNode[$a.labels.node] = $a.fingerprint
    }
    if ($nodes[0] -eq $nodes[1] -or ($nodes -notcontains "node-a") -or ($nodes -notcontains "node-b")) {
        throw "Expected distinct node labels 'node-a' and 'node-b', got: $($nodes -join ', ')"
    }
    Write-Host "  Grouped firing verified: Single webhook contains 2 alert instances ('node-a': $($firingFingerprintsByNode['node-a']), 'node-b': $($firingFingerprintsByNode['node-b']))." -ForegroundColor Green

    # Strict Grouped Resolution Verification (Audit Finding 2)
    Write-Host "  Resolving multi-instance alert (val=0, instances=2, run_id=$MainRunId)..." -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/trigger?val=0&instances=2&run_id=$MainRunId" -Method Post | Out-Null

    Write-Host "  Waiting for strictly correlated resolved webhook delivery for grouped alerts on /alerts/warning..." -ForegroundColor Cyan
    $groupedResolvedEvent = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
            foreach ($ev in $events) {
                # 1. Validate route, receiver, message-level status
                if ($ev.route -eq "warning" -and
                    $ev.payload.status -eq "resolved" -and
                    $ev.payload.receiver -eq "warning-receiver") {
                    
                    # 2. Must contain exactly 2 alert instances
                    if ($ev.payload.alerts.Count -eq 2) {
                        $resolvedNodes = @()
                        $allMatch = $true

                        foreach ($ra in $ev.payload.alerts) {
                            # 3. Assert alert-level status and labels
                            if ($ra.status -ne "resolved" -or
                                $ra.labels.alertname -ne "AegisEdgeE2ELifecycleWarning" -or
                                $ra.labels.severity -ne "warning" -or
                                $ra.labels.tier -ne "integration-test" -or
                                $ra.labels.test_run_id -ne $MainRunId) {
                                $allMatch = $false
                                break
                            }

                            # 4. Check node label and fingerprint correlation against firing instance
                            $node = $ra.labels.node
                            $resolvedNodes += $node
                            if (-not $firingFingerprintsByNode.ContainsKey($node) -or
                                $ra.fingerprint -ne $firingFingerprintsByNode[$node]) {
                                $allMatch = $false
                                break
                            }

                            # 5. Check timestamps: endsAt > startsAt
                            $rStartsAt = [DateTime]::Parse($ra.startsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                            $rEndsAt = [DateTime]::Parse($ra.endsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                            if ($rEndsAt.Year -lt 2024 -or $rEndsAt -le $rStartsAt) {
                                $allMatch = $false
                                break
                            }
                        }

                        # 6. Must contain distinct node-a and node-b
                        if ($allMatch -and ($resolvedNodes -contains "node-a") -and ($resolvedNodes -contains "node-b") -and ($resolvedNodes[0] -ne $resolvedNodes[1])) {
                            $groupedResolvedEvent = $ev
                            break
                        }
                    }
                }
            }
            if ($groupedResolvedEvent) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $groupedResolvedEvent) {
        $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
        Write-Host "  Events recorded in receiver at timeout ($($events.Count)):" -ForegroundColor Red
        $events | ForEach-Object { Write-Host "    - route: $($_.route), receiver: $($_.payload.receiver), status: $($_.payload.status), alerts: $($_.payload.alerts.Count)" -ForegroundColor Red }
        throw "Webhook receiver did not receive strictly correlated grouped resolution payload with 2 instances ('node-a' and 'node-b')."
    }
    Write-Host "  Multi-instance grouped alert resolution verified with exact fingerprint matching per node." -ForegroundColor Green

    # =========================================================================
    # Scenario E: Alertmanager Restart & Real Post-Restart Delivery (Audit Finding 3)
    # =========================================================================
    Write-Host "`n==> 12. Testing Alertmanager Restart Resilience & Post-Restart Delivery (RunID=$RestartRunId)..." -ForegroundColor Yellow
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events" -Method Delete | Out-Null

    # Restart Alertmanager
    docker restart aegisedge-alertmanager | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "docker restart aegisedge-alertmanager failed with exit code $LASTEXITCODE"
    }

    # Wait for Alertmanager readiness
    $amReady = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt 20) {
        try {
            $resp = Invoke-WebRequest -Uri "http://127.0.0.1:9093/-/ready" -UseBasicParsing -TimeoutSec 2
            if ($resp.StatusCode -eq 200) {
                $amReady = $true
                break
            }
        } catch {}
        Start-Sleep -Milliseconds 500
    }
    if (-not $amReady) {
        throw "Alertmanager failed to become ready after restart."
    }
    Write-Host "  Alertmanager ready after restart." -ForegroundColor Green

    # Verify Prometheus still discovers active Alertmanager
    $amDiscoveredPostRestart = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt 20) {
        try {
            $amResp = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/alertmanagers"
            foreach ($am in $amResp.data.activeAlertmanagers) {
                if ($am.url -like "*9093*") {
                    $amDiscoveredPostRestart = $true
                    break
                }
            }
            if ($amDiscoveredPostRestart) { break }
        } catch {}
        Start-Sleep -Milliseconds 500
    }
    if (-not $amDiscoveredPostRestart) {
        throw "Prometheus lost active Alertmanager discovery after restart."
    }
    Write-Host "  Prometheus re-confirmed active Alertmanager connection." -ForegroundColor Green

    # Trigger post-restart synthetic warning metric with dedicated RestartRunId
    Write-Host "  Triggering warning alert post-restart (val=1, run_id=$RestartRunId)..." -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/trigger?val=1&run_id=$RestartRunId" -Method Post | Out-Null

    # Wait for rule to fire in Prometheus
    $restartRuleFiring = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $rulesResp = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/rules"
            foreach ($group in $rulesResp.data.groups) {
                if ($group.name -eq "aegisedge-lifecycle-test") {
                    foreach ($rule in $group.rules) {
                        if ($rule.name -eq "AegisEdgeE2ELifecycleWarning" -and $rule.state -eq "firing") {
                            $restartRuleFiring = $true
                            break
                        }
                    }
                }
            }
            if ($restartRuleFiring) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $restartRuleFiring) {
        throw "Post-restart alert AegisEdgeE2ELifecycleWarning failed to fire in Prometheus."
    }

    # Verify firing notification arrives on webhook receiver with RestartRunId
    $restartFiringEvent = $null
    $postRestartFingerprint = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
            foreach ($ev in $events) {
                if ($ev.route -eq "warning" -and $ev.payload.status -eq "firing" -and $ev.payload.receiver -eq "warning-receiver") {
                    foreach ($alert in $ev.payload.alerts) {
                        if ($alert.status -eq "firing" -and
                            $alert.labels.alertname -eq "AegisEdgeE2ELifecycleWarning" -and
                            $alert.labels.severity -eq "warning" -and
                            $alert.labels.tier -eq "integration-test" -and
                            $alert.labels.test_run_id -eq $RestartRunId) {
                            $restartFiringEvent = $ev
                            $postRestartFingerprint = $alert.fingerprint
                            break
                        }
                    }
                }
                if ($restartFiringEvent) { break }
            }
            if ($restartFiringEvent) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $restartFiringEvent) {
        throw "Post-restart firing alert failed to reach webhook receiver on /alerts/warning for run_id $RestartRunId."
    }

    Write-Host "  Post-restart firing notification received and verified (RunID: $RestartRunId, Fingerprint: $postRestartFingerprint)." -ForegroundColor Green

    # Trigger post-restart resolution
    Write-Host "  Triggering post-restart alert resolution (val=0, run_id=$RestartRunId)..." -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/trigger?val=0&run_id=$RestartRunId" -Method Post | Out-Null

    # Wait for strictly correlated resolved notification (Audit Finding 3)
    $restartResolvedEvent = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:9095/api/events"
            foreach ($ev in $events) {
                if ($ev.route -eq "warning" -and $ev.payload.status -eq "resolved" -and $ev.payload.receiver -eq "warning-receiver") {
                    foreach ($alert in $ev.payload.alerts) {
                        if ($alert.fingerprint -eq $postRestartFingerprint -and
                            $alert.status -eq "resolved" -and
                            $alert.labels.alertname -eq "AegisEdgeE2ELifecycleWarning" -and
                            $alert.labels.severity -eq "warning" -and
                            $alert.labels.tier -eq "integration-test" -and
                            $alert.labels.test_run_id -eq $RestartRunId) {
                            
                            $rrStartsAt = [DateTime]::Parse($alert.startsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                            $rrEndsAt = [DateTime]::Parse($alert.endsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                            if ($rrEndsAt.Year -ge 2024 -and $rrEndsAt -gt $rrStartsAt) {
                                $restartResolvedEvent = $ev
                                break
                            }
                        }
                    }
                }
                if ($restartResolvedEvent) { break }
            }
            if ($restartResolvedEvent) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $restartResolvedEvent) {
        throw "Post-restart resolved alert failed to reach webhook receiver for fingerprint $postRestartFingerprint (run_id $RestartRunId)."
    }

    Write-Host "  Post-restart resolution verified (Fingerprint: $postRestartFingerprint, RunID: $RestartRunId, EndsAt > StartsAt)." -ForegroundColor Green

    Write-Host "`n==========================================================" -ForegroundColor Green
    Write-Host "ALL INTEGRATION TEST SCENARIOS COMPLETED SUCCESSFULLY!" -ForegroundColor Green
    Write-Host "==========================================================" -ForegroundColor Green
}
catch {
    $testError = $_
    Write-Host "Test Execution Failed: $testError" -ForegroundColor Red
}
finally {
    # Teardown and cleanup (Audit Finding 4)
    $cleanupErrors = Invoke-CleanupResources

    if ($cleanupErrors.Count -gt 0) {
        $cleanupSummary = $cleanupErrors -join "; "
        if ($testError) {
            Write-Host "FATAL: Integration test failed: $($testError.ToString()). Additionally, teardown errors occurred: $cleanupSummary" -ForegroundColor Red
            exit 1
        } else {
            Write-Host "FATAL: Teardown failed: $cleanupSummary" -ForegroundColor Red
            exit 1
        }
    } elseif ($testError) {
        Write-Host "FATAL: Integration test failed: $($testError.ToString())" -ForegroundColor Red
        exit 1
    } else {
        Write-Host "Test and teardown completed successfully with 0 errors." -ForegroundColor Green
        exit 0
    }
}
