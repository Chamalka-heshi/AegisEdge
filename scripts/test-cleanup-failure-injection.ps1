# AegisEdge Cleanup Failure-Injection Verification Suite
# Verifies that cleanup errors and test errors are properly isolated, reported, and cause non-zero exit codes.
# Ensures production restoration is directly verified via Docker engine and Prometheus REST APIs,
# without relying exclusively on output-pattern matching.

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "AegisEdge: Cleanup Failure-Injection Verification Suite" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$testScript = ".\scripts\test-alertmanager-lifecycle.ps1"

function Verify-ProductionStateDirectly {
    # 1. Direct Docker check: No leftover test receiver containers
    $leftovers = cmd.exe /c "docker ps -a --filter name=aegisedge-recv --format {{.Names}}"
    if (-not [string]::IsNullOrWhiteSpace($leftovers)) {
        Write-Host "  FAILED: Leaked temporary receiver container detected: $leftovers" -ForegroundColor Red
        return $false
    }

    # 2. Direct Prometheus readiness check
    $ready = $false
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt 20) {
        try {
            $r = Invoke-WebRequest -Uri "http://127.0.0.1:9090/-/ready" -UseBasicParsing -TimeoutSec 2
            if ($r.StatusCode -eq 200) { $ready = $true; break }
        } catch {
            Start-Sleep -Milliseconds 500
        }
    }
    if (-not $ready) {
        Write-Host "  FAILED: Production Prometheus failed HTTP 200 readiness check." -ForegroundColor Red
        return $false
    }

    # 3. Direct rules check: No lifecycle test rule groups
    try {
        $rules = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/rules" -TimeoutSec 5
        $hasTestRules = ($rules.data.groups | Where-Object { $_.name -eq "aegisedge-lifecycle-test" }).Count -gt 0
        if ($hasTestRules) {
            Write-Host "  FAILED: Lifecycle test rule group remains in Prometheus." -ForegroundColor Red
            return $false
        }
    } catch {
        Write-Host "  FAILED: Could not query Prometheus rules: $_" -ForegroundColor Red
        return $false
    }

    # 4. Direct targets check: aegisedge-control-plane is healthy (health=up), no test targets
    $sw.Restart()
    $cpHealthy = $false
    while ($sw.Elapsed.TotalSeconds -lt 20) {
        try {
            $targets = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/targets" -TimeoutSec 3
            $hasTestTargets = ($targets.data.activeTargets | Where-Object { $_.scrapePool -eq "aegisedge-lifecycle-test" }).Count -gt 0
            if ($hasTestTargets) {
                Write-Host "  FAILED: Lifecycle test scrape target remains in Prometheus." -ForegroundColor Red
                return $false
            }
            $cp = $targets.data.activeTargets | Where-Object { $_.scrapePool -eq "aegisedge-control-plane" }
            $am = $targets.data.activeTargets | Where-Object { $_.scrapePool -eq "aegisedge-alertmanager" }
            if ($cp -ne $null -and $cp.health -eq "up" -and $am -ne $null -and $am.health -eq "up") {
                $cpHealthy = $true
                break
            }
        } catch {}
        Start-Sleep -Milliseconds 500
    }

    if (-not $cpHealthy) {
        Write-Host "  FAILED: Production Control Plane target 'aegisedge-control-plane' missing or not healthy." -ForegroundColor Red
        return $false
    }

    # 5. Direct secret cleanup check: No leftover ephemeral secret file or directory
    $secretFile = "infra/prometheus/.test_secret/token"
    $secretDir = "infra/prometheus/.test_secret"
    if (Test-Path $secretFile) {
        Write-Host "  FAILED: Leaked ephemeral secret file detected: $secretFile" -ForegroundColor Red
        return $false
    }
    if (Test-Path $secretDir) {
        Write-Host "  FAILED: Leaked ephemeral secret directory detected: $secretDir" -ForegroundColor Red
        return $false
    }

    Write-Host "  Direct API & Secret Verification: 0 leaked containers, 0 leaked secrets, Prometheus HTTP 200, 0 test rules/targets, Control Plane target UP." -ForegroundColor Green
    return $true
}

function Run-Scenario {
    param(
        [string]$Name,
        [string]$FailureMode,
        [string[]]$ExpectedPatterns,
        [int]$ExpectedExitCode = 1,
        [switch]$ExpectLeakedSecret
    )

    Write-Host "`n--- Testing Scenario: $Name ($FailureMode) ---" -ForegroundColor Yellow
    
    # Save ErrorActionPreference to prevent native stderr progress messages from aborting the runner
    $prevEAP = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = cmd.exe /c "powershell.exe -NoProfile -ExecutionPolicy Bypass -File $testScript -SimulateFailure $FailureMode 2>&1"
    $code = $LASTEXITCODE
    $ErrorActionPreference = $prevEAP

    $outputStr = $output -join "`n"
    Write-Host "Exit Code: $code (Expected: $ExpectedExitCode)" -ForegroundColor Gray
    if ($code -ne $ExpectedExitCode) {
        Write-Host "FAILED: Exit code $code != $ExpectedExitCode" -ForegroundColor Red
        return $false
    }

    $matchedAll = $true
    foreach ($pat in $ExpectedPatterns) {
        if ($outputStr -notmatch [regex]::Escape($pat)) {
            Write-Host "FAILED: Missing expected diagnostic pattern: '$pat'" -ForegroundColor Red
            $matchedAll = $false
        } else {
            Write-Host "  Diagnostic verified: '$pat'" -ForegroundColor Green
        }
    }

    if ($ExpectLeakedSecret) {
        $secretFile = "infra/prometheus/.test_secret/token"
        if (-not (Test-Path $secretFile)) {
            Write-Host "FAILED: Expected secret file '$secretFile' to remain on disk during fail-secret-cleanup, but it was already deleted." -ForegroundColor Red
            return $false
        }
        Write-Host "  Actual failure outcome verified: Ephemeral secret file remained on disk as expected before test suite cleanup." -ForegroundColor Green
        # Now clean it up so subsequent state verification passes cleanly
        $currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent()
        $userSid = $currentUser.User.Value
        cmd.exe /c "icacls `"$secretFile`" /grant *${userSid}:(F) >nul 2>&1"
        Remove-Item -Path $secretFile -Force -ErrorAction SilentlyContinue
        Remove-Item -Path "infra/prometheus/.test_secret" -Recurse -Force -ErrorAction SilentlyContinue
    }

    # Verify production state directly without relying solely on output matching
    $stateOk = Verify-ProductionStateDirectly
    if (-not $stateOk) {
        Write-Host "FAILED: Direct state verification failed after scenario $FailureMode" -ForegroundColor Red
        return $false
    }

    return $matchedAll
}

# 1. Simulate Test Failure (Verifies test error is preserved and cleanup restores production stack)
$res1 = Run-Scenario -Name "Preserve Test Exception With Clean Teardown" `
    -FailureMode "fail-test" `
    -ExpectedPatterns @(
        "[SIMULATED] Test execution failure injected",
        "Teardown: Restoring production Compose stack",
        "Production restoration fully verified",
        "FATAL: Integration test failed"
    )

# 2. Simulate Partial Receiver Startup (Verifies container created before launch failure is detected and safely removed)
$res2 = Run-Scenario -Name "Partial Receiver Startup Container Removed Safely" `
    -FailureMode "fail-partial-receiver-startup" `
    -ExpectedPatterns @(
        "[SIMULATED] Docker receiver container launch reported failure after container creation",
        "Temporary receiver container",
        "removed",
        "FATAL: Integration test failed"
    )

# 3. Simulate Receiver Cleanup Failure (Verifies receiver failure does not block Compose restore or target verify)
$res3 = Run-Scenario -Name "Receiver Removal Failure Does Not Prevent Compose Restoration" `
    -FailureMode "fail-receiver-cleanup" `
    -ExpectedPatterns @(
        "[SIMULATED] Receiver container removal failed",
        "Production Compose stack restored",
        "Production Control Plane target 'aegisedge-control-plane' verified active and healthy",
        "FATAL: Teardown failed"
    )

# 4. Simulate Compose Restore Failure (Verifies Compose restoration failure is reported)
$res4 = Run-Scenario -Name "Compose Restoration Failure Reported" `
    -FailureMode "fail-compose-restore" `
    -ExpectedPatterns @(
        "[SIMULATED] Production Compose restoration failure",
        "FATAL: Teardown failed"
    )

# 5. Simulate Target Verification Failure (Verifies missing/unhealthy target reports failure)
$res5 = Run-Scenario -Name "Target Verification Failure Reported" `
    -FailureMode "fail-target-verify" `
    -ExpectedPatterns @(
        "[SIMULATED] Production Control Plane target verification failed",
        "FATAL: Teardown failed"
    )

# 6. Simulate Both Test and Cleanup Failure (Verifies both errors are preserved together)
$res6 = Run-Scenario -Name "Test and Cleanup Failures Preserved Together" `
    -FailureMode "fail-both" `
    -ExpectedPatterns @(
        "[SIMULATED] Test execution failure injected",
        "[SIMULATED] Receiver container removal failed",
        "FATAL: Integration test failed",
        "Additionally, teardown errors occurred"
    )

# 7. Simulate Secret Creation Failure (Verifies secret file and directory are cleaned up even if secret creation fails midway)
$res7 = Run-Scenario -Name "Secret Cleaned Up If Secret Configuration Fails" `
    -FailureMode "fail-secret-creation" `
    -ExpectedPatterns @(
        "[SIMULATED] Secret configuration failed during verification",
        "Teardown: Restoring production Compose stack & cleaning test resources",
        "Ephemeral token file removal verified (file is absent)",
        "Ephemeral secret directory removal verified (directory is absent)",
        "FATAL: Integration test failed"
    )

# 8. Simulate Secret Removal Failure (Verifies secret removal failure and residual presence are reported as teardown failure)
$res8 = Run-Scenario -Name "Secret Removal Failure Reported and Detected" `
    -FailureMode "fail-secret-cleanup" `
    -ExpectedPatterns @(
        "[SIMULATED] Ephemeral secret file removal failed",
        "Verification failed: Ephemeral secret file",
        "FATAL: Teardown failed"
    ) `
    -ExpectLeakedSecret

# 9. Final Suite Summary
if ($res1 -and $res2 -and $res3 -and $res4 -and $res5 -and $res6 -and $res7 -and $res8) {
    Write-Host "`n==========================================================" -ForegroundColor Green
    Write-Host "ALL 8 CLEANUP FAILURE-INJECTION SCENARIOS PASSED!" -ForegroundColor Green
    Write-Host "Direct API and Docker verification confirmed for all runs." -ForegroundColor Green
    Write-Host "==========================================================" -ForegroundColor Green
    exit 0
} else {
    Write-Host "`nFATAL: One or more failure-injection scenarios failed." -ForegroundColor Red
    exit 1
}
