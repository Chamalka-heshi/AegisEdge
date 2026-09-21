# AegisEdge NATS & JetStream Environment Validation Script
# Reference: Phase 4.2 Infrastructure Setup & ADR-0006

param(
    [string]$BaseUrl = "http://127.0.0.1:8222",
    [int]$ClientPort = 4222,
    [string]$StreamName = "AEGISEDGE_TELEMETRY"
)

$ErrorActionPreference = "Stop"

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host " AegisEdge Phase 4.2: NATS & JetStream Validation" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan

# 1. Check NATS TCP Client Port Reachability
Write-Host "`n==> 1. Checking NATS Client TCP Port ($ClientPort)..." -ForegroundColor Cyan
try {
    $tcp = New-Object System.Net.Sockets.TcpClient
    $asyncResult = $tcp.BeginConnect("127.0.0.1", $ClientPort, $null, $null)
    $success = $asyncResult.AsyncWaitHandle.WaitOne(2000, $false)
    if ($success -and $tcp.Connected) {
        $tcp.EndConnect($asyncResult)
        $tcp.Close()
        Write-Host "  [OK] NATS client port $ClientPort is listening." -ForegroundColor Green
    } else {
        $tcp.Close()
        Write-Host "  [FAIL] NATS client port $ClientPort is NOT reachable." -ForegroundColor Yellow
    }
} catch {
    Write-Host "  [FAIL] TCP connection error: $_" -ForegroundColor Yellow
}

# 2. Check NATS HTTP Monitoring & Healthz Endpoint
Write-Host "`n==> 2. Checking NATS HTTP Healthz Endpoint ($BaseUrl/healthz)..." -ForegroundColor Cyan
try {
    $health = Invoke-RestMethod -Uri "$BaseUrl/healthz" -Method Get -TimeoutSec 3
    if ($health.status -eq "ok") {
        Write-Host "  [OK] NATS healthz status: OK" -ForegroundColor Green
    } else {
        Write-Host "  [WARN] NATS healthz responded: $($health | ConvertTo-Json -Compress)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  [ERROR] NATS monitoring endpoint is not reachable at $BaseUrl" -ForegroundColor Red
    Write-Host "  Please ensure the NATS server is running:" -ForegroundColor Yellow
    Write-Host "    - Via Docker Compose: docker compose -f infra/nats/docker-compose.yml up -d" -ForegroundColor Yellow
    Write-Host "    - Via local binary:   nats-server -c infra/nats/nats-server.conf" -ForegroundColor Yellow
    exit 1
}

# 3. Query Server Info (/varz)
Write-Host "`n==> 3. Querying NATS Server Info ($BaseUrl/varz)..." -ForegroundColor Cyan
try {
    $varz = Invoke-RestMethod -Uri "$BaseUrl/varz" -Method Get -TimeoutSec 3
    Write-Host "  Server Name:    $($varz.server_name)" -ForegroundColor Green
    Write-Host "  NATS Version:   $($varz.version)" -ForegroundColor Green
    Write-Host "  Uptime:         $($varz.uptime)" -ForegroundColor Green
    Write-Host "  Connections:    $($varz.connections)" -ForegroundColor Green
} catch {
    Write-Host "  [ERROR] Failed to query /varz: $_" -ForegroundColor Red
    exit 1
}

# 4. Verify JetStream Status (/jsz)
Write-Host "`n==> 4. Verifying JetStream Subsystem ($BaseUrl/jsz)..." -ForegroundColor Cyan
try {
    $jsz = Invoke-RestMethod -Uri "$BaseUrl/jsz?streams=1" -Method Get -TimeoutSec 3
    if ($null -eq $jsz.config) {
        Write-Host "  [FAIL] JetStream is NOT enabled on this NATS server." -ForegroundColor Red
        exit 1
    }
    Write-Host "  [OK] JetStream is ENABLED." -ForegroundColor Green
    Write-Host "  Storage Dir:    $($jsz.config.store_dir)" -ForegroundColor Green
    Write-Host "  Max Memory:     $($jsz.config.max_memory) bytes" -ForegroundColor Green
    Write-Host "  Max Storage:    $($jsz.config.max_storage) bytes" -ForegroundColor Green
    Write-Host "  Active Streams: $($jsz.streams)" -ForegroundColor Green
    
    # 5. Check if Expected Stream Exists
    Write-Host "`n==> 5. Inspecting Stream: $StreamName..." -ForegroundColor Cyan
    $streamFound = $false
    if ($jsz.account_details) {
        foreach ($acc in $jsz.account_details) {
            if ($acc.streams) {
                foreach ($str in $acc.streams) {
                    if ($str.name -eq $StreamName) {
                        $streamFound = $true
                        Write-Host "  [OK] Stream '$StreamName' found." -ForegroundColor Green
                        Write-Host "  Subjects:       $($str.config.subjects -join ', ')" -ForegroundColor Green
                        Write-Host "  Storage Type:   $($str.config.storage)" -ForegroundColor Green
                        Write-Host "  Messages:       $($str.state.messages)" -ForegroundColor Green
                        Write-Host "  Bytes:          $($str.state.bytes)" -ForegroundColor Green
                    }
                }
            }
        }
    }
    
    if (-not $streamFound) {
        Write-Host "  [INFO] Stream '$StreamName' is not yet provisioned." -ForegroundColor Yellow
        Write-Host "  Provision using: infra/nats/streams/telemetry-stream.json or NATS CLI." -ForegroundColor Yellow
    }
} catch {
    Write-Host "  [ERROR] Failed to query /jsz: $_" -ForegroundColor Red
    exit 1
}

Write-Host "`n==> Phase 4.2 NATS environment verification completed successfully!" -ForegroundColor Green
