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
            if ($acc.stream_detail) {
                foreach ($str in $acc.stream_detail) {
                    if ($str.name -eq $StreamName) {
                        $streamFound = $true
                        Write-Host "  [OK] Stream '$StreamName' found." -ForegroundColor Green
                        Write-Host "  Created:        $($str.created)" -ForegroundColor Green
                        Write-Host "  Messages:       $($str.state.messages)" -ForegroundColor Green
                        Write-Host "  Bytes:          $($str.state.bytes)" -ForegroundColor Green
                        Write-Host "  Consumers:      $($str.state.consumer_count)" -ForegroundColor Green
                    }
                }
            }
        }
    }
    
    if (-not $streamFound) {
        Write-Host "  [INFO] Stream '$StreamName' is not yet provisioned." -ForegroundColor Yellow
        Write-Host "  Provision using: scripts/provision-streams.ps1 or NATS CLI." -ForegroundColor Yellow
    }
} catch {
    Write-Host "  [ERROR] Failed to query /jsz: $_" -ForegroundColor Red
    exit 1
}

# 6. Query stream configuration via NATS protocol (if stream was found)
if ($streamFound) {
    Write-Host "`n==> 6. Querying Stream Configuration via NATS Protocol..." -ForegroundColor Cyan
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient("127.0.0.1", $ClientPort)
        $netStream = $tcp.GetStream()
        $netStream.ReadTimeout = 5000
        $enc = [System.Text.UTF8Encoding]::new($false)
        $rdr = New-Object System.IO.StreamReader($netStream, $enc)
        $wtr = New-Object System.IO.StreamWriter($netStream, $enc)
        $wtr.NewLine = "`r`n"
        $wtr.AutoFlush = $true

        # Read INFO, send CONNECT
        $rdr.ReadLine() | Out-Null
        $wtr.WriteLine('CONNECT {"verbose":false,"pedantic":false,"lang":"powershell","version":"1.0.0"}')

        # Subscribe to reply inbox
        $inbox = "_INBOX.check.$([guid]::NewGuid().ToString('N'))"
        $wtr.WriteLine("SUB $inbox 1")

        # Request stream info
        $reqPayload = "{`"name`":`"$StreamName`"}"
        $reqBytes = [System.Text.Encoding]::UTF8.GetBytes($reqPayload)
        $wtr.WriteLine("PUB `$JS.API.STREAM.INFO.$StreamName $inbox $($reqBytes.Length)")
        $wtr.Write($reqPayload)
        $wtr.WriteLine("")
        $wtr.WriteLine("PING")

        # Read response
        $deadline = (Get-Date).AddSeconds(3)
        $streamInfo = ""
        while ((Get-Date) -lt $deadline) {
            Start-Sleep -Milliseconds 200
            while ($netStream.DataAvailable) {
                $line = $rdr.ReadLine()
                if ($line -match "^MSG\s") {
                    $parts = $line -split "\s+"
                    $pLen = [int]$parts[$parts.Length - 1]
                    if ($pLen -gt 0) {
                        $buf = New-Object char[] $pLen
                        $read = 0
                        while ($read -lt $pLen) {
                            $n = $rdr.Read($buf, $read, $pLen - $read)
                            if ($n -le 0) { break }
                            $read += $n
                        }
                        $streamInfo = New-Object string(,$buf)
                        $rdr.ReadLine() | Out-Null
                    }
                    break
                }
            }
            if ($streamInfo) { break }
        }
        $tcp.Close()

        if ($streamInfo) {
            $info = $streamInfo | ConvertFrom-Json
            if ($info.config) {
                $cfg = $info.config
                Write-Host "  Stream Name:      $($cfg.name)" -ForegroundColor Green
                Write-Host "  Subjects:         $($cfg.subjects -join ', ')" -ForegroundColor Green
                Write-Host "  Storage:          $($cfg.storage)" -ForegroundColor Green
                Write-Host "  Retention:        $($cfg.retention)" -ForegroundColor Green
                Write-Host "  Replicas:         $($cfg.num_replicas)" -ForegroundColor Green
                Write-Host "  Max Messages:     $($cfg.max_msgs)" -ForegroundColor Green
                Write-Host "  Max Bytes:        $($cfg.max_bytes)" -ForegroundColor Green

                # Convert nanoseconds to human-readable
                $maxAgeDays = [math]::Round($cfg.max_age / 86400000000000, 1)
                Write-Host "  Max Age:          $($cfg.max_age) ns ($maxAgeDays days)" -ForegroundColor Green
                Write-Host "  Max Msg Size:     $($cfg.max_msg_size) bytes ($([math]::Round($cfg.max_msg_size / 1048576, 1)) MB)" -ForegroundColor Green
                Write-Host "  Discard Policy:   $($cfg.discard_policy)" -ForegroundColor Green

                $dupWindowHrs = [math]::Round($cfg.duplicate_window / 3600000000000, 1)
                Write-Host "  Dup Window:       $($cfg.duplicate_window) ns ($dupWindowHrs hours)" -ForegroundColor Green
            }
        } else {
            Write-Host "  [WARN] Could not query stream config via NATS protocol." -ForegroundColor Yellow
        }
    } catch {
        Write-Host "  [WARN] Stream config query failed: $_" -ForegroundColor Yellow
    }
}

Write-Host "`n==> Phase 4.2 NATS environment verification completed successfully!" -ForegroundColor Green
