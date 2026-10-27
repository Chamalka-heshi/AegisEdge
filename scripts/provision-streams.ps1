# Temporary NATS JetStream Stream Provisioning Script
# Uses raw NATS TCP protocol to create streams via the JetStream API.
# This is a development tool, NOT an application dependency.
# It does not add Go dependencies or modify application code.

param(
    [string]$Server = "127.0.0.1",
    [int]$Port = 4222,
    [string[]]$ConfigFiles = @(
        "C:\AegisEdge\infra\nats\streams\telemetry-stream.json",
        "C:\AegisEdge\infra\nats\streams\advisories-stream.json"
    )
)

$ErrorActionPreference = "Stop"

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host " Provisioning JetStream Streams via NATS Protocol" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan

function Provision-Stream {
    param(
        [string]$Server,
        [int]$Port,
        [string]$ConfigFile
    )

    if (-not (Test-Path $ConfigFile)) {
        Write-Error "Stream config file not found: $ConfigFile"
        return
    }
    $configText = (Get-Content -Path $ConfigFile -Raw).Trim()
    $configObj = $configText | ConvertFrom-Json
    $streamName = $configObj.name
    $configBytes = [System.Text.Encoding]::UTF8.GetBytes($configText)
    $byteLen = $configBytes.Length

    Write-Host "`n==> Loading stream config for: $streamName ($byteLen bytes)" -ForegroundColor Cyan
    Write-Host "    Source: $ConfigFile"

    # Connect to NATS via raw TCP
    Write-Host "==> Connecting to NATS at ${Server}:${Port}..." -ForegroundColor Cyan
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient($Server, $Port)
    } catch {
        Write-Error "Cannot connect to NATS at ${Server}:${Port}. Is the server running?"
        return
    }

    $netStream = $tcp.GetStream()
    $netStream.ReadTimeout = 5000
    $encoding = [System.Text.UTF8Encoding]::new($false)
    $reader = New-Object System.IO.StreamReader($netStream, $encoding)
    $writer = New-Object System.IO.StreamWriter($netStream, $encoding)
    $writer.NewLine = "`r`n"
    $writer.AutoFlush = $true

    # Read server INFO line
    $infoLine = $reader.ReadLine()

    # Send CONNECT handshake
    $connectPayload = '{"verbose":false,"pedantic":false,"lang":"powershell","version":"1.0.0"}'
    $writer.WriteLine("CONNECT $connectPayload")

    # Subscribe to a unique reply inbox
    $inbox = "_INBOX.provision.$([guid]::NewGuid().ToString('N'))"
    $writer.WriteLine("SUB $inbox 1")

    # Publish stream creation request to JetStream API
    $subject = "`$JS.API.STREAM.CREATE.$streamName"
    Write-Host "==> Publishing to: $subject" -ForegroundColor Cyan
    Write-Host "    Reply inbox:   $inbox"

    $writer.WriteLine("PUB $subject $inbox $byteLen")
    $writer.Write($configText)
    $writer.WriteLine("")

    # Send PING to flush the pipeline
    $writer.WriteLine("PING")

    # Wait for response with timeout
    $deadline = (Get-Date).AddSeconds(5)
    $response = ""

    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds 200
        while ($netStream.DataAvailable) {
            $line = $reader.ReadLine()
            if ($null -eq $line) { continue }

            if ($line -match "^MSG\s") {
                $parts = $line -split "\s+"
                $payloadLen = [int]$parts[$parts.Length - 1]
                if ($payloadLen -gt 0) {
                    $buf = New-Object char[] $payloadLen
                    $totalRead = 0
                    while ($totalRead -lt $payloadLen) {
                        $n = $reader.Read($buf, $totalRead, $payloadLen - $totalRead)
                        if ($n -le 0) { break }
                        $totalRead += $n
                    }
                    $response = New-Object string(,$buf)
                    $reader.ReadLine() | Out-Null
                }
                break
            }
            if ($line -match "^-ERR") {
                Write-Host "  [NATS ERROR] $line" -ForegroundColor Red
            }
        }
        if ($response) { break }
    }

    $tcp.Close()

    if (-not $response) {
        Write-Host "  [ERROR] No response received from JetStream API within timeout for $streamName." -ForegroundColor Red
        return
    }

    try {
        $parsed = $response | ConvertFrom-Json

        if ($parsed.error) {
            $code = $parsed.error.code
            $desc = $parsed.error.description
            if ($code -eq 400 -and $desc -match "already in use") {
                Write-Host "  [INFO] Stream '$streamName' already exists." -ForegroundColor Yellow
            } else {
                Write-Host "  [ERROR] Code $code : $desc" -ForegroundColor Red
            }
        } else {
            Write-Host "  [OK] Stream '$($parsed.config.name)' provisioned successfully!" -ForegroundColor Green
        }

        if ($parsed.config) {
            $cfg = $parsed.config
            Write-Host "  Stream Name:      $($cfg.name)" -ForegroundColor Green
            Write-Host "  Subjects:         $($cfg.subjects -join ', ')" -ForegroundColor Green
            Write-Host "  Retention:        $($cfg.retention)" -ForegroundColor Green
            Write-Host "  Storage:          $($cfg.storage)" -ForegroundColor Green
            Write-Host "  Discard Policy:   $($cfg.discard_policy)" -ForegroundColor Green
        }
    } catch {
        Write-Host "  Raw response: $response" -ForegroundColor Yellow
    }
}

foreach ($cfgFile in $ConfigFiles) {
    Provision-Stream -Server $Server -Port $Port -ConfigFile $cfgFile
}

Write-Host "`n==> Stream provisioning completed." -ForegroundColor Green
