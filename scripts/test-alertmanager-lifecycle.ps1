# AegisEdge End-to-End Alertmanager Lifecycle Integration Test
# Verifies: Prometheus -> Alertmanager -> Webhook Receiver complete lifecycle
# Requirements: Docker Desktop running, isolated network aegisedge_test_network

param(
    [int]$TimeoutSeconds = 60,
    [ValidateSet("none", "fail-test", "fail-receiver-cleanup", "fail-compose-restore", "fail-target-verify", "fail-both", "fail-partial-receiver-startup", "fail-secret-creation", "fail-secret-cleanup")]
    [string]$SimulateFailure = "none",
    [switch]$Isolated = $true
)

if (-not $Isolated) {
    Write-Error "Non-isolated execution is unsafe and disabled. This lifecycle script runs exclusively in isolated mode."
    exit 1
}

$promPort = 19090
$amPort = 19093
$recvPort = 19095
$isolatedComposeFile = "infra/prometheus/docker-compose.isolated-test.yml"

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "AegisEdge: End-to-End Alert Lifecycle Verification" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# -------------------------------------------------------------------------
# Host-Level Concurrency Lock (Exclusive Host-Wide Named Mutex)
# Enforces strictly single execution across all sessions and elevation levels
# on the host machine to prevent collisions on fixed loopback ports
# (19090, 19093, 19095) and shared filesystem paths.
# -------------------------------------------------------------------------
$mutexName = "Global\AegisEdge_Alertmanager_Lifecycle_Test_Mutex"
$mutex = $null
$mutexAcquired = $false

# Initialize resource tracking variables prior to protected try block
$pwdPath = (Get-Item -Path ".").FullName
$secretDir = Join-Path $pwdPath "infra\prometheus\.test_secret"
$secretFile = Join-Path $secretDir "token"

# Complete GUIDs for per-execution isolation & strict ownership
$MainRunGuid = [Guid]::NewGuid().ToString("N")
$RestartRunGuid = [Guid]::NewGuid().ToString("N")
$InhibitRunGuid = [Guid]::NewGuid().ToString("N")
$ReceiverGuid = [Guid]::NewGuid().ToString("N")

$MainRunId = "run-" + $MainRunGuid
$RestartRunId = "rst-" + $RestartRunGuid
$InhibitRunId = "inh-" + $InhibitRunGuid

# Compose project name uses the complete GUID
$composeProject = "aegisedge-test-" + $MainRunGuid
$amContainer = "${composeProject}-alertmanager-1"
$promContainer = "${composeProject}-prometheus-1"
$testNetwork = "${composeProject}_test_network"

# Receiver container name uses the complete GUID
$ReceiverContainerName = "aegisedge-recv-" + $ReceiverGuid
$ReceiverOwnershipKey = "aegisedge.test.receiver_owner"
$ReceiverOwnershipVal = $ReceiverContainerName

# Resource tracking flags for cleanup
$secretDirCreatedByThisRun = $false
$secretFileCreatedByThisRun = $false
$composeProjectPreflightPassed = $false
$composeProjectStartupAttempted = $false
$composeProjectStarted = $false
$receiverContainerAttempted = $false
$receiverContainerStarted = $false
$testError = $null
$authHeaders = @{}
$cleanupErrors = @()

function Test-MutexSecurityConservativePolicy {
    param(
        [Parameter(Mandatory = $true)]
        [System.Threading.Mutex]$MutexHandle,
        [Parameter(Mandatory = $true)]
        [string]$MutexIdentifier
    )

    # Limitation Disclosure:
    # This is a conservative static DACL policy check, not a dynamic Win32 AccessCheck() kernel API call.
    # It strictly validates that:
    # 1. Zero Deny ACEs exist in the DACL (Deny ACEs unconditionally fail validation to prevent masking or DoS).
    # 2. Authenticated Users (S-1-5-11) is explicitly granted Synchronize, Modify, and ReadPermissions (mandatory for cross-session coordination).
    # 3. Builtin Administrators (S-1-5-32-544) or Local System (S-1-5-18) is explicitly granted Synchronize and Modify.
    # 4. The current caller identity (or Authenticated Users) is granted Synchronize and Modify.

    try {
        $sec = $MutexHandle.GetAccessControl()
        $rules = @($sec.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier]))
    } catch {
        Write-Error "Failed to retrieve security descriptor for mutex '$MutexIdentifier': $_"
        return $false
    }

    $currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $userSidStr = $currentUser.User.Value

    $authUsersSid = "S-1-5-11"        # WellKnownSidType: AuthenticatedUserSid
    $adminSid = "S-1-5-32-544"        # WellKnownSidType: BuiltinAdministratorsSid
    $systemSid = "S-1-5-18"           # WellKnownSidType: LocalSystemSid

    $requiredRights = [System.Security.AccessControl.MutexRights]::Synchronize -bor [System.Security.AccessControl.MutexRights]::Modify
    $requiredAuthRights = [System.Security.AccessControl.MutexRights]::Synchronize -bor `
                          [System.Security.AccessControl.MutexRights]::Modify -bor `
                          [System.Security.AccessControl.MutexRights]::ReadPermissions

    # Rule 1: Zero Deny ACEs allowed. Any Deny ACE fails validation immediately.
    foreach ($rule in $rules) {
        if ($rule.AccessControlType -eq [System.Security.AccessControl.AccessControlType]::Deny) {
            Write-Error "Mutex '$MutexIdentifier' contains an explicit Deny ACE for SID '$($rule.IdentityReference.Value)'. Conservative policy rejects all Deny ACEs."
            return $false
        }
    }

    # Rule 2: Authenticated Users (S-1-5-11) MUST have explicit Allow for Synchronize, Modify, and ReadPermissions.
    # An Administrators-only ACE is strictly insufficient to prove ordinary authenticated users can coordinate across sessions.
    $hasAuthUsersCoord = $false
    foreach ($rule in $rules) {
        if ($rule.AccessControlType -eq [System.Security.AccessControl.AccessControlType]::Allow -and
            $rule.IdentityReference.Value -eq $authUsersSid) {
            if (($rule.MutexRights -band $requiredAuthRights) -eq $requiredAuthRights) {
                $hasAuthUsersCoord = $true
                break
            }
        }
    }

    if (-not $hasAuthUsersCoord) {
        Write-Error "Mutex '$MutexIdentifier' DACL lacks mandatory cross-session coordination: Authenticated Users ($authUsersSid) is not granted Synchronize, Modify, and ReadPermissions."
        return $false
    }

    # Rule 3: Administrative / Elevated oversight (Builtin Administrators or Local System) MUST have Synchronize and Modify.
    $hasAdminCoord = $false
    foreach ($rule in $rules) {
        if ($rule.AccessControlType -eq [System.Security.AccessControl.AccessControlType]::Allow -and
            ($rule.IdentityReference.Value -eq $adminSid -or $rule.IdentityReference.Value -eq $systemSid)) {
            if (($rule.MutexRights -band $requiredRights) -eq $requiredRights) {
                $hasAdminCoord = $true
                break
            }
        }
    }

    if (-not $hasAdminCoord) {
        Write-Error "Mutex '$MutexIdentifier' DACL lacks elevated runner oversight: neither Administrators ($adminSid) nor Local System ($systemSid) is granted Synchronize and Modify."
        return $false
    }

    # Rule 4: Verify caller has explicit Allow (either via direct user SID or via Authenticated Users).
    $callerExplicitAllow = $false
    if ($hasAuthUsersCoord) {
        if ($null -ne $currentUser.Groups) {
            foreach ($g in $currentUser.Groups) {
                if ($g.Value -eq $authUsersSid) {
                    $callerExplicitAllow = $true
                    break
                }
            }
        }
    }
    if (-not $callerExplicitAllow) {
        foreach ($rule in $rules) {
            if ($rule.AccessControlType -eq [System.Security.AccessControl.AccessControlType]::Allow -and
                $rule.IdentityReference.Value -eq $userSidStr) {
                if (($rule.MutexRights -band $requiredRights) -eq $requiredRights) {
                    $callerExplicitAllow = $true
                    break
                }
            }
        }
    }

    if (-not $callerExplicitAllow) {
        Write-Error "Mutex '$MutexIdentifier' DACL does not grant Synchronize and Modify to current caller token ($userSidStr)."
        return $false
    }

    return $true
}

function Format-WindowsCommandLine {
    param(
        [string[]]$ArgsList
    )
    if ($null -eq $ArgsList -or $ArgsList.Count -eq 0) { return "" }
    $parts = [System.Collections.Generic.List[string]]::new()
    foreach ($arg in $ArgsList) {
        if ($null -eq $arg) { continue }
        $s = [string]$arg
        if ($s.Length -eq 0) {
            $parts.Add('""')
            continue
        }
        if ($s -notmatch '[\s\t\n\v"]') {
            $parts.Add($s)
            continue
        }
        $sb = [System.Text.StringBuilder]::new()
        [void]$sb.Append('"')
        $numBackslashes = 0
        for ($i = 0; $i -lt $s.Length; $i++) {
            $c = $s[$i]
            if ($c -eq '\') {
                $numBackslashes++
            } elseif ($c -eq '"') {
                [void]$sb.Append('\', (2 * $numBackslashes + 1))
                [void]$sb.Append('"')
                $numBackslashes = 0
            } else {
                if ($numBackslashes -gt 0) {
                    [void]$sb.Append('\', $numBackslashes)
                    $numBackslashes = 0
                }
                [void]$sb.Append($c)
            }
        }
        if ($numBackslashes -gt 0) {
            [void]$sb.Append('\', (2 * $numBackslashes))
        }
        [void]$sb.Append('"')
        $parts.Add($sb.ToString())
    }
    return [string]::Join(" ", $parts)
}

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$Executable,
        [Parameter(ValueFromRemainingArguments = $true, Position = 1)]
        [string[]]$Arguments
    )

    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = $Executable
    if ($Arguments -and $Arguments.Count -gt 0) {
        $psi.Arguments = Format-WindowsCommandLine -ArgsList $Arguments
    }
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.CreateNoWindow = $true

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $psi

    try {
        [void]$process.Start()
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()

        $process.WaitForExit()
        [System.Threading.Tasks.Task]::WaitAll($stdoutTask, $stderrTask)

        $stdoutRaw = $stdoutTask.Result
        $stderrRaw = $stderrTask.Result
        $exitCode = $process.ExitCode

        $stdoutLines = if ([string]::IsNullOrEmpty($stdoutRaw)) {
            @()
        } else {
            @($stdoutRaw -split "`r?\n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
        }

        $stderrLines = if ([string]::IsNullOrEmpty($stderrRaw)) {
            @()
        } else {
            @($stderrRaw -split "`r?\n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
        }

        return [PSCustomObject]@{
            ExitCode = $exitCode
            Stdout   = [string[]]$stdoutLines
            Stderr   = [string[]]$stderrLines
            Combined = ($stdoutRaw + $stderrRaw).Trim()
        }
    } finally {
        $process.Dispose()
    }
}

function Invoke-DockerCmd {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )
    return (Invoke-NativeCommand -Executable "docker" -Arguments $Arguments)
}

function Invoke-CleanupResources {
    Write-Host "`n==> Teardown: Stopping isolated test Compose stack & cleaning test resources..." -ForegroundColor Cyan
    $cleanupErrors = [System.Collections.Generic.List[string]]::new()

    # Step 1: Stop and remove temporary receiver container if launch was attempted and container exists
    if ($receiverContainerAttempted) {
        try {
            if ($SimulateFailure -in @("fail-receiver-cleanup", "fail-both")) {
                $cleanupErrors.Add("[SIMULATED] Receiver container removal failed")
                Write-Host "  [SIMULATION] Injected receiver cleanup failure." -ForegroundColor Magenta
            }

            # Retrieve container names using docker ps -a --format '{{.Names}}'
            $psRes = Invoke-DockerCmd "ps", "-a", "--format", "{{.Names}}"
            if ($psRes.ExitCode -ne 0) {
                $cleanupErrors.Add("Failed to list Docker containers during receiver cleanup (exit code $($psRes.ExitCode)): $($psRes.Stderr -join ' ')")
            } else {
                $existing = @($psRes.Stdout | ForEach-Object { $_.Trim() } | Where-Object { $_ -ceq $ReceiverContainerName })
                if ($existing.Count -gt 0) {
                    # Verify ownership label before stopping or removing
                    $inspectRes = Invoke-DockerCmd "inspect", "--format", "{{ index .Config.Labels `"$ReceiverOwnershipKey`" }}", $ReceiverContainerName
                    if ($inspectRes.ExitCode -ne 0) {
                        $cleanupErrors.Add("Failed to inspect labels on container '$ReceiverContainerName' (exit code $($inspectRes.ExitCode)): $($inspectRes.Stderr -join ' ')")
                    } else {
                        $actualLabel = ($inspectRes.Stdout -join "").Trim()
                        if ($actualLabel -ceq $ReceiverOwnershipVal) {
                            $stopRes = Invoke-DockerCmd "stop", $ReceiverContainerName
                            if ($stopRes.ExitCode -ne 0) {
                                $cleanupErrors.Add("Failed to stop receiver container '$ReceiverContainerName' (exit code $($stopRes.ExitCode)): $($stopRes.Stderr -join ' ')")
                            }
                            $rmRes = Invoke-DockerCmd "rm", "-f", $ReceiverContainerName
                            if ($rmRes.ExitCode -ne 0) {
                                $cleanupErrors.Add("Failed to remove receiver container '$ReceiverContainerName' (exit code $($rmRes.ExitCode)): $($rmRes.Stderr -join ' ')")
                            }

                            # Verify absence afterward using exact case-sensitive matching
                            $verRes = Invoke-DockerCmd "ps", "-a", "--format", "{{.Names}}"
                            if ($verRes.ExitCode -ne 0) {
                                $cleanupErrors.Add("Failed to verify receiver container absence (exit code $($verRes.ExitCode)): $($verRes.Stderr -join ' ')")
                            } else {
                                $remaining = @($verRes.Stdout | ForEach-Object { $_.Trim() } | Where-Object { $_ -ceq $ReceiverContainerName })
                                if ($remaining.Count -gt 0) {
                                    $cleanupErrors.Add("Failed to remove temporary receiver container '$ReceiverContainerName'")
                                } else {
                                    Write-Host "  Temporary receiver container '$ReceiverContainerName' (ownership verified) removed." -ForegroundColor Green
                                }
                            }
                        } else {
                            $cleanupErrors.Add("Container '$ReceiverContainerName' exists but ownership label did not match (expected '$ReceiverOwnershipVal', got '$actualLabel'). Skipping removal to prevent deleting unowned container.")
                        }
                    }
                } else {
                    Write-Host "  Temporary receiver container '$ReceiverContainerName' does not exist on Docker daemon (skipped removal)." -ForegroundColor Gray
                }
            }
        } catch {
            $cleanupErrors.Add("Exception during receiver cleanup: $_")
        }
    } else {
        Write-Host "  Temporary receiver container launch was not attempted (skipped removal)." -ForegroundColor Gray
    }

    # Step 2: Stop isolated test Compose stack and remove test volumes
    # Teardown is STRICTLY FAIL-CLOSED:
    # 1. We enumerate ALL containers, networks, and volumes carrying com.docker.compose.project=$composeProject,
    #    plus any resources matching the generated resource names.
    # 2. Every single discovered resource must pass exact ownership and expected service/role label validation.
    # 3. Any listing failure, inspection failure, unexpected resource, or label mismatch sets $teardownBlocked = $true.
    # 4. If $teardownBlocked is $true, destructive teardown ('docker compose down -v') is strictly BLOCKED.
    #    Resources are preserved on the host engine to prevent deleting unowned or colliding resources.
    if ($composeProjectStartupAttempted) {
        try {
            if ($SimulateFailure -eq "fail-compose-restore") {
                $cleanupErrors.Add("[SIMULATED] Isolated Compose teardown failure")
                Write-Host "  [SIMULATION] Injected Compose teardown failure." -ForegroundColor Magenta
            }

            $teardownBlocked = $false
            $unverifiedResources = [System.Collections.Generic.List[string]]::new()
            $verifiedContainers = [System.Collections.Generic.List[string]]::new()
            $verifiedNetworks = [System.Collections.Generic.List[string]]::new()
            $verifiedVolumes = [System.Collections.Generic.List[string]]::new()

            # 1. Comprehensive Container Enumeration & Validation
            # Enumerate ALL containers carrying the project label
            $psByLabelRes = Invoke-DockerCmd "ps", "-a", "--filter", "label=com.docker.compose.project=$composeProject", "--format", "{{.ID}}|{{.Names}}"
            if ($psByLabelRes.ExitCode -ne 0) {
                $cleanupErrors.Add("Failed to list Docker containers by project label (exit code $($psByLabelRes.ExitCode)): $($psByLabelRes.Stderr -join ' ')")
                $teardownBlocked = $true
            }

            # Also enumerate all containers by name to catch any unlabelled name collisions
            $psAllRes = Invoke-DockerCmd "ps", "-a", "--format", "{{.Names}}"
            if ($psAllRes.ExitCode -ne 0) {
                $cleanupErrors.Add("Failed to list all Docker containers (exit code $($psAllRes.ExitCode)): $($psAllRes.Stderr -join ' ')")
                $teardownBlocked = $true
            }

            $allContainersToInspect = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
            if ($psByLabelRes.ExitCode -eq 0) {
                foreach ($line in $psByLabelRes.Stdout) {
                    if ([string]::IsNullOrWhiteSpace($line)) { continue }
                    $cName = ($line -split '\|')[1].Trim()
                    if (-not [string]::IsNullOrEmpty($cName)) { [void]$allContainersToInspect.Add($cName) }
                }
            }
            if ($psAllRes.ExitCode -eq 0) {
                foreach ($cName in $psAllRes.Stdout) {
                    $trimName = $cName.Trim()
                    if ($trimName -cin @($amContainer, $promContainer) -or $trimName.StartsWith("${composeProject}-")) {
                        [void]$allContainersToInspect.Add($trimName)
                    }
                }
            }

            foreach ($cName in $allContainersToInspect) {
                $cInspect = Invoke-DockerCmd "inspect", "--format", "{{ index .Config.Labels `"com.docker.compose.project`" }}|{{ index .Config.Labels `"com.docker.compose.service`" }}", $cName
                if ($cInspect.ExitCode -ne 0) {
                    $cleanupErrors.Add("Failed to inspect container '$cName' (exit code $($cInspect.ExitCode)): $($cInspect.Stderr -join ' ')")
                    $unverifiedResources.Add("container:$cName (inspect failed)")
                    $teardownBlocked = $true
                } else {
                    $parts = ($cInspect.Stdout -join "").Trim() -split '\|'
                    $projLabel = if ($parts.Length -gt 0) { $parts[0] } else { "" }
                    $svcLabel = if ($parts.Length -gt 1) { $parts[1] } else { "" }
                    if ($projLabel -ceq $composeProject -and $svcLabel -in @("alertmanager", "prometheus")) {
                        $verifiedContainers.Add($cName)
                    } else {
                        $unverifiedResources.Add("container:$cName (project='$projLabel', service='$svcLabel')")
                        $teardownBlocked = $true
                    }
                }
            }

            # 2. Comprehensive Network Enumeration & Validation
            # Enumerate ALL networks carrying the project label
            $netByLabelRes = Invoke-DockerCmd "network", "ls", "--filter", "label=com.docker.compose.project=$composeProject", "--format", "{{.Name}}"
            if ($netByLabelRes.ExitCode -ne 0) {
                $cleanupErrors.Add("Failed to list Docker networks by project label (exit code $($netByLabelRes.ExitCode)): $($netByLabelRes.Stderr -join ' ')")
                $teardownBlocked = $true
            }

            # Also enumerate all networks by name
            $netAllRes = Invoke-DockerCmd "network", "ls", "--format", "{{.Name}}"
            if ($netAllRes.ExitCode -ne 0) {
                $cleanupErrors.Add("Failed to list all Docker networks (exit code $($netAllRes.ExitCode)): $($netAllRes.Stderr -join ' ')")
                $teardownBlocked = $true
            }

            $allNetworksToInspect = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
            if ($netByLabelRes.ExitCode -eq 0) {
                foreach ($nName in $netByLabelRes.Stdout) {
                    $trimNet = $nName.Trim()
                    if (-not [string]::IsNullOrEmpty($trimNet)) { [void]$allNetworksToInspect.Add($trimNet) }
                }
            }
            if ($netAllRes.ExitCode -eq 0) {
                foreach ($nName in $netAllRes.Stdout) {
                    $trimNet = $nName.Trim()
                    if ($trimNet -ceq $testNetwork -or $trimNet.StartsWith("${composeProject}_")) {
                        [void]$allNetworksToInspect.Add($trimNet)
                    }
                }
            }

            foreach ($nName in $allNetworksToInspect) {
                $nInspect = Invoke-DockerCmd "network", "inspect", "--format", "{{ index .Labels `"com.docker.compose.project`" }}|{{ index .Labels `"com.docker.compose.network`" }}", $nName
                if ($nInspect.ExitCode -ne 0) {
                    $cleanupErrors.Add("Failed to inspect network '$nName' (exit code $($nInspect.ExitCode)): $($nInspect.Stderr -join ' ')")
                    $unverifiedResources.Add("network:$nName (inspect failed)")
                    $teardownBlocked = $true
                } else {
                    $nParts = ($nInspect.Stdout -join "").Trim() -split '\|'
                    $nProj = if ($nParts.Length -gt 0) { $nParts[0] } else { "" }
                    $nNet = if ($nParts.Length -gt 1) { $nParts[1] } else { "" }
                    if ($nProj -ceq $composeProject -and $nNet -ceq "test_network") {
                        $verifiedNetworks.Add($nName)
                    } else {
                        $unverifiedResources.Add("network:$nName (project='$nProj', network='$nNet')")
                        $teardownBlocked = $true
                    }
                }
            }

            # 3. Comprehensive Volume Enumeration & Validation
            # Enumerate ALL volumes carrying the project label
            $volByLabelRes = Invoke-DockerCmd "volume", "ls", "--filter", "label=com.docker.compose.project=$composeProject", "--format", "{{.Name}}"
            if ($volByLabelRes.ExitCode -ne 0) {
                $cleanupErrors.Add("Failed to list Docker volumes by project label (exit code $($volByLabelRes.ExitCode)): $($volByLabelRes.Stderr -join ' ')")
                $teardownBlocked = $true
            }

            # Also enumerate all volumes by name
            $volAllRes = Invoke-DockerCmd "volume", "ls", "--format", "{{.Name}}"
            if ($volAllRes.ExitCode -ne 0) {
                $cleanupErrors.Add("Failed to list all Docker volumes (exit code $($volAllRes.ExitCode)): $($volAllRes.Stderr -join ' ')")
                $teardownBlocked = $true
            }

            $allVolumesToInspect = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
            if ($volByLabelRes.ExitCode -eq 0) {
                foreach ($vName in $volByLabelRes.Stdout) {
                    $trimVol = $vName.Trim()
                    if (-not [string]::IsNullOrEmpty($trimVol)) { [void]$allVolumesToInspect.Add($trimVol) }
                }
            }
            if ($volAllRes.ExitCode -eq 0) {
                foreach ($vName in $volAllRes.Stdout) {
                    $trimVol = $vName.Trim()
                    if ($trimVol.StartsWith("${composeProject}_") -or $trimVol -cin @("${composeProject}_test_prometheus_data", "${composeProject}_test_alertmanager_data")) {
                        [void]$allVolumesToInspect.Add($trimVol)
                    }
                }
            }

            foreach ($vName in $allVolumesToInspect) {
                $vInspect = Invoke-DockerCmd "volume", "inspect", "--format", "{{ index .Labels `"com.docker.compose.project`" }}|{{ index .Labels `"com.docker.compose.volume`" }}", $vName
                if ($vInspect.ExitCode -ne 0) {
                    $cleanupErrors.Add("Failed to inspect volume '$vName' (exit code $($vInspect.ExitCode)): $($vInspect.Stderr -join ' ')")
                    $unverifiedResources.Add("volume:$vName (inspect failed)")
                    $teardownBlocked = $true
                } else {
                    $vParts = ($vInspect.Stdout -join "").Trim() -split '\|'
                    $vProj = if ($vParts.Length -gt 0) { $vParts[0] } else { "" }
                    $vVol = if ($vParts.Length -gt 1) { $vParts[1] } else { "" }
                    if ($vProj -ceq $composeProject -and $vVol -in @("test_prometheus_data", "test_alertmanager_data")) {
                        $verifiedVolumes.Add($vName)
                    } else {
                        $unverifiedResources.Add("volume:$vName (project='$vProj', volume='$vVol')")
                        $teardownBlocked = $true
                    }
                }
            }

            # 4. Strict Fail-Closed Execution Gate
            if ($teardownBlocked -or $unverifiedResources.Count -gt 0) {
                $unverifiedSummary = if ($unverifiedResources.Count -gt 0) { $unverifiedResources -join ", " } else { "Docker command failures occurred" }
                $cleanupErrors.Add("STRICT FAIL-CLOSED GATE ACTIVATED: Refusing destructive compose down. Unverified resource(s) or inspection failure(s) detected ($unverifiedSummary). Resources preserved on Docker daemon to prevent data loss.")
                Write-Host "  WARNING: Strict fail-closed gate activated: Compose teardown blocked ($unverifiedSummary)." -ForegroundColor Yellow
            } else {
                # Only execute teardown if verified resources were found
                if ($verifiedContainers.Count -gt 0 -or $verifiedNetworks.Count -gt 0 -or $verifiedVolumes.Count -gt 0) {
                    $downRes = Invoke-DockerCmd "compose", "-p", $composeProject, "-f", $isolatedComposeFile, "down", "-v"
                    if ($downRes.ExitCode -ne 0) {
                        $cleanupErrors.Add("docker compose down -v failed with exit code $($downRes.ExitCode): $($downRes.Stderr -join ' ')")
                    } else {
                        Write-Host "  Isolated test Compose stack stopped and test volumes removed (project: $composeProject; verified: $($verifiedContainers.Count) containers, $($verifiedNetworks.Count) networks, $($verifiedVolumes.Count) volumes)." -ForegroundColor Green
                    }
                } else {
                    Write-Host "  No Compose project resources were present on Docker daemon (skipped teardown)." -ForegroundColor Gray
                }
            }
        } catch {
            $cleanupErrors.Add("Exception during isolated Compose teardown: $_")
        }
    } else {
        Write-Host "  Compose project launch was not attempted (skipped teardown)." -ForegroundColor Gray
    }

    # Step 3: Verify isolated test containers and receiver are absent from Docker engine
    if ($composeProjectStartupAttempted) {
        try {
            if ($SimulateFailure -eq "fail-target-verify") {
                $cleanupErrors.Add("[SIMULATED] Isolated resource teardown verification failed")
                Write-Host "  [SIMULATION] Injected teardown verification failure." -ForegroundColor Magenta
            }

            # Retrieve container names using docker ps -a --format '{{.Names}}'
            $verListRes = Invoke-DockerCmd "ps", "-a", "--format", "{{.Names}}"
            if ($verListRes.ExitCode -ne 0) {
                $cleanupErrors.Add("Failed to list Docker containers during isolated teardown verification (exit code $($verListRes.ExitCode)): $($verListRes.Stderr -join ' ')")
            } else {
                $allNames = @($verListRes.Stdout | ForEach-Object { $_.Trim() } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })

                # Exact case-sensitive matching against expected isolated test containers (no substring or regex matching)
                $expectedIsolatedContainers = @($amContainer, $promContainer)
                $lingeringIsolated = @($allNames | Where-Object { $_ -cin $expectedIsolatedContainers })
                if ($lingeringIsolated.Count -gt 0) {
                    $cleanupErrors.Add("Isolated test container(s) still present on Docker daemon: $($lingeringIsolated -join ', ')")
                } else {
                    Write-Host "  Isolated test containers verified absent from Docker daemon ($($expectedIsolatedContainers -join ', '))." -ForegroundColor Green
                }

                # Verify test-only webhook receiver container is absent via exact case-sensitive matching
                if ($receiverContainerAttempted) {
                    $receiverLingering = @($allNames | Where-Object { $_ -ceq $ReceiverContainerName })
                    if ($receiverLingering.Count -gt 0) {
                        $cleanupErrors.Add("Test-only receiver container '$ReceiverContainerName' still present on Docker daemon")
                    } else {
                        Write-Host "  Test-only webhook receiver container is confirmed absent ($ReceiverContainerName)." -ForegroundColor Green
                    }
                }
            }
        } catch {
            $cleanupErrors.Add("Exception verifying isolated teardown: $_")
        }
    }

    # Step 4: Ephemeral secret token cleanup and removal verification
    # Note: Overwriting file contents in place mitigates plaintext residue in volatile caches,
    # but does NOT guarantee physical data destruction on SSD wear-leveling controllers,
    # copy-on-write filesystems, snapshots, or storage backups.
    if ($secretFileCreatedByThisRun -and (Test-Path $secretFile)) {
        try {
            if ($SimulateFailure -eq "fail-secret-cleanup") {
                $cleanupErrors.Add("[SIMULATED] Ephemeral secret file removal failed")
                Write-Host "  [SIMULATION] Injected secret file removal failure." -ForegroundColor Magenta
            } else {
                $currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent()
                $userSid = $currentUser.User.Value
                $grantRes = Invoke-NativeCommand -Executable "icacls.exe" -Arguments @("$secretFile", "/grant", "*${userSid}:(F)")
                if ($grantRes.ExitCode -ne 0) {
                    Write-Host "  Warning: icacls grant full control returned exit code $($grantRes.ExitCode): $($grantRes.Stderr -join ' ')" -ForegroundColor Yellow
                }

                # Best-effort overwrite before deletion
                try {
                    $len = (Get-Item $secretFile).Length
                    if ($len -gt 0) {
                        [System.IO.File]::WriteAllBytes($secretFile, (New-Object byte[] $len))
                    }
                } catch {
                    Write-Host "  Warning: could not overwrite token file prior to deletion: $_" -ForegroundColor Yellow
                }

                Remove-Item -Path $secretFile -Force
            }
        } catch {
            $cleanupErrors.Add("Failed to remove ephemeral secret file '$secretFile': $_")
        }

        # Explicitly verify token file no longer exists
        if (Test-Path $secretFile) {
            $cleanupErrors.Add("Verification failed: Ephemeral secret file '$secretFile' still exists after cleanup")
        } else {
            Write-Host "  Ephemeral token file removal verified (file is absent)." -ForegroundColor Green
        }
    }

    if ($secretDirCreatedByThisRun -and (Test-Path $secretDir)) {
        try {
            $remainingFiles = @(Get-ChildItem -Path $secretDir -Force)
            if ($remainingFiles.Count -eq 0) {
                Remove-Item -Path $secretDir -Force
            } else {
                $cleanupErrors.Add("Secret directory '$secretDir' contains unexpected files; skipped removal to protect unrelated files: $($remainingFiles.Name -join ', ')")
            }
        } catch {
            $cleanupErrors.Add("Failed to remove secret directory '$secretDir': $_")
        }

        # Explicitly verify secret directory no longer exists if it was empty
        if (Test-Path $secretDir) {
            $remainingFiles = @(Get-ChildItem -Path $secretDir -Force)
            if ($remainingFiles.Count -eq 0) {
                $cleanupErrors.Add("Verification failed: Secret directory '$secretDir' still exists after cleanup")
            }
        } else {
            Write-Host "  Ephemeral secret directory removal verified (directory is absent)." -ForegroundColor Green
        }
    }

    if ($cleanupErrors.Count -eq 0) {
        Write-Host "  Isolated test teardown fully verified (all checks passed, 0 errors)." -ForegroundColor Green
    } else {
        foreach ($ce in $cleanupErrors) {
            Write-Host "  Cleanup Error: $ce" -ForegroundColor Red
        }
    }

    return , [string[]]$cleanupErrors.ToArray()
}

# -------------------------------------------------------------------------
# Host-Level Concurrency Lock (Exclusive Host-Wide Named Mutex)
# Enforces strictly single execution across all sessions and elevation levels
# on the host machine to prevent collisions on fixed loopback ports
# (19090, 19093, 19095) and shared filesystem paths.
# -------------------------------------------------------------------------
try {
    # Configure restricted MutexSecurity for user, administrator, and service/CI identities:
    # - Current User: FullControl
    # - Builtin Administrators: FullControl
    # - Local System (services / CI runners): FullControl
    # - Authenticated Users (standard users / cross-session): Synchronize | Modify | ReadPermissions
    # (Enables cross-session lock acquisition, release, and DACL validation, while forbidding TakeOwnership and WriteDac)
    $mSec = [System.Security.AccessControl.MutexSecurity]::new()
    $currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $userSid = $currentUser.User
    $adminSid = [System.Security.Principal.SecurityIdentifier]::new([System.Security.Principal.WellKnownSidType]::BuiltinAdministratorsSid, $null)
    $systemSid = [System.Security.Principal.SecurityIdentifier]::new([System.Security.Principal.WellKnownSidType]::LocalSystemSid, $null)
    $authUsersSid = [System.Security.Principal.SecurityIdentifier]::new([System.Security.Principal.WellKnownSidType]::AuthenticatedUserSid, $null)

    $mSec.AddAccessRule([System.Security.AccessControl.MutexAccessRule]::new($userSid, [System.Security.AccessControl.MutexRights]::FullControl, [System.Security.AccessControl.AccessControlType]::Allow))
    $mSec.AddAccessRule([System.Security.AccessControl.MutexAccessRule]::new($adminSid, [System.Security.AccessControl.MutexRights]::FullControl, [System.Security.AccessControl.AccessControlType]::Allow))
    $mSec.AddAccessRule([System.Security.AccessControl.MutexAccessRule]::new($systemSid, [System.Security.AccessControl.MutexRights]::FullControl, [System.Security.AccessControl.AccessControlType]::Allow))

    $authRights = [System.Security.AccessControl.MutexRights]::Synchronize -bor [System.Security.AccessControl.MutexRights]::Modify -bor [System.Security.AccessControl.MutexRights]::ReadPermissions
    $mSec.AddAccessRule([System.Security.AccessControl.MutexAccessRule]::new($authUsersSid, $authRights, [System.Security.AccessControl.AccessControlType]::Allow))

    $createdNew = $false
    $mutex = [System.Threading.Mutex]::new($false, $mutexName, [ref]$createdNew, $mSec)

    if (-not $createdNew -and $null -ne $mutex) {
        # Inspect and validate effective access on the opened mutex
        if (-not (Test-MutexSecurityEffectiveAccess -MutexHandle $mutex -MutexIdentifier $mutexName)) {
            Write-Error "Existing host mutex '$mutexName' failed security validation. Aborting safely."
            $mutex.Dispose()
            exit 1
        }
    }
} catch [System.UnauthorizedAccessException] {
    # Fallback if mutex already exists with an established DACL
    try {
        $reqRights = [System.Security.AccessControl.MutexRights]::Synchronize -bor [System.Security.AccessControl.MutexRights]::Modify -bor [System.Security.AccessControl.MutexRights]::ReadPermissions
        $mutex = [System.Threading.Mutex]::OpenExisting($mutexName, $reqRights)

        # Inspect and validate effective access on the opened mutex
        if (-not (Test-MutexSecurityEffectiveAccess -MutexHandle $mutex -MutexIdentifier $mutexName)) {
            Write-Error "Existing host mutex '$mutexName' failed security validation. Aborting safely."
            if ($null -ne $mutex) { $mutex.Dispose() }
            exit 1
        }
    } catch {
        Write-Error "Failed to open or validate existing host mutex '$mutexName': $_"
        exit 1
    }
} catch {
    Write-Error "Failed to initialize host mutex '$mutexName': $_"
    exit 1
}

try {
    $mutexAcquired = $mutex.WaitOne(0, $false)
} catch [System.Threading.AbandonedMutexException] {
    # A previous process terminated abnormally without releasing the mutex.
    # The mutex is now acquired by the current thread.
    $mutexAcquired = $true
} catch {
    Write-Error "Failed to acquire host mutex '$mutexName': $_"
    if ($null -ne $mutex) { $mutex.Dispose() }
    exit 1
}

if (-not $mutexAcquired) {
    Write-Error "Another AegisEdge lifecycle test execution is currently running and holds the host lock ('$mutexName'). Aborting safely without modifying shared resources."
    if ($null -ne $mutex) { $mutex.Dispose() }
    exit 1
}

Write-Host "Host concurrency lock acquired ($mutexName)." -ForegroundColor Green

# -------------------------------------------------------------------------
# Protected Lifecycle Block
# Guaranteed execution inside try/finally ensures the host mutex is released
# and test resources are cleaned up on ANY exit path, failure, or cancellation.
# -------------------------------------------------------------------------
try {
    Write-Host "Execution Run IDs: Main=$MainRunId, Restart=$RestartRunId, Inhibit=$InhibitRunId" -ForegroundColor Cyan
    Write-Host "Compose Project Name: $composeProject" -ForegroundColor Cyan
    Write-Host "Isolated Containers: Alertmanager=$amContainer, Prometheus=$promContainer" -ForegroundColor Cyan
    Write-Host "Isolated Network: $testNetwork" -ForegroundColor Cyan
    Write-Host "Temporary Receiver Container: $ReceiverContainerName" -ForegroundColor Cyan

    # 1. Check Docker daemon
    Write-Host "`n==> 1. Checking Docker daemon..." -ForegroundColor Yellow
    $infoRes = Invoke-DockerCmd "info"
    if ($infoRes.ExitCode -ne 0) {
        throw "Docker daemon is unavailable (exit code $($infoRes.ExitCode)). This test requires Docker Desktop."
    }
    Write-Host "Docker daemon is active." -ForegroundColor Green

    # 2. Build Linux webhook-receiver binary
    Write-Host "`n==> 2. Building webhook-receiver Linux binary..." -ForegroundColor Yellow
    if (-not (Test-Path "bin")) {
        New-Item -ItemType Directory -Path "bin" -Force | Out-Null
    }
    $origCgo = $env:CGO_ENABLED
    $origGoos = $env:GOOS
    $origGoarch = $env:GOARCH
    try {
        $env:CGO_ENABLED = "0"
        $env:GOOS = "linux"
        $env:GOARCH = "amd64"
        $goRes = Invoke-NativeCommand -Executable "go" -Arguments @("build", "-o", "bin/webhook-receiver", "./services/control-plane/cmd/webhook-receiver")
        if ($goRes.ExitCode -ne 0) {
            throw "Failed to compile webhook-receiver binary (exit code $($goRes.ExitCode)): $($goRes.Stderr -join ' ')"
        }
        Write-Host "webhook-receiver binary ready." -ForegroundColor Green
    } finally {
        $env:CGO_ENABLED = $origCgo
        $env:GOOS = $origGoos
        $env:GOARCH = $origGoarch
    }

    # 2.5 Generate CSPRNG ephemeral token and configure secure test secret file
    Write-Host "`n==> 2.5 Configuring ephemeral test secret..." -ForegroundColor Yellow

    # Verify no pre-existing secret file exists (protect pre-existing secret files)
    if (Test-Path $secretFile) {
        throw "Pre-existing secret file found at '$secretFile'. Aborting safely without modifying or deleting pre-existing secret file."
    }

    # Verify secret directory does not contain unexpected files
    if (Test-Path $secretDir) {
        $existingItems = @(Get-ChildItem -Path $secretDir -Force)
        if ($existingItems.Count -gt 0) {
            throw "Pre-existing files found in secret directory '$secretDir' ($($existingItems.Name -join ', ')). Aborting safely to protect unexpected files."
        }
    } else {
        New-Item -ItemType Directory -Path $secretDir -Force | Out-Null
        $secretDirCreatedByThisRun = $true
    }

    $tokenBytes = New-Object byte[] 32
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($tokenBytes)
    # Standard Base64 without padding (RFC 4648 § 4, exactly 43 characters for 32 bytes)
    $ephemeralToken = [Convert]::ToBase64String($tokenBytes).TrimEnd('=')

    if ($ephemeralToken.Length -ne 43) {
        throw "Ephemeral token generation error: expected 43 characters, got $($ephemeralToken.Length)"
    }

    $currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $userSid = $currentUser.User.Value

    # Write raw ASCII bytes using FileMode::CreateNew to atomically fail if the file already exists.
    # Ownership flag ($secretFileCreatedByThisRun = $true) is established immediately upon file creation
    # so that any subsequent partial-write error triggers cleanup of the created file.
    $rawTokenBytes = [System.Text.Encoding]::ASCII.GetBytes($ephemeralToken)
    $secretFileStream = [System.IO.File]::Open($secretFile, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    $secretFileCreatedByThisRun = $true
    try {
        $secretFileStream.Write($rawTokenBytes, 0, $rawTokenBytes.Length)
        $secretFileStream.Flush()
    } finally {
        $secretFileStream.Dispose()
    }

    # Simulated failure during secret configuration (after file is created on disk)
    if ($SimulateFailure -eq "fail-secret-creation") {
        throw "[SIMULATED] Secret configuration failed during verification"
    }

    # Verify written bytes: exactly 43 bytes, no BOM, content matches
    $readBytes = [System.IO.File]::ReadAllBytes($secretFile)
    if ($readBytes.Length -ne 43) {
        throw "Secret file verification failed: expected 43 bytes, got $($readBytes.Length)"
    }
    if ($readBytes[0] -eq 0xEF -and $readBytes[1] -eq 0xBB -and $readBytes[2] -eq 0xBF) {
        throw "Secret file contains UTF-8 BOM"
    }
    $readString = [System.Text.Encoding]::ASCII.GetString($readBytes)
    if ($readString -ne $ephemeralToken) {
        throw "Secret file content does not match generated token"
    }

    # Set restrictive Windows ACL using current user SID
    $aclRes = Invoke-NativeCommand -Executable "icacls.exe" -Arguments @("$secretFile", "/inheritance:r", "/grant:r", "*${userSid}:(R)")
    if ($aclRes.ExitCode -ne 0) {
        throw "Failed to set restrictive ACL on ephemeral token file (exit code $($aclRes.ExitCode)): $($aclRes.Stderr -join ' ')"
    }
    Write-Host "Ephemeral token file configured with restrictive user SID ACL (*$userSid)." -ForegroundColor Green

    $authHeaders = @{ Authorization = "Bearer $ephemeralToken" }

    # 3. Start Compose stack with isolated test configuration override
    Write-Host "`n==> 3. Preflight check and starting Prometheus & Alertmanager ($composeProject)..." -ForegroundColor Yellow

    # Preflight ownership verification: verify no conflicting resources exist before starting Compose stack
    # 1. Check all Docker networks
    $allNetworksRes = Invoke-DockerCmd "network", "ls", "--format", "{{.Name}}"
    if ($allNetworksRes.ExitCode -ne 0) {
        throw "Preflight failed: cannot retrieve Docker networks (exit code $($allNetworksRes.ExitCode)): $($allNetworksRes.Stderr -join ' ')"
    }
    $allNetworkNames = @($allNetworksRes.Stdout | ForEach-Object { $_.Trim() } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($allNetworkNames -cin @($testNetwork)) {
        throw "Preflight ownership check failed: network '$testNetwork' already exists on Docker daemon. Aborting safely without modifying existing resources."
    }

    # 2. Check all Docker volumes
    $allVolumesRes = Invoke-DockerCmd "volume", "ls", "--format", "{{.Name}}"
    if ($allVolumesRes.ExitCode -ne 0) {
        throw "Preflight failed: cannot retrieve Docker volumes (exit code $($allVolumesRes.ExitCode)): $($allVolumesRes.Stderr -join ' ')"
    }
    $allVolumeNames = @($allVolumesRes.Stdout | ForEach-Object { $_.Trim() } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $expectedVolumes = @(
        "${composeProject}_test_prometheus_data",
        "${composeProject}_test_alertmanager_data"
    )
    $conflictingVolumes = @($allVolumeNames | Where-Object { $_ -cin $expectedVolumes })
    if ($conflictingVolumes.Count -gt 0) {
        throw "Preflight ownership check failed: volume(s) already exist for project '$composeProject' ($($conflictingVolumes -join ', ')). Aborting safely without modifying existing resources."
    }

    # 3. Check all Docker containers
    $allContainersRes = Invoke-DockerCmd "ps", "-a", "--format", "{{.Names}}"
    if ($allContainersRes.ExitCode -ne 0) {
        throw "Preflight failed: cannot retrieve Docker containers (exit code $($allContainersRes.ExitCode)): $($allContainersRes.Stderr -join ' ')"
    }
    $allContainerNames = @($allContainersRes.Stdout | ForEach-Object { $_.Trim() } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $expectedContainers = @($amContainer, $promContainer)
    $conflictingContainers = @($allContainerNames | Where-Object { $_ -cin $expectedContainers })
    if ($conflictingContainers.Count -gt 0) {
        throw "Preflight ownership check failed: container(s) already exist for project '$composeProject' ($($conflictingContainers -join ', ')). Aborting safely without modifying existing resources."
    }

    # Also check if any container carries compose project label for this project
    $composeProjectContainersRes = Invoke-DockerCmd "ps", "-a", "--filter", "label=com.docker.compose.project=$composeProject", "--format", "{{.Names}}"
    if ($composeProjectContainersRes.ExitCode -ne 0) {
        throw "Preflight failed: cannot query Docker containers by Compose project label (exit code $($composeProjectContainersRes.ExitCode)): $($composeProjectContainersRes.Stderr -join ' ')"
    }
    if ($composeProjectContainersRes.Stdout.Count -gt 0) {
        throw "Preflight ownership check failed: container(s) with label com.docker.compose.project=$composeProject already exist ($($composeProjectContainersRes.Stdout -join ', ')). Aborting safely without modifying existing resources."
    }

    # Preflight passed: zero conflicting networks, volumes, or containers exist
    $composeProjectPreflightPassed = $true
    Write-Host "  Preflight passed: zero conflicting networks, volumes, or containers for '$composeProject'." -ForegroundColor Green

    $composeProjectStartupAttempted = $true
    $upRes = Invoke-DockerCmd "compose", "-p", $composeProject, "-f", $isolatedComposeFile, "up", "-d"
    if ($upRes.ExitCode -ne 0) {
        throw "Failed to start docker-compose stack with isolated test configuration (exit code $($upRes.ExitCode)): $($upRes.Stderr -join ' ')"
    }
    $composeProjectStarted = $true

    # Flush Alertmanager to clear any residual active alerts from prior aborted runs
    $flushRes = Invoke-DockerCmd "restart", $amContainer
    if ($flushRes.ExitCode -ne 0) {
        throw "Failed to restart Alertmanager container '$amContainer' (exit code $($flushRes.ExitCode)): $($flushRes.Stderr -join ' ')"
    }

    # 4. Start temporary webhook-receiver container on Compose network with unique name and ownership label
    Write-Host "`n==> 4. Starting temporary webhook-receiver container '$ReceiverContainerName' on $testNetwork..." -ForegroundColor Yellow
    
    # Preflight check for receiver container name
    $recvPreflightRes = Invoke-DockerCmd "ps", "-a", "--format", "{{.Names}}"
    if ($recvPreflightRes.ExitCode -ne 0) {
        throw "Failed to query Docker containers during receiver preflight (exit code $($recvPreflightRes.ExitCode)): $($recvPreflightRes.Stderr -join ' ')"
    }
    $existingRecv = @($recvPreflightRes.Stdout | Where-Object { $_ -ceq $ReceiverContainerName })
    if ($existingRecv.Count -gt 0) {
        throw "Preflight ownership check failed: receiver container name '$ReceiverContainerName' already exists on Docker engine. Aborting without modifying existing container."
    }
    $receiverContainerAttempted = $true

    $pwdPath = (Get-Item -Path ".").FullName
    $runRes = Invoke-DockerCmd "run", "-d", `
        "--name", $ReceiverContainerName, `
        "--label", "${ReceiverOwnershipKey}=${ReceiverOwnershipVal}", `
        "--network", $testNetwork, `
        "--network-alias", "webhook-receiver", `
        "-p", "127.0.0.1:${recvPort}:9095", `
        "-v", "${pwdPath}\bin\webhook-receiver:/webhook-receiver:ro", `
        "-v", "${secretFile}:/etc/webhook-receiver/secrets/token:ro", `
        "--entrypoint", "/webhook-receiver", `
        "prom/alertmanager:v0.28.1", `
        "-auth-token-file", "/etc/webhook-receiver/secrets/token"

    if ($runRes.ExitCode -ne 0) {
        throw "Failed to launch webhook-receiver container '$ReceiverContainerName' (exit code $($runRes.ExitCode)): $($runRes.Stderr -join ' ')"
    }
    $receiverContainerStarted = $true

    # Verify secret token mount is strictly read-only
    $touchRes = Invoke-DockerCmd "exec", $ReceiverContainerName, "touch", "/etc/webhook-receiver/secrets/token"
    $touchOutput = $touchRes.Combined
    if ($touchOutput -notmatch "Read-only file system") {
        throw "Secret file is NOT mounted read-only: $touchOutput"
    }
    Write-Host "  Secret token file verified mounted read-only (:ro)." -ForegroundColor Green

    # Simulate launch reporting failure after container was created on Docker engine
    if ($SimulateFailure -eq "fail-partial-receiver-startup") {
        throw "[SIMULATED] Docker receiver container launch reported failure after container creation"
    }

    # Check for simulated test failure (after receiver created so receiver cleanup is also exercised)
    if ($SimulateFailure -in @("fail-test", "fail-both")) {
        throw "[SIMULATED] Test execution failure injected for cleanup verification"
    }

    # If simulating cleanup failure on running stack without executing full scenarios:
    if ($SimulateFailure -in @("fail-receiver-cleanup", "fail-compose-restore", "fail-target-verify", "fail-secret-cleanup")) {
        Write-Host "`n==> [SIMULATION] Skipping alert test scenarios to execute teardown failure mode '$SimulateFailure'..." -ForegroundColor Magenta
        return
    }

    # 5. Wait for readiness
    Write-Host "`n==> 5. Waiting for services readiness..." -ForegroundColor Yellow
    $ready = $false
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    while ($stopwatch.Elapsed.TotalSeconds -lt 30) {
        try {
            $promResp = Invoke-WebRequest -Uri "http://127.0.0.1:${promPort}/-/ready" -UseBasicParsing -TimeoutSec 2
            $amResp = Invoke-WebRequest -Uri "http://127.0.0.1:${amPort}/-/ready" -UseBasicParsing -TimeoutSec 2
            $recvResp = Invoke-WebRequest -Uri "http://127.0.0.1:${recvPort}/health" -UseBasicParsing -TimeoutSec 2
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

    # Verify unauthenticated request to /api/events is rejected with HTTP 401
    try {
        $unauthResp = Invoke-WebRequest -Uri "http://127.0.0.1:${recvPort}/api/events" -UseBasicParsing -TimeoutSec 2
        throw "Expected unauthenticated request to /api/events to fail with 401, but got HTTP $($unauthResp.StatusCode)"
    } catch {
        $statusCode = $null
        if ($_.Exception -and $_.Exception.Response) {
            $statusCode = [int]$_.Exception.Response.StatusCode
        }
        if ($statusCode -eq 401) {
            Write-Host "  Unauthenticated request to /api/events successfully rejected with HTTP 401 Unauthorized." -ForegroundColor Green
        } else {
            throw "Unexpected error testing unauthenticated request to /api/events: $_"
        }
    }

    # 6. Verify Prometheus active Alertmanager discovery
    Write-Host "`n==> 6. Verifying Prometheus active Alertmanager discovery..." -ForegroundColor Yellow
    $amDiscovered = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt 20) {
        try {
            $amResp = Invoke-RestMethod -Uri "http://127.0.0.1:${promPort}/api/v1/alertmanagers"
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
            $targets = Invoke-RestMethod -Uri "http://127.0.0.1:${promPort}/api/v1/targets"
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
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders -Method Delete | Out-Null
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/trigger?val=1&run_id=$MainRunId" -Headers $authHeaders -Method Post | Out-Null

    Write-Host "  Waiting for Prometheus rule AegisEdgeE2ELifecycleWarning to fire..." -ForegroundColor Cyan
    $ruleFiring = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $rulesResp = Invoke-RestMethod -Uri "http://127.0.0.1:${promPort}/api/v1/rules"
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
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
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
        $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
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
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/trigger?val=0&run_id=$MainRunId" -Headers $authHeaders -Method Post | Out-Null

    Write-Host "  Waiting for resolved webhook delivery correlated with firing fingerprint $firingFingerprint..." -ForegroundColor Cyan
    $warnResolvedEvent = $null
    $rAlert = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
            foreach ($ev in $events) {
                if ($ev.route -eq "warning" -and
                    $ev.payload.status -eq "resolved" -and
                    $ev.payload.receiver -eq "warning-receiver") {
                    
                    foreach ($candAlert in $ev.payload.alerts) {
                        if ($candAlert.fingerprint -eq $firingFingerprint -and
                            $candAlert.status -eq "resolved" -and
                            $candAlert.labels.alertname -eq "AegisEdgeE2ELifecycleWarning" -and
                            $candAlert.labels.severity -eq "warning" -and
                            $candAlert.labels.tier -eq "integration-test" -and
                            $candAlert.labels.test_run_id -eq $MainRunId -and
                            $candAlert.annotations.summary -eq "E2E lifecycle test warning alert" -and
                            $candAlert.annotations.description -eq "Integration test alert for Prometheus -> Alertmanager -> webhook delivery") {

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
        $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
        Write-Host "  Events recorded in receiver at timeout ($($events.Count)):" -ForegroundColor Red
        $events | ForEach-Object { Write-Host "    - route: $($_.route), receiver: $($_.payload.receiver), status: $($_.payload.status), alerts: $($_.payload.alerts.Count)" -ForegroundColor Red }
        throw "Webhook receiver did not receive strictly correlated resolved warning alert payload for fingerprint $firingFingerprint (run_id $MainRunId)."
    }

    Write-Host "  Correlated resolved notification verified (Fingerprint: $($rAlert.fingerprint), EndsAt ($($rAlert.endsAt)) > StartsAt ($($rAlert.startsAt)))." -ForegroundColor Green

    # =========================================================================
    # Scenario B: Critical Alert Routing (Severity Matching) & Resolution
    # =========================================================================
    Write-Host "`n==> 9. Testing Critical Alert Routing (/alerts/critical)..." -ForegroundColor Yellow
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders -Method Delete | Out-Null
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/trigger?val=2&run_id=$MainRunId" -Headers $authHeaders -Method Post | Out-Null

    Write-Host "  Waiting for Prometheus rule AegisEdgeE2ELifecycleCritical to fire..." -ForegroundColor Cyan
    $critFiring = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $rulesResp = Invoke-RestMethod -Uri "http://127.0.0.1:${promPort}/api/v1/rules"
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
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
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
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/trigger?val=0&run_id=$MainRunId" -Headers $authHeaders -Method Post | Out-Null

    Write-Host "  Waiting for resolved webhook delivery on /alerts/critical..." -ForegroundColor Cyan
    $critResolvedEvent = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
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
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders -Method Delete | Out-Null
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/trigger?val=3&run_id=$MainRunId" -Headers $authHeaders -Method Post | Out-Null

    Write-Host "  Waiting for rule AegisEdgeE2ELifecycleDefaultFallback to fire..." -ForegroundColor Cyan
    $fallbackFiring = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $rulesResp = Invoke-RestMethod -Uri "http://127.0.0.1:${promPort}/api/v1/rules"
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
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
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
        $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
        Write-Host "  Events recorded in receiver at timeout ($($events.Count)):" -ForegroundColor Red
        $events | ForEach-Object { Write-Host "    - route: $($_.route), receiver: $($_.payload.receiver), status: $($_.payload.status), alerts: $($_.payload.alerts.Count)" -ForegroundColor Red }
        throw "Webhook receiver did not receive unmatched severity alert on /alerts/default."
    }
    Write-Host "  Unmatched severity alert fell back to receiver: $($defaultEvent.payload.receiver) (Fingerprint: $defaultFingerprint)" -ForegroundColor Green

    # Resolve fallback alert
    Write-Host "  Triggering fallback alert resolution (val=0, run_id=$MainRunId)..." -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/trigger?val=0&run_id=$MainRunId" -Headers $authHeaders -Method Post | Out-Null

    Write-Host "  Waiting for resolved webhook delivery on /alerts/default..." -ForegroundColor Cyan
    $defaultResolvedEvent = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
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
    # Scenario D: Strict Alert Grouping & Grouped Resolution Verification
    # =========================================================================
    Write-Host "`n==> 11. Testing Alert Grouping Behavior (Multi-instance Alert Batching)..." -ForegroundColor Yellow
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders -Method Delete | Out-Null
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/trigger?val=1&instances=2&run_id=$MainRunId" -Headers $authHeaders -Method Post | Out-Null

    Write-Host "  Waiting for multi-instance grouped webhook delivery on /alerts/warning..." -ForegroundColor Cyan
    $groupedEvent = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
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

    # Strict Grouped Resolution Verification
    Write-Host "  Resolving multi-instance alert (val=0, instances=2, run_id=$MainRunId)..." -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/trigger?val=0&instances=2&run_id=$MainRunId" -Headers $authHeaders -Method Post | Out-Null

    Write-Host "  Waiting for strictly correlated resolved webhook delivery for grouped alerts on /alerts/warning..." -ForegroundColor Cyan
    $groupedResolvedEvent = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
            foreach ($ev in $events) {
                if ($ev.route -eq "warning" -and
                    $ev.payload.status -eq "resolved" -and
                    $ev.payload.receiver -eq "warning-receiver") {
                    
                    if ($ev.payload.alerts.Count -eq 2) {
                        $resolvedNodes = @()
                        $allMatch = $true

                        foreach ($ra in $ev.payload.alerts) {
                            if ($ra.status -ne "resolved" -or
                                $ra.labels.alertname -ne "AegisEdgeE2ELifecycleWarning" -or
                                $ra.labels.severity -ne "warning" -or
                                $ra.labels.tier -ne "integration-test" -or
                                $ra.labels.test_run_id -ne $MainRunId) {
                                $allMatch = $false
                                break
                            }

                            $node = $ra.labels.node
                            $resolvedNodes += $node
                            if (-not $firingFingerprintsByNode.ContainsKey($node) -or
                                $ra.fingerprint -ne $firingFingerprintsByNode[$node]) {
                                $allMatch = $false
                                break
                            }

                            $rStartsAt = [DateTime]::Parse($ra.startsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                            $rEndsAt = [DateTime]::Parse($ra.endsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                            if ($rEndsAt.Year -lt 2024 -or $rEndsAt -le $rStartsAt) {
                                $allMatch = $false
                                break
                            }
                        }

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
        $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
        Write-Host "  Events recorded in receiver at timeout ($($events.Count)):" -ForegroundColor Red
        $events | ForEach-Object { Write-Host "    - route: $($_.route), receiver: $($_.payload.receiver), status: $($_.payload.status), alerts: $($_.payload.alerts.Count)" -ForegroundColor Red }
        throw "Webhook receiver did not receive strictly correlated grouped resolution payload with 2 instances ('node-a' and 'node-b')."
    }
    Write-Host "  Multi-instance grouped alert resolution verified with exact fingerprint matching per node." -ForegroundColor Green

    # =========================================================================
    # Scenario E: Alertmanager Restart & Real Post-Restart Delivery
    # =========================================================================
    Write-Host "`n==> 12. Testing Alertmanager Restart Resilience & Post-Restart Delivery (RunID=$RestartRunId)..." -ForegroundColor Yellow
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders -Method Delete | Out-Null

    # Restart Alertmanager
    $restartRes = Invoke-DockerCmd "restart", $amContainer
    if ($restartRes.ExitCode -ne 0) {
        throw "docker restart $amContainer failed with exit code $($restartRes.ExitCode): $($restartRes.Stderr -join ' ')"
    }

    # Wait for Alertmanager readiness
    $amReady = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt 20) {
        try {
            $resp = Invoke-WebRequest -Uri "http://127.0.0.1:${amPort}/-/ready" -UseBasicParsing -TimeoutSec 2
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
            $amResp = Invoke-RestMethod -Uri "http://127.0.0.1:${promPort}/api/v1/alertmanagers"
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
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/trigger?val=1&run_id=$RestartRunId" -Headers $authHeaders -Method Post | Out-Null

    # Wait for rule to fire in Prometheus
    $restartRuleFiring = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $rulesResp = Invoke-RestMethod -Uri "http://127.0.0.1:${promPort}/api/v1/rules"
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
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
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
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/trigger?val=0&run_id=$RestartRunId" -Headers $authHeaders -Method Post | Out-Null

    # Wait for strictly correlated resolved notification
    $restartResolvedEvent = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
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

    # =========================================================================
    # Scenario F: Simultaneous Alertmanager Inhibition Verification
    # =========================================================================
    Write-Host "`n==> 13. Testing Alertmanager Simultaneous Alert Inhibition (Scenario F, RunID=$InhibitRunId)..." -ForegroundColor Yellow
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders -Method Delete | Out-Null

    # Trigger simultaneous critical + warning + unrelated warning
    Write-Host "  Triggering simultaneous alerts for inhibition test (val=1, run_id=$InhibitRunId)..." -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/trigger/inhibition?val=1&run_id=$InhibitRunId" -Headers $authHeaders -Method Post | Out-Null

    # 1. Verify all 3 intended alerts fire in Prometheus
    Write-Host "  Waiting for rules to fire in Prometheus..." -ForegroundColor Cyan
    $critFiringProm = $false
    $warnFiringProm = $false
    $unrelFiringProm = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $rulesResp = Invoke-RestMethod -Uri "http://127.0.0.1:${promPort}/api/v1/rules"
            foreach ($group in $rulesResp.data.groups) {
                if ($group.name -eq "aegisedge-lifecycle-test") {
                    foreach ($rule in $group.rules) {
                        if ($rule.state -eq "firing" -and $rule.alerts) {
                            foreach ($al in $rule.alerts) {
                                if ($al.labels.test_run_id -eq $InhibitRunId -and $al.state -eq "firing") {
                                    if ($rule.name -eq "AegisEdgeE2ELifecycleCritical") { $critFiringProm = $true }
                                    if ($rule.name -eq "AegisEdgeE2ELifecycleWarning") { $warnFiringProm = $true }
                                    if ($rule.name -eq "AegisEdgeE2EUnrelatedWarning") { $unrelFiringProm = $true }
                                }
                            }
                        }
                    }
                }
            }
            if ($critFiringProm -and $warnFiringProm -and $unrelFiringProm) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not ($critFiringProm -and $warnFiringProm -and $unrelFiringProm)) {
        throw "Not all 3 inhibition rules fired in Prometheus within $TimeoutSeconds s for run_id $InhibitRunId (Crit=$critFiringProm, Warn=$warnFiringProm, Unrel=$unrelFiringProm)."
    }
    Write-Host "  Prometheus: AegisEdgeE2ELifecycleCritical, AegisEdgeE2ELifecycleWarning, and AegisEdgeE2EUnrelatedWarning are all FIRING for run_id $InhibitRunId." -ForegroundColor Green

    # 2. Query Alertmanager /api/v2/alerts to verify inhibition state:
    # - AegisEdgeE2ELifecycleWarning is suppressed and inhibitedBy identifies critical alert fingerprint
    # - AegisEdgeE2EUnrelatedWarning remains active and is not inhibited
    # - AegisEdgeE2ELifecycleCritical remains active
    Write-Host "  Querying Alertmanager /api/v2/alerts to verify inhibition state..." -ForegroundColor Cyan
    $amStateVerified = $false
    $expectedCriticalFingerprint = $null
    $lifecycleWarningFingerprint = $null
    $unrelatedWarningFingerprint = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt 20) {
        try {
            $amAlerts = Invoke-RestMethod -Uri "http://127.0.0.1:${amPort}/api/v2/alerts"
            $critAlert = $null
            $warnAlert = $null
            $unrelAlert = $null

            foreach ($al in $amAlerts) {
                if ($al.labels.test_run_id -eq $InhibitRunId) {
                    if ($al.labels.alertname -eq "AegisEdgeE2ELifecycleCritical") { $critAlert = $al }
                    if ($al.labels.alertname -eq "AegisEdgeE2ELifecycleWarning") { $warnAlert = $al }
                    if ($al.labels.alertname -eq "AegisEdgeE2EUnrelatedWarning") { $unrelAlert = $al }
                }
            }

            if ($critAlert -ne $null -and $warnAlert -ne $null -and $unrelAlert -ne $null) {
                $expectedCriticalFingerprint = $critAlert.fingerprint
                $lifecycleWarningFingerprint = $warnAlert.fingerprint
                $unrelatedWarningFingerprint = $unrelAlert.fingerprint

                $critIsActive = ($critAlert.status -ne $null -and $critAlert.status.state -eq "active")
                $warnIsSuppressed = ($warnAlert.status -ne $null -and $warnAlert.status.state -eq "suppressed" -and
                                     $warnAlert.status.inhibitedBy -ne $null -and $warnAlert.status.inhibitedBy -contains $expectedCriticalFingerprint)
                $unrelIsActive = ($unrelAlert.status -ne $null -and $unrelAlert.status.state -eq "active" -and
                                  ($unrelAlert.status.inhibitedBy -eq $null -or $unrelAlert.status.inhibitedBy.Count -eq 0))

                if ($critIsActive -and $warnIsSuppressed -and $unrelIsActive) {
                    $amStateVerified = $true
                    Write-Host "  Alertmanager confirms:" -ForegroundColor Green
                    Write-Host "    - AegisEdgeE2ELifecycleCritical is active (Fingerprint: $expectedCriticalFingerprint)" -ForegroundColor Green
                    Write-Host "    - AegisEdgeE2ELifecycleWarning is SUPPRESSED, inhibitedBy: $($warnAlert.status.inhibitedBy -join ', ')" -ForegroundColor Green
                    Write-Host "    - AegisEdgeE2EUnrelatedWarning is ACTIVE and NOT inhibited (Fingerprint: $unrelatedWarningFingerprint)" -ForegroundColor Green
                    break
                }
            }
        } catch {}
        Start-Sleep -Milliseconds 500
    }
    if (-not $amStateVerified) {
        throw "Alertmanager /api/v2/alerts verification failed for run_id ${InhibitRunId}: expected AegisEdgeE2ELifecycleWarning to be suppressed by critical fingerprint '$expectedCriticalFingerprint' and AegisEdgeE2EUnrelatedWarning to remain active."
    }

    # 3. Verify webhook deliveries for firing alerts:
    # - Critical alert reaches critical-receiver on /alerts/critical
    # - Unrelated warning alert reaches warning-receiver on /alerts/warning
    # - Inhibited lifecycle warning is NOT delivered
    Write-Host "  Waiting for firing webhook deliveries on receiver..." -ForegroundColor Cyan
    $critFiringEvent = $null
    $unrelFiringEvent = $null
    $inhibitedDelivered = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
            foreach ($ev in $events) {
                if ($ev.payload.status -eq "firing") {
                    foreach ($al in $ev.payload.alerts) {
                        if ($al.labels.test_run_id -eq $InhibitRunId -and $al.status -eq "firing") {
                            if ($al.labels.alertname -eq "AegisEdgeE2ELifecycleCritical" -and
                                $ev.route -eq "critical" -and $ev.payload.receiver -eq "critical-receiver" -and
                                $al.fingerprint -eq $expectedCriticalFingerprint) {
                                $critFiringEvent = $ev
                            }
                            if ($al.labels.alertname -eq "AegisEdgeE2EUnrelatedWarning" -and
                                $ev.route -eq "warning" -and $ev.payload.receiver -eq "warning-receiver" -and
                                $al.fingerprint -eq $unrelatedWarningFingerprint) {
                                $unrelFiringEvent = $ev
                            }
                            if ($al.labels.alertname -eq "AegisEdgeE2ELifecycleWarning") {
                                $inhibitedDelivered = $true
                            }
                        }
                    }
                }
            }
            if ($critFiringEvent -and $unrelFiringEvent) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }

    if (-not $critFiringEvent) {
        $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
        Write-Host "  Events recorded in receiver at timeout ($($events.Count)):" -ForegroundColor Red
        $events | ForEach-Object { Write-Host "    - route: $($_.route), receiver: $($_.payload.receiver), status: $($_.payload.status), alerts: $($_.payload.alerts.Count)" -ForegroundColor Red }
        throw "AegisEdgeE2ELifecycleCritical (fingerprint $expectedCriticalFingerprint) was not delivered to critical-receiver on /alerts/critical within $TimeoutSeconds s."
    }
    if (-not $unrelFiringEvent) {
        $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
        Write-Host "  Events recorded in receiver at timeout ($($events.Count)):" -ForegroundColor Red
        $events | ForEach-Object { Write-Host "    - route: $($_.route), receiver: $($_.payload.receiver), status: $($_.payload.status), alerts: $($_.payload.alerts.Count)" -ForegroundColor Red }
        throw "AegisEdgeE2EUnrelatedWarning (fingerprint $unrelatedWarningFingerprint) was not delivered to warning-receiver on /alerts/warning within $TimeoutSeconds s (unrelated warnings must NOT be inhibited)."
    }
    if ($inhibitedDelivered) {
        throw "AegisEdgeE2ELifecycleWarning was LEAKED to webhook receiver while it was supposed to be inhibited!"
    }
    Write-Host "  Inhibition firing delivery verified: Critical delivered, Unrelated Warning delivered, Inhibited Warning suppressed." -ForegroundColor Green

    # 4. Trigger resolution and verify resolution is actually observed, not merely requested
    Write-Host "  Triggering resolution of inhibition test alerts (val=0, run_id=$InhibitRunId)..." -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/trigger/inhibition?val=0&run_id=$InhibitRunId" -Headers $authHeaders -Method Post | Out-Null
    Write-Host "  Inhibition resolution requested." -ForegroundColor Green

    # Bounded polling: verify all 3 rules stop firing in Prometheus
    Write-Host "  Waiting for Prometheus rules to observe resolution..." -ForegroundColor Cyan
    $promRulesResolved = $false
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $rulesResp = Invoke-RestMethod -Uri "http://127.0.0.1:${promPort}/api/v1/rules"
            $anyInhibitAlertFiring = $false
            foreach ($group in $rulesResp.data.groups) {
                if ($group.name -eq "aegisedge-lifecycle-test") {
                    foreach ($rule in $group.rules) {
                        if ($rule.alerts) {
                            foreach ($al in $rule.alerts) {
                                if ($al.labels.test_run_id -eq $InhibitRunId -and $al.state -eq "firing") {
                                    $anyInhibitAlertFiring = $true
                                    break
                                }
                            }
                        }
                    }
                }
            }
            if (-not $anyInhibitAlertFiring) {
                $promRulesResolved = $true
                break
            }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (-not $promRulesResolved) {
        throw "Prometheus rules failed to transition away from firing for run_id $InhibitRunId within $TimeoutSeconds s."
    }
    Write-Host "  Prometheus confirms all 3 inhibition test rules are no longer firing." -ForegroundColor Green

    # Bounded polling: verify correlated resolved webhook deliveries on receiver
    Write-Host "  Waiting for correlated resolved webhook deliveries on receiver..." -ForegroundColor Cyan
    $critResolvedEvent = $null
    $unrelResolvedEvent = $null
    $rCritAlert = $null
    $rUnrelAlert = $null
    $stopwatch.Restart()
    while ($stopwatch.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        try {
            $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
            foreach ($ev in $events) {
                if ($ev.payload.status -eq "resolved") {
                    if ($ev.route -eq "critical" -and $ev.payload.receiver -eq "critical-receiver") {
                        foreach ($candAlert in $ev.payload.alerts) {
                            if ($candAlert.fingerprint -eq $expectedCriticalFingerprint -and
                                $candAlert.status -eq "resolved" -and
                                $candAlert.labels.alertname -eq "AegisEdgeE2ELifecycleCritical" -and
                                $candAlert.labels.test_run_id -eq $InhibitRunId) {
                                
                                $rStartsAt = [DateTime]::Parse($candAlert.startsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                                $rEndsAt = [DateTime]::Parse($candAlert.endsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                                if ($rEndsAt.Year -ge 2024 -and $rEndsAt -gt $rStartsAt) {
                                    $critResolvedEvent = $ev
                                    $rCritAlert = $candAlert
                                    break
                                }
                            }
                        }
                    }
                    if ($ev.route -eq "warning" -and $ev.payload.receiver -eq "warning-receiver") {
                        foreach ($candAlert in $ev.payload.alerts) {
                            if ($candAlert.fingerprint -eq $unrelatedWarningFingerprint -and
                                $candAlert.status -eq "resolved" -and
                                $candAlert.labels.alertname -eq "AegisEdgeE2EUnrelatedWarning" -and
                                $candAlert.labels.test_run_id -eq $InhibitRunId) {
                                
                                $rStartsAt = [DateTime]::Parse($candAlert.startsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                                $rEndsAt = [DateTime]::Parse($candAlert.endsAt, $null, [System.Globalization.DateTimeStyles]::RoundtripKind)
                                if ($rEndsAt.Year -ge 2024 -and $rEndsAt -gt $rStartsAt) {
                                    $unrelResolvedEvent = $ev
                                    $rUnrelAlert = $candAlert
                                    break
                                }
                            }
                        }
                    }
                }
            }
            if ($critResolvedEvent -and $unrelResolvedEvent) { break }
        } catch {}
        Start-Sleep -Seconds 1
    }

    if (-not $critResolvedEvent) {
        $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
        Write-Host "  Events recorded in receiver at timeout ($($events.Count)):" -ForegroundColor Red
        $events | ForEach-Object { Write-Host "    - route: $($_.route), receiver: $($_.payload.receiver), status: $($_.payload.status), alerts: $($_.payload.alerts.Count)" -ForegroundColor Red }
        throw "Webhook receiver did not receive resolved notification on /alerts/critical for AegisEdgeE2ELifecycleCritical (fingerprint $expectedCriticalFingerprint, run_id $InhibitRunId) within $TimeoutSeconds s."
    }
    if (-not $unrelResolvedEvent) {
        $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
        Write-Host "  Events recorded in receiver at timeout ($($events.Count)):" -ForegroundColor Red
        $events | ForEach-Object { Write-Host "    - route: $($_.route), receiver: $($_.payload.receiver), status: $($_.payload.status), alerts: $($_.payload.alerts.Count)" -ForegroundColor Red }
        throw "Webhook receiver did not receive resolved notification on /alerts/warning for AegisEdgeE2EUnrelatedWarning (fingerprint $unrelatedWarningFingerprint, run_id $InhibitRunId) within $TimeoutSeconds s."
    }
    Write-Host "  Correlated resolved notifications verified for Critical (Fingerprint: $($rCritAlert.fingerprint)) and Unrelated Warning (Fingerprint: $($rUnrelAlert.fingerprint))." -ForegroundColor Green

    # 5. Verify that NO inappropriate resolved notification was delivered for the inhibited warning
    $events = Invoke-RestMethod -Uri "http://127.0.0.1:${recvPort}/api/events" -Headers $authHeaders
    $inappropriateResolvedFound = $false
    foreach ($ev in $events) {
        if ($ev.payload.status -eq "resolved") {
            foreach ($candAlert in $ev.payload.alerts) {
                if ($candAlert.labels.alertname -eq "AegisEdgeE2ELifecycleWarning" -and
                    $candAlert.labels.test_run_id -eq $InhibitRunId) {
                    $inappropriateResolvedFound = $true
                    break
                }
            }
        }
        if ($inappropriateResolvedFound) { break }
    }
    if ($inappropriateResolvedFound) {
        throw "Inappropriate resolved notification delivered for inhibited warning AegisEdgeE2ELifecycleWarning (run_id $InhibitRunId)!"
    }
    Write-Host "  Verified no inappropriate resolved notification was delivered for the inhibited warning." -ForegroundColor Green

    # 6. Overall Success Reporting
    Write-Host "`n==========================================================" -ForegroundColor Green
    Write-Host "ALL INTEGRATION TEST SCENARIOS COMPLETED SUCCESSFULLY!" -ForegroundColor Green
    Write-Host "==========================================================" -ForegroundColor Green
}
catch {
    $testError = $_
    Write-Host "Test Execution Failed: $testError" -ForegroundColor Red
}
finally {
    # Teardown and cleanup while still holding the host concurrency lock
    try {
        $cleanupErrors = Invoke-CleanupResources
    } catch {
        Write-Host "Unexpected exception during cleanup: $_" -ForegroundColor Red
        if ($null -eq $cleanupErrors) {
            $cleanupErrors = @("Unexpected exception during cleanup: $_")
        } else {
            $cleanupErrors = @($cleanupErrors) + "Unexpected exception during cleanup: $_"
        }
    } finally {
        # Release and dispose the host concurrency lock
        if ($mutexAcquired -and $null -ne $mutex) {
            try {
                $mutex.ReleaseMutex()
                Write-Host "Host concurrency lock released ($mutexName)." -ForegroundColor Green
            } catch {
                Write-Host "Warning: error releasing host mutex: $_" -ForegroundColor Yellow
            }
            $mutex.Dispose()
            $mutex = $null
        }
    }

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
