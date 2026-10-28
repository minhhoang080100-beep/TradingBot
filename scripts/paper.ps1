# Run the keyless PAPER simulator in the foreground from the repository root.
# Examples (PowerShell):
#   .\scripts\paper.ps1 live --symbol ETHUSDT --balance 100 --seconds 300
#   .\scripts\paper.ps1 live --poll-seconds 5 --warmup 500 --fee-bps 6 --stop-slippage-bps 2
#   .\scripts\paper.ps1 replay --input paper-runs\LIVE_SESSION.jsonl --output paper-runs\replay.jsonl
# Replace LIVE_SESSION.jsonl with the complete session path printed by live.
#
# Native Cargo is preferred. Otherwise, start Docker Desktop yourself in Linux
# container mode first. The fallback uses rust:1.89-bookworm and named Cargo /
# target caches; it does not depend on a verification container or forward host
# environment variables/credentials into the container.
#
# Relative input/output paths are relative to this repository, not the caller's
# current directory. Docker can only access input/output paths inside the repo.
# Ctrl+C stays attached to the foreground process. A finally block also stops
# this invocation's ownership-labelled Docker container; caches are preserved.
# Invoke this script normally, not by dot-sourcing it into another session.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Convert-PaperContainerPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Value,
        [Parameter(Mandatory = $true)]
        [string] $Repository
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        throw 'The paper input/output path must not be empty.'
    }
    $paperCandidate = if ([System.IO.Path]::IsPathRooted($Value)) {
        $Value
    } else {
        Join-Path -Path $Repository -ChildPath $Value
    }
    $paperResolved = [System.IO.Path]::GetFullPath($paperCandidate)
    $paperPrefix = $Repository.TrimEnd([char[]] @('\', '/')) + [System.IO.Path]::DirectorySeparatorChar
    if ([string]::Equals($paperResolved, $Repository, [System.StringComparison]::OrdinalIgnoreCase)) {
        return '/workspace'
    }
    if (-not $paperResolved.StartsWith($paperPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw 'Docker paper input/output paths must stay inside the repository. Use native Cargo for other locations.'
    }
    $paperRelative = $paperResolved.Substring($paperPrefix.Length).Replace('\', '/')
    return '/workspace/' + $paperRelative
}

function Convert-PaperContainerArguments {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Arguments,
        [Parameter(Mandatory = $true)]
        [string] $Repository
    )

    $paperConverted = [System.Collections.Generic.List[string]]::new()
    for ($paperIndex = 0; $paperIndex -lt $Arguments.Length; $paperIndex++) {
        $paperArgument = $Arguments[$paperIndex]
        if ($paperArgument -in @('--input', '--output')) {
            if ($paperIndex + 1 -ge $Arguments.Length) {
                throw "Missing path after $paperArgument."
            }
            $paperConverted.Add($paperArgument)
            $paperIndex++
            $paperConverted.Add((Convert-PaperContainerPath -Value $Arguments[$paperIndex] -Repository $Repository))
        } elseif ($paperArgument.StartsWith('--input=', [System.StringComparison]::Ordinal)) {
            $paperConverted.Add('--input=' + (Convert-PaperContainerPath -Value $paperArgument.Substring(8) -Repository $Repository))
        } elseif ($paperArgument.StartsWith('--output=', [System.StringComparison]::Ordinal)) {
            $paperConverted.Add('--output=' + (Convert-PaperContainerPath -Value $paperArgument.Substring(9) -Repository $Repository))
        } else {
            $paperConverted.Add($paperArgument)
        }
    }
    return $paperConverted.ToArray()
}

function Stop-OwnedPaperContainer {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Docker,
        [Parameter(Mandatory = $true)]
        [string] $Container,
        [Parameter(Mandatory = $true)]
        [string] $OwnershipToken
    )

    # Never stop a generic/shared container. Match the exact invocation name
    # and its independent ownership label before any stop/remove operation.
    try {
        $paperFindArguments = @(
            'container', 'ls', '--all',
            '--filter', ('name=^/' + $Container + '$'),
            '--filter', ('label=com.ict-paper.wrapper-run=' + $OwnershipToken),
            '--format', '{{.ID}}'
        )
        $paperOwnedIds = @(& $Docker @paperFindArguments 2>$null)
        if ($LASTEXITCODE -ne 0) {
            throw 'Unable to inspect the paper container.'
        }
        if ($paperOwnedIds.Count -eq 0) {
            return
        }
        if ($paperOwnedIds.Count -ne 1 -or $paperOwnedIds[0] -notmatch '^[0-9a-f]{12,64}$') {
            Write-Warning 'Paper container ownership could not be verified; no container was stopped.'
            return
        }
        $paperOwnedId = $paperOwnedIds[0]
        try {
            $paperStopArguments = @('container', 'stop', '--time', '10', $paperOwnedId)
            & $Docker @paperStopArguments *> $null
        } catch {
            # A stop failure must not skip the ownership-checked force fallback.
        }
        # --rm normally removes the container. This fallback also covers an
        # interrupted Docker client or a failed graceful stop, but only for the
        # ownership-verified invocation. Named build caches are never removed.
        try {
            $paperRemoveArguments = @('container', 'rm', '--force', $paperOwnedId)
            & $Docker @paperRemoveArguments *> $null
        } catch {
            # The --rm deletion can race this explicit cleanup; check below.
        }
        $paperRemaining = @(& $Docker @paperFindArguments 2>$null)
        if ($LASTEXITCODE -ne 0 -or $paperRemaining.Count -ne 0) {
            throw 'Paper container cleanup did not complete.'
        }
    } catch {
        Write-Warning "Could not confirm paper container cleanup. Check the exact container '$Container' with Docker Desktop."
    }
}

$paperExitCode = 1
$paperLocationPushed = $false
$paperDockerPath = $null
$paperContainerName = $null
$paperOwnershipToken = $null

try {
    $paperRepository = [System.IO.Path]::GetFullPath((Join-Path -Path $PSScriptRoot -ChildPath '..'))
    $paperManifest = Join-Path -Path $paperRepository -ChildPath 'Cargo.toml'
    if (-not (Test-Path -LiteralPath $paperManifest -PathType Leaf)) {
        throw 'Cannot locate Cargo.toml in the repository containing scripts/paper.ps1.'
    }
    [string[]] $paperArguments = @($args | ForEach-Object { [string] $_ })
    if ($paperArguments.Length -eq 0) {
        Write-Host 'Usage: .\scripts\paper.ps1 live [options]'
        Write-Host '       .\scripts\paper.ps1 replay --input PATH [--output PATH]'
        $paperExitCode = 2
    } else {
        Push-Location -LiteralPath $paperRepository
        $paperLocationPushed = $true
        $paperCargo = Get-Command -Name 'cargo' -CommandType Application -ErrorAction SilentlyContinue
        if ($null -ne $paperCargo) {
            [string[]] $paperCargoArguments = @('run', '--locked', '--release', '--bin', 'paper', '--') + $paperArguments
            & $paperCargo.Source @paperCargoArguments
            $paperExitCode = $LASTEXITCODE
        } else {
            $paperDocker = Get-Command -Name 'docker' -CommandType Application -ErrorAction SilentlyContinue
            if ($null -eq $paperDocker) {
                throw 'Install native Cargo, or install and start Docker Desktop in Linux-container mode.'
            }
            $paperDockerPath = $paperDocker.Source
            try {
                $paperInfoArguments = @('info', '--format', '{{.OSType}}')
                $paperEngine = @(& $paperDockerPath @paperInfoArguments 2>$null)
                if ($LASTEXITCODE -ne 0 -or ($paperEngine -join '').Trim() -ne 'linux') {
                    throw 'Docker Linux engine is not ready.'
                }
            } catch {
                throw 'Start Docker Desktop in Linux-container mode, then run this script again. The wrapper does not start Docker automatically.'
            }

            [string[]] $paperContainerArguments = @(Convert-PaperContainerArguments -Arguments $paperArguments -Repository $paperRepository)
            $paperOwnershipToken = [Guid]::NewGuid().ToString('N')
            $paperContainerName = 'ict-paper-' + $paperOwnershipToken.Substring(0, 12)
            [string[]] $paperDockerArguments = @(
                'run', '--rm', '--init', '--sig-proxy=true',
                '--name', $paperContainerName,
                '--label', ('com.ict-paper.wrapper-run=' + $paperOwnershipToken),
                '--workdir', '/workspace',
                '--volume', ($paperRepository + ':/workspace'),
                '--volume', 'ict-paper-cargo-cache:/usr/local/cargo',
                '--volume', 'ict-paper-target-cache:/workspace/target'
            )
            $paperInteractive = $false
            try {
                $paperInteractive = [Environment]::UserInteractive -and
                    -not [Console]::IsInputRedirected -and -not [Console]::IsOutputRedirected
            } catch {
                $paperInteractive = $false
            }
            if ($paperInteractive) {
                $paperDockerArguments += @('--interactive', '--tty')
            }
            $paperDockerArguments += @(
                'rust:1.89-bookworm',
                'cargo', 'run', '--locked', '--release', '--bin', 'paper', '--'
            ) + $paperContainerArguments
            & $paperDockerPath @paperDockerArguments
            $paperExitCode = $LASTEXITCODE
        }
    }
} catch {
    Write-Error -Message $_.Exception.Message -ErrorAction Continue
    $paperExitCode = 1
} finally {
    if ($null -ne $paperDockerPath -and $null -ne $paperContainerName -and $null -ne $paperOwnershipToken) {
        Stop-OwnedPaperContainer -Docker $paperDockerPath -Container $paperContainerName -OwnershipToken $paperOwnershipToken
    }
    if ($paperLocationPushed) {
        Pop-Location
    }
}

exit $paperExitCode
