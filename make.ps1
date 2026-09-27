param(
    [ValidateSet(
        'help',
        'prod-up', 'prod-down', 'prod-ps', 'prod-logs', 'prod-config',
        'dev-up', 'dev-down', 'dev-ps', 'dev-logs', 'dev-config',
        'all-up', 'all-down', 'all-ps', 'config',
        'ingest-stock', 'ingest-fund', 'ingest-market'
    )]
    [string]$Target = 'help',

    [string[]]$Services = @(),
    [string]$Compose = 'docker compose',
    [string]$ProdComposeFile = 'compose.yml',
    [string]$DevComposeFile = 'compose.dev.yml',
    [switch]$Build,
    [switch]$NoDetach,
    [string[]]$LogArgs = @(),
    [string]$Uv = 'uv',
    [string[]]$IngestArgs = @(),
    [string[]]$StockArgs = @(),
    [string[]]$FundArgs = @()
)

$ErrorActionPreference = 'Stop'

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ComposeFile,

        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $composeParts = $Compose -split ' '
    $executable = $composeParts[0]
    $baseArgs = @()

    if ($composeParts.Count -gt 1) {
        $baseArgs = $composeParts[1..($composeParts.Count - 1)]
    }

    & $executable @baseArgs -f $ComposeFile @Arguments

    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

function Resolve-IngestionArgs {
    param([string[]]$SpecificArgs)

    if ($SpecificArgs.Count -gt 0) {
        return $SpecificArgs
    }

    return $IngestArgs
}

function Invoke-Ingestion {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ScriptPath,

        [string[]]$Arguments = @()
    )

    & $Uv run python $ScriptPath @Arguments

    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

function Get-UpArgs {
    param(
        [switch]$IncludeBuild,
        [string[]]$SelectedServices
    )

    $composeArgs = @('up')

    if ($IncludeBuild) {
        $composeArgs += '--build'
    }

    if (-not $NoDetach) {
        $composeArgs += '-d'
    }

    $composeArgs += $SelectedServices
    return $composeArgs
}

function Show-Help {
    Write-Host 'Usage: ./make.ps1 <target> [-Services service ...] [-Build]'
    Write-Host ''
    Write-Host "Production-shaped stack from ${ProdComposeFile}:"
    Write-Host '  prod-up       Start all services, or -Services ... for a partial stack'
    Write-Host '  prod-down     Stop and remove the production-shaped stack'
    Write-Host '  prod-ps       Show production-shaped stack status'
    Write-Host '  prod-logs     Show logs, or -Services ... for selected services'
    Write-Host '  prod-config   Render the production-shaped compose configuration'
    Write-Host ''
    Write-Host "Local ELT compatibility stack from ${DevComposeFile}:"
    Write-Host '  dev-up        Start all services, or -Services ... for a partial stack'
    Write-Host '  dev-down      Stop and remove the local ELT stack'
    Write-Host '  dev-ps        Show local ELT stack status'
    Write-Host '  dev-logs      Show logs, or -Services ... for selected services'
    Write-Host '  dev-config    Render the local ELT compose configuration'
    Write-Host ''
    Write-Host 'Combined helpers:'
    Write-Host '  all-up        Start both complete stacks'
    Write-Host '  all-down      Stop both stacks'
    Write-Host '  all-ps        Show both stack statuses'
    Write-Host '  config        Render both compose configurations'
    Write-Host ''
    Write-Host 'Manual ingestion through uv:'
    Write-Host '  ingest-stock  Fetch stock data and write a raw JSON dump'
    Write-Host '  ingest-fund   Fetch fund data and write a raw JSON dump'
    Write-Host '  ingest-market Run stock ingestion, then fund ingestion'
    Write-Host ''
    Write-Host 'Examples:'
    Write-Host '  ./make.ps1 prod-up'
    Write-Host '  ./make.ps1 prod-up -Services postgres,warehouse-postgres'
    Write-Host '  ./make.ps1 all-up -Build'
    Write-Host "  ./make.ps1 ingest-stock -StockArgs @('--symbol','FPT','--start','2026-09-01','--end','2026-09-19')"
    Write-Host "  ./make.ps1 ingest-fund -FundArgs @('--symbol','E1VFVN30','--start','2026-09-01','--end','2026-09-19')"
    Write-Host "  ./make.ps1 ingest-market -StockArgs @('--symbol','FPT','--start','2026-09-01','--end','2026-09-19') -FundArgs @('--symbol','E1VFVN30','--start','2026-09-01','--end','2026-09-19')"
}

$buildImages = $Build

switch ($Target) {
    'help' { Show-Help }
    'prod-up' { Invoke-Compose $ProdComposeFile (Get-UpArgs -IncludeBuild:$buildImages -SelectedServices $Services) }
    'prod-down' { Invoke-Compose $ProdComposeFile @('down') }
    'prod-ps' { Invoke-Compose $ProdComposeFile (@('ps') + $Services) }
    'prod-logs' { Invoke-Compose $ProdComposeFile (@('logs') + $LogArgs + $Services) }
    'prod-config' { Invoke-Compose $ProdComposeFile @('config') }
    'dev-up' { Invoke-Compose $DevComposeFile (Get-UpArgs -IncludeBuild:$buildImages -SelectedServices $Services) }
    'dev-down' { Invoke-Compose $DevComposeFile @('down') }
    'dev-ps' { Invoke-Compose $DevComposeFile (@('ps') + $Services) }
    'dev-logs' { Invoke-Compose $DevComposeFile (@('logs') + $LogArgs + $Services) }
    'dev-config' { Invoke-Compose $DevComposeFile @('config') }
    'all-up' {
        Invoke-Compose $ProdComposeFile (Get-UpArgs -IncludeBuild:$buildImages -SelectedServices @())
        Invoke-Compose $DevComposeFile (Get-UpArgs -IncludeBuild:$buildImages -SelectedServices @())
    }
    'all-down' {
        Invoke-Compose $DevComposeFile @('down')
        Invoke-Compose $ProdComposeFile @('down')
    }
    'all-ps' {
        Invoke-Compose $ProdComposeFile @('ps')
        Invoke-Compose $DevComposeFile @('ps')
    }
    'config' {
        Invoke-Compose $ProdComposeFile @('config')
        Invoke-Compose $DevComposeFile @('config')
    }
    'ingest-stock' {
        Invoke-Ingestion -ScriptPath 'scripts/test_stock_source.py' -Arguments (Resolve-IngestionArgs -SpecificArgs $StockArgs)
    }
    'ingest-fund' {
        Invoke-Ingestion -ScriptPath 'scripts/test_fund_source.py' -Arguments (Resolve-IngestionArgs -SpecificArgs $FundArgs)
    }
    'ingest-market' {
        Invoke-Ingestion -ScriptPath 'scripts/test_stock_source.py' -Arguments (Resolve-IngestionArgs -SpecificArgs $StockArgs)
        Invoke-Ingestion -ScriptPath 'scripts/test_fund_source.py' -Arguments (Resolve-IngestionArgs -SpecificArgs $FundArgs)
    }
}
