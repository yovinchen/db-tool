param(
    [Parameter(Mandatory = $true)]
    [string]$Binary
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Binary -PathType Leaf)) {
    throw "portable Windows binary is missing: $Binary"
}

$version = & $Binary --version
if ($LASTEXITCODE -ne 0 -or $version -notmatch '^dbtool [0-9]+\.[0-9]+\.[0-9]+') {
    throw "portable Windows binary did not return a valid version"
}

$pingJson = & $Binary --dsn "sqlite::memory:" ping
if ($LASTEXITCODE -ne 0) {
    throw "portable Windows binary could not ping SQLite"
}
$ping = $pingJson | ConvertFrom-Json
if (-not $ping.ok -or $ping.data.status -ne "ok") {
    throw "portable Windows SQLite ping returned an invalid envelope"
}

$queryJson = & $Binary --dsn "sqlite::memory:" sql query "select 1 as value"
if ($LASTEXITCODE -ne 0) {
    throw "portable Windows binary could not query SQLite"
}
$query = $queryJson | ConvertFrom-Json
if (-not $query.ok -or $query.data.rows.Count -ne 1 -or $query.data.rows[0][0] -ne 1) {
    throw "portable Windows SQLite query returned unexpected data"
}

$writeOutput = & $Binary --dsn "sqlite::memory:" sql exec "create table blocked(id integer)" 2>&1
$writeStatus = $LASTEXITCODE
if ($writeStatus -eq 0) {
    throw "portable Windows binary allowed an unguarded SQLite write"
}
$writeError = (($writeOutput | Out-String).Trim()) | ConvertFrom-Json
if ($writeError.error.code -ne "WRITE_NOT_ALLOWED") {
    throw "portable Windows write guard returned an unexpected error"
}

Write-Output "dbtool portable Windows core smoke passed"

# The expected rejected write above leaves PowerShell's native-command status
# at a non-zero value. Return success explicitly after every assertion passes.
exit 0
