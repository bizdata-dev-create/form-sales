Param(
  [string]$RelativeNotebooksDir = "notebooks/form_url_fetching_and_messege_writing",
  [double]$DebounceSeconds = 1.5,
  [int]$PostWriteDelayMs = 200,
  [switch]$LogSuppressed
)

$ErrorActionPreference = "Stop"

function Write-Log {
  Param([string]$Message)
  $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $line = "[$timestamp] $Message"
  Write-Host $line
  Add-Content -Path $Global:LogFile -Value $line
}

function Sync-File {
  Param([string]$Path)

  if (-not $Path -or -not (Test-Path -LiteralPath $Path)) { return }
  if (-not $Path.ToLower().EndsWith(".ipynb")) { return }

  $now = Get-Date
  $key = (Resolve-Path -LiteralPath $Path).Path
  if ($Global:LastSync.ContainsKey($key)) {
    $elapsed = ($now - $Global:LastSync[$key]).TotalSeconds
    if ($elapsed -lt $DebounceSeconds) {
      if ($LogSuppressed) { Write-Log "Suppressed duplicate event ($([math]::Round($elapsed,3))s < $DebounceSeconds s): $key" }
      return
    }
  }
  $Global:LastSync[$key] = $now

  try {
    Start-Sleep -Milliseconds $PostWriteDelayMs
    Write-Log "Sync start: $key"
    Push-Location $Global:ProjectRoot
    # Use python -m to avoid PATH issues for 'jupytext'
    $cmd = "python"
    $args = @("-m", "jupytext", "--sync", $key)
    $proc = Start-Process -FilePath $cmd -ArgumentList $args -NoNewWindow -PassThru -Wait -RedirectStandardOutput "$Global:ProjectRoot\watch_ipynb_sync.stdout.log" -RedirectStandardError "$Global:ProjectRoot\watch_ipynb_sync.stderr.log"
    if ($proc.ExitCode -ne 0) {
      Write-Log "Sync failed (exit $($proc.ExitCode)): $key"
    } else {
      Write-Log "Sync done: $key"
    }
  }
  catch {
    Write-Log "Sync error: $key :: $($_.Exception.Message)"
  }
  finally {
    Pop-Location -ErrorAction SilentlyContinue
  }
}

# Init paths and state
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$Global:ProjectRoot = Split-Path -Parent $ScriptRoot
$Global:LogFile = Join-Path $Global:ProjectRoot "watch_ipynb_sync.log"
$pidFile = Join-Path $Global:ProjectRoot "watch_ipynb_sync.pid"
Set-Content -Path $pidFile -Value $PID
$Global:LastSync = @{}

$notebooksDir = Join-Path $Global:ProjectRoot $RelativeNotebooksDir
if (-not (Test-Path -LiteralPath $notebooksDir)) {
  throw "Notebooks directory not found: $notebooksDir"
}

Write-Log "Jupytext watcher starting"
Write-Log "ProjectRoot: $Global:ProjectRoot"
Write-Log "Watching: $notebooksDir"

# Create the FileSystemWatcher
$watcher = New-Object System.IO.FileSystemWatcher
$watcher.Path = $notebooksDir
$watcher.Filter = "*.ipynb"
$watcher.IncludeSubdirectories = $true
$watcher.NotifyFilter = [IO.NotifyFilters]'FileName, LastWrite, Size'
$watcher.EnableRaisingEvents = $true

# Register for events
$changedReg = Register-ObjectEvent -InputObject $watcher -EventName Changed -SourceIdentifier JtxChanged -Action {
  param($sender, $eventArgs)
  Sync-File -Path $eventArgs.FullPath
}
$createdReg = Register-ObjectEvent -InputObject $watcher -EventName Created -SourceIdentifier JtxCreated -Action {
  param($sender, $eventArgs)
  Sync-File -Path $eventArgs.FullPath
}
$renamedReg = Register-ObjectEvent -InputObject $watcher -EventName Renamed -SourceIdentifier JtxRenamed -Action {
  param($sender, $eventArgs)
  Sync-File -Path $eventArgs.FullPath
}

Write-Log "Jupytext watcher is running (Ctrl+C to stop)"

try {
  while ($true) {
    Wait-Event -Timeout 5 | Out-Null
  }
}
finally {
  Unregister-Event -SourceIdentifier JtxChanged -ErrorAction SilentlyContinue
  Unregister-Event -SourceIdentifier JtxCreated -ErrorAction SilentlyContinue
  Unregister-Event -SourceIdentifier JtxRenamed -ErrorAction SilentlyContinue
  $watcher.EnableRaisingEvents = $false
  $watcher.Dispose()
  Write-Log "Jupytext watcher stopped"
}


