Param(
  [string]$RelativeNotebooksDir = "notebooks/form_url_fetching_and_messege_writing",
  [int]$IntervalSeconds = 10
)

$ErrorActionPreference = "Stop"

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptRoot
$LogFile = Join-Path $ProjectRoot "watch_ipynb_sync.log"
$StdOut = Join-Path $ProjectRoot "watch_ipynb_sync.stdout.log"
$StdErr = Join-Path $ProjectRoot "watch_ipynb_sync.stderr.log"
$PidFile = Join-Path $ProjectRoot "watch_ipynb_sync.pid"
Set-Content -Path $PidFile -Value $PID

function Write-Log {
  Param([string]$Message)
  $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $line = "[$timestamp] $Message"
  Write-Host $line
  Add-Content -Path $LogFile -Value $line
}

$NotebooksDir = Join-Path $ProjectRoot $RelativeNotebooksDir
if (-not (Test-Path -LiteralPath $NotebooksDir)) {
  throw "Notebooks directory not found: $NotebooksDir"
}

Write-Log "Jupytext polling sync starting (every $IntervalSeconds s)"
Write-Log "ProjectRoot: $ProjectRoot"
Write-Log "Scanning: $NotebooksDir"

try {
  while ($true) {
    # Find all ipynb files and sync them one by one
    Get-ChildItem -LiteralPath $NotebooksDir -Recurse -Filter "*.ipynb" | ForEach-Object {
      $ipynb = $_.FullName
      # Normalize to POSIX-style path relative to ProjectRoot for jupytext matching
      $rel = Resolve-Path -LiteralPath $ipynb | ForEach-Object { $_.Path.Substring($ProjectRoot.Length).TrimStart('\/') }
      $rel = $rel -replace '\\','/'
      $rel = $rel
      try {
        Write-Log "Sync start: $ipynb"
        Push-Location $ProjectRoot
        $cmd = "python"
        $jtxArgs = @("-m", "jupytext", "--sync", $rel)
        $proc = Start-Process -FilePath $cmd -ArgumentList $jtxArgs -NoNewWindow -PassThru -Wait -RedirectStandardOutput $StdOut -RedirectStandardError $StdErr
        if ($proc.ExitCode -ne 0) {
          Write-Log "Sync failed (exit $($proc.ExitCode)): $ipynb"
        } else {
          Write-Log "Sync done: $ipynb"
        }
      }
      catch {
        Write-Log "Sync error: $ipynb :: $($_.Exception.Message)"
      }
      finally {
        Pop-Location -ErrorAction SilentlyContinue
      }
    }

    Start-Sleep -Seconds $IntervalSeconds
  }
}
finally {
  Write-Log "Jupytext polling sync stopped"
}


