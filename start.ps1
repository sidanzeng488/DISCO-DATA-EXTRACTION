# DISCODATA Explorer - Start Script (PowerShell)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  DISCODATA Explorer - Starting..." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Set-Location $PSScriptRoot

Write-Host "  Server: http://localhost:5000" -ForegroundColor Blue
Write-Host ""

# Open browser after delay
Start-Job -ScriptBlock { Start-Sleep -Seconds 2; Start-Process "http://localhost:5000" } | Out-Null

# Start server
python -m waitress --host=127.0.0.1 --port=5000 app:app
