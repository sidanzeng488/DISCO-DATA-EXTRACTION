# DISCODATA Explorer - Start & Share Script
# Creates a public share link using ngrok

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  DISCODATA Explorer - Share Mode" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Change to project root directory (one level up from scripts/)
$projectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $projectRoot

# Check if ngrok is installed
$ngrokPath = Get-Command ngrok -ErrorAction SilentlyContinue
if (-not $ngrokPath) {
    Write-Host "[!] ngrok not found. Please install it first:" -ForegroundColor Red
    Write-Host ""
    Write-Host "    1. Install: winget install ngrok.ngrok" -ForegroundColor Gray
    Write-Host "    2. Get free token: https://dashboard.ngrok.com/signup" -ForegroundColor Gray
    Write-Host "    3. Run: ngrok config add-authtoken YOUR_TOKEN" -ForegroundColor Gray
    Write-Host ""
    
    $continue = Read-Host "Continue without ngrok (local only)? (y/n)"
    if ($continue -ne "y") {
        exit 1
    }
}

# Start server
Write-Host "[1/2] Starting Server (port 5000)..." -ForegroundColor Yellow
$serverProcess = Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$projectRoot'; python -m waitress --host=0.0.0.0 --port=5000 web.app:app" -PassThru

Start-Sleep -Seconds 3

# Start ngrok if available
if ($ngrokPath) {
    Write-Host "[2/2] Starting ngrok tunnel..." -ForegroundColor Yellow
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "ngrok http 5000"
    
    Start-Sleep -Seconds 3
    
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "  Ready for sharing!" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "  Check ngrok window for public URL" -ForegroundColor Yellow
    Write-Host "  Example: https://xxxx-xxxx.ngrok-free.app" -ForegroundColor Gray
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "  Started (Local Only)" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
}

Write-Host "  Local URL: http://localhost:5000" -ForegroundColor Blue
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
