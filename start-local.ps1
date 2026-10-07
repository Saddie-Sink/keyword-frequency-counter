# Member 6 — Local Development Launcher (Windows PowerShell)
# Starts both the FastAPI Backend (port 8000) and the React Vite Dashboard (port 5173)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "Starting Real-Time Keyword Analytics Backend on http://localhost:8000 ..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; .\.venv\Scripts\python.exe -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000"

Write-Host "Starting React Dashboard on http://localhost:5173 ..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot\frontend'; npm run dev"

Write-Host "Both services launched! Open http://localhost:5173 in your browser." -ForegroundColor Yellow
