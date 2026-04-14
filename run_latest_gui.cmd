@echo off
setlocal
cd /d "%~dp0"

echo [1/3] Restart collector...
powershell -NoProfile -ExecutionPolicy Bypass -File ".\stop_collector.ps1"
powershell -NoProfile -ExecutionPolicy Bypass -File ".\start_collector.ps1"

echo [2/3] Start latest GUI from source...
if exist ".\venv\Scripts\python.exe" (
    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
      "Start-Process -FilePath '.\venv\Scripts\python.exe' -ArgumentList '.\good_density_gui.py' -WorkingDirectory '.'"
) else if exist ".\.venv\Scripts\python.exe" (
    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
      "Start-Process -FilePath '.\.venv\Scripts\python.exe' -ArgumentList '.\good_density_gui.py' -WorkingDirectory '.'"
) else (
    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
      "Start-Process -FilePath 'py' -ArgumentList '-3','.\\good_density_gui.py' -WorkingDirectory '.'"
)

echo [3/3] Done. In GUI status, verify build tag is build=r12
endlocal
exit /b 0
