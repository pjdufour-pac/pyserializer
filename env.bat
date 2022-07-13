@echo off

rem update PATH to include python installation
set PATH=%USERPROFILE%\AppData\Local\Programs\Python\Python310\Scripts;%PATH%
set PATH=%USERPROFILE%\AppData\Local\Programs\Python\Python310;%PATH%

rem update path to include git
set PATH=%USERPROFILE%\AppData\Local\Programs\Git\bin;%PATH%

rem update PATH to include local bin folder
set PATH=%~dp0bin;%PATH%

rem run local configurations if present
if exist "%~dp0env.local.bat" (
  call "%~dp0env.local.bat"
)
