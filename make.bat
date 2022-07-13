@echo off

rem isolate changes to local environment
setlocal

rem create local bin folder if it doesn't exist
if not exist "%~dp0bin" (
  mkdir %~dp0bin
)

rem configure environment
call env.bat

rem if no target, then print usage and exit
if [%1]==[] (
  goto:usage
)

REM remove bin, data, and temp directories

if %1%==clean (

  rem if bin exists, then delete it
  if exist %~dp0bin (
    rd /s /q %~dp0bin
  )

  rem if data exists, then delete it
  if exist %~dp0data (
    rd /s /q %~dp0data
  )

  rem if temp exists, then delete it
  if exist %~dp0temp (
    rd /s /q %~dp0temp
  )

  rem if pyserializer.egg-info exists, then delete it
  if exist %~dp0pyserializer.egg-info (
    rd /s /q %~dp0pyserializer.egg-info
  )

  exit /B 0
)

if %1%==help (
  goto:usage
)

if %1%==python_build (

  where python >nul 2>&1 || (
    echo|set /p="python is missing."
    exit /B 1
  )

  rem create local dist folder if it doesn't exist
  if not exist "%~dp0dist" (
    mkdir %~dp0dist
  )

  rem delete old wheel if exists
  if exist "%~dp0dist\pyserializer-0.0.1-py3-none-any.whl" (
    del "%~dp0dist\pyserializer-0.0.1-py3-none-any.whl"
  )

  rem build python wheel
  python -m build --wheel

  exit /B 0
)

if %1%==python_flake8 (

  where flake8 >nul 2>&1 || (
    echo|set /p="flake8 is missing."
    exit /B 1
  )

  flake8 cmd pyserializer setup.py

  exit /B 0
)

if %1%==python_setup (

  where python >nul 2>&1 || (
    echo|set /p="python is missing."
    exit /B 1
  )

  where pip >nul 2>&1 || (
    pushd "%~dp0bin"
    rem download pip
    if not exist get-pip.py (
      powershell -Command "$ProgressPreference = 'SilentlyContinue'; Invoke-WebRequest https://bootstrap.pypa.io/get-pip.py -OutFile get-pip.py"
    )
    rem install pip
    python get-pip.py
    del get-pip.py
    popd
  )

  rem install build and flake8 tools
  pip install build flake8

  rem create virtual environment
  python -m venv .venv

  rem install requirements
  .venv\Scripts\pip install -r requirements.txt

  rem install project as editable
  .venv\Scripts\pip install -e .

  exit /B 0
)

if %1%==python_test (

  where flake8 >nul 2>&1 || (
    echo|set /p="flake8 is missing."
    exit /B 1
  )

  .venv\Scripts\python -m unittest -v pyserializer.tests

  exit /B 0
)

:usage
  set targets="clean" "help" "python_build" "python_flake8" "python_setup" "python_test"
  (for %%t in (%targets%) do (
   echo|set /p=%%t
   echo.
  ))
  exit /B 1
