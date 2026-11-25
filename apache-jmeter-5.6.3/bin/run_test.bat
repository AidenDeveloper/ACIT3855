@echo off
cd /d %~dp0
if exist results.jtl del results.jtl
if exist report rmdir /s /q report

set T=%1
if "%T%"=="" set T=10
set L=%2
if "%L%"=="" set L=10

echo %T% threads, %L% loops
call "C:\Users\Aiden\mmoanalytics\apache-jmeter-5.6.3\bin\jmeter.bat" -n -t "Test Plan.jmx" -Jthreads=%T% -Jloops=%L% -l results.jtl -e -o report