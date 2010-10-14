@echo off

set LOCALCLASSPATH=
for %%i in ("%COG_INSTALL_PATH%\lib\*.jar") do call "%COG_INSTALL_PATH%\etc\lcp.bat" "%%i"
set LOCALCLASSPATH="%COG_INSTALL_PATH%\etc";%LOCALCLASSPATH%
