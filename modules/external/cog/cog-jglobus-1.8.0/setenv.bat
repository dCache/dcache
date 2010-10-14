@echo off
for %%i in (lib\*.jar) do call etc\windows\lcp.bat %%i
set CLASSPATH=build\classes;etc;%LOCALCLASSPATH%
