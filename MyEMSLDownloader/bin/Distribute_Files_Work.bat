echo.
echo Copying to %1
xcopy Debug\net48\*.exe "%1" /D /Y
xcopy Debug\net48\*.dll "%1" /D /Y
xcopy ..\*.txt "%1" /D /Y
xcopy ..\*.md "%1" /D /Y
