echo.
echo Copying to %1
xcopy debug\*.exe "%1" /D /Y
xcopy debug\*.dll "%1" /D /Y
xcopy ..\*.txt "%1" /D /Y
