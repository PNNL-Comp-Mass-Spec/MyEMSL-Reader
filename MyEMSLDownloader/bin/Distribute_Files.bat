@echo off

pushd F:\Documents\Projects\DataMining\DMS_Managers\MyEMSL_Reader\MyEMSLDownloader\bin
call Distribute_Files_Work.bat C:\DMS_Programs\MyEMSLDownloader
call Distribute_Files_Work.bat \\floyd\software\MyEMSLDownloader
call Distribute_Files_Work.bat \\pnl\projects\OmicsSW\DMS_Programs\AnalysisToolManagerDistribution\MyEMSLDownloader
call Distribute_Files_Work.bat \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\MyEMSLDownloader

xcopy Debug\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Programs\DMS_Dataset_Retriever\Lib" /D /Y
xcopy Debug\Pacifica.dll     "F:\Documents\Projects\DataMining\DMS_Programs\DMS_Dataset_Retriever\Lib" /D /Y

rem xcopy Debug\MyEMSLReader.dll  "F:\Documents\Projects\DataMining\DMS_Programs\DMS_Dataset_Retriever\Lib" /D /Y

popd

if not "%1"=="NoPause" pause
