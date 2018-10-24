@echo off

pushd F:\Documents\Projects\DataMining\DMS_Managers\MyEMSL_Reader\MyEMSLDownloader\bin
call Distribute_Files_Work.bat C:\DMS_Programs\MyEMSLDownloader
call Distribute_Files_Work.bat \\floyd\software\MyEMSLDownloader
call Distribute_Files_Work.bat \\pnl\projects\OmicsSW\DMS_Programs\AnalysisToolManagerDistribution\MyEMSLDownloader
call Distribute_Files_Work.bat \\pnl\projects\OmicsSW\DMS_Programs\CaptureTaskManagerDistribution\MyEMSLDownloader
popd

pause
