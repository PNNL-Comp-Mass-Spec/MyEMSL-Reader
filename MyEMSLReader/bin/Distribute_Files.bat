@echo off

echo.
echo.
echo The copy commands in this batch file are deprecated
echo.
echo Obtain Pacifica from NuGet on Proto-2
echo \\proto-2\CI_Publish\NuGet
echo.
echo.

echo Instead, calling MyEMSL_Reader\MyEMSLDownloader\bin\Distribute_Files.bat
pause

call ..\..\MyEMSLDownloader\bin\Distribute_Files.bat

Goto Done

xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\AM_Common" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\AM_Shared\bin\Debug" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\AM_Program\bin" /D /Y

xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_Ape_PlugIn\bin\Debug" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_AScore_PlugIn\bin\Debug" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_Cyclops_PlugIn\bin\Debug" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_Decon2ls_PlugIn_Decon2LSV2\bin\Debug" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_DtaRefinery_PlugIn\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_DTASpectraFileGen_PlugIn\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_Extraction_PlugIn\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_IDM_Plugin\bin\Debug" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_IDPicker_PlugIn\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_LCMSFeatureFinder_Plugin\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_Masic_Plugin\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_MSAlign_Plugin\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_MSAlign_Quant_Plugin\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_MSDeconv_Plugin\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_MSGF_PlugIn\Bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_MSGFDB_PlugIn\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_MSGFDB_PlugIn\MSGFPlusIndexFileCopier\bin\Debug" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_MSMSSpectrumFilter_PlugIn\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_MSXML_Bruker_PlugIn\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_MSXML_Gen_PlugIn\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_Phospho_FDR_Aggregator_PlugIn\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_PRIDE_Converter_PlugIn\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_ResultsXfer_PlugIn\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_SMAQC_PlugIn\bin" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\Plugins\AM_XTandem_PlugIn\bin" /D /Y

@echo off
echo.
echo.
if not "%1"=="NoPause" pause
echo.
echo.
echo.
@echo on

xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\DataPackage_Archive_Manager\Lib" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\DataPackage_Archive_Manager\bin\Debug" /D /Y

xcopy Debug\net8.0\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Capture_Task_Manager\RefLib" /D /Y
xcopy Debug\net8.0\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Capture_Task_Manager\DeployedFiles" /D /Y
xcopy Debug\net8.0\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Capture_Task_Manager\CaptureTaskManager\bin\Debug" /D /Y
xcopy Debug\net8.0\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\Capture_Task_Manager\ArchiveVerifyPlugin\bin\Debug" /D /Y

xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\MyEMSL_MTS_File_Cache_Manager\Lib" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\MyEMSL_MTS_File_Cache_Manager\bin\Debug" /D /Y

xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\DMS_Space_Manager\DMS_Space_Manager\RefLib" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\DMS_Space_Manager\DMS_Space_Manager\Bin\Debug" /D /Y
xcopy Debug\net48\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Managers\DMS_Space_Manager\DMS_Space_Manager\Bin\Release" /D /Y

@echo off
echo.
echo.
if not "%1"=="NoPause" pause
echo.
echo.
echo.
@echo on

xcopy Debug\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\Mage\lib" /D /Y

xcopy Debug\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\DeployedFiles\MageFileProcessor" /D /Y
xcopy Debug\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\DeployedFiles\MageExtractor" /D /Y
xcopy Debug\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\DeployedFiles\MageFilePackager" /D /Y
xcopy Debug\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\DeployedFiles\MageMetaDataProcessor" /D /Y
xcopy Debug\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\DeployedFiles\Ranger" /D /Y

xcopy Debug\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Programs\PurgedMzMLFileRetriever\bin" /D /Y
xcopy Debug\MyEMSLReader.dll "F:\Documents\Projects\DataMining\DMS_Programs\PurgedMzMLFileRetriever\lib" /D /Y

xcopy Debug\MyEMSLReader.dll "F:\Documents\Projects\John_Sandoval\APE\lib" /D /Y

xcopy Debug\MyEMSLReader.dll "F:\Documents\Projects\Josh_Aldrich\InterferenceDetection\InterDetect\DLLLibrary" /D /Y
xcopy Debug\MyEMSLReader.dll "F:\Documents\Projects\Josh_Aldrich\ProteinParsimony\SetCover\Lib" /D /Y

xcopy Debug\MyEMSLReader.dll "C:\DMS_Programs\MyEMSLDownloader" /D /Y

xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\AM_Common" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\AM_Program\bin" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\Capture_Task_Manager\ArchiveVerifyPlugin\bin\Debug" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\Capture_Task_Manager\CaptureTaskManager\bin\Debug" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\Capture_Task_Manager\DeployedFiles" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\Capture_Task_Manager\RefLib" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\DataPackage_Archive_Manager\bin\Debug" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\DataPackage_Archive_Manager\Lib" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\DMS_Space_Manager\DMS_Space_Manager\Bin\Debug" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\DMS_Space_Manager\DMS_Space_Manager\Bin\Release" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\DMS_Space_Manager\DMS_Space_Manager\RefLib" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\MyEMSL_MTS_File_Cache_Manager\bin\Debug" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Managers\MyEMSL_MTS_File_Cache_Manager\Lib" /D /Y

xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Programs\DMS_Dataset_Retriever\bin" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Programs\DMS_Dataset_Retriever\Lib" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\DeployedFiles\MageExtractor" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\DeployedFiles\MageFileProcessor" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\Mage\bin\Release" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\Mage\lib" /D /Y

xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Programs\PurgedMzMLFileRetriever\bin" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\DataMining\DMS_Programs\PurgedMzMLFileRetriever\lib" /D /Y

xcopy Debug\Pacifica.dll "F:\Documents\Projects\John_Sandoval\APE\lib" /D /Y

xcopy Debug\Pacifica.dll "F:\Documents\Projects\Josh_Aldrich\InterferenceDetection\InterDetect\DLLLibrary" /D /Y
xcopy Debug\Pacifica.dll "F:\Documents\Projects\Josh_Aldrich\ProteinParsimony\SetCover\Lib" /D /Y

xcopy Debug\Pacifica.dll "C:\DMS_Programs\MyEMSLDownloader" /D /Y

xcopy Debug\Newtonsoft.Json.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\AM_Common" /D /Y
xcopy Debug\Newtonsoft.Json.dll "F:\Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\AM_Program\bin" /D /Y

xcopy Debug\Newtonsoft.Json.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\Mage\lib" /D /Y
xcopy Debug\Newtonsoft.Json.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\Mage\bin\Release" /D /Y
xcopy Debug\Newtonsoft.Json.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\DeployedFiles\MageFileProcessor" /D /Y
xcopy Debug\Newtonsoft.Json.dll "F:\Documents\Projects\DataMining\DMS_Programs\Mage\DeployedFiles\MageExtractor" /D /Y

xcopy Debug\Newtonsoft.Json.dll "F:\Documents\Projects\DataMining\DMS_Managers\MyEMSL_MTS_File_Cache_Manager\Lib" /D /Y
xcopy Debug\Newtonsoft.Json.dll "F:\Documents\Projects\DataMining\DMS_Managers\MyEMSL_MTS_File_Cache_Manager\bin\Debug" /D /Y

xcopy Debug\Newtonsoft.Json.dll "F:\Documents\Projects\DataMining\DMS_Programs\PurgedMzMLFileRetriever\bin" /D /Y
xcopy Debug\Newtonsoft.Json.dll "F:\Documents\Projects\DataMining\DMS_Programs\PurgedMzMLFileRetriever\lib" /D /Y

xcopy Debug\Newtonsoft.Json.dll "C:\DMS_Programs\MyEMSLDownloader" /D /Y

:Done
if not "%1"=="NoPause" pause
