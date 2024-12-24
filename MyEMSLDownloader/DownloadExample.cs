using System;
using System.Collections.Generic;
using MyEMSLReader;
using PRISM;

namespace MyEMSLDownloader
{
    internal class DownloadExample
    {
        // Ignore Spelling: Downloader, Virol

        public void StartTest()
        {
            var datasetListInfo = new DatasetListInfo
            {
                IncludeAllRevisions = false,
                ReportMetadataURLs = true,
                ThrowErrors = false,
                TraceMode = false
            };

            datasetListInfo.DebugEvent += OnDebugEvent;
            datasetListInfo.StatusEvent += OnStatusEvent;
            datasetListInfo.ErrorEvent += OnErrorEvent;
            datasetListInfo.WarningEvent += OnWarningEvent;

            GetFileStart(datasetListInfo);
        }

        private static void GetFileStart(DatasetListInfo datasetListInfo)
        {
            const string datasetDirectoryPathFromDMS = @"\\MyEMSL\Exact01\2010_2\SysVirol_SM001_mock-7d_3_B_11May10_Phoenix_10-03-35";

            if (datasetDirectoryPathFromDMS.StartsWith(@"\\MyEMSL"))
            {
                const string datasetName = "SysVirol_SM001_mock-7d_3_B_11May10_Phoenix_10-03-35";

                const string outputDirectoryPath = @"C:\temp";

                GetRawFile(datasetListInfo, datasetName, outputDirectoryPath);
            }
        }

        private static void GetRawFile(DatasetListInfo datasetListInfo, string datasetName, string outputDirectoryPath)
        {
            datasetListInfo.AddDataset(datasetName);

            Console.WriteLine("Searching for " + datasetName + ".raw");
            var archiveFiles = datasetListInfo.FindFiles(datasetName + ".raw", subdirectoryName: string.Empty, datasetName: string.Empty, fileIDList: string.Empty, recurse: false);

            DownloadFiles(datasetListInfo, archiveFiles, outputDirectoryPath);
        }

        private static void DownloadFiles(DatasetInfoBase datasetListInfo, IEnumerable<DatasetDirectoryOrFileInfo> archiveFiles, string outputDirectoryPath)
        {
            datasetListInfo.ClearDownloadQueue();

            foreach (var archiveFile in archiveFiles)
            {
                Console.WriteLine("Downloading File ID {0}: {1}", archiveFile.FileID, archiveFile.FileInfo.Filename);
                datasetListInfo.AddFileToDownloadQueue(archiveFile.FileInfo);
            }

            Downloader.DownloadLayout directoryLayout;

            if (string.IsNullOrEmpty(outputDirectoryPath))
                directoryLayout = Downloader.DownloadLayout.DatasetNameAndSubdirectories;
            else
                directoryLayout = Downloader.DownloadLayout.SingleDataset;

            var success = datasetListInfo.ProcessDownloadQueue(outputDirectoryPath, directoryLayout);

            if (success)
            {
                Console.WriteLine("Download complete");
            }
            else
            {
                ShowErrorMessage("Download failed");
            }
        }

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private void OnDebugEvent(string message)
        {
            ConsoleMsgUtils.ShowDebug(message);
        }

        private void OnStatusEvent(string message)
        {
            Console.WriteLine(message);
        }

        private void OnErrorEvent(string message, Exception ex)
        {
            ShowErrorMessage(message, ex);
        }

        private void OnWarningEvent(string message)
        {
            ConsoleMsgUtils.ShowWarning(message);
        }
    }
}
