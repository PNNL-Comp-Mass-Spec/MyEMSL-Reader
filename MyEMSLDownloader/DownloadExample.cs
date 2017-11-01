using System;
using System.Collections.Generic;
using MyEMSLReader;
using PRISM;

namespace MyEMSLDownloader
{
    internal class DownloadExample
    {

        public void StartTest()
        {

            var datasetListInfo = new DatasetListInfo
            {
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

        private void GetFileStart(DatasetListInfo datasetListInfo)
        {

            var datasetFolderPathFromDMS = @"\\MyEMSL\Exact01\2010_2\SysVirol_SM001_mock-7d_3_B_11May10_Phoenix_10-03-35";

            if (datasetFolderPathFromDMS.StartsWith(@"\\MyEMSL"))
            {
                var datasetName = "SysVirol_SM001_mock-7d_3_B_11May10_Phoenix_10-03-35";

                var outputFolderPath = @"C:\temp";

                GetRawFile(datasetListInfo, datasetName, outputFolderPath);
            }

        }

        private void GetRawFile(DatasetListInfo datasetListInfo, string datasetName, string outputFolderPath)
        {
            datasetListInfo.AddDataset(datasetName);

            Console.WriteLine("Searching for " + datasetName + ".raw");
            var archiveFiles = datasetListInfo.FindFiles(datasetName + ".raw", subFolderName: string.Empty, recurse: false);

            DownloadFiles(datasetListInfo, archiveFiles, outputFolderPath);
        }

        private void DownloadFiles(DatasetInfoBase datasetListInfo, IEnumerable<DatasetFolderOrFileInfo> archiveFiles, string outputFolderPath)
        {
            datasetListInfo.ClearDownloadQueue();
            foreach (var archiveFile in archiveFiles)
            {
                Console.WriteLine("Downloading File ID {0}: {1}", archiveFile.FileID, archiveFile.FileInfo.Filename);
                datasetListInfo.AddFileToDownloadQueue(archiveFile.FileInfo);
            }

            Downloader.DownloadFolderLayout folderLayout;
            if (string.IsNullOrEmpty(outputFolderPath))
                folderLayout = Downloader.DownloadFolderLayout.DatasetNameAndSubFolders;
            else
                folderLayout = Downloader.DownloadFolderLayout.SingleDataset;

            var success = datasetListInfo.ProcessDownloadQueue(outputFolderPath, folderLayout);

            if (success)
            {
                Console.WriteLine("Download complete");
            }
            else
            {
                ShowErrorMessage("Download failed");
            }

        }

        private void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        #region "Event Handlers"

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

        #endregion

    }

}
