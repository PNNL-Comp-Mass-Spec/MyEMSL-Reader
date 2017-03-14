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

            var datasetListInfo = new DatasetListInfo();
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


        private void DownloadFiles(DatasetListInfo datasetListInfo, IEnumerable<DatasetFolderOrFileInfo> archiveFiles, string outputFolderPath)
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

        private void ShowErrorMessage(string strMessage)
        {
            const string strSeparator = "------------------------------------------------------------------------------";

            Console.WriteLine();
            Console.WriteLine(strSeparator);
            Console.WriteLine(strMessage);
            Console.WriteLine(strSeparator);
            Console.WriteLine();

        }

        #region "Event Handlers"

        private void OnDebugEvent(string message)
        {
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.WriteLine(message);
            Console.ResetColor();
        }

        private void OnStatusEvent(string message)
        {
            Console.WriteLine(message);
        }

        private void OnErrorEvent(string message, Exception ex)
        {
            ShowErrorMessage(message);

            if (ex != null)
            {
                Console.WriteLine(clsStackTraceFormatter.GetExceptionStackTraceMultiLine(ex));
            }
        }

        private void OnWarningEvent(string message)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(message);
            Console.ResetColor();
        }

        #endregion

    }

}
