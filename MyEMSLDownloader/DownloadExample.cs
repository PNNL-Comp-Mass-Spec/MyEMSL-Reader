using System;
using System.Collections.Generic;
using MyEMSLReader;

namespace MyEMSLDownloader
{
    class DownloadExample
    {

        public void StartTest()
        {

            DatasetListInfo datasetListInfo = new DatasetListInfo();
            datasetListInfo.ErrorEvent += datasetListInfo_ErrorEvent;
            datasetListInfo.MessageEvent += datasetListInfo_MessageEvent;

            GetFileStart(datasetListInfo);
            
        }


        protected void GetFileStart(DatasetListInfo datasetListInfo)
        {

            string datasetFolderPathFromDMS = @"\\MyEMSL\Exact01\2010_2\SysVirol_SM001_mock-7d_3_B_11May10_Phoenix_10-03-35";

            if (datasetFolderPathFromDMS.StartsWith(@"\\MyEMSL"))
            {
                string datasetName = "SysVirol_SM001_mock-7d_3_B_11May10_Phoenix_10-03-35";

                string outputFolderPath = @"C:\temp";

                GetRawFile(datasetListInfo, datasetName, outputFolderPath);
            }

        }

        protected void GetRawFile(DatasetListInfo datasetListInfo, string datasetName, string outputFolderPath)
        {            
            datasetListInfo.AddDataset(datasetName);

            var archiveFiles = datasetListInfo.FindFiles(datasetName + ".raw", subFolderName: string.Empty, recurse: false);

            DownloadFiles(datasetListInfo, archiveFiles, outputFolderPath);
        }


		private void DownloadFiles(DatasetListInfo datasetListInfo, IEnumerable<DatasetFolderOrFileInfo> archiveFiles, string outputFolderPath)
		{
			datasetListInfo.ClearDownloadQueue();
			foreach (var archiveFile in archiveFiles)
			{
				datasetListInfo.AddFileToDownloadQueue(archiveFile.FileInfo);
			}

			Downloader.DownloadFolderLayout folderLayout;
			if (string.IsNullOrEmpty(outputFolderPath))
				folderLayout = Downloader.DownloadFolderLayout.DatasetNameAndSubFolders;
			else
				folderLayout = Downloader.DownloadFolderLayout.SingleDataset;

			bool success = datasetListInfo.ProcessDownloadQueue(outputFolderPath, folderLayout);

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

		protected void datasetListInfo_ErrorEvent(object sender, MessageEventArgs e)
		{
			ShowErrorMessage(e.Message);
		}

		protected void datasetListInfo_MessageEvent(object sender, MessageEventArgs e)
		{
			Console.WriteLine(e.Message);
		}

		#endregion

    }

}
