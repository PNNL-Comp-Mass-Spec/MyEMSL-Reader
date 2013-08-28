using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Tester
{
	class Program
	{
		static double mPercentComplete;
		static DateTime mLastProgressUpdateTime = DateTime.UtcNow;

		static void Main(string[] args)
		{
			var lstFileIDs = TestReader();

			TestDownloader(lstFileIDs);
		}

		static List<long> TestReader()
		{
			var reader = new MyEMSLReader.Reader();
			reader.IncludeAllRevisions = false;

			// Attach events
			reader.ErrorEvent += new MyEMSLReader.MyEMSLBase.MessageEventHandler(reader_ErrorEvent);
			reader.MessageEvent += new MyEMSLReader.MyEMSLBase.MessageEventHandler(reader_MessageEvent);
			reader.ProgressEvent += new MyEMSLReader.MyEMSLBase.ProgressEventHandler(reader_ProgressEvent);


			string datasetName;
			string subDir = "";

			//datasetName = "Blank_B-2_20Apr12_Draco_12-02-37";

			datasetName = "QC_Shew_11_06_pt5_d2_11Jun12_Draco_12-04-14";

			// Dataset where all of the files are purged from spinning disk
			//datasetName = "DRAD224_17_28Apr05_Doc_0105-07";
			//subDir = "MSG201203122150_Auto805773";

			var lstFileIDs = new List<long>();

			bool testExistingCart = false;
			if (testExistingCart)
			{
				lstFileIDs.Add(662508);
				lstFileIDs.Add(662509);
				lstFileIDs.Add(662510);
				lstFileIDs.Add(662511);
				lstFileIDs.Add(662512);
				lstFileIDs.Add(662513);
				lstFileIDs.Add(662514);
				lstFileIDs.Add(662515);
				lstFileIDs.Add(662516);

			}
			else
			{
				try
				{
					var results = reader.FindFilesByDatasetName(datasetName, subDir);

					foreach (var archivedFile in results)
						lstFileIDs.Add(archivedFile.FileID);
				}
				catch (Exception ex)
				{
					Console.WriteLine("Exception from reader: " + ex.Message);
				}

			}

			return lstFileIDs;

		}

		static void TestDownloader(List<long> lstFileIDs)
		{
			var downloader = new MyEMSLReader.Downloader();

			downloader.ErrorEvent += new MyEMSLReader.MyEMSLBase.MessageEventHandler(reader_ErrorEvent);
			downloader.MessageEvent += new MyEMSLReader.MyEMSLBase.MessageEventHandler(reader_MessageEvent);
			downloader.ProgressEvent += new MyEMSLReader.MyEMSLBase.ProgressEventHandler(reader_ProgressEvent);

			// Temporary override for debugging purposes
			lstFileIDs.RemoveRange(10, lstFileIDs.Count - 10);

			try
			{
				downloader.DownloadFiles(lstFileIDs, @"F:\Temp\MyEMSL", MyEMSLReader.Downloader.DownloadFolderLayout.SingleDataset);
			}
			catch (Exception ex)
			{
				Console.WriteLine("Exception from downloader: " + ex.Message);
			}

		}

		#region "Event Handlers"
		static void reader_ErrorEvent(object sender, MyEMSLReader.MessageEventArgs e)
		{
			Console.WriteLine("Error: " + e.Message);
		}

		static void reader_MessageEvent(object sender, MyEMSLReader.MessageEventArgs e)
		{
			Console.WriteLine(e.Message);
		}

		static void reader_ProgressEvent(object sender, MyEMSLReader.ProgressEventArgs e)
		{
			if (e.PercentComplete > mPercentComplete || DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 30)
			{
				if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 1)
				{
					Console.WriteLine("Percent complete: " + e.PercentComplete);
					mPercentComplete = e.PercentComplete;
					mLastProgressUpdateTime = DateTime.UtcNow;
				}
			}
		}


		#endregion
	}
}
