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

			if (lstFileIDs.Count == 0)
				Console.WriteLine("Reader did not find any files");
			else if (true)
				TestDownloader(lstFileIDs);
		}

		static List<long> TestReader()
		{
			var reader = new MyEMSLReader.Reader();
			reader.IncludeAllRevisions = false;

			// Attach events			
			reader.ErrorEvent += new MyEMSLReader.MessageEventHandler(reader_ErrorEvent);
			reader.MessageEvent += new MyEMSLReader.MessageEventHandler(reader_MessageEvent);
			reader.ProgressEvent += new MyEMSLReader.ProgressEventHandler(reader_ProgressEvent);

			var lstFileIDs = new List<long>();
			
			//lstFileIDs = TestMultiDataset(reader);
			//Console.WriteLine();
			//Console.WriteLine();

			//lstFileIDs = TestMultiDatasetID(reader);
			//Console.WriteLine();
			//Console.WriteLine();
			
			//lstFileIDs = TestOneDataset(reader);
			//Console.WriteLine();
			//Console.WriteLine();
			

			lstFileIDs = TestOneDataPackage(reader);
			Console.WriteLine();
			Console.WriteLine();

			return lstFileIDs;

		}

		static List<long> TestOneDataPackage(MyEMSLReader.Reader reader)
		{
			var lstFileIDs = new List<long>();
			int dataPkgID = 810;
			string subDir = "";

			try
			{
				var results = reader.FindFilesByDataPackageID(dataPkgID, subDir);

				foreach (var archivedFile in results)
				{
					Console.WriteLine(archivedFile.RelativePathUnix);
					lstFileIDs.Add(archivedFile.FileID);
				}

				var dataPackageInfoCache = new MyEMSLReader.DatasetPackageListInfo();
				dataPackageInfoCache.AddDataPackage(810);

				var archiveFiles = dataPackageInfoCache.FindFiles("files.txt", @"PPX201306061649_Auto952181");
				var archiveFiles2 = dataPackageInfoCache.FindFiles("files.txt", @"810_DatasetQC_Pride_Submission__VOrbitrap_part_2\PPX201306061649_Auto952181");

			}
			catch (Exception ex)
			{
				Console.WriteLine("Exception from reader: " + ex.Message);
			}

			return lstFileIDs;
		}

		static List<long> TestOneDataset(MyEMSLReader.Reader reader)
		{
			var lstFileIDs = new List<long>();
			string datasetName;
			string subDir = "";

			//datasetName = "Blank_B-2_20Apr12_Draco_12-02-37";
			// datasetName = "QC_Shew_11_06_pt5_d2_11Jun12_Draco_12-04-14";
			// Dataset where all of the files were purged from spinning disk (but have now been unpurged)
			//datasetName = "2013_05_28_U01-B_Wilkins_neg_4M_0p1acc_8x_144_000001";
			//subDir = "";

			datasetName = "SWT_LCQData_300";
			subDir = "SIC201309041722_Auto976603";

			try
			{
				var results = reader.FindFilesByDatasetName(datasetName, subDir);

				foreach (var archivedFile in results)
				{
					Console.WriteLine(archivedFile.RelativePathUnix);
					lstFileIDs.Add(archivedFile.FileID);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine("Exception from reader: " + ex.Message);
			}

			return lstFileIDs;
		}

		static List<long> TestMultiDataset(MyEMSLReader.Reader reader)
		{
			var lstFileIDs = new List<long>();

			var dctDatasetsAndSubDirs = new Dictionary<string, string>();
			dctDatasetsAndSubDirs.Add("SWT_LCQData_300", "SIC201309041722_Auto976603");
			dctDatasetsAndSubDirs.Add("SysVirol_IFL001_10xA_07_11Sep13_Tiger_13-07-36", "SIC201309112159_Auto977994");
			dctDatasetsAndSubDirs.Add("SysVirol_IFL001_10xA_08_11Sep13_Tiger_13-07-34", "");

			try
			{
				var results = reader.FindFilesByDatasetName(dctDatasetsAndSubDirs);

				foreach (var archivedFile in results)
				{
					Console.WriteLine(archivedFile.RelativePathUnix);
					lstFileIDs.Add(archivedFile.FileID);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine("Exception from reader: " + ex.Message);
			}

			return lstFileIDs;
		}

		static List<long> TestMultiDatasetID(MyEMSLReader.Reader reader)
		{
			var lstFileIDs = new List<long>();

			var dctDatasetsAndSubDirs = new Dictionary<int, string>();
			dctDatasetsAndSubDirs.Add(54007, "SIC201309041722_Auto976603");
			dctDatasetsAndSubDirs.Add(334448, "SIC201309112159_Auto977994");
			dctDatasetsAndSubDirs.Add(334455, "");

			try
			{
				var results = reader.FindFilesByDatasetID(dctDatasetsAndSubDirs);

				foreach (var archivedFile in results)
				{
					Console.WriteLine(archivedFile.RelativePathUnix);
					lstFileIDs.Add(archivedFile.FileID);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine("Exception from reader: " + ex.Message);
			}

			return lstFileIDs;
		}
		static void TestDownloader(List<long> lstFileIDs)
		{
			var downloader = new MyEMSLReader.Downloader();

			downloader.ErrorEvent += new MyEMSLReader.MessageEventHandler(reader_ErrorEvent);
			downloader.MessageEvent += new MyEMSLReader.MessageEventHandler(reader_MessageEvent);
			downloader.ProgressEvent += new MyEMSLReader.ProgressEventHandler(reader_ProgressEvent);

			downloader.OverwriteMode = MyEMSLReader.Downloader.Overwrite.IfChanged;

			try
			{
				downloader.DownloadFiles(lstFileIDs, @"F:\Temp\MyEMSL", MyEMSLReader.Downloader.DownloadFolderLayout.DatasetNameAndSubFolders);
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
					Console.WriteLine("Percent complete: " + e.PercentComplete.ToString("0.0") + "%");
					mPercentComplete = e.PercentComplete;
					mLastProgressUpdateTime = DateTime.UtcNow;
				}
			}
		}


		#endregion
	}
}
