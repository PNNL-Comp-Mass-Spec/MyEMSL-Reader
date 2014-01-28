using System;
using System.Collections.Generic;
using FileProcessor;
using MyEMSLReader;

namespace MyEMSLDownloader
{
	class Program
	{
		private const string PROGRAM_DATE = "December 16, 2013";

		static double mPercentComplete;
		static DateTime mLastProgressUpdateTime = DateTime.UtcNow;

		private static string mDatasetName;
		private static int mDataPkgID;
		private static string mSubfolder;
		private static string mFileMask;
		private static string mOutputFolderPath;

		private static bool mMultiDatasetMode;
		private static bool mPreviewMode;
		private static bool mAutoTestMode;

		private static MyEMSLReader.DatasetListInfo mDatasetListInfo;
		private static MyEMSLReader.DataPackageListInfo mDataPackageListInfo;

		static int Main(string[] args)
		{
			var objParseCommandLine = new FileProcessor.clsParseCommandLine();

			mDatasetName = string.Empty;
			mDataPkgID = 0;
			mSubfolder = string.Empty;
			mFileMask = string.Empty;
			mOutputFolderPath = string.Empty;

			mMultiDatasetMode = false;

			mPreviewMode = false;
			mAutoTestMode = false;

			try
			{
				bool success = false;

				if (objParseCommandLine.ParseCommandLine())
				{
					if (SetOptionsUsingCommandLineParameters(objParseCommandLine))
						success = true;
				}

				if (!success ||
					objParseCommandLine.NeedToShowHelp ||
					objParseCommandLine.ParameterCount + objParseCommandLine.NonSwitchParameterCount == 0)
				{
					ShowProgramHelp();
					return -1;

				}

				mDatasetListInfo = new DatasetListInfo();
				mDataPackageListInfo = new DataPackageListInfo();

				mDatasetListInfo.ErrorEvent += mDatasetListInfo_ErrorEvent;
				mDatasetListInfo.MessageEvent += mDatasetListInfo_MessageEvent;

				mDataPackageListInfo.ErrorEvent += mDatasetListInfo_ErrorEvent;
				mDataPackageListInfo.MessageEvent += mDatasetListInfo_MessageEvent;

				if (mAutoTestMode)
				{

					// var exampleDownloader = new DownloadExample();
					// exampleDownloader.StartTest();

					var lstFileIDs = TestReader();

					if (lstFileIDs.Count == 0)
					{
						Console.WriteLine("Reader did not find any files");
					}
					else if (!mPreviewMode)
					{
						TestDownloader(lstFileIDs);
					}
					Console.WriteLine();

					var archiveFiles = TestDatasetListInfo();

					if (archiveFiles.Count == 0)
						Console.WriteLine("DatasetListInfo did not find any files");
					else
					{
						ShowFiles(archiveFiles);

						if (!mPreviewMode)
							TestDownloader(archiveFiles);
					}

				}
				else
				{
					List<DatasetFolderOrFileInfo> archiveFiles;

					if (mDataPkgID > 0)
						archiveFiles = FindDataPkgFiles(mDataPkgID, mSubfolder, mFileMask);
					else
						archiveFiles = FindDatasetFiles(mDatasetName, mSubfolder, mFileMask);

					if (mPreviewMode)
						Console.WriteLine("\nPreviewing files that would be downloaded\n");

					ShowFiles(archiveFiles);

					if (!mPreviewMode)
					{
						Console.WriteLine();
						if (mDataPkgID > 0)
							DownloadDataPackageFiles(archiveFiles, mOutputFolderPath);
						else
							DownloadDatasetFiles(archiveFiles, mOutputFolderPath);
					}

				}
			}
			catch (Exception ex)
			{
				Console.WriteLine("Error occurred in Program->Main: " + Environment.NewLine + ex.Message);
				Console.WriteLine(ex.StackTrace);
				return -1;
			}

			return 0;
		}

		private static void DownloadDatasetFiles(IEnumerable<DatasetFolderOrFileInfo> archiveFiles, string outputFolderPath)
		{
			DownloadFiles(mDatasetListInfo, archiveFiles, outputFolderPath);
		}

		private static void DownloadDataPackageFiles(IEnumerable<DatasetFolderOrFileInfo> archiveFiles, string outputFolderPath)
		{
			DownloadFiles(mDataPackageListInfo, archiveFiles, outputFolderPath);
		}

		private static void DownloadFiles(DatasetInfoBase downloader, IEnumerable<DatasetFolderOrFileInfo> archiveFiles, string outputFolderPath)
		{
			downloader.ClearDownloadQueue();
			foreach (var archiveFile in archiveFiles)
			{
				downloader.AddFileToDownloadQueue(archiveFile.FileInfo);
			}

			Downloader.DownloadFolderLayout folderLayout;
			if (mMultiDatasetMode)
				folderLayout = Downloader.DownloadFolderLayout.DatasetNameAndSubFolders;
			else
				folderLayout = Downloader.DownloadFolderLayout.SingleDataset;

			bool success = downloader.ProcessDownloadQueue(outputFolderPath, folderLayout);

			if (success)
			{
				Console.WriteLine("Download complete");
			}
			else
			{
				ShowErrorMessage("Download failed");
			}

		}

		private static List<DatasetFolderOrFileInfo> FindDatasetFiles(string datasetName, string subfolder, string fileMask)
		{

			mDatasetListInfo.AddDataset(datasetName);

			if (string.IsNullOrEmpty(fileMask))
				fileMask = "*";

			var archiveFiles = mDatasetListInfo.FindFiles(fileMask, subfolder);

			return archiveFiles;
		}

		private static List<DatasetFolderOrFileInfo> FindDataPkgFiles(int dataPkgID, string subfolder, string fileMask)
		{
			mDataPackageListInfo.AddDataPackage(dataPkgID);

			if (string.IsNullOrEmpty(fileMask))
				fileMask = "*";

			var archiveFiles = mDataPackageListInfo.FindFiles(fileMask, subfolder);

			return archiveFiles;
		}

		private static void ShowFiles(IEnumerable<DatasetFolderOrFileInfo> archiveFiles)
		{
			foreach (var archiveFile in archiveFiles)
			{
				Console.WriteLine(archiveFile.FileInfo.RelativePathWindows);
			}
		}

		private static string GetAppVersion()
		{
			return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString() + " (" + PROGRAM_DATE + ")";
		}

		private static bool SetOptionsUsingCommandLineParameters(FileProcessor.clsParseCommandLine objParseCommandLine)
		{
			// Returns True if no problems; otherwise, returns false
			var lstValidParameters = new List<string> { "Dataset", "DataPkg", "SubDir", "Files", "O", "D", "Preview", "Test" };

			try
			{
				// Make sure no invalid parameters are present
				if (objParseCommandLine.InvalidParametersPresent(lstValidParameters))
				{
					var badArguments = new List<string>();
					foreach (string item in objParseCommandLine.InvalidParameters(lstValidParameters))
					{
						badArguments.Add("/" + item);
					}

					ShowErrorMessage("Invalid commmand line parameters", badArguments);

					return false;
				}

				// Query objParseCommandLine to see if various parameters are present						

				if (objParseCommandLine.NonSwitchParameterCount > 0)
					mDatasetName = objParseCommandLine.RetrieveNonSwitchParameter(0);

				if (objParseCommandLine.NonSwitchParameterCount > 1)
					mSubfolder = objParseCommandLine.RetrieveNonSwitchParameter(1);

				if (!ParseParameter(objParseCommandLine, "Dataset", "a dataset name", ref mDatasetName)) return false;

				string dataPkgString = "";
				if (!ParseParameter(objParseCommandLine, "DataPkg", "a data package ID", ref dataPkgString)) return false;
				if (!string.IsNullOrEmpty(dataPkgString))
				{
					if (!int.TryParse(dataPkgString, out mDataPkgID))
					{
						ShowErrorMessage("Data package ID must be an integer: " + dataPkgString);
						return false;
					}
				}

				if (!ParseParameter(objParseCommandLine, "SubDir", "a subfolder name", ref mSubfolder)) return false;
				if (!ParseParameter(objParseCommandLine, "Files", "a file mas", ref mFileMask)) return false;

				if (!ParseParameter(objParseCommandLine, "O", "an output folder path", ref mOutputFolderPath)) return false;

				if (objParseCommandLine.IsParameterPresent("D"))
				{
					mMultiDatasetMode = true;
				}

				if (objParseCommandLine.IsParameterPresent("Preview"))
				{
					mPreviewMode = true;
				}

				if (objParseCommandLine.IsParameterPresent("Test"))
				{
					mAutoTestMode = true;
				}

				return true;
			}
			catch (Exception ex)
			{
				ShowErrorMessage("Error parsing the command line parameters: " + Environment.NewLine + ex.Message);
			}

			return false;
		}

		private static bool ParseParameter(clsParseCommandLine objParseCommandLine, string parameterName, string description, ref string targetVariable)
		{
			string value;
			if (objParseCommandLine.RetrieveValueForParameter(parameterName, out value))
			{
				if (string.IsNullOrWhiteSpace(value))
				{
					ShowErrorMessage("/" + parameterName + " does not have " + description);
					return false;
				}
				targetVariable = string.Copy(value);
			}
			return true;
		}

		private static void ShowErrorMessage(string strMessage)
		{
			const string strSeparator = "------------------------------------------------------------------------------";

			Console.WriteLine();
			Console.WriteLine(strSeparator);
			Console.WriteLine(strMessage);
			Console.WriteLine(strSeparator);
			Console.WriteLine();

			WriteToErrorStream(strMessage);
		}

		private static void ShowErrorMessage(string strTitle, IEnumerable<string> items)
		{
			const string strSeparator = "------------------------------------------------------------------------------";

			Console.WriteLine();
			Console.WriteLine(strSeparator);
			Console.WriteLine(strTitle);
			string strMessage = strTitle + ":";

			foreach (string item in items)
			{
				Console.WriteLine("   " + item);
				strMessage += " " + item;
			}
			Console.WriteLine(strSeparator);
			Console.WriteLine();

			WriteToErrorStream(strMessage);
		}


		private static void ShowProgramHelp()
		{
			string exeName = System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location);

			try
			{
				Console.WriteLine();
				Console.WriteLine("This program downloads files from MyEMSL");
				Console.WriteLine();

				Console.Write("Program syntax #1:" + Environment.NewLine + exeName);
				Console.WriteLine(" DatasetName [SubFolderName] [/Files:FileMask] [/O:OutputFolder] [/D] [/Preview]");

				Console.WriteLine();
				Console.Write("Program syntax #2:" + Environment.NewLine + exeName);
				Console.WriteLine(" /Dataset:DatasetName [/SubDir:SubFolderName] [/Files:FileMask] [/O:OutputFolder] [/D] [/Preview]");

				Console.WriteLine();
				Console.Write("Program syntax #3:" + Environment.NewLine + exeName);
				Console.WriteLine(" /Test [/Preview]");

				Console.WriteLine();
				Console.WriteLine("To download files for a given dataset, enter the dataset name, plus optionally the SubFolder name");
				Console.WriteLine("The names can be entered separated by spaces, or using /Dataset plus optionally /SubDir");
				Console.WriteLine();
				Console.WriteLine("Use /Files to filter for specific files, for example /Files:*.txt");
				Console.WriteLine("Files will be downloaded to the folder with the .exe; override using /O");
				Console.WriteLine("Use /D to create a folder with the dataset name, then store the files within that folder");
				Console.WriteLine();
				Console.WriteLine("Alternatively, use /Test to perform automatic tests using predefined dataset names");
				Console.WriteLine();
				Console.WriteLine("Use /Preview to view files that would be downloaded, but not actually download them");
				Console.WriteLine();
				Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2013");
				Console.WriteLine("Version: " + GetAppVersion());
				Console.WriteLine();

				Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com");
				Console.WriteLine("Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov or http://www.sysbio.org/resources/staff/");
				Console.WriteLine();

				// Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
				System.Threading.Thread.Sleep(750);

			}
			catch (Exception ex)
			{
				Console.WriteLine("Error displaying the program syntax: " + ex.Message);
			}

		}


		static List<DatasetFolderOrFileInfo> TestDatasetListInfo()
		{
			Console.WriteLine("Looking for files for test datasets using the DatasetListInfo class");
			Console.WriteLine();

			mDatasetListInfo.AddDataset("2013_10_01_CMPD04_000005");

			var archiveFiles = mDatasetListInfo.FindFiles("*");

			return archiveFiles;

		}
		static List<long> TestReader()
		{

			Console.WriteLine("Looking for files for test datasets using the Reader class");
			Console.WriteLine();

			var reader = new MyEMSLReader.Reader
			{
				IncludeAllRevisions = false
			};

			// Attach events			
			reader.ErrorEvent += reader_ErrorEvent;
			reader.MessageEvent += reader_MessageEvent;
			reader.ProgressEvent += reader_ProgressEvent;

			var lstFileIDs1 = TestMultiDataset(reader);
			Console.WriteLine();
			Console.WriteLine();

			var lstFileIDs2 = TestMultiDatasetID(reader);
			Console.WriteLine();
			Console.WriteLine();

			var lstFileIDs3 = TestOneDataset(reader);
			Console.WriteLine();
			Console.WriteLine();

			var lstFileIDs4 = TestOneDataPackage(reader);
			Console.WriteLine();
			Console.WriteLine();

			return lstFileIDs1;

		}

		static List<long> TestOneDataPackage(MyEMSLReader.Reader reader)
		{
			var lstFileIDs = new List<long>();
			int dataPkgID = 814;
			string subDir = "";

			try
			{
				var results = reader.FindFilesByDataPackageID(dataPkgID, subDir);

				foreach (var archivedFile in results)
				{
					Console.WriteLine(archivedFile.RelativePathUnix);
					lstFileIDs.Add(archivedFile.FileID);
				}

				var dataPackageInfoCache = new MyEMSLReader.DataPackageListInfo();
				dataPackageInfoCache.AddDataPackage(814);

				var archiveFiles = dataPackageInfoCache.FindFiles("SamplePrepTest_Plasma*", @"misc\final melissa tables");

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

			//datasetName = "SWT_LCQData_300";
			//subDir = "SIC201309041722_Auto976603";

			datasetName = "SysVirol_SM001_MA15_10-4pfu_7d_5_A_11May10_Phoenix_10-03-34";

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

			var dctDatasetsAndSubDirs = new Dictionary<string, string>
			{
				// {"SWT_LCQData_300", "SIC201309041722_Auto976603"},
				{"SysVirol_IFL001_10xA_07_11Sep13_Tiger_13-07-36", "SIC201309112159_Auto977994"},
				{"SysVirol_IFL001_10xA_08_11Sep13_Tiger_13-07-34", ""}
			};

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

			var dctDatasetsAndSubDirs = new Dictionary<int, string>
			{
				{54007, "SIC201309041722_Auto976603"},
				{334448, "SIC201309112159_Auto977994"},
				{334455, ""}
			};

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
			Console.WriteLine("Downloading " + lstFileIDs.Count + " files");
			Console.WriteLine();

			var downloader = new MyEMSLReader.Downloader();

			downloader.ErrorEvent += reader_ErrorEvent;
			downloader.MessageEvent += reader_MessageEvent;
			downloader.ProgressEvent += reader_ProgressEvent;

			downloader.OverwriteMode = MyEMSLReader.Downloader.Overwrite.IfChanged;

			try
			{
				string outputFolder;
				if (string.IsNullOrEmpty(mOutputFolderPath))
					outputFolder = @"F:\Temp\MyEMSL";
				else
					outputFolder = mOutputFolderPath;

				downloader.DownloadFiles(lstFileIDs, outputFolder, MyEMSLReader.Downloader.DownloadFolderLayout.DatasetNameAndSubFolders);
			}
			catch (Exception ex)
			{
				Console.WriteLine("Exception from downloader: " + ex.Message);
			}

		}

		static void TestDownloader(List<DatasetFolderOrFileInfo> archiveFiles)
		{
			Console.WriteLine("Downloading " + archiveFiles.Count + " files");
			Console.WriteLine();

			DownloadDatasetFiles(archiveFiles, mOutputFolderPath);
		}

		private static void WriteToErrorStream(string strErrorMessage)
		{
			try
			{
				using (var swErrorStream = new System.IO.StreamWriter(Console.OpenStandardError()))
				{
					swErrorStream.WriteLine(strErrorMessage);
				}
			}
			// ReSharper disable once EmptyGeneralCatchClause
			catch
			{
				// Ignore errors here
			}
		}

		#region "Event Handlers"

		static void mDatasetListInfo_ErrorEvent(object sender, MessageEventArgs e)
		{
			ShowErrorMessage(e.Message);
		}

		static void mDatasetListInfo_MessageEvent(object sender, MessageEventArgs e)
		{
			Console.WriteLine(e.Message);
		}

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