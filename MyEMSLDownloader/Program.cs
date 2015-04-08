using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FileProcessor;
using MyEMSLReader;

namespace MyEMSLDownloader
{
    internal struct udtFileInfo
    {
        public string FileMask;
        public string SubDir;
    }

    class Program
    {
        private const string PROGRAM_DATE = "April 8, 2015";

        static double mPercentComplete;
        static DateTime mLastProgressUpdateTime = DateTime.UtcNow;

        private static string mDatasetName;
        private static int mDataPkgID;
        private static string mSubfolder;
        private static string mFileMask;
        private static string mFileListPath;
        private static string mOutputFolderPath;

        private static bool mMultiDatasetMode;
        private static bool mDisableCart;

        private static bool mPreviewMode;
        private static bool mAutoTestMode;

        private static DatasetListInfo mDatasetListInfo;
        private static DataPackageListInfo mDataPackageListInfo;

        static int Main(string[] args)
        {
            var objParseCommandLine = new clsParseCommandLine();

            mDatasetName = string.Empty;
            mDataPkgID = 0;
            mSubfolder = string.Empty;
            mFileMask = string.Empty;
            mOutputFolderPath = string.Empty;

            mMultiDatasetMode = false;
            mDisableCart = false;

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
                    AutoTestModeStart();                   
                    return 0;
                }

                List<DatasetFolderOrFileInfo> archiveFiles;

                if (mDataPkgID > 0)
                    archiveFiles = FindDataPkgFiles(mDataPkgID, mSubfolder, mFileMask);
                else
                {
                    if (string.IsNullOrWhiteSpace(mFileListPath))
                    {
                        archiveFiles = FindDatasetFiles(mDatasetName, mSubfolder, mFileMask);
                    }
                    else
                    {
                        var fiFileListFile = new FileInfo(mFileListPath);
                        if (!fiFileListFile.Exists)
                        {
                            Console.WriteLine("File not found: " + fiFileListFile.FullName);
                            return -1;
                        }

                        archiveFiles = FindFileListFiles(fiFileListFile);
                    }
                }

                Console.WriteLine();

                if (mPreviewMode)
                    Console.WriteLine("Previewing files that would be downloaded; count = " + archiveFiles.Count);
                else
                    Console.WriteLine("Downloading files from MyEMSL; count = " + archiveFiles.Count);

                Console.WriteLine();

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
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred in Program->Main: " + Environment.NewLine + ex.Message);
                Console.WriteLine(ex.StackTrace);
                return -1;
            }

            return 0;
        }

        private static void AutoTestModeStart()
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

        private static void DownloadDatasetFiles(IEnumerable<DatasetFolderOrFileInfo> archiveFiles, string outputFolderPath)
        {
            DownloadFiles(mDatasetListInfo, archiveFiles, outputFolderPath);
        }

        private static void DownloadDataPackageFiles(IEnumerable<DatasetFolderOrFileInfo> archiveFiles, string outputFolderPath)
        {
            DownloadFiles(mDataPackageListInfo, archiveFiles, outputFolderPath);
        }

        private static void DownloadFiles(
            DatasetInfoBase myEMSLInfoCache,
            IEnumerable<DatasetFolderOrFileInfo> archiveFiles,
            string outputFolderPath)
        {
            myEMSLInfoCache.ClearDownloadQueue();
            myEMSLInfoCache.DisableCart = mDisableCart;

            foreach (var archiveFile in archiveFiles)
            {
                myEMSLInfoCache.AddFileToDownloadQueue(archiveFile.FileInfo);
            }

            Downloader.DownloadFolderLayout folderLayout;
            if (mMultiDatasetMode)
                folderLayout = Downloader.DownloadFolderLayout.DatasetNameAndSubFolders;
            else
                folderLayout = Downloader.DownloadFolderLayout.SingleDataset;

            bool success = myEMSLInfoCache.ProcessDownloadQueue(outputFolderPath, folderLayout);

            if (success)
            {
                Console.WriteLine("Download complete");
            }
            else
            {
                ShowErrorMessage("Download failed");
            }

        }

        private static List<DatasetFolderOrFileInfo> FindDatasetFiles(
            string datasetName,
            string subfolder,
            string fileMask)
        {

            mDatasetListInfo.AddDataset(datasetName, subfolder);

            if (string.IsNullOrEmpty(fileMask))
                fileMask = "*";

            var archiveFiles = mDatasetListInfo.FindFiles(fileMask, subfolder, datasetName, true);

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

        private static List<DatasetFolderOrFileInfo> FindFileListFiles(FileInfo fiFileListFile)
        {
            const string DATASET_COLUMN = "Dataset";
            const string SUBDIR_COLUMN = "SubDir";
            const string FILE_COLUMN = "File";

            try
            {
                var datasetsToSearch = new Dictionary<string, List<udtFileInfo>>();

                using (var fileReader = new StreamReader(new FileStream(fiFileListFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    var headerMap = new Dictionary<string, int>();
                    var headerNames = new List<string>
                    {
                        DATASET_COLUMN, SUBDIR_COLUMN, FILE_COLUMN
                    };

                    while (!fileReader.EndOfStream)
                    {
                        var dataLine = fileReader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var dataValues = dataLine.Split('\t').ToList();

                        if (dataValues.Count == 0)
                            continue;

                        if (dataValues.Count < 2)
                        {
                            Console.WriteLine("Skipping line since less than 2 columns: " + dataLine);
                            continue;
                        }

                        if (headerMap.Count == 0)
                        {
                            MapHeaders(dataValues, headerNames, headerMap);

                            if (!headerMap.ContainsKey(DATASET_COLUMN) ||
                                !headerMap.ContainsKey(FILE_COLUMN))
                            {
                                Console.WriteLine("Missing columns in " + fiFileListFile.Name);
                                Console.WriteLine("Header line must contain columns " + DATASET_COLUMN + " and " + FILE_COLUMN + " and optionally " + SUBDIR_COLUMN);
                                return new List<DatasetFolderOrFileInfo>();
                            }
                            continue;
                        }

                        var dataset = dataValues[headerMap[DATASET_COLUMN]];
                        var fileToFind = new udtFileInfo
                        {
                            FileMask = dataValues[headerMap[FILE_COLUMN]]
                        };

                        if (headerMap.ContainsKey(SUBDIR_COLUMN))
                            fileToFind.SubDir = dataValues[headerMap[SUBDIR_COLUMN]];
                        else
                            fileToFind.SubDir = string.Empty;

                        List<udtFileInfo> datasetFiles;

                        if (!datasetsToSearch.TryGetValue(dataset, out datasetFiles))
                        {
                            datasetFiles = new List<udtFileInfo>();
                            datasetsToSearch.Add(dataset, datasetFiles);
                        }

                        datasetFiles.Add(fileToFind);

                        // Add the dataset name so that all of its tracked files will be determined 
                        // when MyEMSL is first queried via RefreshInfoIfStale (which calls RefreshInfo)
                        mDatasetListInfo.AddDataset(dataset, fileToFind.SubDir);

                    }
                }

                var archiveFiles = new List<DatasetFolderOrFileInfo>();

                foreach (var dataset in datasetsToSearch)
                {
                    foreach (var fileToFind in dataset.Value)
                    {
                        var archiveFilesToAdd = FindDatasetFiles(dataset.Key, fileToFind.SubDir, fileToFind.FileMask).ToList();

                        foreach (var archiveFile in archiveFilesToAdd)
                        {
                            if (String.Equals(archiveFile.FileInfo.Dataset, dataset.Key, StringComparison.OrdinalIgnoreCase))
                            {
                                var alreadyAdded = (from item in archiveFiles where item.FileID == archiveFile.FileID select item).ToList().Any();

                                if (!alreadyAdded)
                                    archiveFiles.Add(archiveFile);
                            }
                            else
                            {
                                Console.WriteLine("Unexpected dataset name: " + dataset.Key);
                            }
                        }

                    }

                }

                return archiveFiles;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in FindFileListFiles: " + ex.Message);
                return new List<DatasetFolderOrFileInfo>();
            }
        }

        private static void MapHeaders(IList<string> dataValues, List<string> headerNames, IDictionary<string, int> headerMap)
        {
            for (var colIndex = 0; colIndex < dataValues.Count; colIndex++)
            {
                foreach (var headerName in headerNames)
                {
                    if (String.Equals(dataValues[colIndex], headerName, StringComparison.OrdinalIgnoreCase))
                    {
                        headerMap.Add(headerName, colIndex);
                        break;
                    }
                }
            }
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
            return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine objParseCommandLine)
        {
            // Returns True if no problems; otherwise, returns false
            var lstValidParameters = new List<string> { "Dataset", "DataPkg", "SubDir", "Files", "O", "D", "FileList", "DisableCart", "Preview", "Test" };

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

                if (!ParseParameter(objParseCommandLine, "FileList", "a filename", ref mFileListPath)) return false;

                if (!string.IsNullOrWhiteSpace(mFileListPath) || objParseCommandLine.IsParameterPresent("D"))
                {
                    mMultiDatasetMode = true;
                }

                if (objParseCommandLine.IsParameterPresent("DisableCart"))
                {
                    mDisableCart = true;
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

        private static bool ParseParameter(
            clsParseCommandLine objParseCommandLine,
            string parameterName,
            string description,
            ref string targetVariable)
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
            string exeName = Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location);

            try
            {
                Console.WriteLine();
                Console.WriteLine("This program downloads files from MyEMSL");
                Console.WriteLine();

                Console.Write("Program syntax #1:" + Environment.NewLine + exeName);
                Console.WriteLine(" DatasetName [SubFolderName] [/Files:FileMask] [/O:OutputFolder] ");
                Console.WriteLine(" [/D] [/Preview] [/DisableCart]");

                Console.WriteLine();
                Console.Write("Program syntax #2:" + Environment.NewLine + exeName);
                Console.WriteLine(" /Dataset:DatasetName [/SubDir:SubFolderName] [/Files:FileMask] [/O:OutputFolder]");
                Console.WriteLine(" [/D] [/Preview] [/DisableCart]");

                Console.WriteLine();
                Console.Write("Program syntax #3:" + Environment.NewLine + exeName);
                Console.WriteLine(" /DataPkg:DataPackageID [/SubDir:SubFolderName] [/Files:FileMask] [/O:OutputFolder]");
                Console.WriteLine(" [/Preview] [/DisableCart]");

                Console.WriteLine();
                Console.Write("Program syntax #4:" + Environment.NewLine + exeName);
                Console.WriteLine(" /FileList:FileInfoFile.txt [/O:OutputFolder]");
                Console.WriteLine(" [/Preview] [/DisableCart]");

                Console.WriteLine();
                Console.Write("Program syntax #5:" + Environment.NewLine + exeName);
                Console.WriteLine(" /Test [/Preview]  [/DisableCart]");

                Console.WriteLine();
                Console.WriteLine("To download files for a given dataset, enter the dataset name, plus optionally the SubFolder name");
                Console.WriteLine("The names can be entered separated by spaces, or using /Dataset plus optionally /SubDir");
                Console.WriteLine();
                Console.WriteLine("Use /Files to filter for specific files, for example /Files:*.txt");
                Console.WriteLine("Files will be downloaded to the folder with the .exe; override using /O");
                Console.WriteLine("Use /D to create a folder with the dataset name, then store the files within that folder");
                Console.WriteLine();
                Console.WriteLine("Use /DataPkg to retrieve files from a specific data package");
                Console.WriteLine();
                Console.WriteLine("Use /FileList to specify a file with a list of datasets and files to retrieve");
                Console.WriteLine("The file must be a tab-delimited text file, with columns Dataset and File, and optionally with column SubDir");
                Console.WriteLine("The file names in the File column are allowed to contain wildcards");
                Console.WriteLine("When /FileList is used, /D is automatically enabled");
                Console.WriteLine();
                Console.WriteLine("Alternatively, use /Test to perform automatic tests using predefined dataset names");
                Console.WriteLine();
                Console.WriteLine("Use /Preview to view files that would be downloaded, but not actually download them");
                Console.WriteLine("Use /DisableCart to disable use of the download cart mechanism for retrieving files that exist on tape but not on spinning disk");
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

            var reader = new Reader
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

            var lstFileIDs2 = TestOneDatasetByID(reader);
            Console.WriteLine();
            Console.WriteLine();

            var lstFileIDs3 = TestMultiDatasetID(reader);
            Console.WriteLine();
            Console.WriteLine();

            var lstFileIDs4 = TestOneDataset(reader);
            Console.WriteLine();
            Console.WriteLine();

            var lstFileIDs5 = TestOneDataPackage(reader);
            Console.WriteLine();
            Console.WriteLine();

            return lstFileIDs1;

        }

        static List<long> TestOneDataPackage(Reader reader)
        {
            var lstFileIDs = new List<long>();
            const int dataPkgID = 814;
            const string subDir = "";

            try
            {
                var results = reader.FindFilesByDataPackageID(dataPkgID, subDir);

                foreach (var archivedFile in results)
                {
                    Console.WriteLine(archivedFile.RelativePathUnix);
                    lstFileIDs.Add(archivedFile.FileID);
                }

                var dataPackageInfoCache = new DataPackageListInfo();
                dataPackageInfoCache.AddDataPackage(814);

                var archiveFiles = dataPackageInfoCache.FindFiles("SamplePrepTest_Plasma*", @"misc\final melissa tables");

            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception from reader: " + ex.Message);
            }

            return lstFileIDs;
        }

        static List<long> TestOneDataset(Reader reader)
        {
            var lstFileIDs = new List<long>();
            const string subDir = "";

            //datasetName = "Blank_B-2_20Apr12_Draco_12-02-37";
            // datasetName = "QC_Shew_11_06_pt5_d2_11Jun12_Draco_12-04-14";
            // Dataset where all of the files were purged from spinning disk (but have now been unpurged)
            //datasetName = "2013_05_28_U01-B_Wilkins_neg_4M_0p1acc_8x_144_000001";
            //subDir = "";

            //datasetName = "SWT_LCQData_300";
            //subDir = "SIC201309041722_Auto976603";

            const string datasetName = "SysVirol_SM001_MA15_10-4pfu_7d_5_A_11May10_Phoenix_10-03-34";

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

        static List<long> TestMultiDataset(Reader reader)
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

        private static List<long> TestDatasetByID(Reader reader, Dictionary<int, string> dctDatasetsAndSubDirs)
        {
            var lstFileIDs = new List<long>();

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

        private static List<long> TestOneDatasetByID(Reader reader)
        {
            var dctDatasetsAndSubDirs = new Dictionary<int, string>
			{
				{382287, ""}
			};

            var lstFileIDs = TestDatasetByID(reader, dctDatasetsAndSubDirs);

            return lstFileIDs;
        }

        static List<long> TestMultiDatasetID(Reader reader)
        {
            var dctDatasetsAndSubDirs = new Dictionary<int, string>
			{
				{54007, "SIC201309041722_Auto976603"},
				{334448, "SIC201309112159_Auto977994"},
				{334455, ""}
			};

            var lstFileIDs = TestDatasetByID(reader, dctDatasetsAndSubDirs);

            return lstFileIDs;
        }

        static void TestDownloader(List<long> lstFileIDs)
        {
            Console.WriteLine("Downloading " + lstFileIDs.Count + " files");
            Console.WriteLine();

            var downloader = new Downloader();

            downloader.ErrorEvent += reader_ErrorEvent;
            downloader.MessageEvent += reader_MessageEvent;
            downloader.ProgressEvent += reader_ProgressEvent;

            downloader.OverwriteMode = Downloader.Overwrite.IfChanged;

            try
            {
                string outputFolder;
                if (string.IsNullOrEmpty(mOutputFolderPath))
                    outputFolder = @"F:\Temp\MyEMSL";
                else
                    outputFolder = mOutputFolderPath;

                downloader.DownloadFiles(lstFileIDs, outputFolder, Downloader.DownloadFolderLayout.DatasetNameAndSubFolders);
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
                using (var swErrorStream = new StreamWriter(Console.OpenStandardError()))
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

        static void reader_ErrorEvent(object sender, MessageEventArgs e)
        {
            Console.WriteLine("Error: " + e.Message);
        }

        static void reader_MessageEvent(object sender, MessageEventArgs e)
        {
            Console.WriteLine(e.Message);
        }

        static void reader_ProgressEvent(object sender, ProgressEventArgs e)
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