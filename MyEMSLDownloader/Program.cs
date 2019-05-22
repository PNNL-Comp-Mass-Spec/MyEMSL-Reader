using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MyEMSLReader;
using PRISM;

namespace MyEMSLDownloader
{
    internal struct TargetFileInfo
    {
        public string FileMask;
        public string SubDir;
    }

    internal static class Program
    {
        private const string PROGRAM_DATE = "May 17, 2019";

        static double mPercentComplete;
        static DateTime mLastProgressUpdateTime = DateTime.UtcNow;

        private static string mDatasetName;
        private static int mDataPkgID;
        private static string mSubdirectory;

        /// <summary>
        /// File spec for finding files
        /// </summary>
        /// <remarks>
        /// Single file name or a file spec like *.txt
        /// Specify a list of names and/or specs by separating with a semicolon and using switch /FileSplit
        /// For example: analysis.baf|ser
        /// </remarks>
        private static string mFileMask;

        /// <summary>
        /// Set to true to indicate that mFileMask contains a semicolon-separated list of file names and/or file specs
        /// </summary>
        private static bool mFileSplit;

        private static string mFileIDList;
        private static string mFileListPath;
        private static string mOutputDirectoryPath;

        private static bool mMultiDatasetMode;

        private static bool mPreviewMode;
        private static bool mVerbosePreview;

        private static bool mAutoTestMode;
        private static bool mTraceMode;
        private static bool mUseTestInstance;

        private static DatasetListInfo mDatasetListInfo;
        private static DataPackageListInfo mDataPackageListInfo;

        static int Main(string[] args)
        {
            var commandLineParser = new clsParseCommandLine();

            mDatasetName = string.Empty;
            mDataPkgID = 0;
            mSubdirectory = string.Empty;
            mFileMask = string.Empty;
            mFileSplit = false;
            mOutputDirectoryPath = string.Empty;

            mMultiDatasetMode = false;

            mPreviewMode = false;
            mVerbosePreview = false;
            mAutoTestMode = false;
            mTraceMode = false;
            mUseTestInstance = false;

            try
            {
                var success = false;

                if (commandLineParser.ParseCommandLine())
                {
                    if (SetOptionsUsingCommandLineParameters(commandLineParser))
                        success = true;
                }

                if (!success ||
                    commandLineParser.NeedToShowHelp ||
                    commandLineParser.ParameterCount + commandLineParser.NonSwitchParameterCount == 0)
                {
                    ShowProgramHelp();
                    return -1;

                }

                mDatasetListInfo = new DatasetListInfo
                {
                    ReportMetadataURLs = mPreviewMode || mTraceMode,
                    ThrowErrors = false,
                    TraceMode = mTraceMode
                };
                RegisterEvents(mDatasetListInfo);

                mDataPackageListInfo = new DataPackageListInfo
                {
                    ReportMetadataURLs = mPreviewMode || mTraceMode,
                    ThrowErrors = false,
                    TraceMode = mTraceMode
                };
                RegisterEvents(mDataPackageListInfo);

                mDatasetListInfo.UseTestInstance = mUseTestInstance;
                mDataPackageListInfo.UseTestInstance = mUseTestInstance;

                if (mAutoTestMode)
                {
                    AutoTestModeStart();
                    System.Threading.Thread.Sleep(1000);
                    return 0;
                }

                List<DatasetDirectoryOrFileInfo> archiveFiles;

                if (mDataPkgID > 0)
                    archiveFiles = FindDataPkgFiles(mDataPkgID, mSubdirectory, mFileMask, mFileSplit);
                else
                {
                    if (!string.IsNullOrWhiteSpace(mFileListPath))
                    {
                        var fiFileListFile = new FileInfo(mFileListPath);
                        if (!fiFileListFile.Exists)
                        {
                            ShowErrorMessage("File not found: " + fiFileListFile.FullName);
                            System.Threading.Thread.Sleep(1000);
                            return -1;
                        }

                        archiveFiles = FindFileListFiles(fiFileListFile);
                    }
                    else
                    {
                        if (!string.IsNullOrWhiteSpace(mFileIDList))
                        {
                            archiveFiles = ParseExplicitFileIDs(mFileIDList);
                            if (archiveFiles.Count == 0)
                            {
                                ShowErrorMessage("No File IDs were found with the /FileID parameter");
                                System.Threading.Thread.Sleep(1000);
                                return -1;
                            }
                        }
                        else
                        {
                            if (string.IsNullOrWhiteSpace(mDatasetName))
                            {
                                ShowErrorMessage("Dataset or data package name not specified. Use /Dataset or /DataPkg or /FileList or /FileID");
                                System.Threading.Thread.Sleep(1000);
                                return -1;
                            }
                            archiveFiles = FindDatasetFiles(mDatasetName, mSubdirectory, mFileMask, mFileSplit);
                        }
                    }
                }

                Console.WriteLine();

                if (mPreviewMode)
                    Console.WriteLine("Previewing files that would be downloaded; count = " + archiveFiles.Count);
                else
                    Console.WriteLine("Downloading files from MyEMSL; count = " + archiveFiles.Count);

                Console.WriteLine();

                ShowFiles(archiveFiles, mVerbosePreview);

                if (mPreviewMode)
                {
                    System.Threading.Thread.Sleep(1500);
                    return 0;
                }

                Console.WriteLine();
                if (mDataPkgID > 0)
                    DownloadDataPackageFiles(archiveFiles, mOutputDirectoryPath);
                else
                    DownloadDatasetFiles(archiveFiles, mOutputDirectoryPath);
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error occurred in Program->Main", ex);
                System.Threading.Thread.Sleep(1000);
                return -1;
            }

            System.Threading.Thread.Sleep(1000);
            return 0;
        }

        private static void MyEMSLReader_MyEMSLOffline(string message)
        {
            OnWarningEvent(message);
        }

        private static void AutoTestModeStart()
        {
            var exampleDownloader = new DownloadExample();
            exampleDownloader.StartTest();

            var filesToDownload = TestReader();

            if (filesToDownload.Count == 0)
            {
                ConsoleMsgUtils.ShowWarning("Reader did not find any files");
            }
            else if (!mPreviewMode)
            {
                TestDownloader(filesToDownload);
            }
            Console.WriteLine();

            var archiveFiles = TestDatasetListInfo();

            if (archiveFiles.Count == 0)
                ConsoleMsgUtils.ShowWarning("DatasetListInfo did not find any files");
            else
            {
                ShowFiles(archiveFiles, mVerbosePreview);

                if (!mPreviewMode)
                    TestDownloader(archiveFiles);
            }
        }

        private static void DownloadDatasetFiles(IEnumerable<DatasetDirectoryOrFileInfo> archiveFiles, string outputDirectoryPath)
        {
            DownloadFiles(mDatasetListInfo, archiveFiles, outputDirectoryPath);
        }

        private static void DownloadDataPackageFiles(IEnumerable<DatasetDirectoryOrFileInfo> archiveFiles, string outputDirectoryPath)
        {
            DownloadFiles(mDataPackageListInfo, archiveFiles, outputDirectoryPath);
        }

        private static void DownloadFiles(
            DatasetInfoBase myEMSLInfoCache,
            IEnumerable<DatasetDirectoryOrFileInfo> archiveFiles,
            string outputDirectoryPath)
        {
            myEMSLInfoCache.ClearDownloadQueue();

            foreach (var archiveFile in archiveFiles)
            {
                myEMSLInfoCache.AddFileToDownloadQueue(archiveFile.FileInfo);
            }

            Downloader.DownloadLayout directoryLayout;
            if (mMultiDatasetMode)
                directoryLayout = Downloader.DownloadLayout.DatasetNameAndSubdirectories;
            else
                directoryLayout = Downloader.DownloadLayout.SingleDataset;

            var success = myEMSLInfoCache.ProcessDownloadQueue(outputDirectoryPath, directoryLayout);

            if (success)
            {
                Console.WriteLine("Download complete");
            }
            else
            {
                ShowErrorMessage("Download failed");
            }

        }

        /// <summary>
        /// Find files for the given dataset
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="subdirectory">Subdirectory to filter on (optional)</param>
        /// <param name="fileMask">File name or file spec like *.txt to filter on (optional)</param>
        /// <param name="fileSplit"></param>
        /// <returns></returns>
        /// <remarks>
        /// For fileMask, specify a list of names and/or specs by separating with a vertical bar
        /// For example: analysis.baf|ser
        /// </remarks>
        private static List<DatasetDirectoryOrFileInfo> FindDatasetFiles(
            string datasetName,
            string subdirectory,
            string fileMask,
            bool fileSplit)
        {

            mDatasetListInfo.AddDataset(datasetName, subdirectory);

            if (string.IsNullOrEmpty(fileMask))
                fileMask = "*";

            var archiveFiles = mDatasetListInfo.FindFiles(fileMask, subdirectory, datasetName, true, fileSplit);

            return archiveFiles;
        }

        private static List<DatasetDirectoryOrFileInfo> FindDataPkgFiles(
            int dataPkgID,
            string subdirectory,
            string fileMask,
            bool fileSplit)
        {
            mDataPackageListInfo.AddDataPackage(dataPkgID);

            if (string.IsNullOrEmpty(fileMask))
                fileMask = "*";

            var archiveFiles = mDataPackageListInfo.FindFiles(fileMask, subdirectory, true, fileSplit);

            return archiveFiles;
        }

        private static List<DatasetDirectoryOrFileInfo> FindFileListFiles(FileSystemInfo fiFileListFile)
        {
            const string DATASET_COLUMN = "Dataset";
            const string SUBDIR_COLUMN = "SubDir";
            const string FILE_COLUMN = "File";

            try
            {
                var datasetsToSearch = new Dictionary<string, List<TargetFileInfo>>();

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
                            ConsoleMsgUtils.ShowWarning("Skipping line since less than 2 columns: " + dataLine);
                            continue;
                        }

                        var lastIndex = dataValues.Count - 1;

                        if (headerMap.Count == 0)
                        {
                            MapHeaders(dataValues, headerNames, headerMap);

                            if (!headerMap.ContainsKey(DATASET_COLUMN) ||
                                !headerMap.ContainsKey(FILE_COLUMN))
                            {
                                ConsoleMsgUtils.ShowWarning("Missing columns in " + fiFileListFile.Name);
                                ConsoleMsgUtils.ShowWarning("Header line must contain columns " + DATASET_COLUMN + " and " + FILE_COLUMN + " and optionally " + SUBDIR_COLUMN);
                                return new List<DatasetDirectoryOrFileInfo>();
                            }
                            continue;
                        }

                        if (headerMap[DATASET_COLUMN] > lastIndex || headerMap[FILE_COLUMN] > lastIndex)
                        {
                            ConsoleMsgUtils.ShowWarning("Data line has fewer columns than the header line; skipping: " + dataLine);
                            continue;
                        }

                        var dataset = dataValues[headerMap[DATASET_COLUMN]].Trim();
                        var fileToFind = new TargetFileInfo
                        {
                            FileMask = dataValues[headerMap[FILE_COLUMN]].Trim()
                        };

                        if (headerMap.ContainsKey(SUBDIR_COLUMN))
                        {
                            if (headerMap[SUBDIR_COLUMN] > lastIndex)
                            {
                                ConsoleMsgUtils.ShowWarning("Data line has fewer columns than the header line; skipping: " + dataLine);
                                continue;
                            }
                            fileToFind.SubDir = dataValues[headerMap[SUBDIR_COLUMN]].Trim();
                            if (fileToFind.SubDir.Equals("."))
                            {
                                // Treat a source directory of "." as ""
                                fileToFind.SubDir = string.Empty;
                            }
                        }
                        else
                            fileToFind.SubDir = string.Empty;

                        if (!datasetsToSearch.TryGetValue(dataset, out var datasetFiles))
                        {
                            datasetFiles = new List<TargetFileInfo>();
                            datasetsToSearch.Add(dataset, datasetFiles);
                        }

                        if (string.IsNullOrWhiteSpace(fileToFind.FileMask))
                        {
                            ConsoleMsgUtils.ShowWarning("Ignoring line with empty filename: " + dataLine.Replace("\t", "<tab>"));
                            continue;
                        }

                        datasetFiles.Add(fileToFind);

                        // Add the dataset name so that all of its tracked files will be determined
                        // when MyEMSL is first queried via RefreshInfoIfStale (which calls RefreshInfo)
                        mDatasetListInfo.AddDataset(dataset, fileToFind.SubDir);

                    }
                }

                var archiveFiles = new List<DatasetDirectoryOrFileInfo>();

                foreach (var dataset in datasetsToSearch)
                {
                    foreach (var fileToFind in dataset.Value)
                    {
                        var archiveFilesToAdd = FindDatasetFiles(dataset.Key, fileToFind.SubDir, fileToFind.FileMask, fileSplit: false).ToList();

                        foreach (var archiveFile in archiveFilesToAdd)
                        {
                            if (string.Equals(archiveFile.FileInfo.Dataset, dataset.Key, StringComparison.OrdinalIgnoreCase))
                            {
                                var alreadyAdded = (from item in archiveFiles where item.FileID == archiveFile.FileID select item).ToList().Any();

                                if (!alreadyAdded)
                                    archiveFiles.Add(archiveFile);
                            }
                            else
                            {
                                ConsoleMsgUtils.ShowWarning("Unexpected dataset name: " + dataset.Key);
                            }
                        }

                    }

                }

                return archiveFiles;
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Exception in FindFileListFiles", ex);
                return new List<DatasetDirectoryOrFileInfo>();
            }
        }

        private static void MapHeaders(
            IList<string> dataValues,
            ICollection<string> headerNames,
            IDictionary<string, int> headerMap)
        {
            for (var colIndex = 0; colIndex < dataValues.Count; colIndex++)
            {
                foreach (var headerName in headerNames)
                {
                    if (string.Equals(dataValues[colIndex], headerName, StringComparison.OrdinalIgnoreCase))
                    {
                        headerMap.Add(headerName, colIndex);
                        break;
                    }
                }
            }
        }

        private static List<DatasetDirectoryOrFileInfo> ParseExplicitFileIDs(string fileIdList)
        {
            var archiveFiles = new List<DatasetDirectoryOrFileInfo>();

            var fileIDs = fileIdList.Split(',');
            foreach (var fileID in fileIDs)
            {
                if (!long.TryParse(fileID, out var fileIdValue))
                {
                    ConsoleMsgUtils.ShowWarning("Warning: " + fileID + " is not an integer");
                    continue;
                }

                var fileInfo = new ArchivedFileInfo("Unknown_Dataset", "Unknown_Filename", "")
                {
                    FileID = fileIdValue
                };

                archiveFiles.Add(new DatasetDirectoryOrFileInfo(fileIdValue, false, fileInfo));
            }

            return archiveFiles;
        }

        private static void ShowFiles(IEnumerable<DatasetDirectoryOrFileInfo> archiveFiles, bool verbosePreview)
        {
            foreach (var archiveFile in archiveFiles)
            {
                Console.WriteLine(archiveFile.FileInfo.RelativePathWindows);
                if (verbosePreview)
                {
                    ConsoleMsgUtils.ShowDebug(
                        string.Format(
                            "  FileID {0}, TransID {1}, Submitted {2}, Size {3:F1} KB, Hash {4}, HashType {5}",
                            archiveFile.FileID,
                            archiveFile.FileInfo.TransactionID,
                            archiveFile.FileInfo.SubmissionTime,
                            archiveFile.FileInfo.FileSizeBytes / 1024.0,
                            archiveFile.FileInfo.Sha1Hash,
                            archiveFile.FileInfo.HashType));
                    Console.WriteLine();
                }

            }
        }

        private static string GetAppVersion()
        {
            return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        /// <summary>
        /// Set options using command line parameters
        /// </summary>
        /// <param name="commandLineParser"></param>
        /// <returns>True if no problems; otherwise, false</returns>
        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine commandLineParser)
        {
            var lstValidParameters = new List<string> {
                "Dataset", "DatasetID", "DataPkg", "SubDir", "Files", "FileSplit",
                "O", "D", "FileList", "FileID",
                "Preview", "V", "Verbose", "Test", "Trace", "UseTest" };

            try
            {
                // Make sure no invalid parameters are present
                if (commandLineParser.InvalidParametersPresent(lstValidParameters))
                {
                    var badArguments = new List<string>();
                    foreach (var item in commandLineParser.InvalidParameters(lstValidParameters))
                    {
                        badArguments.Add("/" + item);
                    }

                    ShowErrorMessage("Invalid command line parameters", badArguments);

                    return false;
                }

                // Query commandLineParser to see if various parameters are present

                if (commandLineParser.NonSwitchParameterCount > 0)
                    mDatasetName = commandLineParser.RetrieveNonSwitchParameter(0);

                if (commandLineParser.NonSwitchParameterCount > 1)
                    mSubdirectory = commandLineParser.RetrieveNonSwitchParameter(1);

                if (!ParseParameter(commandLineParser, "Dataset", "a dataset name", ref mDatasetName))
                    return false;

                var dataPkgString = "";
                if (!ParseParameter(commandLineParser, "DataPkg", "a data package ID", ref dataPkgString))
                    return false;
                if (!string.IsNullOrEmpty(dataPkgString))
                {
                    if (!int.TryParse(dataPkgString, out mDataPkgID))
                    {
                        ShowErrorMessage("Data package ID must be an integer: " + dataPkgString);
                        return false;
                    }
                }

                if (!ParseParameter(commandLineParser, "SubDir", "a subdirectory name", ref mSubdirectory))
                    return false;
                if (!ParseParameter(commandLineParser, "Files", "a file mas", ref mFileMask))
                    return false;

                if (commandLineParser.IsParameterPresent("FileSplit"))
                    mFileSplit = true;

                if (!ParseParameter(commandLineParser, "O", "an output directory path", ref mOutputDirectoryPath))
                    return false;

                if (!ParseParameter(commandLineParser, "FileList", "a filename", ref mFileListPath))
                    return false;

                if (!ParseParameter(commandLineParser, "FileID", "a file ID (or comma-separated list of file IDs)", ref mFileIDList))
                    return false;

                if (!string.IsNullOrWhiteSpace(mFileListPath))
                {
                    mMultiDatasetMode = true;
                }

                if (commandLineParser.IsParameterPresent("D"))
                {
                    if (commandLineParser.RetrieveValueForParameter("D", out var paramValue) && !string.IsNullOrWhiteSpace(paramValue))
                    {
                        ShowErrorMessage("The /D switch should not have a value; use /Dataset to specify a dataset name");
                        return false;
                    }
                    mMultiDatasetMode = true;
                }

                mPreviewMode = commandLineParser.IsParameterPresent("Preview");
                mAutoTestMode = commandLineParser.IsParameterPresent("Test");
                mTraceMode = commandLineParser.IsParameterPresent("Trace");
                mUseTestInstance = commandLineParser.IsParameterPresent("UseTest");
                mVerbosePreview = commandLineParser.IsParameterPresent("V") || commandLineParser.IsParameterPresent("Verbose");

                return true;
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error parsing the command line parameters: " + Environment.NewLine + ex.Message);
            }

            return false;
        }

        private static bool ParseParameter(
            clsParseCommandLine commandLineParser,
            string parameterName,
            string description,
            ref string targetVariable)
        {
            if (commandLineParser.RetrieveValueForParameter(parameterName, out var value))
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

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static void ShowErrorMessage(string message, ICollection<string> additionalInfo)
        {
            ConsoleMsgUtils.ShowErrors(message, additionalInfo);
        }

        private static void ShowProgramHelp()
        {
            var exeName = Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location);

            try
            {
                Console.WriteLine();
                Console.WriteLine("This program downloads files from MyEMSL");
                Console.WriteLine();

                Console.Write("Program syntax #1:" + Environment.NewLine + exeName);
                Console.WriteLine(" DatasetName [SubdirectoryName] [/Files:FileMask] [/FileSplit]");
                Console.WriteLine(" [/O:OutputDirectory] [/D] [/Preview] [/V] [/Trace] [/DisableCart] [/ForceCart] [/UseTest]");

                Console.WriteLine();
                Console.Write("Program syntax #2:" + Environment.NewLine + exeName);
                Console.WriteLine(" /Dataset:DatasetName [/SubDir:SubdirectoryName] [/Files:FileMask] [/FileSplit]");
                Console.WriteLine(" [/O:OutputDirectory] [/D] [/Preview] [/V] [/Trace] [/DisableCart] [/ForceCart] [/UseTest]");

                Console.WriteLine();
                Console.Write("Program syntax #3:" + Environment.NewLine + exeName);
                Console.WriteLine(" /DataPkg:DataPackageID [/SubDir:SubdirectoryName] [/Files:FileMask] [/FileSplit]");
                Console.WriteLine(" [/O:OutputDirectory] [/Preview] [/V] [/Trace] [/DisableCart] [/ForceCart] [/UseTest]");

                Console.WriteLine();
                Console.Write("Program syntax #4:" + Environment.NewLine + exeName);
                Console.WriteLine(" /FileList:FileInfoFile.txt [/O:OutputDirectory]");
                Console.WriteLine(" [/Preview] [/V] [/Trace] [/DisableCart] [/ForceCart] [/UseTest]");

                Console.WriteLine();
                Console.Write("Program syntax #5:" + Environment.NewLine + exeName);
                Console.WriteLine(" /FileID:1234 [/Preview] [/V] [/Trace]");


                Console.WriteLine();
                Console.Write("Program syntax #6:" + Environment.NewLine + exeName);
                Console.WriteLine(" /Test [/Preview] [/V] [/Trace] [/DisableCart] [/ForceCart]");

                Console.WriteLine();
                Console.WriteLine("To download files for a given dataset, enter the dataset name, plus optionally the subdirectory name");
                Console.WriteLine("The names can be entered separated by spaces, or using /Dataset plus optionally /SubDir");
                Console.WriteLine();
                Console.WriteLine("Use /Files to filter for specific files, for example /Files:*.txt");
                Console.WriteLine("Files will be downloaded to the directory with the .exe; override using /O");
                Console.WriteLine("Use /FileSplit to indicate that /Files contains a list of filenames and/or file specs, separated by semicolons");
                Console.WriteLine("For example, use /Files:analysis.baf;ser /FileSplit");
                Console.WriteLine();
                Console.WriteLine("Use /D to create a directory with the dataset name, then store the files within that directory");
                Console.WriteLine();
                Console.WriteLine("Use /DataPkg to retrieve files from a specific data package");
                Console.WriteLine();
                Console.WriteLine("Use /FileList to specify a file with a list of datasets and files to retrieve");
                Console.WriteLine("The file must be a tab-delimited text file, with columns Dataset and File, and optionally with column SubDir");
                Console.WriteLine("The file names in the File column are allowed to contain wildcards");
                Console.WriteLine("When /FileList is used, /D is automatically enabled");
                Console.WriteLine();
                Console.WriteLine("Use /FileId to specify the MyEMSL ID of a file to download (as seen with /V)");
                Console.WriteLine("This mode does not use Simple Search to find files and can thus be used " +
                                  "to retrieve a file that Simple Search does not find. Provide a comma separated list to retrieve multiple files.");
                Console.WriteLine();
                Console.WriteLine("Alternatively, use /Test to perform automatic tests using predefined dataset names");
                Console.WriteLine();
                Console.WriteLine("Use /Preview to view files that would be downloaded, but not actually download them");
                Console.WriteLine("Use /V to enable verbose preview, showing extended details about each file");
                Console.WriteLine();
                Console.WriteLine("Use /Trace to display additional debug information");
                Console.WriteLine();
                Console.WriteLine("Use /DisableCart to disable use of the download cart mechanism for retrieving files that exist on tape but not on spinning disk");
                Console.WriteLine("Use /ForceCart to force the use of the download cart; this option overrides /DisableCart");
                Console.WriteLine();
                Console.WriteLine("Use /UseTest to connect to test0.my.emsl.pnl.gov instead of my.emsl.pnl.gov");
                Console.WriteLine();
                Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2013");
                Console.WriteLine("Version: " + GetAppVersion());
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov");
                Console.WriteLine("Website: https://omics.pnl.gov/ or https://panomics.pnnl.gov/");
                Console.WriteLine();

                // Delay for 1 second in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                System.Threading.Thread.Sleep(1000);

            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error displaying the program syntax", ex);
            }

        }

        static List<DatasetDirectoryOrFileInfo> TestDatasetListInfo()
        {
            Console.WriteLine("Looking for files for test datasets using the DatasetListInfo class");
            Console.WriteLine();

            // ReSharper disable once StringLiteralTypo
            mDatasetListInfo.AddDataset("2013_10_01_CMPD04_000005");

            var archiveFiles = mDatasetListInfo.FindFiles("*");

            return archiveFiles;

        }

        static Dictionary<long, ArchivedFileInfo> TestReader()
        {

            Console.WriteLine("Looking for files for test datasets using the Reader class");
            Console.WriteLine();

            var reader = new Reader
            {
                IncludeAllRevisions = false,
                UseTestInstance = mUseTestInstance,
                ReportMetadataURLs = mPreviewMode || mTraceMode,
                TraceMode = mTraceMode
            };

            // Attach events
            RegisterEvents(reader);

            var filesToDownload1 = TestMultiDataset(reader);
            Console.WriteLine("TestMultiDataset returned {0} records", filesToDownload1.Count);
            Console.WriteLine();

            var filesToDownload2a = TestOneDatasetByID(reader);
            Console.WriteLine("TestOneDatasetByID returned {0} records", filesToDownload2a.Count);
            Console.WriteLine();

            reader.IncludeAllRevisions = true;
            var filesToDownload2b = TestOneDatasetByID(reader);
            Console.WriteLine("TestOneDatasetByID returned {0} records", filesToDownload2b.Count);
            Console.WriteLine();
            reader.IncludeAllRevisions = false;

            var filesToDownload3 = TestMultiDatasetID(reader);
            Console.WriteLine("TestMultiDatasetID returned {0} records", filesToDownload3.Count);
            Console.WriteLine();

            var filesToDownload4 = TestOneDataset(reader);
            Console.WriteLine("TestOneDataset returned {0} records", filesToDownload4.Count);
            Console.WriteLine();

            var filesToDownload5 = TestOneDataPackage(reader);
            Console.WriteLine("TestOneDataPackage returned {0} records", filesToDownload5.Count);
            Console.WriteLine();

            return filesToDownload1;

        }

        static Dictionary<long, ArchivedFileInfo> TestOneDataPackage(Reader reader)
        {
            var filesToDownload = new Dictionary<long, ArchivedFileInfo>();
            const int dataPkgID = 814;
            const string subDir = "";

            try
            {
                var results = reader.FindFilesByDataPackageID(dataPkgID, subDir);

                Console.WriteLine("{0,-10} {1}", "Hash", "RelativePath");
                foreach (var archivedFile in results)
                {
                    Console.WriteLine("{0,-10} {1}", archivedFile.Hash.Substring(0, 10), archivedFile.RelativePathUnix);
                    filesToDownload.Add(archivedFile.FileID, archivedFile);
                }

                var dataPackageInfoCache = new DataPackageListInfo
                {
                    ReportMetadataURLs = mPreviewMode || mTraceMode,
                    ThrowErrors = false,
                    TraceMode = mTraceMode
                };
                RegisterEvents(dataPackageInfoCache);

                dataPackageInfoCache.AddDataPackage(814);

                // ReSharper disable once StringLiteralTypo
                var archiveFiles = dataPackageInfoCache.FindFiles("SamplePrepTest_Plasma*", @"misc\final melissa tables");
                Console.WriteLine("dataPackageInfoCache.FindFiles found {0} items", archiveFiles.Count);

            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowWarning("Exception from reader: " + ex.Message);
            }

            return filesToDownload;
        }

        static Dictionary<long, ArchivedFileInfo> TestOneDataset(Reader reader)
        {
            var filesToDownload = new Dictionary<long, ArchivedFileInfo>();
            const string subDir = "";

            //datasetName = "Blank_B-2_20Apr12_Draco_12-02-37";
            // datasetName = "QC_Shew_11_06_pt5_d2_11Jun12_Draco_12-04-14";
            // Dataset where all of the files were purged from spinning disk (but have now been un-purged)
            //datasetName = "2013_05_28_U01-B_Wilkins_neg_4M_0p1acc_8x_144_000001";
            //subDir = "";

            //datasetName = "SWT_LCQData_300";
            //subDir = "SIC201309041722_Auto976603";

            const string datasetName = "SysVirol_SM001_MA15_10-4pfu_7d_5_A_11May10_Phoenix_10-03-34";

            try
            {
                var results = reader.FindFilesByDatasetName(datasetName, subDir);

                Console.WriteLine();
                Console.WriteLine("Files for dataset " + datasetName);
                Console.WriteLine("{0,-10} {1}", "Hash", "RelativePath");
                foreach (var archivedFile in results)
                {
                    Console.WriteLine("{0,-10} {1}", archivedFile.Hash.Substring(0, 10), archivedFile.RelativePathUnix);
                    filesToDownload.Add(archivedFile.FileID, archivedFile);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception from reader: " + ex.Message);
            }

            return filesToDownload;
        }

        static Dictionary<long, ArchivedFileInfo> TestMultiDataset(Reader reader)
        {
            var filesToDownload = new Dictionary<long, ArchivedFileInfo>();

            var dctDatasetsAndSubDirs = new Dictionary<string, string>
            {
                // {"SWT_LCQData_300", "SIC201309041722_Auto976603"},
                // {"SysVirol_IFL001_10xA_07_11Sep13_Tiger_13-07-36", "SIC201309112159_Auto977994"},
                {"SysVirol_IFL001_10xA_07_11Sep13_Tiger_13-07-36", ""},
                {"SysVirol_IFL001_10xA_08_11Sep13_Tiger_13-07-34", ""}
            };

            try
            {
                var results = reader.FindFilesByDatasetName(dctDatasetsAndSubDirs);

                Console.WriteLine("{0,-9} {1,-10} {2}", "DatasetID", "Hash", "RelativePath");
                foreach (var archivedFile in results)
                {
                    Console.WriteLine("{0,-9} {1,-10} {2}", archivedFile.DatasetID, archivedFile.Hash.Substring(0, 10), archivedFile.RelativePathUnix);
                    filesToDownload.Add(archivedFile.FileID, archivedFile);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception from reader: " + ex.Message);
            }

            return filesToDownload;
        }

        private static Dictionary<long, ArchivedFileInfo> TestDatasetByID(Reader reader, Dictionary<int, string> dctDatasetsAndSubDirs)
        {
            var filesToDownload = new Dictionary<long, ArchivedFileInfo>();

            try
            {
                var results = reader.FindFilesByDatasetID(dctDatasetsAndSubDirs);

                Console.WriteLine("{0,-9} {1,-10} {2}", "DatasetID", "Hash", "RelativePath");
                foreach (var archivedFile in results)
                {
                    Console.WriteLine("{0,-9} {1,-10} {2}", archivedFile.DatasetID, archivedFile.Hash.Substring(0, 10), archivedFile.RelativePathUnix);
                    filesToDownload.Add(archivedFile.FileID, archivedFile);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception from reader: " + ex.Message);
            }

            return filesToDownload;
        }

        private static Dictionary<long, ArchivedFileInfo> TestOneDatasetByID(Reader reader)
        {
            var dctDatasetsAndSubDirs = new Dictionary<int, string>
            {
                {382287, ""}
            };

            var filesToDownload = TestDatasetByID(reader, dctDatasetsAndSubDirs);

            return filesToDownload;
        }

        static Dictionary<long, ArchivedFileInfo> TestMultiDatasetID(Reader reader)
        {
            var dctDatasetsAndSubDirs = new Dictionary<int, string>
            {
                {54007, "SIC201309041722_Auto976603"},
                {334448, "SIC201309112159_Auto977994"},
                {334455, ""}
            };

            var filesToDownload = TestDatasetByID(reader, dctDatasetsAndSubDirs);

            return filesToDownload;
        }

        static void TestDownloader(Dictionary<long, ArchivedFileInfo> filesToDownload)
        {
            Console.WriteLine("Downloading " + filesToDownload.Count + " files");
            Console.WriteLine();

            var downloader = new Downloader();
            RegisterEvents(downloader);

            downloader.OverwriteMode = Downloader.Overwrite.IfChanged;
            downloader.UseTestInstance = mUseTestInstance;

            try
            {
                string outputDirectory;
                if (string.IsNullOrEmpty(mOutputDirectoryPath))
                    outputDirectory = @"F:\Temp\MyEMSL";
                else
                    outputDirectory = mOutputDirectoryPath;

                downloader.DownloadFiles(filesToDownload, outputDirectory, Downloader.DownloadLayout.DatasetNameAndSubdirectories);
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Exception from downloader", ex);
            }

        }

        static void TestDownloader(ICollection<DatasetDirectoryOrFileInfo> archiveFiles)
        {
            Console.WriteLine("Downloading " + archiveFiles.Count + " files");
            Console.WriteLine();

            DownloadDatasetFiles(archiveFiles, mOutputDirectoryPath);
        }

        #region "Event Handlers"

        private static void RegisterEvents(MyEMSLBase processingClass)
        {
            processingClass.MyEMSLOffline += MyEMSLReader_MyEMSLOffline;
            RegisterEvents((EventNotifier)processingClass);
        }

        private static void RegisterEvents(DatasetInfoBase processingClass)
        {
            processingClass.MyEMSLOffline += MyEMSLReader_MyEMSLOffline;
            RegisterEvents((EventNotifier)processingClass);
        }

        private static void RegisterEvents(EventNotifier processingClass)
        {
            processingClass.DebugEvent += OnDebugEvent;
            processingClass.StatusEvent += OnStatusEvent;
            processingClass.ErrorEvent += OnErrorEvent;
            processingClass.WarningEvent += OnWarningEvent;
            processingClass.ProgressUpdate += OnProgressUpdate;
        }

        private static void OnDebugEvent(string message)
        {
            ConsoleMsgUtils.ShowDebug(message);
        }

        private static void OnStatusEvent(string message)
        {
            Console.WriteLine(message);
        }

        private static void OnErrorEvent(string message, Exception ex)
        {
            ShowErrorMessage(message, ex);
        }

        private static void OnWarningEvent(string message)
        {
            ConsoleMsgUtils.ShowWarning(message);
        }

        private static void OnProgressUpdate(string progressMessage, float percentComplete)
        {
            if (percentComplete > mPercentComplete || DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 30)
            {
                if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 1)
                {
                    Console.WriteLine("Percent complete: " + percentComplete.ToString("0.0") + "%");
                    mPercentComplete = percentComplete;
                    mLastProgressUpdateTime = DateTime.UtcNow;
                }
            }
        }

        #endregion
    }
}
