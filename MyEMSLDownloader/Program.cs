using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MyEMSLReader;
using PRISM;
using PRISM.Logging;

namespace MyEMSLDownloader
{
    internal struct TargetFileInfo
    {
        public string FileMask;
        public string SubDir;
    }

    internal static class Program
    {
        private const string PROGRAM_DATE = "January 17, 2023";

        // Ignore Spelling: Acq, downloader, melissa, ser, un-purged, Virol

        private static double mPercentComplete;
        private static DateTime mLastProgressUpdateTime = DateTime.UtcNow;

        private static CommandLineOptions mOptions;
        private static DatasetListInfo mDatasetListInfo;
        private static DatasetListInfoByID mDatasetListInfoByID;
        private static DataPackageListInfo mDataPackageListInfo;

        private static int Main(string[] args)
        {
            var assembly = System.Reflection.Assembly.GetExecutingAssembly();
            var programName = assembly.GetName().Name;
            var exeName = Path.GetFileName(assembly.Location);
            var appVersion = "version " + assembly.GetName().Version;

            var parser =
                new CommandLineParser<CommandLineOptions>(programName, appVersion)
                {
                    ProgramInfo = "This program downloads files from MyEMSL" + Environment.NewLine + Environment.NewLine +
                                  "To download files for a given dataset, enter the dataset name or dataset ID, plus optionally the subdirectory name. " +
                                  "Alternatively, use /Dataset or /DatasetID plus optionally /SubDir",
                    ContactInfo = "Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)" +
                                  Environment.NewLine + Environment.NewLine +
                                  "Version: " + AppUtils.GetAppVersion(PROGRAM_DATE) +
                                  Environment.NewLine + Environment.NewLine +
                                  "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                                  "Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics"
                };

            parser.UsageExamples.Add("Syntax #1:" + Environment.NewLine + exeName +
                                     " DatasetNameOrID [SubdirectoryName] [/Files:FileMask] [/FileSplit]" +
                                     " [/O:OutputDirectory] [/D] [/Preview] [/V] [/Trace] [/UseTest]");

            parser.UsageExamples.Add("Syntax #2:" + Environment.NewLine + exeName +
                                     " /Dataset:DatasetName [/SubDir:SubdirectoryName] [/Files:FileMask] [/FileSplit]" +
                                     " [/O:OutputDirectory] [/D] [/Preview] [/V] [/Trace] [/UseTest]");

            parser.UsageExamples.Add("Syntax #3:" + Environment.NewLine + exeName +
                                     " /DatasetID:DatasetID [/SubDir:SubdirectoryName] [/Files:FileMask] [/FileSplit]" +
                                     " [/O:OutputDirectory] [/D] [/Preview] [/V] [/Trace] [/UseTest]");

            parser.UsageExamples.Add("Syntax #4:" + Environment.NewLine + exeName +
                                     " /DataPkg:DataPackageID [/SubDir:SubdirectoryName] [/Files:FileMask] [/FileSplit]" +
                                     " [/O:OutputDirectory] [/Preview] [/V] [/Trace] [/UseTest]");

            parser.UsageExamples.Add("Syntax #5:" + Environment.NewLine + exeName +
                                     " /FileList:FileInfoFile.txt [/O:OutputDirectory]" +
                                     " [/Preview] [/V] [/Trace] [/UseTest]");

            parser.UsageExamples.Add("Syntax #6:" + Environment.NewLine + exeName +
                                     " /FileID:1234 [/Preview] [/V] [/Trace]");

            parser.UsageExamples.Add("Syntax #7:" + Environment.NewLine + exeName +
                                     " /Test [/Preview] [/V] [/Trace]");

            var result = parser.ParseArgs(args);
            mOptions = result.ParsedResults;

            if (!result.Success || !mOptions.Validate())
            {
                if (parser.CreateParamFileProvided)
                {
                    return 0;
                }

                // Delay for 1 second in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                System.Threading.Thread.Sleep(1000);
                return -1;
            }

            try
            {
                mDatasetListInfo = new DatasetListInfo
                {
                    IncludeAllRevisions = mOptions.IncludeAllRevisions,
                    ReportMetadataURLs = mOptions.PreviewMode || mOptions.TraceMode,
                    ThrowErrors = false,
                    TraceMode = mOptions.TraceMode
                };
                RegisterEvents(mDatasetListInfo);

                mDatasetListInfoByID = new DatasetListInfoByID
                {
                    IncludeAllRevisions = mOptions.IncludeAllRevisions,
                    ReportMetadataURLs = mOptions.PreviewMode || mOptions.TraceMode,
                    ThrowErrors = false,
                    TraceMode = mOptions.TraceMode
                };
                RegisterEvents(mDatasetListInfoByID);

                mDataPackageListInfo = new DataPackageListInfo
                {
                    IncludeAllRevisions = mOptions.IncludeAllRevisions,
                    ReportMetadataURLs = mOptions.PreviewMode || mOptions.TraceMode,
                    ThrowErrors = false,
                    TraceMode = mOptions.TraceMode
                };
                RegisterEvents(mDataPackageListInfo);

                mDatasetListInfo.UseTestInstance = mOptions.UseTestInstance;
                mDatasetListInfoByID.UseTestInstance = mOptions.UseTestInstance;
                mDataPackageListInfo.UseTestInstance = mOptions.UseTestInstance;

                if (mOptions.AutoTestMode)
                {
                    AutoTestModeStart();
                    System.Threading.Thread.Sleep(1000);
                    return 0;
                }

                List<DatasetDirectoryOrFileInfo> archiveFiles;

                if (mOptions.DataPkgID > 0)
                {
                    archiveFiles = FindDataPkgFiles(mOptions.DataPkgID, mOptions.Subdirectory, mOptions.FileMask, mOptions.FileSplit);
                }
                else
                {
                    if (!string.IsNullOrWhiteSpace(mOptions.FileListPath))
                    {
                        var fileListFile = new FileInfo(mOptions.FileListPath);
                        if (!fileListFile.Exists)
                        {
                            ShowErrorMessage("File not found: " + fileListFile.FullName);
                            System.Threading.Thread.Sleep(1000);
                            return -1;
                        }

                        archiveFiles = FindFileListFiles(fileListFile);
                    }
                    else
                    {
                        if (!string.IsNullOrWhiteSpace(mOptions.FileIDList))
                        {
                            archiveFiles = ParseExplicitFileIDs(mOptions.FileIDList);
                            if (archiveFiles.Count == 0)
                            {
                                ShowErrorMessage("No File IDs were found with the /FileID parameter");
                                System.Threading.Thread.Sleep(1000);
                                return -1;
                            }
                        }
                        else
                        {
                            if (mOptions.DatasetID > 0)
                            {
                                archiveFiles = FindDatasetFilesByID(mOptions.DatasetID, mOptions.Subdirectory, mOptions.FileMask, mOptions.FileSplit);
                            }
                            else if (!string.IsNullOrWhiteSpace(mOptions.DatasetName))
                            {
                                archiveFiles = FindDatasetFiles(mOptions.DatasetName, mOptions.Subdirectory, mOptions.FileMask, mOptions.FileSplit);
                            }
                            else
                            {
                                ShowErrorMessage("Dataset Name, Dataset ID, or Data Package ID not specified. Use /Dataset or /DatasetID or /DataPkg or /FileList or /FileID");
                                System.Threading.Thread.Sleep(1000);
                                return -1;
                            }
                        }
                    }
                }

                Console.WriteLine();

                if (mOptions.PreviewMode)
                    Console.WriteLine("Previewing files that would be downloaded; count = " + archiveFiles.Count);
                else
                    Console.WriteLine("Downloading files from MyEMSL; count = " + archiveFiles.Count);

                Console.WriteLine();

                ShowFiles(archiveFiles, mOptions.VerbosePreview);

                if (mOptions.PreviewMode)
                {
                    System.Threading.Thread.Sleep(250);
                    return 0;
                }

                Console.WriteLine();

                if (mOptions.DataPkgID > 0)
                    DownloadDataPackageFiles(archiveFiles, mOptions.OutputDirectoryPath);
                else
                    DownloadDatasetFiles(archiveFiles, mOptions.OutputDirectoryPath);
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

        private static void AddFileToFind(ICollection<TargetFileInfo> datasetFiles, TargetFileInfo fileToFind)
        {
            if (datasetFiles.Any(existingFileToFind => existingFileToFind.FileMask.Equals(fileToFind.FileMask) &&
                                                       existingFileToFind.SubDir.Equals(fileToFind.SubDir)))
            {
                // Duplicate file spec; skip it
                return;
            }

            datasetFiles.Add(fileToFind);
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
            else if (!mOptions.PreviewMode)
            {
                TestDownloader(filesToDownload);
            }
            Console.WriteLine();

            var archiveFiles = TestDatasetListInfo();

            if (archiveFiles.Count == 0)
            {
                ConsoleMsgUtils.ShowWarning("DatasetListInfo did not find any files");
            }
            else
            {
                ShowFiles(archiveFiles, mOptions.VerbosePreview);

                if (!mOptions.PreviewMode)
                    TestDownloader(archiveFiles);
            }
        }

        private static void DownloadDatasetFiles(IEnumerable<DatasetDirectoryOrFileInfo> archiveFiles, string outputDirectoryPath)
        {
            if (mDatasetListInfo.Datasets.Count > 0)
            {
                DownloadFiles(mDatasetListInfo, archiveFiles, outputDirectoryPath);
            }
            else if (mDatasetListInfoByID.DatasetIDs.Count > 0)
            {
                DownloadFiles(mDatasetListInfoByID, archiveFiles, outputDirectoryPath);
            }
            else
            {
                ConsoleMsgUtils.ShowWarning("No dataset names or dataset IDs are defined; nothing to download");
            }
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
            if (mOptions.MultiDatasetMode)
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
        /// <remarks>
        /// For fileMask, specify a list of names and/or specs by separating with a vertical bar
        /// For example: analysis.baf|ser
        /// </remarks>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="subdirectory">Subdirectory to filter on (optional)</param>
        /// <param name="fileMask">File name or file spec like *.txt to filter on (optional)</param>
        /// <param name="fileSplit"></param>
        private static List<DatasetDirectoryOrFileInfo> FindDatasetFiles(
            string datasetName,
            string subdirectory,
            string fileMask,
            bool fileSplit)
        {
            mDatasetListInfo.AddDataset(datasetName, subdirectory);

            if (string.IsNullOrEmpty(fileMask))
                fileMask = "*";

            return mDatasetListInfo.FindFiles(fileMask, subdirectory, datasetName, true, fileSplit);
        }

        private static List<DatasetDirectoryOrFileInfo> FindDatasetFilesByID(
            int datasetID,
            string subdirectory,
            string fileMask,
            bool fileSplit)
        {
            mDatasetListInfoByID.AddDataset(datasetID, subdirectory);

            if (string.IsNullOrEmpty(fileMask))
                fileMask = "*";

            return mDatasetListInfoByID.FindFiles(fileMask, subdirectory, string.Empty, true, fileSplit);
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

            return mDataPackageListInfo.FindFiles(fileMask, subdirectory, true, fileSplit);
        }

        private static List<DatasetDirectoryOrFileInfo> FindFileListFiles(FileSystemInfo fileListFile)
        {
            const string DATASET_COLUMN = "Dataset";
            const string DATASET_ID_COLUMN = "DatasetID";
            const string SUB_DIR_COLUMN = "SubDir";
            const string FILE_COLUMN = "File";

            try
            {
                var datasetsToFind = new Dictionary<string, List<TargetFileInfo>>();
                var datasetIDsToFind = new Dictionary<int, List<TargetFileInfo>>();

                using (var reader = new StreamReader(new FileStream(fileListFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    var headerMap = new Dictionary<string, int>();
                    var headerNames = new List<string>
                    {
                        DATASET_COLUMN, DATASET_ID_COLUMN, SUB_DIR_COLUMN, FILE_COLUMN
                    };
                    var datasetNameOrIdColumnIndex = 0;
                    var usingDatasetIDs = false;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

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

                            if (!(headerMap.ContainsKey(DATASET_COLUMN) || headerMap.ContainsKey(DATASET_ID_COLUMN)) ||
                                !headerMap.ContainsKey(FILE_COLUMN))
                            {
                                ConsoleMsgUtils.ShowWarning("Missing columns in " + fileListFile.Name);
                                ConsoleMsgUtils.ShowWarning(
                                    "Header line must contain columns {0} or {1} and column {2}, plus optionally {3}",
                                    DATASET_COLUMN, DATASET_ID_COLUMN, FILE_COLUMN, SUB_DIR_COLUMN);

                                return new List<DatasetDirectoryOrFileInfo>();
                            }

                            if (headerMap.ContainsKey(DATASET_COLUMN))
                            {
                                datasetNameOrIdColumnIndex = headerMap[DATASET_COLUMN];
                            }
                            else
                            {
                                datasetNameOrIdColumnIndex = headerMap[DATASET_ID_COLUMN];
                                usingDatasetIDs = true;
                            }

                            continue;
                        }

                        if (datasetNameOrIdColumnIndex > lastIndex || headerMap[FILE_COLUMN] > lastIndex)
                        {
                            ConsoleMsgUtils.ShowWarning("Data line has fewer columns than the header line; skipping: " + dataLine);
                            continue;
                        }

                        var datasetNameOrID = dataValues[datasetNameOrIdColumnIndex].Trim();

                        var fileSpec = dataValues[headerMap[FILE_COLUMN]].Trim();
                        var lastSlashIndex = fileSpec.LastIndexOf(Path.DirectorySeparatorChar);
                        string directoryFromFileSpec;
                        string fileMask;

                        if (lastSlashIndex >= 1)
                        {
                            // The file column has a relative file path, e.g.
                            // DatasetName_07Feb15_Polaroid.d\AcqData\MSPeak.bin
                            directoryFromFileSpec = fileSpec.Substring(0, lastSlashIndex);
                            fileMask = fileSpec.Substring(lastSlashIndex + 1);
                        }
                        else
                        {
                            directoryFromFileSpec = string.Empty;
                            fileMask = fileSpec;
                        }

                        var fileToFind = new TargetFileInfo
                        {
                            FileMask = fileMask
                        };

                        if (headerMap.ContainsKey(SUB_DIR_COLUMN))
                        {
                            if (headerMap[SUB_DIR_COLUMN] > lastIndex)
                            {
                                ConsoleMsgUtils.ShowWarning("Data line has fewer columns than the header line; skipping: " + dataLine);
                                continue;
                            }
                            fileToFind.SubDir = dataValues[headerMap[SUB_DIR_COLUMN]].Trim();
                            if (fileToFind.SubDir.Equals("."))
                            {
                                // Treat a source directory of "." as ""
                                fileToFind.SubDir = string.Empty;
                            }
                        }
                        else
                        {
                            fileToFind.SubDir = directoryFromFileSpec;
                        }

                        if (string.IsNullOrWhiteSpace(fileToFind.FileMask))
                        {
                            ConsoleMsgUtils.ShowWarning("Ignoring line with empty filename: " + dataLine.Replace("\t", "<tab>"));
                            continue;
                        }

                        if (usingDatasetIDs)
                        {
                            if (!int.TryParse(datasetNameOrID, out var datasetId))
                            {
                                ConsoleMsgUtils.ShowWarning("Dataset ID should be an integer, not: " + datasetNameOrID);
                                continue;
                            }
                            if (!datasetIDsToFind.TryGetValue(datasetId, out var datasetFiles))
                            {
                                datasetFiles = new List<TargetFileInfo>();
                                datasetIDsToFind.Add(datasetId, datasetFiles);
                            }

                            AddFileToFind(datasetFiles, fileToFind);

                            // Add the dataset ID so that all of its tracked files will be determined
                            // when MyEMSL is first queried via RefreshInfoIfStale (which calls RefreshInfo)
                            mDatasetListInfoByID.AddDataset(datasetId, fileToFind.SubDir);
                        }
                        else
                        {
                            var datasetName = datasetNameOrID;
                            if (!datasetsToFind.TryGetValue(datasetName, out var datasetFiles))
                            {
                                datasetFiles = new List<TargetFileInfo>();
                                datasetsToFind.Add(datasetName, datasetFiles);
                            }

                            AddFileToFind(datasetFiles, fileToFind);

                            // Add the dataset name so that all of its tracked files will be determined
                            // when MyEMSL is first queried via RefreshInfoIfStale (which calls RefreshInfo)
                            mDatasetListInfo.AddDataset(datasetName, fileToFind.SubDir);
                        }
                    }
                }

                var archiveFiles = new List<DatasetDirectoryOrFileInfo>();

                FindFilesForDatasets(datasetsToFind, archiveFiles);

                FindFilesForDatasets(datasetIDsToFind, archiveFiles);

                return archiveFiles;
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Exception in FindFileListFiles", ex);
                return new List<DatasetDirectoryOrFileInfo>();
            }
        }

        private static void FindFilesForDatasets(
            Dictionary<string, List<TargetFileInfo>> datasetsToFind,
            ICollection<DatasetDirectoryOrFileInfo> archiveFiles)
        {
            // This list tracks MyEMSL File IDs that have been added to archiveFiles
            var archiveFileIDs = new SortedSet<long>();

            // Keys in this dictionary are MyEMSL File IDs, values are file info
            var archiveFilesAllDatasets = new Dictionary<long, DatasetDirectoryOrFileInfo>();

            foreach (var dataset in datasetsToFind)
            {
                foreach (var fileToFind in dataset.Value)
                {
                    var archiveFilesToAdd = FindDatasetFiles(dataset.Key, fileToFind.SubDir, fileToFind.FileMask, fileSplit: false).ToList();

                    foreach (var archiveFile in archiveFilesToAdd)
                    {
                        if (!archiveFilesAllDatasets.ContainsKey(archiveFile.FileID))
                        {
                            archiveFilesAllDatasets.Add(archiveFile.FileID, archiveFile);
                        }

                        if (string.Equals(archiveFile.FileInfo.Dataset, dataset.Key, StringComparison.OrdinalIgnoreCase) &&
                            !archiveFileIDs.Contains(archiveFile.FileID))
                        {
                            archiveFiles.Add(archiveFile);
                            archiveFileIDs.Add(archiveFile.FileID);
                        }
                    }
                }
            }

            WarnIfSkippedFiles(archiveFileIDs, archiveFilesAllDatasets);
        }

        private static void FindFilesForDatasets(
            Dictionary<int, List<TargetFileInfo>> datasetIDsToFind,
            ICollection<DatasetDirectoryOrFileInfo> archiveFiles)
        {
            // This list tracks MyEMSL File IDs that have been added to archiveFiles
            var archiveFileIDs = new SortedSet<long>();

            // Keys in this dictionary are MyEMSL File IDs, values are file info
            var archiveFilesAllDatasets = new Dictionary<long, DatasetDirectoryOrFileInfo>();

            foreach (var dataset in datasetIDsToFind)
            {
                foreach (var fileToFind in dataset.Value)
                {
                    var archiveFilesToAdd = FindDatasetFilesByID(dataset.Key, fileToFind.SubDir, fileToFind.FileMask, fileSplit: false).ToList();

                    foreach (var archiveFile in archiveFilesToAdd)
                    {
                        if (!archiveFilesAllDatasets.ContainsKey(archiveFile.FileID))
                        {
                            archiveFilesAllDatasets.Add(archiveFile.FileID, archiveFile);
                        }

                        if (archiveFile.FileInfo.DatasetID == dataset.Key &&
                            !archiveFileIDs.Contains(archiveFile.FileID))
                        {
                            archiveFiles.Add(archiveFile);
                            archiveFileIDs.Add(archiveFile.FileID);
                        }
                    }
                }
            }

            WarnIfSkippedFiles(archiveFileIDs, archiveFilesAllDatasets);
        }

        private static void MapHeaders(
            IList<string> dataValues,
            IReadOnlyCollection<string> headerNames,
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
                    ConsoleMsgUtils.ShowWarning("Warning: FileID should be an integer, not: " + fileID);
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
                if (!verbosePreview)
                    continue;

                ConsoleMsgUtils.ShowDebug(
                        "  FileID {0}, TransID {1}, Submitted {2}, Size {3:F1} KB, Hash {4}, HashType {5}",
                        archiveFile.FileID,
                        archiveFile.FileInfo.TransactionID,
                        archiveFile.FileInfo.SubmissionTime,
                        archiveFile.FileInfo.FileSizeBytes / 1024.0,
                        archiveFile.FileInfo.Sha1Hash,
                        archiveFile.FileInfo.HashType);
                Console.WriteLine();
            }
        }

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static List<DatasetDirectoryOrFileInfo> TestDatasetListInfo()
        {
            Console.WriteLine("Looking for files for test datasets using the DatasetListInfo class");
            Console.WriteLine();

            // ReSharper disable once StringLiteralTypo
            mDatasetListInfo.AddDataset("2013_10_01_CMPD04_000005");

            return mDatasetListInfo.FindFiles("*");
        }

        private static Dictionary<long, ArchivedFileInfo> TestReader()
        {
            Console.WriteLine("Looking for files for test datasets using the Reader class");
            Console.WriteLine();

            var reader = new Reader
            {
                IncludeAllRevisions = false,
                UseTestInstance = mOptions.UseTestInstance,
                ReportMetadataURLs = mOptions.PreviewMode || mOptions.TraceMode,
                TraceMode = mOptions.TraceMode
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

        private static Dictionary<long, ArchivedFileInfo> TestOneDataPackage(Reader reader)
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
                    IncludeAllRevisions = mOptions.IncludeAllRevisions,
                    ReportMetadataURLs = mOptions.PreviewMode || mOptions.TraceMode,
                    ThrowErrors = false,
                    TraceMode = mOptions.TraceMode
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

        private static Dictionary<long, ArchivedFileInfo> TestOneDataset(Reader reader)
        {
            var filesToDownload = new Dictionary<long, ArchivedFileInfo>();
            const string subDir = "";

            // datasetName = "Blank_B-2_20Apr12_Draco_12-02-37";
            // datasetName = "QC_Shew_11_06_pt5_d2_11Jun12_Draco_12-04-14";

            // Dataset where all of the files were purged from spinning disk (but were later un-purged and now possibly re-purged)
            // datasetName = "2013_05_28_U01-B_Wilkins_neg_4M_0p1acc_8x_144_000001";
            // subDir = "";

            // datasetName = "SWT_LCQData_300";
            // subDir = "SIC201309041722_Auto976603";

            // ReSharper disable once StringLiteralTypo
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

        private static Dictionary<long, ArchivedFileInfo> TestMultiDataset(Reader reader)
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

            return TestDatasetByID(reader, dctDatasetsAndSubDirs);
        }

        private static Dictionary<long, ArchivedFileInfo> TestMultiDatasetID(Reader reader)
        {
            var dctDatasetsAndSubDirs = new Dictionary<int, string>
            {
                {54007, "SIC201309041722_Auto976603"},
                {334448, "SIC201309112159_Auto977994"},
                {334455, ""}
            };

            return TestDatasetByID(reader, dctDatasetsAndSubDirs);
        }

        private static void TestDownloader(Dictionary<long, ArchivedFileInfo> filesToDownload)
        {
            Console.WriteLine("Downloading " + filesToDownload.Count + " files");
            Console.WriteLine();

            var downloader = new Downloader();
            RegisterEvents(downloader);

            downloader.OverwriteMode = Downloader.Overwrite.IfChanged;
            downloader.UseTestInstance = mOptions.UseTestInstance;

            try
            {
                string outputDirectory;
                if (string.IsNullOrEmpty(mOptions.OutputDirectoryPath))
                    outputDirectory = @"E:\Temp\MyEMSL";
                else
                    outputDirectory = mOptions.OutputDirectoryPath;

                downloader.DownloadFiles(filesToDownload, outputDirectory, Downloader.DownloadLayout.DatasetNameAndSubdirectories);
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Exception from downloader", ex);
            }
        }

        private static void TestDownloader(IReadOnlyCollection<DatasetDirectoryOrFileInfo> archiveFiles)
        {
            Console.WriteLine("Downloading " + archiveFiles.Count + " files");
            Console.WriteLine();

            DownloadDatasetFiles(archiveFiles, mOptions.OutputDirectoryPath);
        }

        private static void WarnIfSkippedFiles(
            ICollection<long> archiveFileIDs,
            IDictionary<long, DatasetDirectoryOrFileInfo> archiveFilesAllDatasets)
        {
            // Check for any files in archiveFilesAllDatasets that did not get included in archiveFiles
            foreach (var item in archiveFilesAllDatasets)
            {
                if (!archiveFileIDs.Contains(item.Key))
                {
                    ConsoleMsgUtils.ShowWarning(
                        @"MyEMSL search result did not match any of the expected datasets: {0}\{1}",
                        item.Value.FileInfo.Dataset,
                        item.Value.FileInfo.RelativePathWindows);
                }
            }
        }

        private static void MyEMSLReader_MyEMSLOffline(string message)
        {
            OnWarningEvent(message);
        }

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

        private static void RegisterEvents(IEventNotifier processingClass)
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
    }
}
