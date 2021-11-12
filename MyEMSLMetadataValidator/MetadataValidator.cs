using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MyEMSLReader;
using PRISM;
using PRISMDatabaseUtils;

namespace MyEMSLMetadataValidator
{
    internal class MetadataValidator : EventNotifier
    {
        private const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

        private ValidatorOptions Options { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public MetadataValidator(ValidatorOptions options)
        {
            Options = options;
        }

        /// <summary>
        /// Append a result to the output file
        /// </summary>
        /// <param name="resultsWriter"></param>
        /// <param name="ingestTask"></param>
        /// <param name="taskStats"></param>
        /// <param name="filesInMyEMSL"></param>
        /// <param name="bytesInMyEMSL"></param>
        private void AppendResult(
            TextWriter resultsWriter,
            DMSMetadata ingestTask,
            IngestTaskStats taskStats,
            IReadOnlyCollection<ArchivedFileInfo> filesInMyEMSL,
            long bytesInMyEMSL)
        {
            // StatusDate  EntryID  Job  DatasetID  Subdirectory  StatusNum  TransactionID  Entered  Files  FilesInMyEMSL  Bytes  BytesInMyEMSL
            var resultLine = new List<string>
            {
                DateTime.Now.ToString(DATE_TIME_FORMAT),
                ingestTask.EntryID.ToString(),
                ingestTask.Job.ToString(),
                ingestTask.DatasetID.ToString(),
                ingestTask.Subdirectory,
                ingestTask.StatusNum.ToString(),
                ingestTask.TransactionID.ToString(),
                ingestTask.Entered.ToString(DATE_TIME_FORMAT),
                taskStats.TotalFiles.ToString(),
                filesInMyEMSL.Count.ToString(),
                taskStats.TotalBytes.ToString(),
                bytesInMyEMSL.ToString()
            };

            // Append MatchRatio and Comment
            if (filesInMyEMSL.Count == 0)
            {
                resultLine.Add("0");
                resultLine.Add("Missing");
            }
            else
            {
                string rootFolderWarning;
                if (string.IsNullOrWhiteSpace(ingestTask.Subdirectory))
                {
                    // Assure that at least one file in filesInMyEMSL has an empty subdirectory
                    var myEMSLFilesInDatasetDirectory = (from item in filesInMyEMSL where string.IsNullOrWhiteSpace(item.SubDirPath) select item).Count();
                    if (myEMSLFilesInDatasetDirectory == 0)
                        rootFolderWarning = "Empty dataset directory (found files in subdirectories but not in the root directory)";
                    else
                        rootFolderWarning = string.Empty;
                }
                else
                {
                    rootFolderWarning = string.Empty;
                }

                if (filesInMyEMSL.Count == taskStats.TotalFiles)
                {
                    if (bytesInMyEMSL == taskStats.TotalBytes)
                    {
                        // Exact match
                        AppendMatchRatio(resultLine, 1, string.Empty, rootFolderWarning);
                    }
                    else if (bytesInMyEMSL < taskStats.TotalBytes)
                    {
                        // Exact match on files, but fewer bytes
                        var matchRatio = bytesInMyEMSL / (double)taskStats.TotalBytes;
                        AppendMatchRatio(resultLine, matchRatio, "Files match, but fewer bytes", rootFolderWarning);
                    }
                    else if (bytesInMyEMSL > taskStats.TotalBytes)
                    {
                        // Extra bytes
                        AppendMatchRatio(resultLine, 1, "Files match, but extra bytes", rootFolderWarning);
                    }
                }
                else if (filesInMyEMSL.Count > taskStats.TotalFiles && bytesInMyEMSL == taskStats.TotalBytes)
                {
                    // Exact match on bytes, but extra files
                    AppendMatchRatio(resultLine, 1, "Extra files, but bytes match", rootFolderWarning);
                }
                else if (filesInMyEMSL.Count > taskStats.TotalFiles && bytesInMyEMSL > taskStats.TotalBytes)
                {
                    // Extra bytes and extra files
                    if (string.IsNullOrWhiteSpace(ingestTask.Subdirectory))
                    {
                        // Because of how we compute the expected files in the root folder, this is common (and is not an error)
                        AppendMatchRatio(resultLine, 1, string.Empty, rootFolderWarning);
                    }
                    else
                    {
                        // Extra bytes and extra files in a subdirectory
                        // This is common for DeconTools jobs in fall 2013 where initially weren't creating QC graphics,
                        // then we started creating QC Graphics files and back-filled the files for existing DeconTools job folders
                        // For example, dataset ID 336749 in folder
                        // \\Proto-5\Exact03\2013_3\SysVirol_SM012_dORF6_1d_4_Protein_B_27Sep13_Earth_13-07-39\DLS201310011249_Auto984027
                        AppendMatchRatio(resultLine, 1, "Extra files and extra bytes", rootFolderWarning);
                    }
                }
                else
                {
                    var matchRatio = filesInMyEMSL.Count / (double)taskStats.TotalFiles;
                    AppendMatchRatio(resultLine, matchRatio, "Missing files", rootFolderWarning);
                }
            }

            resultsWriter.WriteLine(string.Join("\t", resultLine));
        }

        private void AppendMatchRatio(ICollection<string> resultLine, double matchRatio, string warning, string rootFolderWarning)
        {
            string warningToWrite;
            if (string.IsNullOrWhiteSpace(rootFolderWarning))
                warningToWrite = warning;
            else
            {
                if (string.IsNullOrWhiteSpace(warning))
                    warningToWrite = rootFolderWarning;
                else
                    warningToWrite = rootFolderWarning + "; " + warning;
            }

            if (Math.Abs(matchRatio - 1) < double.Epsilon && !string.IsNullOrWhiteSpace(rootFolderWarning))
            {
                resultLine.Add("-1");
                resultLine.Add(warningToWrite);
            }
            else
            {
                resultLine.Add(matchRatio.ToString("0.00"));
                resultLine.Add(warningToWrite);
            }
        }

        private void CompareDMSDataToMyEMSL(
            TextWriter resultsWriter,
            IEnumerable<int> datasetIDs,
            IReadOnlyCollection<DMSMetadata> dmsMetadata,
            IReadOnlyCollection<DatasetDirectoryOrFileInfo> archiveFiles)
        {
            try
            {
                foreach (var datasetId in datasetIDs)
                {
                    var datasetUploadTasks = (from item in dmsMetadata where item.DatasetID == datasetId select item).ToList();

                    // Estimate the number of files uploaded for each folder for this dataset
                    // Note that the initial upload of files for a dataset typically includes both files in the dataset folder plus files in the QC folder
                    var statsByFolder = new Dictionary<string, IngestTaskStats>();

                    foreach (var uploadTask in datasetUploadTasks)
                    {
                        if (statsByFolder.TryGetValue(uploadTask.Subdirectory, out var taskStats))
                        {
                            if (taskStats.TotalFiles < uploadTask.FilesAddedOrUpdated)
                                taskStats.TotalFiles = uploadTask.FilesAddedOrUpdated;

                            if (taskStats.TotalBytes < uploadTask.Bytes)
                                taskStats.TotalBytes = uploadTask.Bytes;

                            taskStats.IngestTasks.Add(uploadTask);
                            continue;
                        }

                        statsByFolder.Add(uploadTask.Subdirectory, new IngestTaskStats(uploadTask, uploadTask.FilesAddedOrUpdated, uploadTask.Bytes));
                    }

                    var datasetFilesInMyEMSL = (from item in archiveFiles
                                                where item.FileInfo.DatasetID == datasetId && !item.IsDirectory
                                                select item.FileInfo).ToList();

                    var bytesTrackedInMyEMSL = GetTotalBytes(datasetFilesInMyEMSL);

                    var archiveTask = (from item in datasetUploadTasks
                                       where string.IsNullOrWhiteSpace(item.Subdirectory)
                                       select item).FirstOrDefault();

                    var datasetFoundInMyEMSL = false;

                    if (archiveTask != null && archiveTask.DatasetID == datasetId)
                    {
                        // Dataset archive task (empty subdirectory)
                        // Save a results entry with the bulk stats

                        var taskStats = statsByFolder[string.Empty];

                        AppendResult(resultsWriter, archiveTask, taskStats, datasetFilesInMyEMSL, bytesTrackedInMyEMSL);
                        datasetFoundInMyEMSL = true;

                        if (datasetFilesInMyEMSL.Count == 0)
                        {
                            // Dataset was not found in MyEMSL
                            // No point in checking subdirectories
                            continue;
                        }
                    }

                    foreach (var subdirectory in statsByFolder.Keys)
                    {
                        if (string.IsNullOrWhiteSpace(subdirectory))
                        {
                            // Dataset archive task; already logged
                            continue;
                        }
                        var taskStats = statsByFolder[subdirectory];

                        var subdirectoryFilesInMyEMSL = (from item in archiveFiles
                                                         where item.FileInfo.DatasetID == datasetId && !item.IsDirectory &&
                                                           (item.FileInfo.SubDirPath.Equals(subdirectory, StringComparison.OrdinalIgnoreCase) ||
                                                            item.FileInfo.SubDirPath.StartsWith(subdirectory + "/", StringComparison.OrdinalIgnoreCase))
                                                         select item.FileInfo).ToList();

                        if (subdirectoryFilesInMyEMSL.Count == 0)
                        {
                            // Subdirectory not found in MyEMSL, it should have been found
                            AppendResult(resultsWriter, taskStats.IngestTasks.First(), taskStats, new List<ArchivedFileInfo>(), 0);
                            continue;
                        }

                        datasetFoundInMyEMSL = true;
                        var subdirectoryBytesTrackedInMyEMSL = GetTotalBytes(subdirectoryFilesInMyEMSL);

                        AppendResult(resultsWriter, taskStats.IngestTasks.First(), taskStats, subdirectoryFilesInMyEMSL, subdirectoryBytesTrackedInMyEMSL);
                    }

                    if (!datasetFoundInMyEMSL)
                    {
                        // No record of this dataset in MyEMSL
                        foreach (var item in statsByFolder)
                        {
                            AppendResult(resultsWriter, item.Value.IngestTasks.First(), item.Value, new List<ArchivedFileInfo>(), 0);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CompareDMSDataToMyEMSL: " + ex.Message, ex);
            }
        }

        private int GetMaxDatasetIdInMyEMSL(IDBTools dbTools, bool limitToOldMyEMSL = true)
        {
            try
            {
                var query =
                    " SELECT Max(Dataset_ID)" +
                    " FROM T_MyEMSL_Uploads " +
                    " WHERE Verified > 0";

                if (limitToOldMyEMSL)
                    query += " AND StatusURI_PathID < 265";

                dbTools.GetQueryResults(query, out var queryResults);

                if (queryResults.Count > 0)
                {
                    var firstRow = queryResults.First();
                    var maxDatasetId = int.Parse(firstRow[0]);
                    return maxDatasetId;
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LookupHighestDatasetIdInMyEMSL: " + ex.Message, ex);
            }

            return 0;
        }

        private List<DMSMetadata> GetMyEMSLUploadInfoFromDMS(
            IDBTools dbTools,
            int datasetIdStart,
            int datasetIdEnd)
        {
            try
            {
                var warnCount = 0;

                var columns = new List<string>
                {
                    "Entry_ID",
                    "Job",
                    "Dataset_ID",
                    "Subdirectory",
                    "FileCountNew",
                    "FileCountUpdated",
                    "Bytes",
                    "StatusNum",
                    "TransactionID",
                    "Entered"
                };

                var query =
                    " SELECT " + string.Join(", ", columns) +
                    " FROM ( SELECT Entry_ID, Job, Dataset_ID, Subfolder As Subdirectory,  " +
                    "              FileCountNew, FileCountUpdated, Bytes, " +
                    "              StatusNum, TransactionID, Entered, " +
                    "              Row_Number() OVER ( PARTITION BY Dataset_ID, Subfolder " +
                    "                                  ORDER BY FileCountNew + FileCountUpdated DESC ) AS Ranking " +
                    "       FROM T_MyEMSL_Uploads " +
                    "       WHERE Dataset_ID IN ( SELECT TOP 1000 Dataset_ID " +
                    "                             FROM ( SELECT Dataset_ID " +
                    "                                    FROM T_MyEMSL_Uploads " +
                    $"                                   WHERE (Dataset_ID BETWEEN {datasetIdStart} AND {datasetIdEnd}) AND " +
                    "                                           Verified > 0 AND " +
                    "                                           StatusURI_PathID < 265 " +
                    "                                    GROUP BY dataset_id ) LookupQ " +
                    "                             ORDER BY Dataset_ID ) AND " +
                    "             Verified = 1  " +
                    "       ) SortQ " +
                    " WHERE SortQ.Ranking = 1 " +
                    " ORDER BY Dataset_ID, Subdirectory, Entry_ID ";

                var columnMap = dbTools.GetColumnMapping(columns);

                dbTools.GetQueryResults(query, out var queryResults);

                var dmsMetadata = new List<DMSMetadata>();

                foreach (var result in queryResults)
                {
                    var entryId = dbTools.GetColumnValue(result, columnMap, "Entry_ID", 0);
                    var job = dbTools.GetColumnValue(result, columnMap, "Job", 0);
                    var datasetId = dbTools.GetColumnValue(result, columnMap, "Dataset_ID", 0);
                    var subdirectory = dbTools.GetColumnValue(result, columnMap, "Subdirectory");
                    var fileCountNew = dbTools.GetColumnValue(result, columnMap, "FileCountNew", 0);
                    var fileCountUpdated = dbTools.GetColumnValue(result, columnMap, "FileCountUpdated", 0);
                    var bytesText = dbTools.GetColumnValue(result, columnMap, "Bytes");
                    if (!long.TryParse(bytesText, out var bytes))
                    {
                        warnCount++;
                        if (warnCount < 10)
                            OnWarningEvent("Could not convert bytes to a long integer: " + bytesText);
                    }

                    var statusNum = dbTools.GetColumnValue(result, columnMap, "StatusNum", 0);
                    var transactionID = dbTools.GetColumnValue(result, columnMap, "TransactionID", 0);
                    var entered = dbTools.GetColumnValue(result, columnMap, "Entered", DateTime.MinValue);

                    var item = new DMSMetadata(datasetId)
                    {
                        EntryID = entryId,
                        Job = job,
                        Subdirectory = subdirectory,
                        FileCountNew = fileCountNew,
                        FileCountUpdated = fileCountUpdated,
                        Bytes = bytes,
                        StatusNum = statusNum,
                        TransactionID = transactionID,
                        Entered = entered
                    };

                    dmsMetadata.Add(item);
                }

                return dmsMetadata;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in GetMyEMSLUploadInfoFromDMS: " + ex.Message, ex);

                return new List<DMSMetadata>();
            }
        }

        private long GetTotalBytes(IEnumerable<ArchivedFileInfo> datasetFilesInMyEMSL)
        {
            return (from item in datasetFilesInMyEMSL select item.FileSizeBytes).Sum();
        }

        /// <summary>
        /// If Options.DatasetIdFile is defined, load the dataset IDs from that file
        /// </summary>
        /// <param name="datasetIdFilePath"></param>
        /// <returns>
        /// Dataset ID list if a dataset ID file is defined, otherwise an empty list.
        /// However, returns null if an error
        /// </returns>
        private SortedSet<int> LoadDatasetIDs(string datasetIdFilePath)
        {
            var datasetIDs = new SortedSet<int>();

            if (string.IsNullOrWhiteSpace(datasetIdFilePath))
            {
                return datasetIDs;
            }

            try
            {
                var datasetIdFile = new FileInfo(datasetIdFilePath);
                if (!datasetIdFile.Exists)
                {
                    OnErrorEvent("Dataset ID File not found: " + datasetIdFile.FullName);
                    return null;
                }

                OnStatusEvent("Reading Dataset IDs from " + datasetIdFile.FullName);

                using (var reader = new StreamReader(new FileStream(datasetIdFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var lineNumber = 0;
                    var warnings = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        lineNumber++;

                        if (!int.TryParse(dataLine, out var datasetID))
                        {
                            if (lineNumber > 1)
                            {
                                warnings++;
                                if (warnings < 10)
                                {
                                    OnWarningEvent("Ignoring line in datasetID file since not an integer: " + dataLine);
                                }
                            }
                            continue;
                        }

                        if (datasetIDs.Contains(datasetID))
                            continue;

                        datasetIDs.Add(datasetID);
                    }
                }

                return datasetIDs;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadDatasetIDs: " + ex.Message, ex);
                return null;
            }
        }

        /// <summary>
        /// Validate metadata in MyEMSL
        /// </summary>
        public bool ValidateMyEMSLMetadata()
        {
            var datasetIdStart = 0;

            try
            {
                var outputFolder = new DirectoryInfo(Options.OutputFolderPath);
                if (!outputFolder.Exists)
                {
                    OnStatusEvent("Creating folder " + outputFolder.FullName);
                    outputFolder.Create();
                }

                var datasetIDsToCheck = LoadDatasetIDs(Options.DatasetIdFile);
                if (datasetIDsToCheck == null)
                    return false;

                var outputFile = new FileInfo(
                    Path.Combine(outputFolder.FullName,
                                 string.Format("MyEMSL_MetadataValidation_{0:yyyy-MM-dd}.txt", DateTime.Now)));

                bool writeHeaders;
                if (outputFile.Exists)
                {
                    if (Options.AppendToOutput)
                    {
                        OnStatusEvent("Appending to " + outputFile.FullName);
                        writeHeaders = (outputFile.Length == 0);
                    }
                    else
                    {
                        OnStatusEvent("Overwriting " + outputFile.FullName);
                        writeHeaders = true;
                        outputFile.Delete();
                    }
                }
                else
                {
                    OnStatusEvent("Creating " + outputFile.FullName);
                    writeHeaders = true;
                }

                var dbTools = DbToolsFactory.GetDBTools(DbServerTypes.MSSQLServer, ValidatorOptions.DMS_CONNECTION_STRING);

                using (var resultsWriter = new StreamWriter(
                    new FileStream(outputFile.FullName, FileMode.Append, FileAccess.Write, FileShare.Read)))
                {
                    if (writeHeaders)
                    {
                        var headers = new List<string>
                        {
                            "StatusDate",
                            "EntryID",
                            "Job",
                            "DatasetID",
                            "Subdirectory",
                            "StatusNum",
                            "TransactionID",
                            "Entered",
                            "Files",
                            "FilesInMyEMSL",
                            "Bytes",
                            "BytesInMyEMSL",
                            "MatchRatio",
                            "Comment"
                        };

                        resultsWriter.WriteLine(string.Join("\t", headers));
                    }

                    var lastResultsFlush = DateTime.UtcNow;

                    var maxDatasetId = GetMaxDatasetIdInMyEMSL(dbTools);

                    int finalDatasetId;
                    int totalDatasetsToProcess;

                    if (datasetIDsToCheck.Count == 0)
                    {
                        datasetIdStart = Options.DatasetIdStart;

                        if (Options.DatasetIdEnd == 0)
                            finalDatasetId = maxDatasetId;
                        else
                            finalDatasetId = Math.Min(Options.DatasetIdEnd, maxDatasetId);

                        totalDatasetsToProcess = finalDatasetId - datasetIdStart + 1;
                        if (totalDatasetsToProcess < 1)
                            throw new Exception(string.Format(
                                                    "Total datasets should not be negative; computed {0} using {1} - {2} + 1",
                                                    totalDatasetsToProcess, finalDatasetId,
                                                    datasetIdStart));
                    }
                    else
                    {
                        datasetIdStart = datasetIDsToCheck.Min();
                        finalDatasetId = Math.Min(datasetIDsToCheck.Max(), maxDatasetId);
                        totalDatasetsToProcess = datasetIDsToCheck.Count;
                    }

                    var datasetsProcessed = 0;

                    while (datasetIdStart <= finalDatasetId)
                    {
                        var basePercentComplete = datasetsProcessed * 100F / totalDatasetsToProcess;

                        var datasetIdEnd = Math.Min(finalDatasetId, datasetIdStart + Options.DMSLookupBatchSize - 1);

                        var success = ValidateDatasetBatch(
                            dbTools, resultsWriter, datasetIDsToCheck,
                            datasetIdStart, datasetIdEnd, totalDatasetsToProcess, basePercentComplete);

                        if (!success)
                            return false;

                        if (datasetIDsToCheck.Count == 0)
                        {
                            datasetsProcessed += Options.DMSLookupBatchSize;
                            datasetIdStart += Options.DMSLookupBatchSize;
                        }
                        else
                        {
                            datasetsProcessed += (from item in datasetIDsToCheck where item >= datasetIdStart && item <= datasetIdEnd select item).Count();
                            datasetIdStart = (from item in datasetIDsToCheck where item > datasetIdEnd select item).Min();
                        }

                        if (DateTime.UtcNow.Subtract(lastResultsFlush).TotalSeconds > 15)
                        {
                            lastResultsFlush = DateTime.UtcNow;
                            resultsWriter.Flush();
                        }
                    }
                } // Close the writer

                OnProgressUpdate("Processing Complete", 100);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateMyEMSLMetadata " +
                                 "(datasetIdStart is " + datasetIdStart + "): " + ex.Message, ex);

                return false;
            }

            return true;
        }

        private bool ValidateDatasetBatch(
            IDBTools dbTools,
            TextWriter resultsWriter,
            ICollection<int> datasetIDsToCheck,
            int datasetIdStart,
            int datasetIdEnd,
            int totalDatasetsToProcess,
            float basePercentComplete)
        {
            try
            {
                var dmsMetadata = GetMyEMSLUploadInfoFromDMS(dbTools, datasetIdStart, datasetIdEnd);

                if (dmsMetadata.Count == 0)
                {
                    // No results for this dataset range
                    return true;
                }

                List<DMSMetadata> filteredDMSMetadata;

                if (datasetIDsToCheck.Count == 0)
                {
                    filteredDMSMetadata = dmsMetadata;
                }
                else
                {
                    filteredDMSMetadata = new List<DMSMetadata>();

                    // File dmsMetadata using datasetIDsToCheck
                    foreach (var item in dmsMetadata)
                    {
                        if (datasetIDsToCheck.Contains(item.DatasetID))
                        {
                            filteredDMSMetadata.Add(item);
                        }
                    }
                }

                OnProgressUpdate(string.Format(
                    "Examining {0} uploads for Dataset IDs {1} to {2}",
                    filteredDMSMetadata.Count, datasetIdStart, datasetIdEnd), basePercentComplete);

                var datasetListInfo = new DatasetListInfoByID
                {
                    ReportMetadataURLs = true,
                    ThrowErrors = false,
                    TraceMode = false
                };

                RegisterEvents(datasetListInfo);

                var lastComparisonTime = DateTime.UtcNow;
                var lastProgressTime = DateTime.UtcNow;
                var itemsProcessed = 0;

                foreach (var item in filteredDMSMetadata)
                {
                    datasetListInfo.AddDataset(item.DatasetID);
                    itemsProcessed++;

                    if (itemsProcessed < filteredDMSMetadata.Count && datasetListInfo.DatasetIDs.Count < Options.MyEMSLReaderBatchSize)
                        continue;

                    if (Options.Preview)
                    {
                        OnStatusEvent(string.Format("Preview: retrieve MyEMSL metadata for {0} datasets", datasetListInfo.DatasetIDs.Count));
                    }
                    else
                    {
                        var archiveFiles = datasetListInfo.FindFiles("*");

                        while (DateTime.UtcNow.Subtract(lastComparisonTime).TotalMilliseconds < Options.MSecBetweenLookup)
                        {
                            System.Threading.Thread.Sleep(100);
                        }

                        CompareDMSDataToMyEMSL(resultsWriter, datasetListInfo.DatasetIDs, filteredDMSMetadata, archiveFiles);
                        lastComparisonTime = DateTime.UtcNow;
                    }

                    datasetListInfo.Clear();

                    if (DateTime.UtcNow.Subtract(lastProgressTime).TotalSeconds > 15)
                    {
                        lastProgressTime = DateTime.UtcNow;

                        var subTaskPercentComplete = itemsProcessed * 100F / filteredDMSMetadata.Count;

                        var percentComplete = basePercentComplete +
                                              subTaskPercentComplete * Options.DMSLookupBatchSize / totalDatasetsToProcess;

                        OnProgressUpdate(" ... ", percentComplete);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateDatasetBatch: " + ex.Message, ex);

                return false;
            }
        }
    }
}
