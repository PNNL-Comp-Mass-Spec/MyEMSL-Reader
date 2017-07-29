using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MyEMSLReader;
using PRISM;

namespace MyEMSLMetadataValidator
{
    class MetadataValidator : clsEventNotifier
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
        private void AppendResult(TextWriter resultsWriter, DMSMetadata ingestTask, IngestTaskStats taskStats, int filesInMyEMSL, long bytesInMyEMSL)
        {

            // StatusDate  EntryID  Job  DatasetID  Subfolder  StatusNum  TransactionID  Entered  Files  FilesInMyEMSL  Bytes  BytesInMyEMSL
            var resultLine = new List<string>
            {
                DateTime.Now.ToString(DATE_TIME_FORMAT),
                ingestTask.EntryID.ToString(),
                ingestTask.Job.ToString(),
                ingestTask.DatasetID.ToString(),
                ingestTask.Subfolder,
                ingestTask.StatusNum.ToString(),
                ingestTask.TransactionID.ToString(),
                ingestTask.Entered.ToString(DATE_TIME_FORMAT),
                taskStats.TotalFiles.ToString(),
                filesInMyEMSL.ToString(),
                taskStats.TotalBytes.ToString(),
                bytesInMyEMSL.ToString()
            };

            // Append MatchRatio and Comment
            if (filesInMyEMSL == 0)
            {
                resultLine.Add("0");
                resultLine.Add("Missing");
            }
            else
            {
                if (filesInMyEMSL == taskStats.TotalFiles)
                {
                    if (bytesInMyEMSL == taskStats.TotalBytes)
                    {
                        // Exact match
                        resultLine.Add("1");
                        resultLine.Add(string.Empty);
                    }
                    else if (bytesInMyEMSL < taskStats.TotalBytes)
                    {
                        // Exact match on files, but fewer bytes
                        var matchRatio = bytesInMyEMSL / (double)taskStats.TotalBytes;
                        resultLine.Add(matchRatio.ToString("0.00"));
                        resultLine.Add("Files match, but fewer bytes");
                    }
                    else if (bytesInMyEMSL > taskStats.TotalBytes)
                    {
                        // Extra bytes
                        resultLine.Add("1");
                        resultLine.Add("Files match, but extra bytes");
                    }
                }
                else if (filesInMyEMSL > taskStats.TotalFiles && bytesInMyEMSL == taskStats.TotalBytes)
                {
                    // Exact match on bytes, but extra files
                    resultLine.Add("1");
                    resultLine.Add("Extra files, but bytes match");
                }
                else if (filesInMyEMSL > taskStats.TotalFiles && bytesInMyEMSL > taskStats.TotalBytes)
                {
                    // Extra bytes and extra files
                    resultLine.Add("1");
                    resultLine.Add("Extra files and extra bytes");
                }
                else
                {
                    var matchRatio = filesInMyEMSL / (double)taskStats.TotalFiles;
                    resultLine.Add(matchRatio.ToString("0.00"));
                    resultLine.Add("Missing files");
                }
            }

            resultsWriter.WriteLine(string.Join("\t", resultLine));
        }

        private void CompareDMSDataToMyEMSL(
            TextWriter resultsWriter,
            IEnumerable<int> datasetIDs,
            IReadOnlyCollection<DMSMetadata> dmsMetadata,
            IReadOnlyCollection<DatasetFolderOrFileInfo> archiveFiles)
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

                        if (statsByFolder.TryGetValue(uploadTask.Subfolder, out var taskStats))
                        {
                            if (taskStats.TotalFiles < uploadTask.FilesAddedOrUpdated)
                                taskStats.TotalFiles = uploadTask.FilesAddedOrUpdated;

                            if (taskStats.TotalBytes < uploadTask.Bytes)
                                taskStats.TotalBytes = uploadTask.Bytes;

                            taskStats.IngestTasks.Add(uploadTask);
                            continue;
                        }

                        statsByFolder.Add(uploadTask.Subfolder, new IngestTaskStats(uploadTask, uploadTask.FilesAddedOrUpdated, uploadTask.Bytes));

                    }

                    var datasetFilesInMyEMSL = (from item in archiveFiles
                                                where item.FileInfo.DatasetID == datasetId && !item.IsFolder
                                                select item.FileInfo).ToList();

                    var filesTrackedByMyEMSL = datasetFilesInMyEMSL.Count;
                    var bytesTrackedInMyEMSL = GetTotalBytes(datasetFilesInMyEMSL);

                    var archiveTask = (from item in datasetUploadTasks
                                       where string.IsNullOrWhiteSpace(item.Subfolder)
                                       select item).FirstOrDefault();

                    var datasetFoundInMyEMSL = false;

                    if (archiveTask != null && archiveTask.DatasetID == datasetId)
                    {
                        // Dataset archive task (empty subfolder)
                        // Save a results entry with the bulk stats

                        var taskStats = statsByFolder[string.Empty];

                        AppendResult(resultsWriter, archiveTask, taskStats, filesTrackedByMyEMSL, bytesTrackedInMyEMSL);
                        datasetFoundInMyEMSL = true;

                        if (filesTrackedByMyEMSL == 0)
                        {
                            // Dataset was not found in MyEMSL
                            // No point in checking subfolders
                            continue;
                        }
                    }

                    foreach (var subfolder in statsByFolder.Keys)
                    {
                        if (string.IsNullOrWhiteSpace(subfolder))
                        {
                            // Dataset archive task; already logged
                            continue;
                        }
                        var taskStats = statsByFolder[subfolder];

                        var subfolderFilesInMyEMSL = (from item in archiveFiles
                                                      where item.FileInfo.DatasetID == datasetId && !item.IsFolder &&
                                                        (item.FileInfo.SubDirPath.Equals(subfolder, StringComparison.OrdinalIgnoreCase) ||
                                                         item.FileInfo.SubDirPath.StartsWith(subfolder + "/", StringComparison.OrdinalIgnoreCase))
                                                      select item.FileInfo).ToList();

                        if (subfolderFilesInMyEMSL.Count == 0)
                        {
                            // Subfolder not found in MyEMSL, it should have been found
                            AppendResult(resultsWriter, taskStats.IngestTasks.First(), taskStats, 0, 0);
                            continue;
                        }

                        datasetFoundInMyEMSL = true;
                        var subfolderBytesTrackedInMyEMSL = GetTotalBytes(subfolderFilesInMyEMSL);

                        AppendResult(resultsWriter, taskStats.IngestTasks.First(), taskStats, subfolderFilesInMyEMSL.Count, subfolderBytesTrackedInMyEMSL);
                    }

                    if (!datasetFoundInMyEMSL)
                    {
                        // No record of this dataset in MyEMSL
                        foreach (var item in statsByFolder)
                        {
                            AppendResult(resultsWriter, item.Value.IngestTasks.First(), item.Value, 0, 0);
                        }

                    }

                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CompareDMSDataToMyEMSL: " + ex.Message, ex);

                return;
            }
        }

        private int GetMaxDatasetIdInMyEMSL(clsDBTools dbTools, bool limitToOldMyEMSL = true)
        {
            try
            {
                var query =
                    " SELECT Max(Dataset_ID)" +
                    " FROM T_MyEMSL_Uploads " +
                    " WHERE Verified > 0";

                if (limitToOldMyEMSL)
                    query += " AND StatusURI_PathID < 265";

                dbTools.GetQueryResults(query, out var queryResults, "LookupHighestDatasetIdInMyEMSL");

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
                    clsDBTools dbTools,
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
                    "Subfolder",
                    "FileCountNew",
                    "FileCountUpdated",
                    "Bytes",
                    "StatusNum",
                    "TransactionID",
                    "Entered"
                };

                var query =
                    " SELECT " + string.Join(", ", columns) +
                    " FROM ( SELECT Entry_ID, Job, Dataset_ID, Subfolder,  " +
                    "              FileCountNew, FileCountUpdated, Bytes, " +
                    "              StatusNum, TransactionID, Entered, " +
                    "              Row_Number() OVER ( PARTITION BY Dataset_ID, Subfolder  " +
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
                    " ORDER BY Dataset_ID, Subfolder, Entry_ID ";

                var columnMap = dbTools.GetColumnMapping(columns);

                dbTools.GetQueryResults(query, out var queryResults, "GetMyEMSLUploadInfoFromDMS");

                var dmsMetadata = new List<DMSMetadata>();

                foreach (var result in queryResults)
                {

                    var entryId = dbTools.GetColumnValue(result, columnMap, "Entry_ID", 0);
                    var job = dbTools.GetColumnValue(result, columnMap, "Job", 0);
                    var datasetId = dbTools.GetColumnValue(result, columnMap, "Dataset_ID", 0);
                    var subfolder = dbTools.GetColumnValue(result, columnMap, "Subfolder");
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
                        Subfolder = subfolder,
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
        /// Validate metadata in MyEMSL
        /// </summary>
        /// <returns></returns>
        public bool ValidateMyEMSLMetadata()
        {
            var datasetIdStart = Options.DatasetIdStart;

            try
            {

                var outputFolder = new DirectoryInfo(Options.OutputFolderPath);
                if (!outputFolder.Exists)
                {
                    OnStatusEvent("Creating folder " + outputFolder.FullName);
                    outputFolder.Create();
                }

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

                var dbTools = new clsDBTools(ValidatorOptions.DMS_CONNECTION_STRING);

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
                            "Subfolder",
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

                    if (Options.DatasetIdEnd == 0)
                        finalDatasetId = maxDatasetId;
                    else
                        finalDatasetId = Math.Min(Options.DatasetIdEnd, maxDatasetId);

                    var totalDatasetsToProcess = finalDatasetId - datasetIdStart + 1;
                    if (totalDatasetsToProcess < 1)
                        throw new Exception(string.Format(
                            "Total datasets should not be negative; computed {0} using {1} - {2} + 1",
                            totalDatasetsToProcess, finalDatasetId,
                            datasetIdStart));

                    while (datasetIdStart <= finalDatasetId)
                    {
                        var basePercentComplete = (datasetIdStart - Options.DatasetIdStart) * 100F / totalDatasetsToProcess;

                        var datasetIdEnd = Math.Min(finalDatasetId, datasetIdStart + Options.DMSLookupBatchSize - 1);

                        var success = ValidateDatasetBatch(dbTools, resultsWriter, datasetIdStart, datasetIdEnd, totalDatasetsToProcess, basePercentComplete);

                        if (!success)
                            return false;

                        datasetIdStart += Options.DMSLookupBatchSize;

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
            clsDBTools dbTools,
            TextWriter resultsWriter,
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

                OnProgressUpdate(string.Format(
                    "Examining {0} uploads for Dataset IDs {1} to {2}",
                    dmsMetadata.Count, datasetIdStart, datasetIdEnd), basePercentComplete);

                var datasetListInfo = new DatasetListInfoByID();
                var lastComparisonTime = DateTime.UtcNow;
                var lastProgressTime = DateTime.UtcNow;
                var itemsProcessed = 0;

                foreach (var item in dmsMetadata)
                {
                    datasetListInfo.AddDataset(item.DatasetID);

                    if (datasetListInfo.DatasetIDs.Count < Options.MyEMSLReaderBatchSize)
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

                        CompareDMSDataToMyEMSL(resultsWriter, datasetListInfo.DatasetIDs, dmsMetadata, archiveFiles);
                        lastComparisonTime = DateTime.UtcNow;
                    }

                    datasetListInfo.Clear();

                    itemsProcessed++;
                    if (DateTime.UtcNow.Subtract(lastProgressTime).TotalSeconds > 15)
                    {
                        lastProgressTime = DateTime.UtcNow;

                        var subtaskPercentComplete = itemsProcessed * 100F / dmsMetadata.Count;

                        var percentComplete = basePercentComplete +
                            subtaskPercentComplete * Options.DMSLookupBatchSize / totalDatasetsToProcess;

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
