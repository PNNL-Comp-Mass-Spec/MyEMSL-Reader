﻿using Pacifica.Core;
using Pacifica.DataUpload;
using PRISM;
using PRISMDatabaseUtils;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Pacifica.Json;

namespace Pacifica.DMSDataUpload
{
    /// <summary>
    /// DMS metadata object
    /// </summary>
    public class DMSMetadataObject : EventNotifier
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: ctime, EUS, fileinfo, gzipped, hashsum, hashtype, Json, keyvalue, mtime, ncbi, Pacifica, ssl, subdir, subfolder

        // ReSharper restore CommentTypo

        // ReSharper disable UseCollectionExpression

        /// <summary>
        /// If a dataset archive task involves more 15 GB of data, only archive the root directory and the QC directory
        /// Use property SkippedDatasetArchiveSubdirectories to view the skipped subdirectory names
        /// </summary>
        private const int LARGE_DATASET_ARCHIVE_THRESHOLD_GB = 15;

        /// <summary>
        /// Maximum number of files to archive
        /// </summary>
        /// <remarks>
        /// If uploading an entire dataset directory and all of its subdirectories via a DatasetArchive operation,
        ///   this value applies to all files in the dataset directory (and subdirectories)
        /// If uploading just one dataset subdirectory via an ArchiveUpdate operation,
        ///   this value applies to all files in that subdirectory
        /// </remarks>
        public const int MAX_FILES_TO_ARCHIVE = 500;

        /// <summary>
        /// Error message thrown when the dataset directory is not found
        /// </summary>
        public const string SOURCE_DIRECTORY_NOT_FOUND = "Source directory not found";

        /// <summary>
        /// Error message thrown when the dataset directory has too many files to archive
        /// </summary>
        public static readonly string TOO_MANY_FILES_TO_ARCHIVE = "Source directory has over " + MAX_FILES_TO_ARCHIVE + " files";

        /// <summary>
        /// Error message thrown when the dataset instrument operator does not have an EUS person ID
        /// </summary>
        public const string UNDEFINED_EUS_OPERATOR_ID = "Operator does not have an EUS person ID in DMS";

        /// <summary>
        /// URL of the EUS website
        /// </summary>
        /// <remarks>
        /// Previous URLs:
        ///   https://nexus.emsl.pnl.gov/Portal
        ///   https://eusi.emsl.pnnl.gov/Portal/
        /// </remarks>
        public const string EUS_PORTAL_URL = "https://nexus.emsl.pnnl.gov/Portal/user_info";

        /// <summary>
        /// Text to show when reporting progress
        /// </summary>
        public const string HASHING_FILES = "Hashing files";

        /// <summary>
        /// List of remote files that were found using CacheInfo files
        /// </summary>
        private readonly List<string> mRemoteCacheInfoFilesToRetrieve;

        /// <summary>
        /// Keys in this dictionary are lock directory share paths (for example \\proto-6\DMS_LockFiles)
        /// Values are the corresponding lock file info object
        /// </summary>
        private readonly Dictionary<string, FileInfo> mRemoteCacheInfoLockFiles;

        private readonly FileTools mFileTools;

        private readonly Configuration mPacificaConfig;

        /// <summary>
        /// Event raised when a zero byte file is encountered
        /// </summary>
        public event ZeroByteFileEventEventHandler ZeroByteFileEvent;

        /// <summary>
        /// Archive modes
        /// </summary>
        public enum ArchiveModes
        {
            /// <summary>
            /// Archive files (initial push of a dataset's files into MyEMSL)
            /// </summary>
            archive,

            /// <summary>
            /// Add/update a dataset's files
            /// </summary>
            update
        }

        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName { get; private set; }

        /// <summary>
        /// EUS Info
        /// </summary>
        public Upload.EUSInfo EUSInfo { get; private set; }

        /// <summary>
        /// Job number (in the DMS_Capture database)
        /// </summary>
        /// <remarks>0 if no job number</remarks>
        public int JobNumber { get; }

        /// <summary>
        /// When true, allow over 500 files to be pushed to MyEMSL
        /// </summary>
        public bool IgnoreMaxFileLimit { get; set; }

        /// <summary>
        /// when True, allow the ingest step to proceed, even if MyEMSL is tracking fewer files than expected for a given dataset
        /// </summary>
        public bool IgnoreMyEMSLFileTrackingError { get; set; }

        /// <summary>
        /// Manager name
        /// </summary>
        public string ManagerName { get; }

        /// <summary>
        /// Object that tracks the upload details, including the files to upload
        /// </summary>
        /// <remarks>
        /// The objects in this list are translated to a single JSON string
        /// </remarks>
        public List<IUploadMetadata> MetadataObject { get; private set; }

        /// <summary>
        /// Subdirectory names that were skipped during a DatasetArchive task because we're pushing more than 15 GB of data
        /// </summary>
        public List<string> SkippedDatasetArchiveSubdirectories { get; }

        /// <summary>
        /// Number of bytes to upload
        /// </summary>
        public long TotalFileSizeToUpload { get; set; }

        /// <summary>
        /// Number of new files pushed to MyEMSL
        /// </summary>
        public int TotalFileCountNew { get; set; }

        /// <summary>
        /// Number of files updated in MyEMSL
        /// </summary>
        public int TotalFileCountUpdated { get; set; }

        /// <summary>
        /// True to enable trace mode
        /// </summary>
        public bool TraceMode { get; set; }

        /// <summary>
        /// True to use the Test instance
        /// </summary>
        public bool UseTestInstance { get; set; }

        /// <summary>
        /// Retrieve the metadata JSON as a string
        /// </summary>
        public string MetadataObjectJSON => JsonTools.UploadMetadataToJson(MetadataObject);

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config"></param>
        /// <param name="managerName"></param>
        /// <param name="jobNumber"></param>
        /// <param name="fileTools"></param>
        public DMSMetadataObject(Configuration config, string managerName, int jobNumber, FileTools fileTools)
        {
            mPacificaConfig = config;

            mRemoteCacheInfoFilesToRetrieve = new List<string>();
            mRemoteCacheInfoLockFiles = new Dictionary<string, FileInfo>(StringComparer.OrdinalIgnoreCase);

            if (string.IsNullOrWhiteSpace(managerName))
            {
                managerName = "DMSMetadataObject";
            }

            ManagerName = managerName;

            JobNumber = jobNumber;

            mFileTools = fileTools;

            mFileTools.WaitingForLockQueue += FileTools_WaitingForLockQueue;

            SkippedDatasetArchiveSubdirectories = new List<string>();
        }

        /// <summary>
        /// Construct the metadata that will be included with the ingested data
        /// </summary>
        /// <param name="taskParams"></param>
        /// <param name="mgrParams"></param>
        /// <param name="criticalError">Output: set to true if the job should be failed</param>
        /// <param name="criticalErrorMessage">Output: explanation of the critical error</param>
        /// <returns>True if success, otherwise false</returns>
        public bool SetupMetadata(
            Dictionary<string, string> taskParams,
            Dictionary<string, string> mgrParams,
            out bool criticalError,
            out string criticalErrorMessage)
        {
            DatasetName = Utilities.GetDictionaryValue(taskParams, "Dataset", "Unknown_Dataset");

            var datasetFilesToArchive = FindDatasetFilesToArchive(taskParams, mgrParams, out var uploadMetadata);

            // DMS5 database
            mgrParams.TryGetValue("DefaultDMSConnString", out var dmsConnectionString);

            // DMS_Capture database
            mgrParams.TryGetValue("ConnectionString", out var captureDbConnectionString);

            taskParams.TryGetValue("Dataset_ID", out var datasetID);

            var dmsConnectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, "MyEMSLReader");
            var captureDbConnectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(captureDbConnectionString, "MyEMSLReader");

            var supplementalDataSuccess = GetSupplementalDMSMetadata(dmsConnectionStringToUse, datasetID, uploadMetadata);

            if (!supplementalDataSuccess)
            {
                criticalError = false;
                criticalErrorMessage = string.Empty;
                return false;
            }

            // Calculate the "year_quarter" code used for subdirectories within an instrument directory
            // This value is based on the date the dataset was created in DMS
            uploadMetadata.DateCodeString = GetDatasetYearQuarter(taskParams);

            // Find the files that are new or need to be updated
            var unmatchedFiles = CompareDatasetContentsWithMyEMSLMetadata(
                captureDbConnectionStringToUse,
                datasetFilesToArchive,
                uploadMetadata,
                out criticalError,
                out criticalErrorMessage);

            if (criticalError)
            {
                return false;
            }

            MetadataObject = Upload.CreatePacificaMetadataObject(uploadMetadata, unmatchedFiles, out var eusInfo);

            if (unmatchedFiles.Count > 0)
            {
                var jsonMetadata = JsonTools.UploadMetadataToJson(MetadataObject, 1);

                if (!CheckMetadataValidity(jsonMetadata, out var policyError))
                {
                    if (policyError)
                    {
                        criticalError = true;
                        criticalErrorMessage = string.Format(
                            "Policy validation error, e.g. invalid EUS Project ID or unrecognized instrument operator; " +
                            "metadata has Instrument ID {0}, Project ID {1}, and Uploader ID {2}",
                            eusInfo.EUSInstrumentID, eusInfo.EUSProjectID, eusInfo.EUSUploaderID);
                    }

                    return false;
                }
            }

            var metadataDescription = Upload.GetMetadataObjectDescription(MetadataObject);
            OnDebugEvent(metadataDescription);

            EUSInfo = eusInfo;
            return true;
        }

        private bool GetSupplementalDMSMetadata(
            string dmsConnectionString,
            string datasetID,
            Upload.UploadMetadata uploadMetadata,
            int retryCount = 3)
        {
            var queryString = "SELECT " +
                              "\"omics.dms.dataset_id\", \"omics.dms.dataset_name\", \"omics.dms.experiment_id\", " +
                              "\"omics.dms.experiment_name\", \"omics.dms.campaign_id\", \"omics.dms.campaign_name\", " +
                              "\"omics.dms.organism_id\", organism_name, ncbi_taxonomy_id, \"omics.dms.acquisition_time\", " +
                              "\"omics.dms.acquisition_length_min\", \"omics.dms.number_of_scans\", \"omics.dms.separation_type\", " +
                              "\"omics.dms.dataset_type\", \"omics.dms.requested_run_id\" " +
                              "FROM V_MyEMSL_Supplemental_Metadata " +
                              "WHERE \"omics.dms.dataset_id\" = " + datasetID;

            var dbTools = DbToolsFactory.GetDBTools(dmsConnectionString, debugMode: TraceMode);
            RegisterEvents(dbTools);

            var success = dbTools.GetQueryResultsDataTable(queryString, out var table, retryCount);

            foreach (DataRow row in table.Rows)
            {
                uploadMetadata.CampaignID = row["omics.dms.campaign_id"].CastDBVal(0);
                uploadMetadata.CampaignName = row["omics.dms.campaign_name"].CastDBVal(string.Empty);
                uploadMetadata.ExperimentID = row["omics.dms.experiment_id"].CastDBVal(0);
                uploadMetadata.ExperimentName = row["omics.dms.experiment_name"].CastDBVal(string.Empty);
                uploadMetadata.OrganismName = row["organism_name"].CastDBVal(string.Empty);
                uploadMetadata.NCBITaxonomyID = row["ncbi_taxonomy_id"].CastDBVal(0);
                uploadMetadata.OrganismID = row["omics.dms.organism_id"].CastDBVal(0);
                uploadMetadata.AcquisitionTime = row["omics.dms.acquisition_time"].CastDBVal(string.Empty);
                uploadMetadata.AcquisitionLengthMin = row["omics.dms.acquisition_length_min"].CastDBVal(0);
                uploadMetadata.NumberOfScans = row["omics.dms.number_of_scans"].CastDBVal(0);
                uploadMetadata.SeparationType = row["omics.dms.separation_type"].CastDBVal(string.Empty);
                uploadMetadata.DatasetType = row["omics.dms.dataset_type"].CastDBVal(string.Empty);
                uploadMetadata.RequestedRunID = row["omics.dms.requested_run_id"].CastDBVal(0);
            }

            if (success)
            {
                uploadMetadata.UserOfRecordList = GetRequestedRunUsers(dbTools, uploadMetadata.RequestedRunID);
            }

            return success;
        }

        private static List<int> GetRequestedRunUsers(IDBTools dbTools, int requestedRunID, int retryCount = 3)
        {
            var queryString = "SELECT eus_person_id FROM V_Requested_Run_EUS_Users_Export WHERE request_id = " + requestedRunID;

            dbTools.GetQueryResultsDataTable(queryString, out var table, retryCount);

            var personList = new List<int>();

            foreach (DataRow row in table.Rows)
            {
                var personId = row["eus_person_id"].CastDBVal(-1);

                if (personId > -1)
                {
                    personList.Add(personId);
                }
            }

            return personList;
        }

        private bool CheckMetadataValidity(string jsonMetadata, out bool policyError)
        {
            var policyURL = mPacificaConfig.PolicyServerUri + "/ingest";

            try
            {
                if (!ValidateCertFile("CheckMetadataValidity"))
                {
                    policyError = false;
                    return false;
                }

                if (TraceMode)
                {
                    OnDebugEvent("Contacting " + policyURL);
                }

                var response = EasyHttp.SendViaThreadStart(
                    mPacificaConfig, policyURL, null,
                    out var responseStatusCode,
                    jsonMetadata,
                    EasyHttp.HttpMethod.Post, 100, "application/json"); // TODO: should this timeout be shorter?

                if ((int)responseStatusCode == 200 && response.IndexOf("success", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    if (TraceMode)
                    {
                        OnDebugEvent("Response received " + response);
                    }

                    policyError = false;
                    return true;
                }

                if (response.Contains("request was aborted"))
                {
                    OnErrorEvent("Metadata validation error: the request was aborted");
                    policyError = false;
                }
                else if ((int)responseStatusCode == 412 || response.IndexOf("Precondition failed", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    // ReSharper disable once CommentTypo

                    // JSON response will look like '{ "message": "Precondition Failed: Invalid values for submitter, project, instrument", "postdata": "", "status": "412 Precondition Failed",...}'
                    // TODO: parse out the 'message' and report that?
                    OnErrorEvent("Policy server reports that metadata is not valid: " + policyURL);
                    policyError = true;
                }
                else
                {
                    OnErrorEvent("Error communicating with Policy server: " + policyURL);
                    policyError = false;
                }

                if (jsonMetadata.Length < 1255)
                {
                    OnDebugEvent(jsonMetadata);
                }
                else
                {
                    OnDebugEvent(jsonMetadata.Substring(0, 1250) + " ...");
                }

                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CheckMetadataValidity: " + ex.Message, ex);
                policyError = false;
                return false;
            }
        }

        private bool AddUsingCacheInfoFile(
            FileInfo cacheInfoFile,
            // ReSharper disable once SuggestBaseTypeForParameter
            List<FileInfoObject> fileCollection,
            string baseDSPath,
            out string remoteFilePath,
            out bool remoteFileNotFound)
        {
            remoteFilePath = string.Empty;

            using (var infoFileReader = new StreamReader(new FileStream(cacheInfoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                if (!infoFileReader.EndOfStream)
                {
                    remoteFilePath = infoFileReader.ReadLine();
                }
            }

            if (string.IsNullOrWhiteSpace(remoteFilePath))
            {
                OnErrorEvent("Warning: Cache info file did not contain a file path; see " + cacheInfoFile.FullName);
                remoteFileNotFound = true;
                return false;
            }

            var remoteFile = new FileInfo(remoteFilePath);

            if (!remoteFile.Exists)
            {
                // This is not a fatal error; the file may have been purged
                OnWarningEvent("Note: Remote file referred to by the cache info file was not found: " + remoteFile.FullName);
                remoteFileNotFound = true;
                return false;
            }

            if (cacheInfoFile.Directory == null)
            {
                OnErrorEvent("Unable to determine the parent directory of the cache info file (this should never happen)");
                remoteFileNotFound = true;
                return false;
            }

            var relativeDestinationDirectory = FileInfoObject.GenerateRelativePath(cacheInfoFile.Directory.FullName, baseDSPath);

            // This constructor will auto-compute the SHA-1 hash value for the file
            var fio = new FileInfoObject(remoteFile, relativeDestinationDirectory, sha1Hash: string.Empty);
            fileCollection.Add(fio);

            remoteFileNotFound = false;
            return true;
        }

        /// <summary>
        /// Convert a file size in bytes to gigabytes
        /// </summary>
        /// <param name="sizeBytes">File size, in bytes</param>
        private static double BytesToGB(long sizeBytes)
        {
            return sizeBytes / 1024.0 / 1024 / 1024;
        }

        /// <summary>
        /// Find the files in the path to be archived
        /// </summary>
        /// <param name="archiveMode">Archive for the initial archive of a dataset, or update for updating a specific subdirectory</param>
        /// <param name="pathToBeArchived">Directory path to be archived</param>
        /// <param name="baseDSPath">Base dataset directory path</param>
        /// <param name="recurse">True to find files in all subdirectories</param>
        /// <returns>List of files to be archived</returns>
        private List<FileInfoObject> CollectFileInformation(
            ArchiveModes archiveMode,
            string pathToBeArchived,
            string baseDSPath,
            bool recurse
        )
        {
            var fileCollection = new List<FileInfoObject>();

            var sourceDirectory = new DirectoryInfo(pathToBeArchived);

            if (!sourceDirectory.Exists)
            {
                throw new DirectoryNotFoundException(
                    string.Format("{0}: {1} (CollectFileInformation)", SOURCE_DIRECTORY_NOT_FOUND, sourceDirectory));
            }

            var searchOption = recurse ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

            var fileList = sourceDirectory.GetFiles("*", searchOption).ToList();

            if (fileList.Count >= MAX_FILES_TO_ARCHIVE)
            {
                if (IgnoreMaxFileLimit)
                {
                    OnWarningEvent("Uploading a large number of files to the archive: {0}", fileList.Count);
                }
                else
                {
                    throw new ArgumentOutOfRangeException(
                        TOO_MANY_FILES_TO_ARCHIVE + "; files must be zipped before upload to MyEMSL (CollectFileInformation)");
                }
            }

            var fractionCompleted = 0f;

            // Determine the amount of data to be pushed
            long totalFileSize = 0;

            foreach (var fi in fileList)
            {
                totalFileSize += fi.Length;
            }

            SkippedDatasetArchiveSubdirectories.Clear();

            if (archiveMode == ArchiveModes.archive && recurse && BytesToGB(totalFileSize) > LARGE_DATASET_ARCHIVE_THRESHOLD_GB)
            {
                // Dataset archive task pushing more than 15 GB of data
                // If the dataset has subdirectories, only push the QC subdirectory at this time
                // Create separate ArchiveUpdate jobs for the other directories

                var subDirectories = sourceDirectory.GetDirectories();

                // Clear the list of files, then add dataset files in the dataset directory
                fileList.Clear();
                fileList.AddRange(sourceDirectory.GetFiles("*", SearchOption.TopDirectoryOnly).ToList());

                // Step through the subdirectories
                foreach (var subDirectory in subDirectories)
                {
                    if (string.Equals(subDirectory.Name, "QC", StringComparison.OrdinalIgnoreCase))
                    {
                        fileList.AddRange(subDirectory.GetFiles("*", SearchOption.AllDirectories).ToList());
                    }
                    else
                    {
                        SkippedDatasetArchiveSubdirectories.Add(subDirectory.Name);
                    }
                }

                // Recompute the total file size
                totalFileSize = 0;

                foreach (var fi in fileList)
                {
                    totalFileSize += fi.Length;
                }
            }

            mRemoteCacheInfoFilesToRetrieve.Clear();
            mRemoteCacheInfoLockFiles.Clear();

            long runningFileSize = 0;

            foreach (var dataFile in fileList)
            {
                if (IgnoreFile(dataFile))
                {
                    continue;
                }

                runningFileSize += dataFile.Length;

                if (totalFileSize > 0)
                {
                    fractionCompleted = runningFileSize / (float)totalFileSize;
                }

                ReportProgress(HASHING_FILES + ": " + dataFile.Name, fractionCompleted * 100);

                // This constructor will auto-compute the SHA-1 hash value for the file
                var fio = new FileInfoObject(dataFile, baseDSPath);
                fileCollection.Add(fio);

                if (fio.FileName.EndsWith("_CacheInfo.txt"))
                {
                    // This is a cache info file that likely points to a .mzXML or .mzML file (possibly gzipped)
                    // Auto-include that file in the .tar to be uploaded

                    var success = AddUsingCacheInfoFile(dataFile, fileCollection, baseDSPath, out var remoteFilePath, out var remoteFileNotFound);

                    if (!success)
                    {
                        if (remoteFileNotFound)
                            continue;

                        throw new Exception(
                            string.Format("AddUsingCacheInfoFile reports false for {0} (CollectFileInformation)", dataFile.FullName));
                    }

                    mRemoteCacheInfoFilesToRetrieve.Add(remoteFilePath);
                }

                if (TraceMode)
                {
                    OnDebugEvent("{0}, {1:F1}% complete: {2}", HASHING_FILES, fractionCompleted * 100, dataFile.Name);
                }
            }

            ReportProgress(100);

            return fileCollection;
        }

        /// <summary>
        /// Query server for files and hash codes
        /// </summary>
        /// <param name="captureDbConnectionString">DMS_Capture connection string</param>
        /// <param name="candidateFilesToUpload">List of local files</param>
        /// <param name="uploadMetadata">Upload metadata</param>
        /// <param name="criticalError">Output: set to true if the job should be failed</param>
        /// <param name="criticalErrorMessage">Output: explanation of the critical error</param>
        /// <returns>List of files that need to be uploaded</returns>
        private List<FileInfoObject> CompareDatasetContentsWithMyEMSLMetadata(
            string captureDbConnectionString,
            IEnumerable<FileInfoObject> candidateFilesToUpload,
            Upload.UploadMetadata uploadMetadata,
            out bool criticalError,
            out string criticalErrorMessage)
        {
            TotalFileCountNew = 0;
            TotalFileCountUpdated = 0;
            TotalFileSizeToUpload = 0;

            var currentTask = "Looking for existing files in MyEMSL for DatasetID " + uploadMetadata.DatasetID;

            if (!string.IsNullOrWhiteSpace(uploadMetadata.SubFolder))
            {
                currentTask += ", subdirectory " + uploadMetadata.SubFolder;
            }

            OnStatusEvent(currentTask);

            var datasetID = uploadMetadata.DatasetID;

            // Keys in dictionary remoteFiles are relative file paths (Unix style paths) and values are file details
            // A given remote file could have multiple hash values if multiple versions of the file have been uploaded
            var remoteFiles = GetDatasetFilesInMyEMSL(datasetID);

            if (remoteFiles == null)
            {
                criticalErrorMessage = string.Format(
                    "Aborting upload since GetDatasetFilesInMyEMSL returned null, implying {0} did not return a valid response",
                    mPacificaConfig.MetadataServerUri);

                OnWarningEvent(criticalErrorMessage);
                criticalError = true;
                return new List<FileInfoObject>();
            }

            // Make sure that the number of files reported by MyEMSL for this dataset agrees with what we expect
            var expectedRemoteFileCount = GetDatasetFileCountExpectedInMyEMSL(captureDbConnectionString, datasetID);

            if (expectedRemoteFileCount < 0)
            {
                criticalErrorMessage = "Aborting upload since GetDatasetFileCountExpectedInMyEMSL returned -1";
                OnErrorEvent(criticalErrorMessage);
                criticalError = true;
                return new List<FileInfoObject>();
            }

            double matchTolerance;

            if (expectedRemoteFileCount < 10)
            {
                matchTolerance = 0.7;
            }
            else if (expectedRemoteFileCount < 20)
            {
                matchTolerance = 0.6;
            }
            else if (expectedRemoteFileCount < 40)
            {
                matchTolerance = 0.5;
            }
            else if (expectedRemoteFileCount < 80)
            {
                matchTolerance = 0.4;
            }
            else
            {
                matchTolerance = 0.25;
            }

            if (expectedRemoteFileCount > 0 &&
                remoteFiles.Count < expectedRemoteFileCount * matchTolerance)
            {
                if (IgnoreMyEMSLFileTrackingError)
                {
                    OnWarningEvent("MyEMSL reported {0} files for Dataset ID {1}; it should be tracking at least {2} files; " +
                                   "ignoring because job parameter IgnoreMyEMSLFileTrackingError is True", remoteFiles.Count, datasetID, expectedRemoteFileCount);
                }
                else
                {
                    var addUpdateTaskParam =
                        string.Format(
                            "Call cap.add_update_task_parameter (" +
                              "_job => {0}, " +
                              "_section => 'JobParameters', " +
                              "_paramName => 'IgnoreMyEMSLFileTrackingError', " +
                              "_value => 'True');",
                            JobNumber);

                    criticalErrorMessage = string.Format("MyEMSL reported {0} files for Dataset ID {1}; it should be tracking at least {2} files; " +
                                                         "to ignore this message, use {3}",
                                                         remoteFiles.Count, datasetID, expectedRemoteFileCount, addUpdateTaskParam);

                    OnErrorEvent(criticalErrorMessage);

                    criticalError = true;
                    return new List<FileInfoObject>();
                }
            }

            // Compare the files in remoteFileInfoList to those in candidateFilesToUpload
            // Note that two files in the same directory could have the same hash value, so we cannot simply compare file hashes

            var missingFiles = new List<FileInfoObject>();

            foreach (var fileObj in candidateFilesToUpload)
            {
                var relativeFilePath = PathUtils.CombineLinuxPaths(fileObj.RelativeDestinationDirectory, fileObj.FileName);

                if (remoteFiles.TryGetValue(relativeFilePath, out var fileVersions))
                {
                    if (FileHashExists(fileVersions, fileObj.Sha1HashHex))
                    {
                        // File found
                        continue;
                    }

                    TotalFileCountUpdated++;
                }
                else
                {
                    TotalFileCountNew++;
                }

                missingFiles.Add(fileObj);

                TotalFileSizeToUpload += fileObj.FileSizeInBytes;
            }

            criticalErrorMessage = string.Empty;
            criticalError = false;
            return missingFiles;
        }

        /// <summary>
        /// Create lock files for the files in mRemoteCacheInfoFilesToRetrieve
        /// </summary>
        public void CreateLockFiles()
        {
            const int MAX_LOCKFILE_WAIT_TIME_MINUTES = 20;

            if (mRemoteCacheInfoFilesToRetrieve.Count == 0)
            {
                return;
            }

            mRemoteCacheInfoLockFiles.Clear();

            foreach (var remoteFilePath in mRemoteCacheInfoFilesToRetrieve)
            {
                // Construct a list of the first file required from each distinct server
                var sourceFile = new FileInfo(remoteFilePath);
                var lockDirectoryPathSource = FileTools.GetLockDirectory(sourceFile);

                if (string.IsNullOrWhiteSpace(lockDirectoryPathSource))
                {
                    continue;
                }

                if (mRemoteCacheInfoLockFiles.ContainsKey(lockDirectoryPathSource))
                {
                    continue;
                }

                var sourceFileSizeMB = sourceFile.Length / 1024.0 / 1024.0;

                if (sourceFileSizeMB < FileTools.LOCKFILE_MINIMUM_SOURCE_FILE_SIZE_MB)
                {
                    // Do not use a lock file for this remote file
                    continue;
                }

                var lockFileTimestamp = FileTools.GetLockFileTimeStamp();

                var lockDirectorySource = new DirectoryInfo(lockDirectoryPathSource);

                var targetFilePath = Path.Combine(@"\\MyEMSL\", DatasetName, sourceFile.Name);

                var lockFilePathSource = mFileTools.CreateLockFile(lockDirectorySource, lockFileTimestamp, sourceFile, targetFilePath, ManagerName);

                if (string.IsNullOrEmpty(lockFilePathSource))
                {
                    // Do not use a lock file for this remote file
                    continue;
                }

                mRemoteCacheInfoLockFiles.Add(lockDirectoryPathSource, new FileInfo(lockFilePathSource));

                mFileTools.WaitForLockFileQueue(lockFileTimestamp, lockDirectorySource, sourceFile, MAX_LOCKFILE_WAIT_TIME_MINUTES);
            }
        }

        /// <summary>
        /// Delete lock files tracked by mRemoteCacheInfoLockFiles
        /// </summary>
        public void DeleteLockFiles()
        {
            if (mRemoteCacheInfoLockFiles.Count == 0)
            {
                return;
            }

            foreach (var remoteLockFile in mRemoteCacheInfoLockFiles)
            {
                try
                {
                    if (remoteLockFile.Value.Exists)
                    {
                        remoteLockFile.Value.Delete();
                    }
                }
                // ReSharper disable once EmptyGeneralCatchClause
#pragma warning disable RCS1075
                catch (Exception)
#pragma warning restore RCS1075
                {
                    // Ignore errors here
                }
            }
        }

        /// <summary>
        /// Return true if fileVersions has a file with the given hash
        /// </summary>
        /// <param name="fileVersions">List of files in MyEMSL</param>
        /// <param name="fileHash">SHA-1 hash to find</param>
        /// <returns>True if a match is found, otherwise false</returns>
        private static bool FileHashExists(IEnumerable<MyEMSLFileInfo> fileVersions, string fileHash)
        {
            return (from item in fileVersions where string.Equals(item.HashSum, fileHash) select item).Any();
        }

        /// <summary>
        /// Find new or updated files to archive
        /// </summary>
        /// <param name="taskParams"></param>
        /// <param name="mgrParams"></param>
        /// <param name="uploadMetadata"></param>
        public List<FileInfoObject> FindDatasetFilesToArchive(
            Dictionary<string, string> taskParams,
            Dictionary<string, string> mgrParams,
            out Upload.UploadMetadata uploadMetadata)
        {
            uploadMetadata = new Upload.UploadMetadata();
            uploadMetadata.Clear();

            // Translate values from task/mgr params into usable variables
            var perspective = Utilities.GetDictionaryValue(mgrParams, "perspective", "client");
            string driveLocation;

            // Determine the drive location based on perspective
            // (client perspective means running on a Proto storage server; server perspective means running on another computer)
            if (perspective == "client")
            {
                driveLocation = Utilities.GetDictionaryValue(taskParams, "Storage_Vol_External", string.Empty);
            }
            else
            {
                driveLocation = Utilities.GetDictionaryValue(taskParams, "Storage_Vol", string.Empty);
            }

            // Construct the dataset directory path
            var legacyFolderParam = Utilities.GetDictionaryValue(taskParams, "Folder", string.Empty);
            var datasetDirectory = Utilities.GetDictionaryValue(taskParams, "Directory", legacyFolderParam);

            var sourceDirectoryBase = Path.Combine(Utilities.GetDictionaryValue(taskParams, "Storage_Path", string.Empty), datasetDirectory);
            var sourceDirectoryPath = Path.Combine(driveLocation, sourceDirectoryBase);

            uploadMetadata.DatasetName = Utilities.GetDictionaryValue(taskParams, "Dataset", string.Empty);
            uploadMetadata.DMSInstrumentName = Utilities.GetDictionaryValue(taskParams, "Instrument_Name", string.Empty);
            uploadMetadata.DatasetID = Utilities.GetDictionaryValue(taskParams, "Dataset_ID", 0);

            var baseDSPath = sourceDirectoryPath;
            uploadMetadata.SubFolder = string.Empty;

            ArchiveModes archiveMode;

            if (Utilities.GetDictionaryValue(taskParams, "StepTool", string.Empty).Equals("DatasetArchive", StringComparison.OrdinalIgnoreCase))
            {
                archiveMode = ArchiveModes.archive;
            }
            else
            {
                archiveMode = ArchiveModes.update;
            }

            if (archiveMode == ArchiveModes.update)
            {
                var legacyOutputFolderParam = Utilities.GetDictionaryValue(taskParams, "OutputFolderName", string.Empty);
                uploadMetadata.SubFolder = Utilities.GetDictionaryValue(taskParams, "OutputDirectoryName", legacyOutputFolderParam);

                if (string.IsNullOrWhiteSpace(uploadMetadata.SubFolder))
                {
                    uploadMetadata.SubFolder = string.Empty;
                }
                else
                {
                    // Subdirectory is defined; make sure it has the same capitalization as the one on disk
                    var sourceDirectory = new DirectoryInfo(sourceDirectoryPath);

                    if (!sourceDirectory.Exists)
                    {
                        throw new DirectoryNotFoundException(
                            string.Format("Source directory not found: {0} (FindDatasetFilesToArchive)", sourceDirectory));
                    }

                    // Make sure sourceDirectoryPath is capitalized properly
                    var subDirs = sourceDirectory.GetDirectories(uploadMetadata.SubFolder);

                    if (subDirs.Length == 0)
                    {
                        throw new DirectoryNotFoundException(
                            string.Format("Directory {0} not found below {1} (FindDatasetFilesToArchive)", uploadMetadata.SubFolder, sourceDirectory.FullName));
                    }

                    var matchingDirectory = subDirs[0];

                    if (matchingDirectory.Name != uploadMetadata.SubFolder)
                    {
                        // Case mismatch; update uploadMetadata and taskParams
                        uploadMetadata.SubFolder = matchingDirectory.Name;

                        if (taskParams.ContainsKey("OutputFolderName"))
                        {
                            taskParams["OutputFolderName"] = matchingDirectory.Name;
                        }

                        if (taskParams.ContainsKey("OutputDirectoryName"))
                        {
                            taskParams["OutputDirectoryName"] = matchingDirectory.Name;
                        }
                    }

                    sourceDirectoryPath = matchingDirectory.FullName;
                }
            }

            uploadMetadata.EUSInstrumentID = Utilities.GetDictionaryValue(taskParams, "EUS_Instrument_ID", 0);
            uploadMetadata.EUSProjectID = Utilities.GetDictionaryValue(taskParams, "EUS_Proposal_ID", string.Empty);

            var operatorUsername = Utilities.GetDictionaryValue(taskParams, "Operator_Username", "Unknown_Operator");
            uploadMetadata.EUSOperatorID = Utilities.GetDictionaryValue(taskParams, "EUS_Operator_ID", 0);

            if (uploadMetadata.EUSOperatorID == 0)
            {
                if (operatorUsername.Equals("D3L064"))
                {
                    // Very old, pre-EUS user
                    // Use the EUS user ID for D3L243
                    uploadMetadata.EUSOperatorID = Upload.DEFAULT_EUS_OPERATOR_ID;
                }
                else
                {
                    var jobNumber = Utilities.GetDictionaryValue(taskParams, "Job", string.Empty);

                    var errorMessage =
                        string.Format(
                            "{0}. {1} needs to login at {2} to be assigned an ID, then DMS needs to update T_EUS_Users (occurs daily via procedure update_eus_users_from_eus_imports). " +
                            "Next, the job parameters must be updated with: Call cap.update_parameters_for_task(_jobList => '{3}');",
                            UNDEFINED_EUS_OPERATOR_ID, operatorUsername, EUS_PORTAL_URL, jobNumber);

                    throw new Exception(errorMessage);
                }
            }

            // Possibly override EUS ID
            // ReSharper disable once StringLiteralTypo
            if (uploadMetadata.EUSOperatorID == 52259 && operatorUsername.EndsWith("SWEN778", StringComparison.OrdinalIgnoreCase))
            {
                // This user has two EUS IDs and MyEMSL only recognizes the first one
                // Override the EUS ID
                uploadMetadata.EUSOperatorID = 45413;
            }

            var recurse = Utilities.GetDictionaryValue(taskParams, MyEMSLUploader.RECURSIVE_UPLOAD, true);

            // Grab file information from this dataset directory
            // This process will also compute the SHA-1 hash value for each file
            return CollectFileInformation(archiveMode, sourceDirectoryPath, baseDSPath, recurse);
        }

        /// <summary>
        /// Query the DMS_Capture database to determine the number of files that MyEMSL should be tracking for this dataset
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="datasetID"></param>
        /// <param name="retryCount">Number of times to try again if the data cannot be retrieved</param>
        /// <returns>Number of files that should be in MyEMSL for this dataset; -1 if an error</returns>
        private int GetDatasetFileCountExpectedInMyEMSL(string connectionString, int datasetID, int retryCount = 3)
        {
            var queryString = string.Format(
                "SELECT SUM(File_Count_New) AS Files " +
                "FROM V_MyEMSL_Uploads " +
                "WHERE Dataset_ID = {0} AND " +
                      "(Verified > 0 AND Ingest_Steps_Completed >= 5 AND Error_Code = 0 OR " +
                      " Ingest_Steps_Completed >= 7)",
                datasetID);

            var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: TraceMode);
            RegisterEvents(dbTools);

            var success = dbTools.GetQueryScalar(queryString, out var result, retryCount);

            if (!success)
            {
                return -1;
            }

            return result.CastDBVal(0);
        }

        /// <summary>
        /// Find files in MyEMSL associated with the given dataset ID
        /// </summary>
        /// <param name="datasetID">Dataset ID</param>
        /// <param name="subDirFilter">Optional subdirectory (subfolder) to filter on</param>
        /// <returns>
        /// Dictionary of files in MyEMSL; keys are relative file paths (Unix style paths) and values are file details
        /// Returns null if MyEMSL does not return a valid response
        /// </returns>
        public Dictionary<string, List<MyEMSLFileInfo>> GetDatasetFilesInMyEMSL(int datasetID, string subDirFilter = "")
        {
            const int DUPLICATE_HASH_MESSAGES_TO_LOG = 5;

            if (!ValidateCertFile("GetDatasetFilesInMyEMSL"))
            {
                return new Dictionary<string, List<MyEMSLFileInfo>>();
            }

            // Example metadata URL:
            // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/265031

            // ReSharper disable StringLiteralTypo
            var metadataURL = mPacificaConfig.MetadataServerUri + "/fileinfo/files_for_keyvalue/omics.dms.dataset_id/" + datasetID;
            // ReSharper restore StringLiteralTypo

            // Note that querying by dataset name only works for datasets ingested after July 1, 2017, i.e.
            // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_name/QC_pp_MCF-7_17_01_B_25JUN17_Frodo_REP-17-06-02
            // vs. https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_name/CPTAC_CompRef_P32_TMT11_17_18Jun17_Samwise_REP-17-05-01
            // vs. https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/595858

            if (TraceMode)
            {
                OnDebugEvent("Contacting " + metadataURL);
            }

            // Retrieve a list of files already in MyEMSL for this dataset
            var fileInfoListJSON = EasyHttp.SendViaThreadStart(mPacificaConfig, metadataURL, out _);

            if (string.IsNullOrEmpty(fileInfoListJSON))
            {
                OnErrorEvent("Empty MyEMSL response in GetDatasetFilesInMyEMSL");
                return null;
            }

            if (EasyHttp.IsResponseError(fileInfoListJSON))
            {
                OnErrorEvent("Error response in GetDatasetFilesInMyEMSL: " + fileInfoListJSON);
                return null;
            }

            if (TraceMode)
            {
                var previewLength = Math.Min(fileInfoListJSON.Length, 75);
                OnDebugEvent("Response received, convert to a dictionary: " + fileInfoListJSON.Substring(0, previewLength));
            }

            if (fileInfoListJSON.StartsWith("(no response,"))
            {
                OnDebugEvent("JsonConvert.Import did not return a valid response for {0}", metadataURL);
                return null;
            }

            // Convert the response to a dictionary
            var remoteFileInfoList = JsonTools.JsonToFileList(fileInfoListJSON, metadataURL, "DMSMetadataObjects.GetDatasetFilesInMyEMSL", out var jsonError);

            if (remoteFileInfoList is null || !string.IsNullOrWhiteSpace(jsonError))
            {
                OnWarningEvent(jsonError);
                return null;
            }

            // Keys in this dictionary are relative file paths (Unix style paths); values are file info details
            // A given remote file could have multiple hash values if multiple versions of the file have been uploaded
            var remoteFiles = new Dictionary<string, List<MyEMSLFileInfo>>();

            var duplicateHashCount = 0;

            // Data uploaded to Pacifica (aka MyEMSL) between '2023-10-31 22:13:00' and '2023-12-19 22:00:00' was inadvertently stored as zero-byte files
            // Any files with a Submission time in that range, are assumed to be invalid (Submission time comes from Created time in the metadata)
            var corruptDataStartTime = new DateTime(2023, 10, 31, 22, 13, 00);
            var corruptDataEndTime = new DateTime(2023, 12, 19, 22, 00,00);

            // Note that two files in the same directory could have the same hash value (but different names),
            // so we cannot simply compare file hashes

            foreach (var fileObj in remoteFileInfoList)
            {
                var fileSubDir = fileObj.SubDir;

                if (!string.IsNullOrWhiteSpace(subDirFilter))
                {
                    if (!string.Equals(subDirFilter, fileSubDir, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }
                }

                var fileName = fileObj.Filename;
                var fileHash = fileObj.HashSum;

                // Unix style path
                var relativeFilePath = PathUtils.CombineLinuxPaths(fileSubDir, fileName);

                if (remoteFiles.TryGetValue(relativeFilePath, out var fileVersions))
                {
                    // Make sure that fileVersions doesn't already have a version of this file with this specific SHA-1 hash
                    // This can happen if the same subdirectory is pushed into MyEMSL twice, and the file modification times have changed
                    // but the file contents have not changed

                    if (FileHashExists(fileVersions, fileHash))
                    {
                        if (string.Equals(fileHash, "none", StringComparison.OrdinalIgnoreCase))
                        {
                            // ReSharper disable once GrammarMistakeInComment

                            // Do not log a warning; just silently ignore it
                            // Example of a dataset with hash values of "None" is test dataset SWT_LCQData_300
                            // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/54007
                            continue;
                        }

                        duplicateHashCount++;

                        if (duplicateHashCount <= DUPLICATE_HASH_MESSAGES_TO_LOG)
                        {
                            // This warning is logged as a debug event since it's not a critical error
                            OnDebugEvent("Remote file listing reports the same file with the same hash more than once; " +
                                         "ignoring duplicate hash {0} for {1}", fileHash, relativeFilePath);
                        }
                        continue;
                    }
                }
                else
                {
                    // Add the file to fileVersions
                    fileVersions = new List<MyEMSLFileInfo>();
                    remoteFiles.Add(relativeFilePath, fileVersions);
                }

                if (fileObj.Created >= corruptDataStartTime && fileObj.Created <= corruptDataEndTime)
                {
                    // Ignore this file
                    continue;
                }

                fileVersions.Add(fileObj);
            }

            if (duplicateHashCount > DUPLICATE_HASH_MESSAGES_TO_LOG)
            {
                OnDebugEvent("Duplicate hash value found for {0} files in MyEMSL", duplicateHashCount);
            }

            return remoteFiles;
        }

        /// <summary>
        /// Get the dataset date-code text, e.g. 2040_4
        /// </summary>
        /// <param name="taskParams"></param>
        /// <returns>Year_Quarter</returns>
        public static string GetDatasetYearQuarter(Dictionary<string, string> taskParams)
        {
            var datasetDate = Utilities.GetDictionaryValue(taskParams, "Created", string.Empty);
            var date_code = DateTime.Parse(datasetDate);
            var yq = date_code.Month / 12.0 * 4.0;
            var yearQuarter = (int)Math.Ceiling(yq);
            return date_code.Year + "_" + yearQuarter;
        }

        /// <summary>
        /// Check whether a file should be ignored when either uploading files or comparing existing files to files already in MyEMSL
        /// </summary>
        /// <param name="dataFile"></param>
        /// <param name="raiseZeroByteFileEvent">When true, raise event ZeroByteFileEvent if the file size is 0 bytes</param>
        /// <returns>True if the file should be ignored</returns>
        public bool IgnoreFile(FileInfo dataFile, bool raiseZeroByteFileEvent = true)
        {
            // ReSharper disable once InvertIf
            if (dataFile.Length == 0)
            {
                // Prior to May 2022, MyEMSL handled zero byte files without issue
                // Something changed, and now the upload task fails if any zero byte files are included in the .tar file

                if (raiseZeroByteFileEvent)
                    OnZeroByteFileEvent(dataFile);

                return true;
            }

            return IgnoreFile(dataFile.Name);
        }

        /// <summary>
        /// Check whether a file should be ignored when either uploading files or comparing existing files to files already in MyEMSL
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="fileSizeBytes"></param>
        /// <param name="raiseZeroByteFileEvent">When true, raise event ZeroByteFileEvent if the file size is 0 bytes</param>
        /// <returns>True if the file should be ignored</returns>
        // ReSharper disable once UnusedMember.Global
        public bool IgnoreFile(string fileName, long fileSizeBytes, bool raiseZeroByteFileEvent = true)
        {
            // ReSharper disable once InvertIf
            if (fileSizeBytes == 0)
            {
                // Prior to May 2022, MyEMSL handled zero byte files without issue
                // Something changed, and now the upload task fails if any zero byte files are included in the .tar file

                if (raiseZeroByteFileEvent)
                    OnZeroByteFileEvent(new FileInfo(fileName));

                return true;
            }

            return IgnoreFile(fileName);
        }

        /// <summary>
        /// Check whether a file should be ignored when either uploading files or comparing existing files to files already in MyEMSL
        /// </summary>
        /// <remarks>
        /// Ignore files .DS_Store and Thumbs.db
        /// Also ignore files that end with .sqlite-journal
        /// </remarks>
        /// <param name="fileName"></param>
        /// <returns>True if the file should be ignored</returns>
        public static bool IgnoreFile(string fileName)
        {
            return fileName.Equals(".DS_Store", StringComparison.OrdinalIgnoreCase) ||
                   fileName.Equals("Thumbs.db", StringComparison.OrdinalIgnoreCase) ||
                   fileName.EndsWith(".sqlite-journal", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Get a list of file names that we can ignore when transferring data to MyEMSL or comparing files on disk to files in MyEMSL
        /// </summary>
        /// <returns>SortedSet of strings (case-insensitive)</returns>
        // ReSharper disable once UnusedMember.Global
        [Obsolete("Use method IgnoreFile to check whether a file should be skipped")]
        // ReSharper disable once UnusedMember.Global
        public static SortedSet<string> GetFilesToIgnore()
        {
            return new SortedSet<string>(StringComparer.InvariantCultureIgnoreCase) {
                ".DS_Store",
                "Thumbs.db",
                ".sqlite-journal"
            };
        }

        private void OnZeroByteFileEvent(FileInfo dataFile)
        {
            var message = string.Format("Skipping zero byte file {0} in {1}", dataFile.Name, dataFile.DirectoryName ?? " dataset");

            if (ZeroByteFileEvent == null)
                OnWarningEvent(message);
            else
                ZeroByteFileEvent(dataFile, message);
        }

        private void ReportProgress(float percentComplete)
        {
            ReportProgress("", percentComplete);
        }

        private void ReportProgress(string currentTask, float percentComplete)
        {
            OnProgressUpdate(currentTask, percentComplete);
        }

        /// <summary>
        /// Validate that the MyEMSL certificate file exists
        /// </summary>
        /// <param name="callingMethod">Calling method</param>
        /// <returns>True if the cert file is found, otherwise false</returns>
        private bool ValidateCertFile(string callingMethod)
        {
            var certificateFilePath = EasyHttp.ResolveCertFile(mPacificaConfig, callingMethod, out var errorMessage);

            if (!string.IsNullOrWhiteSpace(certificateFilePath))
            {
                return true;
            }

            OnErrorEvent(errorMessage);
            return false;
        }

        private void FileTools_WaitingForLockQueue(string sourceFilePath, string targetFilePath, int MBBacklogSource, int MBBacklogTarget)
        {
            ConsoleMsgUtils.ShowDebug("  mFileTools_WaitingForLockQueue for " + sourceFilePath);
        }

        /// <summary>
        /// Event for reporting that a zero byte file was skipped when creating the .tar file
        /// </summary>
        /// <param name="dataFile">File information</param>
        /// <param name="message">Warning message</param>
        public delegate void ZeroByteFileEventEventHandler(FileInfo dataFile, string message);
    }
}
