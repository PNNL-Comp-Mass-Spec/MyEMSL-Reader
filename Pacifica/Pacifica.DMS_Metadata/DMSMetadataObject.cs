using Jayrock.Json.Conversion;
using Pacifica.Core;
using PRISM;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using PRISMDatabaseUtils;
using Uploader = Pacifica.Upload;
using Utilities = Pacifica.Core.Utilities;

namespace Pacifica.DMS_Metadata
{
    public class DMSMetadataObject : EventNotifier
    {
        // Ignore Spelling: Json, ssl, ncbi, subfolder, mtime, ctime, fileinfo, keyvalue, hashsum, subdir, hashtype

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
        public const string EUS_PORTAL_URL = "https://eusi.emsl.pnnl.gov/Portal/";

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

        public enum ArchiveModes
        {
            archive, update
        }

        #region "Properties"

        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName { get; private set; }

        /// <summary>
        /// EUS Info
        /// </summary>
        public Uploader.Upload.EUSInfo EUSInfo { get; private set; }

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
        /// The information in this dictionary is translated to JSON;
        /// Keys are key names; values are either strings or dictionary objects or even a list of dictionary objects
        /// </remarks>
        public List<Dictionary<string, object>> MetadataObject { get; private set; }

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
        public string MetadataObjectJSON => Utilities.ObjectToJson(MetadataObject);

        #endregion

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
            // Could use this to ignore all certificates (not wise)
            // System.Net.ServicePointManager.ServerCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;

            // Instead, only allow certain domains, as defined by ValidateRemoteCertificate
            if (ServicePointManager.ServerCertificateValidationCallback == null)
            {
                ServicePointManager.ServerCertificateValidationCallback += ValidateRemoteCertificate;
            }

            DatasetName = Utilities.GetDictionaryValue(taskParams, "Dataset", "Unknown_Dataset");

            var datasetFilesToArchive = FindDatasetFilesToArchive(taskParams, mgrParams, out var uploadMetadata);

            // DMS5 database
            mgrParams.TryGetValue("DefaultDMSConnString", out var connectionString);

            // DMS_Capture database
            mgrParams.TryGetValue("ConnectionString", out var captureDbConnectionString);

            taskParams.TryGetValue("Dataset_ID", out var datasetID);

            var supplementalDataSuccess = GetSupplementalDMSMetadata(connectionString, datasetID, uploadMetadata);
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
                captureDbConnectionString,
                datasetFilesToArchive,
                uploadMetadata,
                out criticalError,
                out criticalErrorMessage);

            if (criticalError)
            {
                return false;
            }

            MetadataObject = Uploader.Upload.CreatePacificaMetadataObject(uploadMetadata, unmatchedFiles, out var eusInfo);

            if (unmatchedFiles.Count > 0)
            {
                var jsonMetadata = Utilities.ObjectToJson(MetadataObject);
                if (!CheckMetadataValidity(jsonMetadata, out var policyError))
                {
                    if (policyError)
                    {
                        criticalError = true;
                        criticalErrorMessage = "Policy validation error, e.g. invalid EUS Project ID";
                    }

                    return false;
                }
            }

            var metadataDescription = Uploader.Upload.GetMetadataObjectDescription(MetadataObject);
            OnDebugEvent(metadataDescription);

            EUSInfo = eusInfo;
            return true;
        }

        private bool GetSupplementalDMSMetadata(
            string dmsConnectionString,
            string datasetID,
            Uploader.Upload.UploadMetadata uploadMetadata,
            int retryCount = 3)
        {
            var queryString = "SELECT * FROM V_MyEMSL_Supplemental_Metadata WHERE [omics.dms.dataset_id] = " + datasetID;

            var dbTools = DbToolsFactory.GetDBTools(dmsConnectionString, debugMode: TraceMode);
            RegisterEvents(dbTools);

            var success = dbTools.GetQueryResultsDataTable(queryString, out var table, retryCount, 5);

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

        private List<int> GetRequestedRunUsers(IDBTools dbTools, int requestedRunID, int retryCount = 3)
        {
            var queryString = "SELECT EUS_Person_ID FROM V_Requested_Run_EUS_Users_Export WHERE Request_ID = " + requestedRunID;

            dbTools.GetQueryResultsDataTable(queryString, out var table, retryCount, 5);

            var personList = new List<int>();

            foreach (DataRow row in table.Rows)
            {
                var personId = row["EUS_Person_ID"].CastDBVal(-1);
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
                    EasyHttp.HttpMethod.Post, 100, "application/json");

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
                else
                {
                    OnErrorEvent("Policy server reports that metadata is not valid: " + policyURL);
                    policyError = true;
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
            ICollection<FileInfoObject> fileCollection,
            string baseDSPath,
            out string remoteFilePath)
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
                return false;
            }

            var remoteFile = new FileInfo(remoteFilePath);
            if (!remoteFile.Exists)
            {
                // This is not a fatal error; the file may have been purged
                OnDebugEvent("Note: Remote file referred to by the cache info file was not found: " + remoteFile.FullName);
                return false;
            }

            if (cacheInfoFile.Directory == null)
            {
                OnErrorEvent("Unable to determine the parent directory of the cache info file (this should never happen)");
                return false;
            }

            var relativeDestinationDirectory = FileInfoObject.GenerateRelativePath(cacheInfoFile.Directory.FullName, baseDSPath);

            // This constructor will auto-compute the SHA-1 hash value for the file
            var fio = new FileInfoObject(remoteFile.FullName, relativeDestinationDirectory, sha1Hash: string.Empty);
            fileCollection.Add(fio);

            return true;
        }

        /// <summary>
        /// Convert a file size in bytes to gigabytes
        /// </summary>
        /// <param name="sizeBytes"></param>
        /// <returns></returns>
        private static double BytesToGB(long sizeBytes)
        {
            return sizeBytes / 1024.0 / 1024 / 1024;
        }

        /// <summary>
        /// Find all of the files in the path to be archived
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

            var eSearchOption = recurse ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

            var fileList = sourceDirectory.GetFiles("*", eSearchOption).ToList();

            if (fileList.Count >= MAX_FILES_TO_ARCHIVE)
            {
                if (IgnoreMaxFileLimit)
                {
                    OnWarningEvent(string.Format("Uploading a large number of files to the archive: {0}", fileList.Count));
                }
                else
                {
                    throw new ArgumentOutOfRangeException(
                        TOO_MANY_FILES_TO_ARCHIVE + "; files must be zipped before upload to MyEMSL (CollectFileInformation)");
                }
            }

            // Get the file names that we can ignore
            var filesToIgnore = GetFilesToIgnore();

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
                if (filesToIgnore.Contains(dataFile.Name))
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
                var fio = new FileInfoObject(dataFile.FullName, baseDSPath);
                fileCollection.Add(fio);

                if (fio.FileName.EndsWith("_CacheInfo.txt"))
                {
                    // This is a cache info file that likely points to a .mzXML or .mzML file (possibly gzipped)
                    // Auto-include that file in the .tar to be uploaded

                    var success = AddUsingCacheInfoFile(dataFile, fileCollection, baseDSPath, out var remoteFilePath);
                    if (!success)
                    {
                        throw new Exception(
                            string.Format("Error reported by AddUsingCacheInfoFile for {0} (CollectFileInformation)", dataFile.FullName));
                    }

                    mRemoteCacheInfoFilesToRetrieve.Add(remoteFilePath);
                }

                if (TraceMode)
                {
                    OnDebugEvent(string.Format("{0}, {1:F1}% complete: {2}", HASHING_FILES, fractionCompleted * 100, dataFile.Name));
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
            Uploader.Upload.UploadMetadata uploadMetadata,
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
                    OnWarningEvent(
                        string.Format("MyEMSL reported {0} files for Dataset ID {1}; it should be tracking at least {2} files; " +
                                      "ignoring because job parameter IgnoreMyEMSLFileTrackingError is True",
                                      remoteFiles.Count, datasetID, expectedRemoteFileCount));
                }
                else
                {
                    var addUpdateJobParam =
                        string.Format(
                            "exec AddUpdateJobParameter @Job = {0}, " +
                            "@Section = 'JobParameters', " +
                            "@ParamName = 'IgnoreMyEMSLFileTrackingError', " +
                            "@Value = 'True'",
                            JobNumber);

                    criticalErrorMessage = string.Format("MyEMSL reported {0} files for Dataset ID {1}; it should be tracking at least {2} files; " +
                                                         "to ignore this message, use {3}",
                                                         remoteFiles.Count, datasetID, expectedRemoteFileCount, addUpdateJobParam);

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
                var lockDirectoryPathSource = mFileTools.GetLockDirectory(sourceFile);

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

                var lockFileTimestamp = mFileTools.GetLockFileTimeStamp();

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
                catch (Exception)
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
        private bool FileHashExists(IEnumerable<MyEMSLFileInfo> fileVersions, string fileHash)
        {
            return (from item in fileVersions where string.Equals(item.HashSum, fileHash) select item).Any();
        }

        /// <summary>
        /// Find new or updated files to archive
        /// </summary>
        /// <param name="taskParams"></param>
        /// <param name="mgrParams"></param>
        /// <param name="uploadMetadata"></param>
        /// <returns></returns>
        public List<FileInfoObject> FindDatasetFilesToArchive(
            Dictionary<string, string> taskParams,
            Dictionary<string, string> mgrParams,
            out Uploader.Upload.UploadMetadata uploadMetadata)
        {
            uploadMetadata = new Uploader.Upload.UploadMetadata();
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

                    var matchingDirectory = subDirs.First();

                    if (matchingDirectory.Name != uploadMetadata.SubFolder)
                    {
                        // Case mis-match; update uploadMetadata and taskParams
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

            var operatorUsername = Utilities.GetDictionaryValue(taskParams, "Operator_PRN", "Unknown_Operator");
            uploadMetadata.EUSOperatorID = Utilities.GetDictionaryValue(taskParams, "EUS_Operator_ID", 0);

            if (uploadMetadata.EUSOperatorID == 0)
            {
                var jobNumber = Utilities.GetDictionaryValue(taskParams, "Job", string.Empty);

                var errorMessage =
                    UNDEFINED_EUS_OPERATOR_ID + ". " +
                    operatorUsername + " needs to login at " + EUS_PORTAL_URL + " to be assigned an ID, " +
                    "then DMS needs to update T_EUS_Users (occurs daily via UpdateEUSUsersFromEUSImports), then the job parameters must be updated with: EXEC UpdateParametersForJob " + jobNumber;

                throw new Exception(errorMessage);
            }

            var recurse = Utilities.GetDictionaryValue(taskParams, MyEMSLUploader.RECURSIVE_UPLOAD, true);

            // Grab file information from this dataset directory
            // This process will also compute the SHA-1 hash value for each file
            var datasetFilesToArchive = CollectFileInformation(archiveMode, sourceDirectoryPath, baseDSPath, recurse);

            return datasetFilesToArchive;
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
                "SELECT SUM(FileCountNew) AS Files " +
                "FROM V_MyEMSL_Uploads " +
                "WHERE Dataset_ID = {0} AND " +
                      "(Verified > 0 AND Ingest_Steps_Completed >= 5 AND ErrorCode = 0 OR " +
                      " Ingest_Steps_Completed >= 7)",
                datasetID);

            var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: TraceMode);
            RegisterEvents(dbTools);

            var success = dbTools.GetQueryScalar(queryString, out var result, retryCount, 5);
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
                OnDebugEvent(string.Format(
                    "JsonConvert.Import did not return a valid response for {0}", metadataURL));
                return null;
            }

            // Convert the response to a dictionary
            if (!(JsonConvert.Import(fileInfoListJSON) is Jayrock.Json.JsonArray jsa))
            {
                OnWarningEvent(string.Format(
                    "JsonConvert.Import did not return a JsonArray object; data returned from {0} is likely not JSON", metadataURL));
                return null;
            }

            var remoteFileInfoList = Utilities.JsonArrayToDictionaryList(jsa);

            // Keys in this dictionary are relative file paths (Unix style paths); values are file info details
            // A given remote file could have multiple hash values if multiple versions of the file have been uploaded
            var remoteFiles = new Dictionary<string, List<MyEMSLFileInfo>>();

            var duplicateHashCount = 0;

            // Note that two files in the same directory could have the same hash value (but different names),
            // so we cannot simply compare file hashes

            foreach (var fileObj in remoteFileInfoList)
            {
                var fileName = Utilities.GetDictionaryValue(fileObj, "name");
                var fileId = Utilities.GetDictionaryValue(fileObj, "_id", 0);
                var fileHash = Utilities.GetDictionaryValue(fileObj, "hashsum");
                var fileSubDir = Utilities.GetDictionaryValue(fileObj, "subdir");

                if (!string.IsNullOrWhiteSpace(subDirFilter))
                {
                    if (!string.Equals(subDirFilter, fileSubDir, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }
                }

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
                            // Do not log a warning; just silently ignore it
                            // Example of a dataset with hash values of "None" is test dataset SWT_LCQData_300
                            // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/54007
                            continue;
                        }

                        duplicateHashCount++;
                        if (duplicateHashCount <= DUPLICATE_HASH_MESSAGES_TO_LOG)
                        {
                            // This warning is logged as a debug event since it's not a critical error
                            OnDebugEvent(string.Format(
                                "Remote file listing reports the same file with the same hash more than once; " +
                                "ignoring duplicate hash {0} for {1}", fileHash, relativeFilePath));
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

                var remoteFileInfo = new MyEMSLFileInfo(fileName, fileId, fileHash)
                {
                    DatasetYearQuarter = string.Empty,
                    HashType = Utilities.GetDictionaryValue(fileObj, "hashtype"),
                    SubDir = fileSubDir,
                    Size = Utilities.GetDictionaryValue(fileObj, "size", 0),
                    TransactionId = Utilities.GetDictionaryValue(fileObj, "transaction_id", 0)
                };

                var createdInMyEMSL = Utilities.GetDictionaryValue(fileObj, "created");
                var updatedInMyEMSL = Utilities.GetDictionaryValue(fileObj, "updated");
                var deletedInMyEMSL = Utilities.GetDictionaryValue(fileObj, "deleted");

                remoteFileInfo.UpdateRemoteFileTimes(createdInMyEMSL, updatedInMyEMSL, deletedInMyEMSL);

                var creationTime = Utilities.GetDictionaryValue(fileObj, "ctime");
                var lastWriteTime = Utilities.GetDictionaryValue(fileObj, "mtime");

                remoteFileInfo.UpdateSourceFileTimes(creationTime, lastWriteTime);

                fileVersions.Add(remoteFileInfo);
            }

            if (duplicateHashCount > DUPLICATE_HASH_MESSAGES_TO_LOG)
            {
                OnDebugEvent(string.Format("Duplicate hash value found for {0} files in MyEMSL", duplicateHashCount));
            }

            return remoteFiles;
        }

        public static string GetDatasetYearQuarter(Dictionary<string, string> taskParams)
        {
            var datasetDate = Utilities.GetDictionaryValue(taskParams, "Created", string.Empty);
            var date_code = DateTime.Parse(datasetDate);
            var yq = date_code.Month / 12.0 * 4.0;
            var yearQuarter = (int)Math.Ceiling(yq);
            var datasetDateCodeString = date_code.Year + "_" + yearQuarter;

            return datasetDateCodeString;
        }

        /// <summary>
        /// Get a list of file names that we can ignore when transferring data to MyEMSL or comparing files on disk to files in MyEMSL
        /// </summary>
        /// <returns>SortedSet of strings (case insensitive)</returns>
        public static SortedSet<string> GetFilesToIgnore()
        {
            var filesToIgnore = new SortedSet<string>(StringComparer.InvariantCultureIgnoreCase) {
                ".DS_Store",
                "Thumbs.db"
            };

            return filesToIgnore;
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

        private bool ValidateRemoteCertificate(object sender, X509Certificate cert, X509Chain chain, SslPolicyErrors policyErrors)
        {
            var success = Utilities.ValidateRemoteCertificate(cert, out var errorMessage);
            if (success)
            {
                return true;
            }

            OnErrorEvent(errorMessage);
            return false;
        }

        #region "Event Functions"

        private void FileTools_WaitingForLockQueue(string sourceFilePath, string targetFilePath, int MBBacklogSource, int MBBacklogTarget)
        {
            ConsoleMsgUtils.ShowDebug("  mFileTools_WaitingForLockQueue for " + sourceFilePath);
        }

        #endregion

    }
}
