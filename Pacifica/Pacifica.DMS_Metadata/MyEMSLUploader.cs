using Pacifica.Core;
using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using Uploader = Pacifica.Upload;
using Utilities = Pacifica.Core.Utilities;

namespace Pacifica.DMS_Metadata
{
    public class MyEMSLUploader : EventNotifier
    {
        // Ignore Spelling: Uploader, Pacifica

        public const string RECURSIVE_UPLOAD = "MyEMSL_Recurse";

        public const string CRITICAL_UPLOAD_ERROR = "Critical Error";

        private readonly Uploader.Upload mUploadWorker;

        private readonly Dictionary<string, string> mMgrParams;
        private readonly Dictionary<string, string> mTaskParams;

        private readonly string mManagerName;

        private readonly FileTools mFileTools;

        private readonly Configuration mPacificaConfig;

        #region "Properties"

        /// <summary>
        /// Number of bytes uploaded
        /// </summary>
        public long Bytes
        {
            get;
            private set;
        }

        /// <summary>
        /// Critical error message, as reported by SetupMetadata in DMSMetadataObject
        /// </summary>
        public string CriticalErrorMessage { get; private set; }

        /// <summary>
        /// Error message from the MyEMSLUploader
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public string ErrorMessage
        {
            get
            {
                if (mUploadWorker == null)
                {
                    return string.Empty;
                }

                return mUploadWorker.ErrorMessage;
            }
        }

        /// <summary>
        /// EUS Info
        /// </summary>
        public Uploader.Upload.EUSInfo EUSInfo
        {
            get;
            private set;
        }

        /// <summary>
        /// New files that were added
        /// </summary>
        public int FileCountNew
        {
            get;
            private set;
        }

        /// <summary>
        /// Existing files that were updated
        /// </summary>
        public int FileCountUpdated
        {
            get;
            private set;
        }

        /// <summary>
        /// DMS Metadata container
        /// </summary>
        public DMSMetadataObject MetadataContainer { get; private set; }

        /// <summary>
        /// Status URI
        /// </summary>
        public string StatusURI
        {
            get;
            private set;
        }

        /// <summary>
        /// True to enable trace mode
        /// </summary>
        public bool TraceMode { get; set; }

        private bool mUseTestInstance;

        /// <summary>
        /// True to use the test instance
        /// </summary>
        public bool UseTestInstance
        {
            get => mUseTestInstance;
            set
            {
                mUseTestInstance = value;
                mUploadWorker.UseTestInstance = value;
                mPacificaConfig.UseTestInstance = value;
            }
        }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Pacifica configuration</param>
        /// <param name="mgrParams"></param>
        /// <param name="taskParams"></param>
        /// <param name="fileTools"></param>
        public MyEMSLUploader(Configuration config, Dictionary<string, string> mgrParams, Dictionary<string, string> taskParams, FileTools fileTools)
        {
            mPacificaConfig = config;

            StatusURI = string.Empty;
            FileCountNew = 0;
            FileCountUpdated = 0;
            Bytes = 0;

            CriticalErrorMessage = string.Empty;

            EUSInfo = new Uploader.Upload.EUSInfo();
            EUSInfo.Clear();

            mMgrParams = mgrParams;
            mTaskParams = taskParams;

            if (!mMgrParams.TryGetValue("MgrName", out mManagerName))
            {
                mManagerName = "MyEMSLUploader_" + Environment.MachineName;
            }

            mFileTools = fileTools;

            var transferFolderPath = Utilities.GetDictionaryValue(mTaskParams, "TransferFolderPath", string.Empty);

            var transferDirectoryPathBase = Utilities.GetDictionaryValue(mTaskParams, "TransferDirectoryPath", transferFolderPath);
            if (string.IsNullOrEmpty(transferDirectoryPathBase))
            {
                throw new InvalidDataException("Job parameters do not have TransferDirectoryPath defined; unable to continue");
            }

            var datasetName = Utilities.GetDictionaryValue(mTaskParams, "Dataset", string.Empty);
            if (string.IsNullOrEmpty(transferDirectoryPathBase))
            {
                throw new InvalidDataException("Job parameters do not have Dataset defined; unable to continue");
            }

            var transferDirectoryPath = Path.Combine(transferDirectoryPathBase, datasetName);

            var jobNumber = Utilities.GetDictionaryValue(mTaskParams, "Job", string.Empty);
            if (string.IsNullOrEmpty(jobNumber))
            {
                throw new InvalidDataException("Job parameters do not have Job defined; unable to continue");
            }

            mUploadWorker = new Uploader.Upload(config, transferDirectoryPath, jobNumber);
            RegisterEvents(mUploadWorker);

            // Attach the events
            mUploadWorker.MyEMSLOffline += MyEmslUploadOnMyEmslOffline;
            mUploadWorker.StatusUpdate += MyEMSLUpload_StatusUpdate;
            mUploadWorker.UploadCompleted += MyEMSLUpload_UploadCompleted;
        }

        /// <summary>
        /// Look for files to upload, compute a SHA-1 hash for each, compare those hashes to existing files in MyEMSL,
        /// and upload new/changed files
        /// </summary>
        /// <param name="config"></param>
        /// <param name="debugMode">
        /// Set to eDebugMode.CreateTarLocal to authenticate with MyEMSL, then create a .tar file locally instead of actually uploading it
        /// Set to eDebugMode.MyEMSLOfflineMode to create the .tar file locally without contacting MyEMSL
        /// </param>
        /// <param name="statusURL">Output: status URL</param>
        /// <returns>True if success, false if an error</returns>
        public bool SetupMetadataAndUpload(Configuration config, Uploader.TarStreamUploader.UploadDebugMode debugMode, out string statusURL)
        {
            var jobNumber = GetParam("Job", 0);

            var ignoreMaxFileLimit = false;
            if (mTaskParams.TryGetValue("IgnoreMaxFileLimit", out var ignoreMaxFileLimitSetting))
            {
                if (int.TryParse(ignoreMaxFileLimitSetting, out var value))
                {
                    ignoreMaxFileLimit = value > 0;
                } else if (bool.TryParse(ignoreMaxFileLimitSetting, out var boolValue))
                {
                    ignoreMaxFileLimit = boolValue;
                }
            }

            var ignoreMyEMSLFileTrackingError = GetParam("IgnoreMyEMSLFileTrackingError", false);

            // Instantiate the metadata object
            MetadataContainer = new DMSMetadataObject(config, mManagerName, jobNumber, mFileTools)
            {
                TraceMode = TraceMode,
                IgnoreMaxFileLimit = ignoreMaxFileLimit,
                IgnoreMyEMSLFileTrackingError = ignoreMyEMSLFileTrackingError
            };

            // Attach the events
            RegisterEvents(MetadataContainer);

            // Also process Progress Updates using Container_ProgressEvent, which triggers event StatusUpdate
            MetadataContainer.ProgressUpdate += Container_ProgressEvent;

            MetadataContainer.UseTestInstance = UseTestInstance;

            var certificateFilePath = EasyHttp.ResolveCertFile(mPacificaConfig, "SetupMetadataAndUpload", out var errorMessage);

            if (string.IsNullOrWhiteSpace(certificateFilePath))
            {
                throw new Exception(errorMessage);
            }

            try
            {
                // Look for files to upload, compute a SHA-1 hash for each, and compare those hashes to existing files in MyEMSL
                var success = MetadataContainer.SetupMetadata(mTaskParams, mMgrParams, out var criticalError, out var criticalErrorMessage);

                if (!success)
                {
                    if (criticalError)
                    {
                        CriticalErrorMessage = criticalErrorMessage;
                    }

                    statusURL = criticalError ? CRITICAL_UPLOAD_ERROR : string.Empty;

                    return false;
                }
            }
            catch (Exception ex)
            {
                OnWarningEvent("Exception calling MetadataContainer.SetupMetadata: " + ex.Message);
                MetadataContainer.DeleteLockFiles();
                throw;
            }

            // Send the metadata object to the calling procedure (in case it wants to log it)
            ReportMetadataDefined("StartUpload", MetadataContainer.MetadataObjectJSON);

            mPacificaConfig.LocalTempDirectory = Utilities.GetDictionaryValue(mMgrParams, "WorkDir", string.Empty);
            FileCountUpdated = MetadataContainer.TotalFileCountUpdated;
            FileCountNew = MetadataContainer.TotalFileCountNew;
            Bytes = MetadataContainer.TotalFileSizeToUpload;

            EUSInfo = MetadataContainer.EUSInfo;

            var fileList = Utilities.GetFileListFromMetadataObject(MetadataContainer.MetadataObject);
            if (fileList.Count == 0)
            {
                OnDebugEvent("File list is empty in StartUpload; nothing to do");
                statusURL = string.Empty;
                var e = new UploadCompletedEventArgs(string.Empty);
                UploadCompleted?.Invoke(this, e);
                return true;
            }

            MetadataContainer.CreateLockFiles();

            bool uploadSuccess;

            try
            {
                uploadSuccess = mUploadWorker.StartUpload(MetadataContainer.MetadataObject, debugMode, out statusURL);
            }
            catch (Exception ex)
            {
                OnWarningEvent("Exception calling UploadWorker.StartUpload: " + ex.Message);
                MetadataContainer.DeleteLockFiles();
                throw;
            }

            MetadataContainer.DeleteLockFiles();

            if (!string.IsNullOrEmpty(statusURL))
            {
                StatusURI = statusURL;
            }

            return uploadSuccess;
        }

        /// <summary>
        /// Gets a job parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        private bool GetParam(string name, bool valueIfMissing)
        {
            if (mTaskParams.TryGetValue(name, out var valueText))
            {
                if (bool.TryParse(valueText, out var value))
                {
                    return value;
                }

                return valueIfMissing;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Gets a job parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        private int GetParam(string name, int valueIfMissing)
        {
            if (mTaskParams.TryGetValue(name, out var valueText))
            {
                if (int.TryParse(valueText, out var value))
                {
                    return value;
                }

                return valueIfMissing;
            }

            return valueIfMissing;
        }

        #region "Events and Event Handlers"

        public event EventHandler<MessageEventArgs> MetadataDefinedEvent;

        public event EventHandler<StatusEventArgs> StatusUpdate;

        public event EventHandler<UploadCompletedEventArgs> UploadCompleted;

        private void ReportMetadataDefined(string callingFunction, string metadataJSON)
        {
            var e = new MessageEventArgs(callingFunction, metadataJSON);

            MetadataDefinedEvent?.Invoke(this, e);
        }

        private void MyEmslUploadOnMyEmslOffline(object sender, MessageEventArgs e)
        {
            OnWarningEvent("MyEMSL is offline; unable to retrieve data or upload files: " + e.Message);
        }

        private void MyEMSLUpload_StatusUpdate(object sender, StatusEventArgs e)
        {
            if (StatusUpdate != null)
            {
                // Multiplying by 0.25 because we're assuming 25% of the time is required for mMetadataContainer to compute the SHA-1 hashes of files to be uploaded while 75% of the time is required to create and upload the .tar file
                var percentCompleteOverall = 25 + e.PercentCompleted * 0.75;
                StatusUpdate(this, new StatusEventArgs(percentCompleteOverall, e.TotalBytesSent, e.TotalBytesToSend, e.StatusMessage));
            }
        }

        private void MyEMSLUpload_UploadCompleted(object sender, UploadCompletedEventArgs e)
        {
            UploadCompleted?.Invoke(this, e);
        }

        private void Container_ProgressEvent(string progressMessage, float percentComplete)
        {
            if (StatusUpdate != null)
            {
                // Multiplying by 0.25 because we're assuming 25% of the time is required for mMetadataContainer to compute the SHA-1 hashes of files to be uploaded while 75% of the time is required to create and upload the .tar file
                var percentCompleteOverall = 0 + percentComplete * 0.25;
                StatusUpdate(this, new StatusEventArgs(percentCompleteOverall, 0, MetadataContainer.TotalFileSizeToUpload, progressMessage));
            }
        }

        #endregion

    }
}
