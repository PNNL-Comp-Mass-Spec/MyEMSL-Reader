using System;
using System.Collections.Generic;
using System.IO;
using Pacifica.Core;
using PRISM;

namespace Pacifica.Upload
{
    /// <summary>
    /// MyEMSL Upload metadata class
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class Upload : EventNotifier
    {
        // Ignore Spelling: Uploader, Frodo, Pacifica, http, ok, Ingester, ncbi, absolutelocalpath, subdir, hashsum, mimetype, hashtype, ctime, mtime, eus

        /// <summary>
        /// EUS Operator ID to use when operator ID is unknown
        /// </summary>
        /// <remarks>43428 is "Monroe, Matthew"</remarks>
        public const int DEFAULT_EUS_OPERATOR_ID = 43428;

        /// <summary>
        /// EUS Project ID to use when the project ID is unknown
        /// </summary>
        /// <remarks>
        /// We originally used Project 17797: "Development of High Throughput Proteomics Production Operations"
        /// We switched to Project 51287 in January 2020
        /// </remarks>
        public const string DEFAULT_EUS_PROJECT_ID = "51287";

        /// <summary>
        /// EUS Instrument ID to use when the EUS Instrument ID is unknown
        /// </summary>
        /// <remarks>34127 is VOrbiETD04</remarks>
        public const int UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID = 34127;

        /// <summary>
        /// DMS Instrument Name corresponding to EUS Instrument ID 34127
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public const string UNKNOWN_INSTRUMENT_NAME = "VOrbiETD04";

        private readonly Configuration mPacificaConfig;

        public class EUSInfo
        {
            /// <summary>
            /// EUS instrument ID
            /// </summary>
            public int EUSInstrumentID;

            /// <summary>
            /// EUS project number
            /// </summary>
            /// <remarks>As of May 2019, these are now referred to as projects, not proposals</remarks>
            public string EUSProjectID;

            /// <summary>
            /// EUS ID of the instrument operator (for datasets) or the data package owner (for Data Packages)
            /// </summary>
            /// <remarks>Aka EUSSubmitterId</remarks>
            public int EUSUploaderID;

            public void Clear()
            {
                EUSInstrumentID = 0;
                EUSProjectID = string.Empty;
                EUSUploaderID = 0;
            }

            public override string ToString()
            {
                return "EUSInstrumentID " + EUSInstrumentID + ", Uploader " + EUSUploaderID + ", Project " + EUSProjectID;
            }
        }

        public class UploadMetadata
        {
            public int DatasetID;               // 0 for data packages
            public int DataPackageID;
            public string SubFolder;
            public string DatasetName;          // Only used for datasets; not Data Packages
            public string DateCodeString;       // Only used for datasets; not Data Packages
            public string DMSInstrumentName;    // Originally only used by datasets. Used by Data Packages starting in July 2017 since required by policy
            public string CampaignName;
            public int CampaignID;
            public int EUSInstrumentID;         // Originally only used by datasets. Used by Data Packages starting in July 2017 since required by policy
            public string EUSProjectID;         // Originally only used by datasets. Used by Data Packages starting in October 2016 since required by policy
            public string ExperimentName;
            public int ExperimentID;
            public string OrganismName;
            public int OrganismID;
            public int NCBITaxonomyID;
            public string AcquisitionTime;
            public int AcquisitionLengthMin;
            public int NumberOfScans;
            public string SeparationType;
            public string DatasetType;
            public int RequestedRunID;
            public List<int> UserOfRecordList;

            /// <summary>
            /// Instrument Operator EUS ID for datasets
            /// Data Package Owner for data packages
            /// </summary>
            /// <remarks>DEFAULT_EUS_OPERATOR_ID if unknown</remarks>
            public int EUSOperatorID;

            // ReSharper disable once UnusedMember.Global
            public void Clear()
            {
                DatasetID = 0;
                DataPackageID = 0;
                SubFolder = string.Empty;
                DatasetName = string.Empty;
                DateCodeString = string.Empty;
                DMSInstrumentName = string.Empty;
                CampaignName = string.Empty;
                CampaignID = 0;
                EUSInstrumentID = 0;
                EUSProjectID = string.Empty;
                ExperimentName = string.Empty;
                ExperimentID = 0;
                OrganismName = string.Empty;
                OrganismID = 0;
                NCBITaxonomyID = 0;
                AcquisitionTime = string.Empty;
                AcquisitionLengthMin = 0;
                NumberOfScans = 0;
                SeparationType = string.Empty;
                DatasetType = string.Empty;
                RequestedRunID = 0;
                UserOfRecordList = new List<int>();
                EUSOperatorID = DEFAULT_EUS_OPERATOR_ID;
            }

            public override string ToString()
            {
                if (DatasetID == 0 && DataPackageID > 0)
                {
                    return "Data package " + DataPackageID;
                }

                return "Dataset " + DatasetID + ", on instrument " + DMSInstrumentName + ": " + DatasetName;
            }
        }

        #region Auto-Properties

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage { get; private set; }

        /// <summary>
        /// The metadata.txt file will be copied to the Transfer Folder if the folder path is not empty
        /// Example: \\proto-4\DMS3_Xfer\QC_Shew_16_01_125ng_CID-STD_newCol-1_5Apr17_Frodo_16-11-08
        /// </summary>
        public string TransferFolderPath { get; set; }

        /// <summary>
        /// Capture Task DB job number for the DatasetArchive or ArchiveUpdate Job
        /// </summary>
        /// <remarks>
        /// The metadata.txt file name will include the JobNumber text in the name, for example MyEMSL_metadata_CaptureJob_12345.txt
        /// For DataPackages we store DataPackageId in JobNumber</remarks>
        public string JobNumber { get; set; }

        /// <summary>
        /// When true, upload to ingestdmsdev.my.emsl.pnl.gov instead of ingestdms.my.emsl.pnl.gov
        /// </summary>
        public bool UseTestInstance { get; set; }

        #endregion

        #region Constructor

        /// <summary>
        /// Constructor (TransferFolderPath and JobNumber will be empty)
        /// </summary>
        /// <param name="config">Pacifica configuration</param>
        /// <remarks>This constructor is used by the DataPackage Archive Manager</remarks>
        // ReSharper disable once UnusedMember.Global
        public Upload(Configuration config) : this(config, string.Empty, string.Empty)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Pacifica config</param>
        /// <param name="transferFolderPath">
        /// Transfer folder path for this dataset,
        /// for example \\proto-4\DMS3_Xfer\IFT001_Pool_17_B_10x_27Aug13_Tiger_13-07-36
        /// </param>
        /// <param name="jobNumber">DMS Data Capture job number</param>
        /// <remarks>The metadata.txt file will be copied to the transfer folder</remarks>
        public Upload(Configuration config, string transferFolderPath, string jobNumber)
        {
            mPacificaConfig = config;

            // Note that EasyHttp is a static class with a static event
            // Be careful about instantiating this class (Upload) multiple times
            EasyHttp.StatusUpdate += Pacifica_StatusUpdate;

            EasyHttp.MyEMSLOffline += EasyHttp_MyEMSLOffline;

            EasyHttp.ErrorEvent += OnErrorEvent;

            ErrorMessage = string.Empty;
            TransferFolderPath = transferFolderPath;
            JobNumber = jobNumber;
        }

        #endregion

        #region Events and Handlers

        public event EventHandler<MessageEventArgs> MyEMSLOffline;
        public event EventHandler<UploadCompletedEventArgs> UploadCompleted;
        public event EventHandler<StatusEventArgs> StatusUpdate;

        private void EasyHttp_MyEMSLOffline(object sender, MessageEventArgs e)
        {
            MyEMSLOffline?.Invoke(this, e);
        }

        private void Pacifica_StatusUpdate(object sender, StatusEventArgs e)
        {
            StatusUpdate?.Invoke(this, e);
        }

        private void RaiseUploadCompleted(string serverResponse)
        {
            UploadCompleted?.Invoke(this, new UploadCompletedEventArgs(serverResponse));
        }

        #endregion

        #region IUpload Members

        /// <summary>
        /// Update the files and data tracked by metadataObject to MyEMSL
        /// </summary>
        /// <param name="metadataObject"></param>
        /// <param name="statusURI">Status URL</param>
        /// <returns>True if successfully uploaded, false if an error</returns>
        /// <remarks>This method is used by the DataPackage Archive Manager</remarks>
        // ReSharper disable once UnusedMember.Global
        public bool StartUpload(List<Dictionary<string, object>> metadataObject, out string statusURI)
        {
            const TarStreamUploader.UploadDebugMode debugMode = TarStreamUploader.UploadDebugMode.DebugDisabled;

            return StartUpload(metadataObject, debugMode, out statusURI);
        }

        /// <summary>
        /// Update the files and data tracked by metadataObject to MyEMSL
        /// </summary>
        /// <param name="metadataObject"></param>
        /// <param name="debugMode">
        /// Set to eDebugMode.CreateTarLocal to authenticate with MyEMSL, then create a .tar file locally instead of actually uploading it
        /// Set to eDebugMode.MyEMSLOfflineMode to create the .tar file locally without contacting MyEMSL
        /// </param>
        /// <param name="statusURI">Status URL</param>
        /// <returns>True if successfully uploaded, false if an error</returns>
        public bool StartUpload(
            List<Dictionary<string, object>> metadataObject,
            TarStreamUploader.UploadDebugMode debugMode,
            out string statusURI)
        {
            statusURI = string.Empty;
            ErrorMessage = string.Empty;

            var certificateFilePath = EasyHttp.ResolveCertFile(mPacificaConfig, "StartUpload", out var errorMessage);

            if (string.IsNullOrWhiteSpace(certificateFilePath))
            {
                OnError(errorMessage);
                return false;
            }

            var fileList = Utilities.GetFileListFromMetadataObject(metadataObject);

            // Grab the list of files from the top-level "file" object
            // Keys in this dictionary are the source file path (Windows paths); values are metadata about the file
            var fileListObject = new SortedDictionary<string, FileInfoObject>();

            foreach (var file in fileList)
            {
                var fio = new FileInfoObject(file.AbsoluteLocalPath, file.RelativeDestinationDirectory, file.Sha1HashHex);
                fileListObject.Add(file.AbsoluteLocalPath, fio);
            }

            // Optionally use the test instance
            mPacificaConfig.UseTestInstance = UseTestInstance;

            var jsonMetadata = Utilities.ObjectToJson(metadataObject);

            // Create the metadata.txt file
            var metadataFilePath = Path.GetTempFileName();
            var metadataFile = new FileInfo(metadataFilePath);
            using (var metadataWriter = metadataFile.CreateText())
            {
                metadataWriter.Write(jsonMetadata);
            }

            try
            {
                // Copy the Metadata.txt file to the transfer folder, renaming it when we copy it.
                // Example path: \\proto-4\DMS3_Xfer\QC_Shew_16_01_125ng_CID-STD_newCol-1_5Apr17_Frodo_16-11-08\MyEMSL_metadata_CaptureJob_2836788.txt
                if (!string.IsNullOrWhiteSpace(TransferFolderPath))
                {
                    var targetFile = new FileInfo(Path.Combine(TransferFolderPath, Utilities.GetMetadataFilenameForJob(JobNumber)));
                    if (targetFile.Directory?.Exists == false)
                    {
                        targetFile.Directory.Create();
                    }

                    metadataFile.CopyTo(targetFile.FullName, true);
                }
            }
            catch
            {
                // Ignore errors here
            }

            if (fileList.Count == 0)
            {
                OnDebugEvent("File list is empty in StartUpload; nothing to do");
                RaiseUploadCompleted(string.Empty);
                return true;
            }

            const string location = "upload";

            var serverUri = "https://ServerIsOffline/dummy_page?test";

            if (debugMode == TarStreamUploader.UploadDebugMode.MyEMSLOfflineMode)
            {
                OnDebugEvent("StartUpload is creating the .tar file locally");
            }
            else
            {
                serverUri = mPacificaConfig.IngestServerUri;

                // URL for posting the virtual .tar file to
                // Typically: https://ingestdms.my.emsl.pnl.gov/upload
                var storageUrl = serverUri + "/" + location;

                OnDebugEvent("StartUpload is sending file to " + storageUrl);
            }

            var streamUploader = new TarStreamUploader();
            streamUploader.StatusUpdate += Pacifica_StatusUpdate;

            var responseData = streamUploader.SendFileListToIngester(
                mPacificaConfig, location, serverUri, fileListObject, metadataFile.FullName, debugMode);

            if (debugMode != TarStreamUploader.UploadDebugMode.DebugDisabled)
            {
                // A .tar file was created locally; it was not sent to the server
                return false;
            }

            try
            {
                var responseJSON = Utilities.JsonToObject(responseData);

                var transactionID = Convert.ToInt32(responseJSON["job_id"].ToString());

                statusURI = mPacificaConfig.IngestServerUri + "/get_state?job_id=" + transactionID;
            }
            catch (Exception ex)
            {
                OnError("Error converting the response data to a JSON object", ex);

                // Delete the local temporary file
                Utilities.DeleteFileIgnoreErrors(metadataFile);
                return false;
            }

            var success = false;

            try
            {
                string statusResult;
                if (responseData.Contains("state"))
                {
                    // We already have a valid server response
                    statusResult = responseData;
                }
                else
                {
                    statusResult = EasyHttp.SendViaThreadStart(mPacificaConfig, statusURI, out _);
                }

                if (EasyHttp.IsResponseError(statusResult))
                {
                    OnError("Error checking status: " + statusResult);
                    return false;
                }

                Dictionary<string, object> statusJSON;

                try
                {
                    statusJSON = Utilities.JsonToObject(statusResult);
                }
                catch (Exception)
                {
                    OnError("Unable to parse response into JSON: " + statusResult);
                    return false;
                }

                var state = statusJSON["state"].ToString().ToLower();

                if (state == "ok")
                {
                    success = true;
                    RaiseUploadCompleted(statusURI);
                }
                else if (state == "failed")
                {
                    OnError("Upload failed during ingest process");
                    RaiseUploadCompleted(statusResult);
                }
                else if (state.Contains("error"))
                {
                    OnError("Ingester Backend is offline or having issues");
                    RaiseUploadCompleted(statusResult);
                }
                else
                {
                    OnError("Unrecognized ingest state: " + statusJSON["state"]);
                }
            }
            catch (Exception ex)
            {
                OnError("Exception examining the MyEMSL response string", ex);
            }
            finally
            {
                // Delete the local temporary file
                Utilities.DeleteFileIgnoreErrors(metadataFile);
            }

            return success;
        }

        #endregion

        #region Member Methods

        private static void AppendKVMetadata(ICollection<Dictionary<string, object>> metadataObject, string keyName, int value)
        {
            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "TransactionKeyValue" },
                { "key", keyName },
                { "value", value }
            });
        }

        private static void AppendKVMetadata(ICollection<Dictionary<string, object>> metadataObject, string keyName, string value)
        {
            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "TransactionKeyValue" },
                { "key", keyName },
                { "value", value }
            });
        }

        private static void AppendTransactionMetadata(ICollection<Dictionary<string, object>> metadataObject, string columnName, int value)
        {
            // Example destination table name:
            //  Transactions.instrument
            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "Transactions." + columnName },
                { "value", value }
            });
        }

        private static void AppendTransactionMetadata(ICollection<Dictionary<string, object>> metadataObject, string columnName, string value)
        {
            // Example destination table names:
            //  Transactions.project
            //  Transactions.submitter
            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "Transactions." + columnName },
                { "value", value }
            });
        }

        /// <summary>
        /// Create the metadata object with the upload details, including the files to upload
        /// </summary>
        /// <param name="uploadMetadata">Upload metadata</param>
        /// <param name="filesToUpload">Files to upload</param>
        /// <param name="eusInfo">Output parameter: EUS instrument ID, project ID, and uploader ID</param>
        /// <returns>
        /// Dictionary of the information to translate to JSON;
        /// Keys are key names; values are either strings or dictionary objects or even a list of dictionary objects
        /// </returns>
        // ReSharper disable once UnusedMember.Global
        public static List<Dictionary<string, object>> CreatePacificaMetadataObject(
            UploadMetadata uploadMetadata,
            List<FileInfoObject> filesToUpload,
            out EUSInfo eusInfo)
        {
            eusInfo = new EUSInfo();
            eusInfo.Clear();

            // new metadata object is just a list of dictionary entries
            var metadataObject = new List<Dictionary<string, object>>();

            if (uploadMetadata.EUSInstrumentID <= 0)
            {
                // Possibly override EUSInstrument ID
                if (uploadMetadata.DMSInstrumentName.IndexOf("LCQ", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    uploadMetadata.EUSInstrumentID = 1163;
                }

                if (string.Equals(uploadMetadata.DMSInstrumentName, "Exact02", StringComparison.OrdinalIgnoreCase))
                {
                    uploadMetadata.EUSInstrumentID = 34111;
                }

                if (string.Equals(uploadMetadata.DMSInstrumentName, "IMS07_AgTOF04", StringComparison.OrdinalIgnoreCase))
                {
                    uploadMetadata.EUSInstrumentID = 34155;
                }
            }

            // Now that EUS instrument ID is defined, store it and lookup other EUS info
            eusInfo.EUSInstrumentID = GetEUSInstrumentID(uploadMetadata.EUSInstrumentID, UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID);
            eusInfo.EUSProjectID = GetEUSProjectID(uploadMetadata.EUSProjectID, DEFAULT_EUS_PROJECT_ID);
            eusInfo.EUSUploaderID = GetEUSSubmitterID(uploadMetadata.EUSOperatorID, DEFAULT_EUS_OPERATOR_ID);

            // Possibly override EUSProject ID
            if (eusInfo.EUSProjectID.StartsWith("EPR"))
            {
                PRISM.Logging.LogTools.LogWarning(string.Format("Overriding project {0} with {1} for dataset {2}", eusInfo.EUSProjectID,
                                                                DEFAULT_EUS_PROJECT_ID, uploadMetadata.DatasetName));
                eusInfo.EUSProjectID = DEFAULT_EUS_PROJECT_ID;
            }

            // Fill out Transaction Key/Value pairs
            if (uploadMetadata.DatasetID > 0)
            {
                AppendKVMetadata(metadataObject, "omics.dms.instrument", uploadMetadata.DMSInstrumentName);
                AppendKVMetadata(metadataObject, "omics.dms.instrument_id", eusInfo.EUSInstrumentID);
                AppendKVMetadata(metadataObject, "omics.dms.date_code", uploadMetadata.DateCodeString);
                AppendKVMetadata(metadataObject, "omics.dms.dataset", uploadMetadata.DatasetName);
                AppendKVMetadata(metadataObject, "omics.dms.campaign_name", uploadMetadata.CampaignName);
                AppendKVMetadata(metadataObject, "omics.dms.experiment_name", uploadMetadata.ExperimentName);
                AppendKVMetadata(metadataObject, "omics.dms.dataset_name", uploadMetadata.DatasetName);
                AppendKVMetadata(metadataObject, "omics.dms.campaign_id", uploadMetadata.CampaignID.ToString());
                AppendKVMetadata(metadataObject, "omics.dms.experiment_id", uploadMetadata.ExperimentID.ToString());
                AppendKVMetadata(metadataObject, "omics.dms.dataset_id", uploadMetadata.DatasetID.ToString());

                if (!string.IsNullOrEmpty(uploadMetadata.OrganismName))
                {
                    AppendKVMetadata(metadataObject, "organism_name", uploadMetadata.OrganismName);
                }

                if (uploadMetadata.OrganismID != 0)
                {
                    AppendKVMetadata(metadataObject, "omics.dms.organism_id", uploadMetadata.OrganismID.ToString());
                }

                if (uploadMetadata.NCBITaxonomyID != 0)
                {
                    AppendKVMetadata(metadataObject, "ncbi_taxonomy_id", uploadMetadata.NCBITaxonomyID.ToString());
                }

                if (!string.IsNullOrEmpty(uploadMetadata.SeparationType))
                {
                    AppendKVMetadata(metadataObject, "omics.dms.separation_type", uploadMetadata.SeparationType);
                }

                if (!string.IsNullOrEmpty(uploadMetadata.DatasetType))
                {
                    AppendKVMetadata(metadataObject, "omics.dms.dataset_type", uploadMetadata.DatasetType);
                }

                AppendKVMetadata(metadataObject, "omics.dms.run_acquisition_length_min", uploadMetadata.AcquisitionLengthMin);

                if (uploadMetadata.UserOfRecordList.Count > 0)
                {
                    foreach (var userId in uploadMetadata.UserOfRecordList)
                    {
                        AppendKVMetadata(metadataObject, "User of Record", userId.ToString());
                        AppendKVMetadata(metadataObject, "user_of_record", userId.ToString());
                    }
                }
            }
            else if (uploadMetadata.DataPackageID > 0)
            {
                AppendKVMetadata(metadataObject, "omics.dms.instrument", uploadMetadata.DMSInstrumentName);

                AppendKVMetadata(metadataObject, "omics.dms.instrument_id", eusInfo.EUSInstrumentID);

                AppendKVMetadata(metadataObject, "omics.dms.datapackage_id", uploadMetadata.DataPackageID.ToString());
            }
            else
            {
                throw new Exception("Must define a non-zero DatasetID or a DataPackageID; cannot create the metadata object");
            }

            // Append the required metadata
            AppendTransactionMetadata(metadataObject, "instrument", eusInfo.EUSInstrumentID);
            AppendTransactionMetadata(metadataObject, "project", eusInfo.EUSProjectID);
            AppendTransactionMetadata(metadataObject, "submitter", eusInfo.EUSUploaderID);

            // Append the files
            foreach (var file in filesToUpload)
            {
                // The subdirectory path must be "data/" or of the form "data/SubDirectory"
                // "data/" is required for files at the root dataset level because the root of the tar file
                // has a metadata.txt file and we would have a conflict if the dataset folder root
                // also had a file named metadata.txt

                // The ingest system will trim out the leading "data/" when storing the SubDir in the system

                // Note the inconsistent requirements; files in the root dataset level must have "data/"
                // while files in subdirectories should have a SubDir that does _not_ end in a forward slash
                // It is likely that this discrepancy has been fixed in the backend python code on the ingest server

                string subDirString;

                if (string.IsNullOrWhiteSpace(file.RelativeDestinationDirectory))
                {
                    subDirString = "data/";
                }
                else
                {
                    subDirString = "data/" + file.RelativeDestinationDirectory.Trim('/');
                }

                if (subDirString.Contains("//"))
                {
                    throw new Exception("File path should not have two forward slashes: " + subDirString);
                }

                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "Files" },
                    { "name", file.FileName },
                    // ReSharper disable once StringLiteralTypo
                    { "absolutelocalpath", file.AbsoluteLocalPath},
                    { "subdir", subDirString },
                    { "size", file.FileSizeInBytes.ToString() },
                    { "hashsum", file.Sha1HashHex },
                    { "mimetype", "application/octet-stream" },
                    { "hashtype", "sha1" },
                    { "ctime", file.CreationTime.ToUniversalTime().ToString("s") },
                    { "mtime", file.LastWriteTime.ToUniversalTime().ToString("s") }
                });
            }

            return metadataObject;
        }

        /// <summary>
        /// Return a string description of the EUS info encoded by metadataObject
        /// </summary>
        /// <param name="metadataObject"></param>
        /// <returns></returns>
        // ReSharper disable once UnusedMember.Global
        public static string GetMetadataObjectDescription(List<Dictionary<string, object>> metadataObject)
        {
            var metadataList = new List<string>();
            var fileCount = 0;

            var kvLookup = new Dictionary<string, object>
            {
                {"omics.dms.dataset_id", "Dataset_ID"},
                {"omics.dms.datapackage_id", "DataPackage_ID"},
                {"omics.dms.instrument", "DMS_Instrument"},
                {"omics.dms.instrument_id", "EUS_Instrument_ID"}
            };

            var transactionValueLookup = new Dictionary<string, string>
            {
                {"Transactions.project", "EUS_Proposal_ID"},
                {"Transactions.submitter", "EUS_User_ID"},
                {"Transactions.instrument", "EUS_Instrument_ID"}
            };

            var matchedKeys = new SortedSet<string>();

            foreach (var item in metadataObject)
            {
                if (!GetDictionaryValue(item, "destinationTable", out var tableName))
                {
                    continue;
                }

                switch (tableName)
                {
                    case "TransactionKeyValue":
                        {
                            if (!GetDictionaryValue(item, "key", out var keyName))
                            {
                                continue;
                            }

                            if (!GetDictionaryValue(item, "value", out var keyValue))
                            {
                                continue;
                            }

                            if (!GetDictionaryValue(kvLookup, keyName, out var valueDescription))
                            {
                                continue;
                            }

                            metadataList.Add(valueDescription + "=" + keyValue);
                            matchedKeys.Add(valueDescription);
                            break;
                        }
                    case "Files":
                        if (item.TryGetValue("size", out _))
                        {
                            fileCount++;
                        }
                        break;
                    default:
                        {
                            if (!transactionValueLookup.TryGetValue(tableName, out var valueDescription))
                            {
                                continue;
                            }

                            if (matchedKeys.Contains(valueDescription))
                            {
                                // This item has already been added (typically EUS_Instrument_ID)
                                continue;
                            }

                            // Include the value for this item in the description
                            if (!GetDictionaryValue(item, "value", out var keyValue))
                            {
                                continue;
                            }

                            metadataList.Add(valueDescription + "=" + keyValue);
                            matchedKeys.Add(valueDescription);
                            break;
                        }
                }
            }

            return string.Join("; ", metadataList) + "; FileCount=" + fileCount;
        }

        private static bool GetDictionaryValue(IReadOnlyDictionary<string, object> eusInfoMapObject, string keyName, out string matchedValue)
        {
            if (eusInfoMapObject.TryGetValue(keyName, out var value))
            {
                matchedValue = value as string;
                if (matchedValue != null)
                {
                    return true;
                }
            }

            matchedValue = string.Empty;
            return false;
        }

        /// <summary>
        /// Return the EUS instrument ID, falling back to instrumentIdIfUnknown if eusInstrumentId is empty
        /// </summary>
        /// <param name="eusInstrumentId"></param>
        /// <param name="instrumentIdIfUnknown"></param>
        /// <returns></returns>
        private static int GetEUSInstrumentID(int eusInstrumentId, int instrumentIdIfUnknown)
        {
            return eusInstrumentId <= 0 ? instrumentIdIfUnknown : eusInstrumentId;
        }

        /// <summary>
        /// Validate the EUS project ID, or use the default
        /// </summary>
        /// <param name="eusProjectId"></param>
        /// <param name="eusProjectIdIfUnknown"></param>
        /// <returns></returns>
        /// <remarks>This is a string because the project ID may contain suffix letters</remarks>
        private static string GetEUSProjectID(string eusProjectId, string eusProjectIdIfUnknown)
        {
            return string.IsNullOrWhiteSpace(eusProjectId) ? eusProjectIdIfUnknown : eusProjectId;
        }

        private static int GetEUSSubmitterID(int eusOperatorId, int eusOperatorIdIfUnknown)
        {
            // For datasets, eusOperatorID is the instrument operator EUS ID
            // For data packages, it is the EUS ID of the data package owner
            return eusOperatorId == 0 ? eusOperatorIdIfUnknown : eusOperatorId;
        }

        private void OnError(string errorMessage, Exception ex = null)
        {
            ErrorMessage = errorMessage;
            if (ex != null && !ErrorMessage.Contains(ex.Message))
            {
                ErrorMessage += ": " + ex.Message;
            }

            OnErrorEvent(errorMessage, ex);
        }

        #endregion
    }
}
