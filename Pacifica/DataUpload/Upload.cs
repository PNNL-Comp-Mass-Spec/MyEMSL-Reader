﻿using System;
using System.Collections.Generic;
using System.IO;
using Pacifica.Core;
using Pacifica.Json;
using PRISM;

namespace Pacifica.DataUpload
{
    /// <summary>
    /// MyEMSL Upload metadata class
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class Upload : EventNotifier
    {
        // ReSharper disable once CommentTypo
        // Ignore Spelling: absolutelocalpath, backend, ctime, eus, Frodo, gz, hashsum, hashtype, http
        // Ignore Spelling: Ingester, mimetype, mtime, ncbi, ok, Pacifica, proteomics, subdir, Uploader

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

        /// <summary>
        /// EUS info
        /// </summary>
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

            /// <summary>
            /// Clear stored EUS info
            /// </summary>
            public void Clear()
            {
                EUSInstrumentID = 0;
                EUSProjectID = string.Empty;
                EUSUploaderID = 0;
            }

            /// <summary>
            /// Return EUS instrument ID, uploader ID, and project ID
            /// </summary>
            public override string ToString()
            {
                return "EUSInstrumentID " + EUSInstrumentID + ", Uploader " + EUSUploaderID + ", Project " + EUSProjectID;
            }
        }

        /// <summary>
        /// Upload metadata
        /// </summary>
        public class UploadMetadata
        {
            /// <summary>
            /// Dataset ID
            /// </summary>
            /// <remarks>
            /// 0 for data packages
            /// </remarks>
            public int DatasetID;

            /// <summary>
            /// Data package ID
            /// </summary>
            public int DataPackageID;

            /// <summary>
            /// Subdirectory
            /// </summary>
            public string SubFolder;

            /// <summary>
            /// Dataset name
            /// </summary>
            /// <remarks>
            /// Only used for datasets; not data packages
            /// </remarks>
            public string DatasetName;

            /// <summary>
            /// Date code string
            /// </summary>
            /// <remarks>
            /// Only used for datasets; not data packages
            /// </remarks>
            public string DateCodeString;

            /// <summary>
            /// DMS instrument name
            /// </summary>
            /// <remarks>
            /// Originally only used by datasets. Used by Data Packages starting in July 2017 since required by policy
            /// </remarks>
            public string DMSInstrumentName;

            /// <summary>
            /// Campaign name
            /// </summary>
            public string CampaignName;

            /// <summary>
            /// Campaign ID
            /// </summary>
            public int CampaignID;

            /// <summary>
            /// EUS instrument ID
            /// </summary>
            /// <remarks>
            /// Originally only used by datasets. Used by Data Packages starting in July 2017 since required by policy
            /// </remarks>
            public int EUSInstrumentID;

            /// <summary>
            /// EUS project ID
            /// </summary>
            /// <remarks>
            /// Originally only used by datasets. Used by Data Packages starting in October 2016 since required by policy
            /// </remarks>
            public string EUSProjectID;

            /// <summary>
            /// Experiment name
            /// </summary>
            public string ExperimentName;

            /// <summary>
            /// Experiment ID
            /// </summary>
            public int ExperimentID;

            /// <summary>
            /// Organism name
            /// </summary>
            public string OrganismName;

            /// <summary>
            /// Organism ID
            /// </summary>
            public int OrganismID;

            /// <summary>
            /// NCBI taxonomy ID
            /// </summary>
            public int NCBITaxonomyID;

            /// <summary>
            /// Acquisition time
            /// </summary>
            public string AcquisitionTime;

            /// <summary>
            /// Acquisition length, in minutes
            /// </summary>
            public int AcquisitionLengthMin;

            /// <summary>
            /// Number of scans
            /// </summary>
            public int NumberOfScans;

            /// <summary>
            /// Separation type
            /// </summary>
            public string SeparationType;

            /// <summary>
            /// Dataset type
            /// </summary>
            public string DatasetType;

            /// <summary>
            /// Requested run ID
            /// </summary>
            public int RequestedRunID;

            /// <summary>
            /// User list
            /// </summary>
            public List<int> UserOfRecordList;

            /// <summary>
            /// Instrument Operator EUS ID for datasets
            /// Data Package Owner for data packages
            /// </summary>
            /// <remarks>DEFAULT_EUS_OPERATOR_ID if unknown</remarks>
            public int EUSOperatorID;

            /// <summary>
            /// Default constructor to ensure default values are populated
            /// </summary>
            public UploadMetadata()
            {
                Clear();
            }

            /// <summary>
            /// Clear stored metadata
            /// </summary>
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

            /// <summary>
            /// Return the dataset ID if available, otherwise data package ID
            /// </summary>
            public override string ToString()
            {
                if (DatasetID == 0 && DataPackageID > 0)
                {
                    return "Data package " + DataPackageID;
                }

                return "Dataset " + DatasetID + ", on instrument " + DMSInstrumentName + ": " + DatasetName;
            }
        }

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

        // ReSharper disable CommentTypo
        /// <summary>
        /// When true, upload to ingestdmsdev.my.emsl.pnl.gov instead of ingestdms.my.emsl.pnl.gov
        /// </summary>
        // ReSharper restore CommentTypo
        public bool UseTestInstance { get; set; }

        /// <summary>
        /// Constructor (TransferFolderPath and JobNumber will be empty)
        /// </summary>
        /// <remarks>This constructor is used by the DataPackage Archive Manager</remarks>
        /// <param name="config">Pacifica configuration</param>
        // ReSharper disable once UnusedMember.Global
        public Upload(Configuration config) : this(config, string.Empty, string.Empty)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>The metadata.txt file will be copied to the transfer folder</remarks>
        /// <param name="config">Pacifica config</param>
        /// <param name="transferFolderPath">
        /// Transfer folder path for this dataset,
        /// for example \\proto-4\DMS3_Xfer\IFT001_Pool_17_B_10x_27Aug13_Tiger_13-07-36
        /// </param>
        /// <param name="jobNumber">DMS Data Capture job number</param>
        public Upload(Configuration config, string transferFolderPath, string jobNumber)
        {
            mPacificaConfig = config;

            // Note that EasyHttp is a static class with a static event
            // Be careful about instantiating this class (Upload) multiple times
            EasyHttp.StatusUpdate += Pacifica_StatusUpdate;

            EasyHttp.MyEMSLOffline += EasyHttp_MyEMSLOffline;

            EasyHttp.ErrorEvent += OnErrorEvent;
            EasyHttp.WarningEvent += OnWarningEvent;

            ErrorMessage = string.Empty;
            TransferFolderPath = transferFolderPath;
            JobNumber = jobNumber;
        }

        /// <summary>
        /// MyEMSL Offline event
        /// </summary>
        public event EventHandler<MessageEventArgs> MyEMSLOffline;

        /// <summary>
        /// Upload completed event
        /// </summary>
        public event EventHandler<UploadCompletedEventArgs> UploadCompleted;

        /// <summary>
        /// Status updated event
        /// </summary>
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

        /// <summary>
        /// Update the files and data tracked by metadataObject to MyEMSL
        /// </summary>
        /// <remarks>This method is used by the DataPackage Archive Manager</remarks>
        /// <param name="metadataObject"></param>
        /// <param name="statusURI">Status URL</param>
        /// <returns>True if successfully uploaded, false if an error</returns>
        // ReSharper disable once UnusedMember.Global
        public bool StartUpload(List<IUploadMetadata> metadataObject, out string statusURI)
        {
            const TarStreamUploader.UploadDebugMode debugMode = TarStreamUploader.UploadDebugMode.DebugDisabled;

            return StartUpload(metadataObject, debugMode, out statusURI);
        }

        /// <summary>
        /// Update the files and data tracked by metadataObject to MyEMSL
        /// </summary>
        /// <param name="metadataObject"></param>
        /// <param name="debugMode">
        /// Set to UploadDebugMode.CreateTarLocal to authenticate with MyEMSL, then create a .tar file locally instead of actually uploading it
        /// Set to UploadDebugMode.MyEMSLOfflineMode to create the .tar file locally without contacting MyEMSL
        /// </param>
        /// <param name="statusURI">Status URL</param>
        /// <returns>True if successfully uploaded, false if an error</returns>
        public bool StartUpload(
            List<IUploadMetadata> metadataObject,
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

            var fileList = metadataObject.GetFileListFromMetadataObject();

            // Grab the list of files from the top-level "file" object
            // Keys in this dictionary are the source file path (Windows paths); values are metadata about the file
            var fileListObject = new SortedDictionary<string, FileInfoObject>();

            foreach (var file in fileList)
            {
                var fio = new FileInfoObject(file.File, file.RelativeDestinationDirectory, file.Sha1HashHex);

                if (fileListObject.ContainsKey(file.AbsoluteLocalPath))
                {
                    // Duplicate file; skip it
                    //   This can happen if a dataset has two CacheInfo.txt files that point to the same file
                    //   For example, Dataset.mzML.gz_CacheInfo.txt referring to \\proto-11\MSXML_Cache\Mz_Refinery_1_230\2020_4\Dataset.mzML.gz

                    // Update metadataObject to remove the duplicate file
                    metadataObject.RemoveFileFromMetadataObject(file.AbsoluteLocalPath);
                    continue;
                }

                fileListObject.Add(file.AbsoluteLocalPath, fio);
            }

            // Optionally use the test instance
            mPacificaConfig.UseTestInstance = UseTestInstance;

            var jsonMetadata = JsonTools.UploadMetadataToJson(metadataObject);

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

            string serverUri;

            switch (debugMode)
            {
                case TarStreamUploader.UploadDebugMode.MyEMSLOfflineMode:
                case TarStreamUploader.UploadDebugMode.CreateTarLocal:
                    OnDebugEvent("StartUpload is creating the .tar file locally (debugMode is {0})", debugMode);
                    serverUri = "https://ServerIsOffline/dummy_page?test";
                    break;

                case TarStreamUploader.UploadDebugMode.DebugDisabled:
                    serverUri = mPacificaConfig.IngestServerUri;

                    // URL for posting the virtual .tar file to
                    // Typically: https://ingestdms.my.emsl.pnl.gov/upload
                    var storageUrl = serverUri + "/" + location;

                    OnDebugEvent("StartUpload is sending file to " + storageUrl);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(debugMode), debugMode, null);
            }

            var streamUploader = new TarStreamUploader();
            streamUploader.StatusUpdate += Pacifica_StatusUpdate;
            streamUploader.ErrorEvent += OnErrorEvent;
            streamUploader.WarningEvent += OnWarningEvent;

            var responseData = streamUploader.SendFileListToIngester(
                mPacificaConfig, location, serverUri, fileListObject, metadataFile.FullName, debugMode);

            if (debugMode != TarStreamUploader.UploadDebugMode.DebugDisabled)
            {
                // A .tar file was created locally; it was not sent to the server
                return false;
            }

            try
            {
                var responseStatus = JsonTools.JsonToTaskStatus(responseData);

                if (!responseStatus.Valid)
                {
                    throw new Exception("Response JSON did not contain a valid job_id or state.");
                }

                statusURI = mPacificaConfig.IngestServerUri + "/get_state?job_id=" + responseStatus.JobId;
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

                MyEMSLTaskStatus statusData;

                try
                {
                    statusData = JsonTools.JsonToTaskStatus(statusResult);
                }
                catch (Exception)
                {
                    OnError("Unable to parse response into JSON: " + statusResult);
                    return false;
                }

                var state = statusData.State.ToLower();

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
                    OnError("Unrecognized ingest state: " + statusData.State);
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
        public static List<IUploadMetadata> CreatePacificaMetadataObject(
            UploadMetadata uploadMetadata,
            List<FileInfoObject> filesToUpload,
            out EUSInfo eusInfo)
        {
            eusInfo = new EUSInfo();
            eusInfo.Clear();

            // new metadata object is just a list of dictionary entries
            var metadataObject = new List<IUploadMetadata>();

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
                PRISM.Logging.LogTools.LogWarning("Overriding project {0} with {1} for dataset {2}", eusInfo.EUSProjectID, DEFAULT_EUS_PROJECT_ID, uploadMetadata.DatasetName);
                eusInfo.EUSProjectID = DEFAULT_EUS_PROJECT_ID;
            }

            // Fill out Transaction Key/Value pairs
            if (uploadMetadata.DatasetID > 0)
            {
                metadataObject.AddKeyValue("omics.dms.instrument", uploadMetadata.DMSInstrumentName);
                metadataObject.AddKeyValue("omics.dms.instrument_id", eusInfo.EUSInstrumentID);
                metadataObject.AddKeyValue("omics.dms.date_code", uploadMetadata.DateCodeString);
                metadataObject.AddKeyValue("omics.dms.dataset", uploadMetadata.DatasetName);
                metadataObject.AddKeyValue("omics.dms.campaign_name", uploadMetadata.CampaignName);
                metadataObject.AddKeyValue("omics.dms.experiment_name", uploadMetadata.ExperimentName);
                metadataObject.AddKeyValue("omics.dms.dataset_name", uploadMetadata.DatasetName);
                metadataObject.AddKeyValue("omics.dms.campaign_id", uploadMetadata.CampaignID.ToString());
                metadataObject.AddKeyValue("omics.dms.experiment_id", uploadMetadata.ExperimentID.ToString());
                metadataObject.AddKeyValue("omics.dms.dataset_id", uploadMetadata.DatasetID.ToString());

                if (!string.IsNullOrEmpty(uploadMetadata.OrganismName))
                {
                    metadataObject.AddKeyValue("organism_name", uploadMetadata.OrganismName);
                }

                if (uploadMetadata.OrganismID != 0)
                {
                    metadataObject.AddKeyValue("omics.dms.organism_id", uploadMetadata.OrganismID.ToString());
                }

                if (uploadMetadata.NCBITaxonomyID != 0)
                {
                    metadataObject.AddKeyValue("ncbi_taxonomy_id", uploadMetadata.NCBITaxonomyID.ToString());
                }

                if (!string.IsNullOrEmpty(uploadMetadata.SeparationType))
                {
                    metadataObject.AddKeyValue("omics.dms.separation_type", uploadMetadata.SeparationType);
                }

                if (!string.IsNullOrEmpty(uploadMetadata.DatasetType))
                {
                    metadataObject.AddKeyValue("omics.dms.dataset_type", uploadMetadata.DatasetType);
                }

                metadataObject.AddKeyValue("omics.dms.run_acquisition_length_min", uploadMetadata.AcquisitionLengthMin);

                if (uploadMetadata.UserOfRecordList.Count > 0)
                {
                    foreach (var userId in uploadMetadata.UserOfRecordList)
                    {
                        metadataObject.AddKeyValue("User of Record", userId.ToString());
                        metadataObject.AddKeyValue("user_of_record", userId.ToString());
                    }
                }
            }
            else if (uploadMetadata.DataPackageID > 0)
            {
                metadataObject.AddKeyValue("omics.dms.instrument", uploadMetadata.DMSInstrumentName);
                metadataObject.AddKeyValue("omics.dms.instrument_id", eusInfo.EUSInstrumentID);
                metadataObject.AddKeyValue("omics.dms.datapackage_id", uploadMetadata.DataPackageID.ToString());
            }
            else
            {
                throw new Exception("Must define a non-zero DatasetID or a DataPackageID; cannot create the metadata object");
            }

            // Append the required metadata
            //  Transactions.instrument
            //  Transactions.project
            //  Transactions.submitter
            metadataObject.AddValue("instrument", eusInfo.EUSInstrumentID);
            metadataObject.AddValue("project", eusInfo.EUSProjectID);
            metadataObject.AddValue("submitter", eusInfo.EUSUploaderID);

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

                metadataObject.AddFile(file, subDirString);
            }

            return metadataObject;
        }

        /// <summary>
        /// Return a string description of the EUS info encoded by metadataObject
        /// </summary>
        /// <param name="metadataObject"></param>
        public static string GetMetadataObjectDescription(List<IUploadMetadata> metadataObject)
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
                if (!item.Valid)
                {
                    continue;
                }

                switch (item)
                {
                    case UploadMetadataKeyValue kv:
                        if (!GetDictionaryValue(kvLookup, kv.Key, out var kvValueDescription))
                        {
                            continue;
                        }

                        metadataList.Add(kvValueDescription + "=" + kv.Value);
                        matchedKeys.Add(kvValueDescription);
                        break;

                    case UploadMetadataFile:
                        fileCount++;
                        break;

                    case UploadMetadataValue v:
                        if (!transactionValueLookup.TryGetValue(item.DestinationTable, out var valueDescription))
                        {
                            continue;
                        }

                        if (matchedKeys.Contains(valueDescription))
                        {
                            // This item has already been added (typically EUS_Instrument_ID)
                            continue;
                        }

                        // Include the value for this item in the description
                        metadataList.Add(valueDescription + "=" + v.Value);
                        matchedKeys.Add(valueDescription);
                        break;

                    default:
                        // Unknown item type
                        break;
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
        private static int GetEUSInstrumentID(int eusInstrumentId, int instrumentIdIfUnknown)
        {
            return eusInstrumentId <= 0 ? instrumentIdIfUnknown : eusInstrumentId;
        }

        /// <summary>
        /// Validate the EUS project ID, or use the default
        /// </summary>
        /// <remarks>This is a string because the project ID may contain suffix letters</remarks>
        /// <param name="eusProjectId"></param>
        /// <param name="eusProjectIdIfUnknown"></param>
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
    }
}
