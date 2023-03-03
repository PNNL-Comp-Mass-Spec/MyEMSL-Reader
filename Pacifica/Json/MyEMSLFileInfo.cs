using System;
using System.ComponentModel;
using System.IO;
using Newtonsoft.Json;

namespace Pacifica.Json
{
    /// <summary>
    /// Metadata for files in MyEMSL
    /// </summary>
    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public class MyEMSLFileInfo
    {
        // Ignore Spelling: absolutelocalpath, ctime, deserialization, hashsum, hashtype, mimetype, mtime, subdir, subfolder

        /// <summary>
        /// True if essential values are set.
        /// </summary>
        public bool Valid => !string.IsNullOrWhiteSpace(Filename) && FileID > 0 && !string.IsNullOrWhiteSpace(HashSum);

        /// <summary>
        /// Filename
        /// </summary>
        [JsonProperty("name")]
        [DefaultValue("")]
        public string Filename { get; }

        /// <summary>
        /// File ID in MyEMSL
        /// </summary>
        [JsonProperty("_id")]
        public long FileID { get; }

        /// <summary>
        /// File hash (typically SHA-1)
        /// </summary>
        [JsonProperty("hashsum")]
        [DefaultValue("")]
        public string HashSum { get; }

        /// <summary>
        /// Hash type
        /// </summary>
        [JsonProperty("hashtype")]
        [DefaultValue("")]
        public string HashType { get; set; }

        /// <summary>
        /// Time created in MyEMSL
        /// </summary>
        /// <remarks>Use UpdateRemoteFileTimes to update this value</remarks>
        [JsonProperty("created")]
        public DateTime Created { get; protected set; }

        /// <summary>
        /// Time updated in MyEMSL (typically identical to Created)
        /// </summary>
        /// <remarks>Use UpdateRemoteFileTimes to update this value</remarks>
        [JsonProperty("updated")]
        public DateTime? Updated { get; protected set; }

        /// <summary>
        /// Time deleted in MyEMSL; null if not deleted
        /// </summary>
        /// <remarks>Use UpdateRemoteFileTimes to update this value</remarks>
        [JsonProperty("deleted")]
        public DateTime? Deleted { get; protected set; }

        /// <summary>
        /// Original file creation time (on the host system prior to MyEMSL ingest)
        /// </summary>
        /// <remarks>Use UpdateSourceFileTimes to update this value</remarks>
        [JsonProperty("ctime")]
        public DateTime FileCreationTime { get; protected set; }

        /// <summary>
        /// Last modification time (on the host system prior to MyEMSL ingest)
        /// </summary>
        /// <remarks>Use UpdateSourceFileTimes to update this value</remarks>
        [JsonProperty("mtime")]
        public DateTime FileLastWriteTime { get; protected set; }

        /// <summary>
        /// File size, in bytes
        /// </summary>
        [JsonProperty("size")]
        [DefaultValue("0")]
        public long Size { get; set; }

        /// <summary>
        /// Subdirectory (subfolder) below the dataset folder
        /// </summary>
        [JsonProperty("subdir")]
        [DefaultValue("")]
        public string SubDir { get; set; }

        /// <summary>
        /// Transaction ID
        /// </summary>
        /// <remarks>All files uploaded in a given batch will have the same transaction ID</remarks>
        [JsonProperty("transaction_id")]
        public long TransactionId { get; set; }

        /// <summary>
        /// Dataset Name
        /// </summary>
        public string Dataset { get; set; }

        /// <summary>
        /// DatasetID
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public int DatasetID { get; set; }

        /// <summary>
        /// Instrument name for this file
        /// </summary>
        /// <remarks>Will be blank for data package files</remarks>
        public string Instrument { get; set; }

        /// <summary>
        /// Dataset year-quarter string, for example 2013_3
        /// </summary>
        /// <remarks>Will be blank for data package files</remarks>
        public string DatasetYearQuarter { get; set; }

        /// <summary>
        /// Relative path to the file, including the dataset name
        /// </summary>
        /// <remarks>Uses Windows-style slashes</remarks>
        public string PathWithDataset
        {
            get
            {
                var fullPath = string.Empty;

                if (!string.IsNullOrWhiteSpace(Dataset))
                {
                    fullPath = Path.Combine(fullPath, Dataset);
                }

                return Path.Combine(fullPath, RelativePathWindows).Replace("/", @"\");
            }
        }

        /// <summary>
        /// Relative path to the file, including the dataset name
        /// </summary>
        /// <remarks>Uses Unix-style slashes</remarks>
        // ReSharper disable once UnusedMember.Global
        public string PathWithDatasetUnix => PathWithDataset.Replace(@"\", "/");

        /// <summary>
        /// Relative path to the file, including the instrument, year_quarter, and dataset
        /// </summary>
        /// <remarks>Uses Unix-style slashes</remarks>
        // ReSharper disable once UnusedMember.Global
        public string PathWithInstrumentAndDatasetUnix => PathWithInstrumentAndDatasetWindows.Replace(@"\", "/");

        /// <summary>
        /// Relative path to the file, including the instrument, year_quarter, and dataset
        /// </summary>
        /// <remarks>
        /// Uses Windows-style slashes.
        /// Note that instrument IMS_TOF_4 was renamed to IMS04_AgTOF05 in 2013, and thus
        /// there are datasets with files associated with both instruments in MyEMSL</remarks>
        public string PathWithInstrumentAndDatasetWindows
        {
            get
            {
                var fullPath = string.Empty;
                if (!string.IsNullOrWhiteSpace(Instrument))
                {
                    fullPath = Path.Combine(fullPath, Instrument);
                }

                if (!string.IsNullOrWhiteSpace(DatasetYearQuarter))
                {
                    fullPath = Path.Combine(fullPath, DatasetYearQuarter);
                }

                if (!string.IsNullOrWhiteSpace(Dataset))
                {
                    fullPath = Path.Combine(fullPath, Dataset);
                }

                return Path.Combine(fullPath, RelativePathWindows).Replace("/", @"\");
            }
        }

        /// <summary>
        /// Path to the file, relative to the dataset folder
        /// </summary>
        /// <remarks>Uses Windows-style slashes</remarks>
        public string RelativePathWindows
        {
            get
            {
                if (string.IsNullOrWhiteSpace(SubDir))
                {
                    return Filename;
                }

                return Path.Combine(SubDir, Filename).Replace("/", @"\");
            }
        }

        /// <summary>
        /// Private constructor that can be accessed for deserialization
        /// </summary>
        private MyEMSLFileInfo()
        {
            Filename = string.Empty;
            FileID = 0;
            HashSum = string.Empty;
            HashType = string.Empty;

            SubDir = string.Empty;
            Dataset = string.Empty;

            Instrument = string.Empty;
            DatasetYearQuarter = string.Empty;
        }

        /// <summary>
        /// Constructor for use during JSON deserialization
        /// </summary>
        /// <remarks>
        /// MyEMSL file ID must be "_id" to match JsonProperty value "_id", which corresponds to a key name in the JSON returned by https://metadata.my.emsl.pnl.gov/
        /// </remarks>
        /// <param name="name"></param>
        /// <param name="_id">MyEMSL file ID</param>
        /// <param name="hashsum"></param>
        /// <param name="created"></param>
        [JsonConstructor]
        private MyEMSLFileInfo(string name, long _id, string hashsum, DateTime created) : this()
        {
            Filename = name ?? string.Empty;
            FileID = _id;
            HashSum = hashsum ?? string.Empty;
            Created = created;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="fileId"></param>
        /// <param name="hashSum"></param>
        public MyEMSLFileInfo(string fileName, long fileId, string hashSum) : this()
        {
            Filename = fileName;
            FileID = fileId;
            HashSum = hashSum;
        }

        /// <summary>
        /// Update times the file was created, updated, and optionally deleted in MyEMSL
        /// </summary>
        /// <param name="createdInMyEMSL"></param>
        /// <param name="updatedInMyEMSL"></param>
        /// <param name="deletedInMyEMSL"></param>
        public void UpdateRemoteFileTimes(string createdInMyEMSL, string updatedInMyEMSL, string deletedInMyEMSL = "")
        {
            if (!string.IsNullOrWhiteSpace(createdInMyEMSL))
            {
                if (DateTime.TryParse(createdInMyEMSL, out var creationTime))
                {
                    Created = creationTime;
                }
            }

            if (!string.IsNullOrWhiteSpace(updatedInMyEMSL))
            {
                if (DateTime.TryParse(updatedInMyEMSL, out var updateTime))
                {
                    Updated = updateTime;
                }
            }

            if (!string.IsNullOrWhiteSpace(deletedInMyEMSL))
            {
                if (DateTime.TryParse(deletedInMyEMSL, out var deletionTime))
                {
                    Deleted = deletionTime;
                }
            }
        }

        /// <summary>
        /// Update file creation and modification times using the string values reported by MyEMSL
        /// </summary>
        /// <param name="creationTimeText"></param>
        /// <param name="lastWriteTimeText"></param>
        public void UpdateSourceFileTimes(string creationTimeText, string lastWriteTimeText)
        {
            if (!string.IsNullOrWhiteSpace(creationTimeText))
            {
                if (DateTime.TryParse(creationTimeText, out var creationTime))
                {
                    FileCreationTime = creationTime;
                }
            }

            if (!string.IsNullOrWhiteSpace(lastWriteTimeText))
            {
                if (DateTime.TryParse(lastWriteTimeText, out var lastWriteTime))
                {
                    FileLastWriteTime = lastWriteTime;
                }
            }
        }

        /// <summary>
        /// Return the relative file path
        /// </summary>
        public override string ToString()
        {
            return RelativePathWindows;
        }
    }
}
