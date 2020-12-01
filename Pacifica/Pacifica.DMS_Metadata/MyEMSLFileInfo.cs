using System;
using System.IO;

namespace Pacifica.DMS_Metadata
{
    /// <summary>
    /// Metadata for files in MyEMSL
    /// </summary>
    public class MyEMSLFileInfo
    {
        /// <summary>
        /// Filename
        /// </summary>
        public string Filename { get; }

        /// <summary>
        /// File ID in MyEMSL
        /// </summary>
        public long FileID { get; }

        /// <summary>
        /// File hash (typically SHA-1)
        /// </summary>
        public string HashSum { get; }

        /// <summary>
        /// Hash type
        /// </summary>
        public string HashType { get; set; }

        /// <summary>
        /// Time created in MyEMSL
        /// </summary>
        /// <remarks>Use UpdateRemoteFileTimes to update this value</remarks>
        public DateTime Created { get; private set; }

        /// <summary>
        /// Time updated in MyEMSL (typically identical to Created)
        /// </summary>
        /// <remarks>Use UpdateRemoteFileTimes to update this value</remarks>
        public DateTime? Updated { get; private set; }

        /// <summary>
        /// Time deleted in MyEMSL; null if not deleted
        /// </summary>
        /// <remarks>Use UpdateRemoteFileTimes to update this value</remarks>
        public DateTime? Deleted { get; private set; }

        /// <summary>
        /// Original file creation time (on the host system prior to MyEMSL ingest)
        /// </summary>
        /// <remarks>Use UpdateSourceFileTimes to update this value</remarks>
        public DateTime FileCreationTime { get; private set; }

        /// <summary>
        /// Last modification time (on the host system prior to MyEMSL ingest)
        /// </summary>
        /// <remarks>Use UpdateSourceFileTimes to update this value</remarks>
        public DateTime FileLastWriteTime { get; private set; }

        /// <summary>
        /// File size, in bytes
        /// </summary>
        public long Size { get; set; }

        /// <summary>
        /// Subdirectory (subfolder) below the dataset folder
        /// </summary>
        public string SubDir { get; set; }

        /// <summary>
        /// Transaction ID
        /// </summary>
        /// <remarks>All files uploaded in a given batch will have the same transaction ID</remarks>
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
        /// Constructor
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="fileId"></param>
        /// <param name="hashSum"></param>
        public MyEMSLFileInfo(string fileName, long fileId, string hashSum)
        {
            Filename = fileName;
            FileID = fileId;
            HashSum = hashSum;
            HashType = string.Empty;

            SubDir = string.Empty;
            Dataset = string.Empty;

            Instrument = string.Empty;
            DatasetYearQuarter = string.Empty;
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

        public override string ToString()
        {
            return RelativePathWindows;
        }
    }
}
