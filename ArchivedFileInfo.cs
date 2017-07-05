using System;
using System.Collections.Generic;
using System.IO;

namespace MyEMSLReader
{
    public class ArchivedFileInfo
    {
        public const string SHA1 = "sha1";

        #region "Auto Properties"

        /// <summary>
        /// Dataset name for this file
        /// </summary>
        /// <remarks>Will be blank for data package files</remarks>
        public string Dataset
        {
            get;
        }

        /// <summary>
        /// Data Package ID for this file
        /// </summary>
        /// <remarks>A file should only have a non-zero DatasetID or a non-zero DataPackageID, not both</remarks>
        public int DataPackageID
        {
            get;
            set;
        }

        /// <summary>
        /// Dataset ID for this file
        /// </summary>
        /// <remarks>A file should only have a non-zero DatasetID or a non-zero DataPackageID, not both</remarks>
        public int DatasetID
        {
            get;
            set;
        }

        /// <summary>
        /// Dataset year-quarter string, for example 2013_3
        /// </summary>
        /// <remarks>Will be blank for data package files</remarks>
        public string DatasetYearQuarter
        {
            get;
            set;
        }

        public string Filename
        {
            get;
        }

        public Int64 FileID
        {
            get;
            set;
        }

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

        public Int64 FileSizeBytes
        {
            get;
            set;
        }

        /// <summary>
        /// Sha-1 or MD5 hash
        /// </summary>
        /// <remarks>Typically a Sha-1 hash</remarks>
        public string Hash
        {
            get;
            set;
        }

        /// <summary>
        /// Either sha1 or md5
        /// </summary>
        public string HashType { get; set; }

        /// <summary>
        /// Instrument name for this file
        /// </summary>
        /// <remarks>Will be blank for data package files</remarks>
        public string Instrument
        {
            get;
            set;
        }

        public bool IsPublicFile
        {
            get;
            set;
        }

        /// <summary>
        /// Dictionary object with the detailed information reported by MyEMSL for this file
        /// Keys are strings while values could be a string, generic list, or even another dictionary (with string keys and object values)
        /// </summary>
        public Dictionary<string, object> Metadata
        {
            get;
            private set;
        }

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
                    fullPath = Path.Combine(fullPath, Dataset);

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
                    fullPath = Path.Combine(fullPath, Instrument);

                if (!string.IsNullOrWhiteSpace(DatasetYearQuarter))
                    fullPath = Path.Combine(fullPath, DatasetYearQuarter);

                if (!string.IsNullOrWhiteSpace(Dataset))
                    fullPath = Path.Combine(fullPath, Dataset);

                return Path.Combine(fullPath, RelativePathWindows).Replace("/", @"\");
            }
        }

        /// <summary>
        /// Path to the file, relative to the dataset folder
        /// </summary>
        /// <remarks>Uses Unix-style slashes</remarks>
        public string RelativePathUnix => RelativePathWindows.Replace(@"\", "/");

        /// <summary>
        /// Path to the file, relative to the dataset folder
        /// </summary>
        /// <remarks>Uses Windows-style slashes</remarks>
        public string RelativePathWindows
        {
            get
            {
                if (string.IsNullOrWhiteSpace(SubDirPath))
                    return Filename;

                return Path.Combine(SubDirPath, Filename).Replace("/", @"\");
            }
        }

        /// <summary>
        /// Sha-1 hash, provided Hashtype is "sha1" or empty
        /// </summary>
        public string Sha1Hash
        {
            get
            {
                if (string.IsNullOrEmpty(HashType) || HashType.Equals(SHA1))
                    return Hash;

                return string.Empty;
            }
        }

        /// <summary>
        /// SubDirectory path of the file, as reported by MyEMSL
        /// This path will have unix-style slashes ('/') if there are sub-folders
        /// </summary>
        /// <remarks>The SubDirPath for Data package files will always start with the data package folder name</remarks>
        public string SubDirPath
        {
            get;
        }

        public string SubmissionTime
        {
            get;
            set;
        }

        public DateTime SubmissionTimeValue
        {
            get
            {
                if (string.IsNullOrWhiteSpace(SubmissionTime))
                    return DateTime.Now;

                DateTime dtSubmissionTime;
                if (DateTime.TryParse(SubmissionTime, out dtSubmissionTime))
                    return dtSubmissionTime;

                return DateTime.Now;
            }
        }

        // ReSharper disable once UnusedMember.Global
        public string SubmissionTimeODBC => SubmissionTimeValue.ToString("yyyy-MM-dd HH:mm:ss");

        // ReSharper disable once UnusedMember.Global
        public string SubmissionTimeODBC12hr => SubmissionTimeValue.ToString("yyyy-MM-dd hh:mm:ss tt");

        /// <summary>
        /// MyEMSL transaction ID for this specific file
        /// </summary>
        /// <remarks>Incremented for every newly uploaded bundle (.tar file), and thus a good substitute for dates when comparing two files to see which is newer</remarks>
        public Int64 TransactionID
        {
            get;
            set;
        }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dataset">Dataset name</param>
        /// <param name="filename">Filename</param>
        /// <param name="subDirPath">Subdirectory below dataset (empty if at the dataset level)</param>
        public ArchivedFileInfo(string dataset, string filename, string subDirPath) :
            this(dataset, filename, subDirPath, 0)
        { }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dataset">Dataset name</param>
        /// <param name="filename">Filename</param>
        /// <param name="subDirPath">Subdirectory below dataset (empty if at the dataset level)</param>
        /// <param name="fileID">MyEMSL File ID</param>
        public ArchivedFileInfo(string dataset, string filename, string subDirPath, Int64 fileID) :
            this(dataset, filename, subDirPath, fileID, "", "", new Dictionary<string, object>())
        { }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dataset">Dataset name</param>
        /// <param name="filename">Filename</param>
        /// <param name="subDirPath">Subdirectory below dataset (empty if at the dataset level)</param>
        /// <param name="fileID">MyEMSL File ID</param>
        /// <param name="instrument">Instrument name</param>
        /// <param name="datasetYearQuarter">Dataset year quarter, e.g. 2013_3</param>
        /// <param name="dctMetadata">Metadata dictionary</param>
        public ArchivedFileInfo(
            string dataset,
            string filename,
            string subDirPath,
            Int64 fileID,
            string instrument,
            string datasetYearQuarter,
            Dictionary<string, object> dctMetadata)
        {
            Dataset = dataset;
            Filename = filename;
            SubDirPath = subDirPath;
            FileID = fileID;
            Instrument = instrument;
            DatasetYearQuarter = datasetYearQuarter;
            Metadata = dctMetadata;
        }

        /// <summary>
        /// Update file creation and modification times using the string values reported by MyEMSL
        /// </summary>
        /// <param name="creationTime"></param>
        /// <param name="lastWriteTime"></param>
        public void UpdateSourceFileTimes(string creationTime, string lastWriteTime)
        {
            if (!string.IsNullOrWhiteSpace(creationTime))
            {
                if (DateTime.TryParse(creationTime, out var dtCreationTime))
                    FileCreationTime = dtCreationTime;
            }

            if (!string.IsNullOrWhiteSpace(lastWriteTime))
            {
                if (DateTime.TryParse(lastWriteTime, out var dtLastWriteTime))
                    FileLastWriteTime = dtLastWriteTime;
            }
        }

        public override string ToString()
        {
            return RelativePathWindows;
        }
    }
}
