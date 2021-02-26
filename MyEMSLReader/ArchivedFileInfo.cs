using System;
using System.IO;

namespace MyEMSLReader
{
    public class ArchivedFileInfo
    {
        public const string SHA1 = "sha1";

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

        /// <summary>
        /// Filename
        /// </summary>
        public string Filename
        {
            get;
        }

        /// <summary>
        /// MyEMSL File ID
        /// </summary>
        public long FileID
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

        /// <summary>
        /// File size, in bytes
        /// </summary>
        public long FileSizeBytes
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
        /// This will typically be sha1, but could be md5
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
        /// Path to the file, relative to the dataset directory
        /// </summary>
        /// <remarks>Uses Unix-style slashes</remarks>
        public string RelativePathUnix => RelativePathWindows.Replace(@"\", "/");

        /// <summary>
        /// Path to the file, relative to the dataset directory
        /// </summary>
        /// <remarks>Uses Windows-style slashes</remarks>
        public string RelativePathWindows
        {
            get
            {
                if (string.IsNullOrWhiteSpace(SubDirPath))
                {
                    return Filename;
                }

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
                {
                    return Hash;
                }

                return string.Empty;
            }
        }

        /// <summary>
        /// SubDirectory path of the file, as reported by MyEMSL
        /// This path will have unix-style slashes ('/') if there are subdirectories
        /// </summary>
        /// <remarks>The SubDirPath for Data package files will always start with the data package directory name</remarks>
        public string SubDirPath
        {
            get;
        }

        /// <summary>
        /// UTC time that the file was submitted (as a string)
        /// </summary>
        public string SubmissionTime
        {
            get;
            set;
        }

        /// <summary>
        /// UTC time that the file was submitted
        /// </summary>
        public DateTime SubmissionTimeValue
        {
            get
            {
                if (string.IsNullOrWhiteSpace(SubmissionTime))
                {
                    return DateTime.Now;
                }

                if (DateTime.TryParse(SubmissionTime, out var dtSubmissionTime))
                {
                    return dtSubmissionTime;
                }

                return DateTime.Now;
            }
        }

        /// <summary>
        /// Submission time in ODBC 24 hour format
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public string SubmissionTimeODBC => SubmissionTimeValue.ToString("yyyy-MM-dd HH:mm:ss");

        /// <summary>
        /// Submission time in ODBC 12 hour format
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public string SubmissionTimeODBC12hr => SubmissionTimeValue.ToString("yyyy-MM-dd hh:mm:ss tt");

        /// <summary>
        /// MyEMSL transaction ID for this specific file
        /// </summary>
        /// <remarks>Incremented for every newly uploaded bundle (.tar file), and thus a good substitute for dates when comparing two files to see which is newer</remarks>
        public long TransactionID
        {
            get;
            set;
        }

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
        public ArchivedFileInfo(string dataset, string filename, string subDirPath, long fileID) :
            this(dataset, filename, subDirPath, fileID, "", "")
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
        public ArchivedFileInfo(
            string dataset,
            string filename,
            string subDirPath,
            long fileID,
            string instrument,
            string datasetYearQuarter)
        {
            Dataset = dataset;
            Filename = filename;
            SubDirPath = subDirPath;
            FileID = fileID;
            Instrument = instrument;
            DatasetYearQuarter = datasetYearQuarter;
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
                {
                    FileCreationTime = dtCreationTime;
                }
            }

            if (!string.IsNullOrWhiteSpace(lastWriteTime))
            {
                if (DateTime.TryParse(lastWriteTime, out var dtLastWriteTime))
                {
                    FileLastWriteTime = dtLastWriteTime;
                }
            }
        }

        public override string ToString()
        {
            return RelativePathWindows;
        }
    }
}
