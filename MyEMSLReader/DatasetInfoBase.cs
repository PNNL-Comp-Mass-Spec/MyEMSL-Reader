using System;
using System.Globalization;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PRISM;

namespace MyEMSLReader
{
    /// <summary>
    /// Tracks the file info for one or more datasets or data packages
    /// </summary>
    public abstract class DatasetInfoBase : EventNotifier
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: acyl, Args, dest, oxoacyl, ser, struct, svc, wildcards

        // ReSharper restore CommentTypo

        /// <summary>
        /// MyEMSL ID flag
        /// </summary>
        // ReSharper disable once IdentifierTypo
        public const string MYEMSL_FILE_ID_TAG = "@MyEMSLID_";

        private const int CACHE_REFRESH_THRESHOLD_MINUTES = 5;

        /// <summary>
        /// Error messages
        /// </summary>
        protected readonly List<string> mErrorMessages;

        /// <summary>
        /// List of files in MyEMSL
        /// </summary>
        protected List<ArchivedFileInfo> mArchivedFiles;

        /// <summary>
        /// Timestamp when metadata was retrieved
        /// </summary>
        protected DateTime mCacheDate;

        /// <summary>
        /// True if the cached metadata is out of date
        /// </summary>
        protected bool mCacheIsStale;

        /// <summary>
        /// MyEMSL Reader
        /// </summary>
        protected readonly Reader mReader;

        /// <summary>
        /// Tracks information on files to download
        /// </summary>
        protected readonly DownloadQueue mDownloadQueue;

        private bool mUseTestInstance;

        /// <summary>
        /// The most recently downloaded files; keys are the full paths to the downloaded file, values are extended file info
        /// </summary>
        protected readonly Dictionary<string, ArchivedFileInfo> mDownloadedFiles;

        // Do not search for * or ? because we treat those as filename wildcards
        private readonly Regex mReplaceReservedRegExChars = new(@"(?<Symbol>[\^\$\.\|\+\(\)\[\{\\])", RegexOptions.Compiled);

        /// <summary>
        /// The most recently downloaded files; keys are the full paths to the downloaded file, values are extended file info
        /// </summary>
        /// <remarks>Keys are the full paths to the downloaded file, values are extended file info</remarks>
        // ReSharper disable once UnusedMember.Global
        public Dictionary<string, ArchivedFileInfo> DownloadedFiles => mDownloadQueue.DownloadedFiles;

        /// <summary>
        /// List of error messages
        /// </summary>
        /// <remarks>The messages are cleared by when these functions are called: ProcessDownloadQueue and RefreshInfo</remarks>
        /// ReSharper disable once UnusedMember.Global
        public List<string> ErrorMessages => mErrorMessages;

        /// <summary>
        /// MyEMSL IDs of files queued to be downloaded
        /// </summary>
        /// <remarks>Keys are MyEMSL File IDs, values are struct FileDownloadInfo</remarks>
        /// ReSharper disable once UnusedMember.Global
        public Dictionary<long, DownloadQueue.FileDownloadInfo> FilesToDownload => mDownloadQueue.FilesToDownload;

        /// <summary>
        /// When True, include all revisions of files that were imported to MyEMSL multiple times
        /// When False, only reports the newest version of a file
        /// </summary>
        /// <remarks>Default is False</remarks>
        public bool IncludeAllRevisions
        {
            get => mReader.IncludeAllRevisions;
            set
            {
                mReader.IncludeAllRevisions = value;
                mDownloadQueue.IncludeAllRevisions = value;
            }
        }

        /// <summary>
        /// When true, raise a DebugEvent prior to contacting the metadata server
        /// </summary>
        public bool ThrowErrors { get; set; }

        /// <summary>
        /// When true, raise a DebugEvent prior to contacting the metadata server
        /// </summary>
        public bool ReportMetadataURLs
        {
            get => mReader.ReportMetadataURLs;
            set => mReader.ReportMetadataURLs = value;
        }

        /// <summary>
        /// When true, use Debug Events to show additional information
        /// </summary>
        public bool TraceMode
        {
            get => mReader.TraceMode;
            set => mReader.TraceMode = value;
        }

        /// <summary>
        /// When False use https://my.emsl.pnl.gov/myemsl/elasticsearch/simple_items
        /// When True use  https://test0.my.emsl.pnl.gov/myemsl/search/simple/index.shtml
        /// </summary>
        public bool UseTestInstance
        {
            get => mUseTestInstance;
            set
            {
                mUseTestInstance = value;
                mDownloadQueue.UseTestInstance = value;
                mReader.UseTestInstance = value;
            }
        }

        /// <summary>
        /// Base dataset info
        /// </summary>
        protected DatasetInfoBase()
        {
            mErrorMessages = new List<string>();

            mReader = new Reader
            {
                IncludeAllRevisions = false
            };

            RegisterEvents(mReader);

            mReader.MyEMSLOffline += Reader_MyEMSLOffline;

            mArchivedFiles = new List<ArchivedFileInfo>();

            mDownloadedFiles = new Dictionary<string, ArchivedFileInfo>(StringComparer.OrdinalIgnoreCase);

            mDownloadQueue = new DownloadQueue
            {
                ThrowErrors = ThrowErrors
            };

            RegisterEvents(mDownloadQueue);

            // Attach events
            mDownloadQueue.FileDownloadedEvent += OnFileDownloadedEvent;
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="fileInfo">Archive File Info</param>
        public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo)
        {
            mDownloadQueue.AddFileToDownloadQueue(fileInfo, unzipRequired: false);
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="fileInfo">Archive File Info</param>
        /// <param name="unzipRequired">
        /// True if the file will need to be unzipped after the download
        /// (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)
        /// </param>
        public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo, bool unzipRequired)
        {
            mDownloadQueue.AddFileToDownloadQueue(fileInfo, unzipRequired);
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <remarks>fileInfo can be null if unzipRequired is false</remarks>
        /// <param name="myEMSLFileID">MyEMSL File ID</param>
        /// <param name="fileInfo">Archive File Info</param>
        /// <param name="unzipRequired">
        /// True if the file will need to be unzipped after the download
        /// (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)
        /// </param>
        public void AddFileToDownloadQueue(long myEMSLFileID, ArchivedFileInfo fileInfo, bool unzipRequired)
        {
            mDownloadQueue.AddFileToDownloadQueue(myEMSLFileID, fileInfo, unzipRequired);
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <remarks>fileInfo can be null if unzipRequired is false</remarks>
        /// ReSharper disable once UnusedMember.Global
        /// <param name="myEMSLFileID">MyEMSL File ID</param>
        /// <param name="fileInfo">Archive File Info</param>
        /// <param name="unzipRequired">True if the file will need to be unzipped after the download (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)</param>
        /// <param name="destFilePath">Explicit destination file path</param>
        public void AddFileToDownloadQueue(long myEMSLFileID, ArchivedFileInfo fileInfo, bool unzipRequired, string destFilePath)
        {
            mDownloadQueue.AddFileToDownloadQueue(myEMSLFileID, fileInfo, unzipRequired, destFilePath);
        }

        /// <summary>
        /// Appends the MyEMSL File ID tag to a given file path
        /// </summary>
        /// <param name="filePath">Path to which the MyEMSL FileID should be appended</param>
        /// <param name="myEmslFileID">MyEMSL File ID</param>
        /// <returns>New path, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw@MyEMSLID_84327</returns>
        /// ReSharper disable once UnusedMember.Global
        public static string AppendMyEMSLFileID(string filePath, long myEmslFileID)
        {
            return filePath + MYEMSL_FILE_ID_TAG + myEmslFileID.ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Verify that svc-dms.pfx exists either in the same directory as Pacifica.core.dll, C:\DMS_Programs\client_certs, or at C:\client_certs\
        /// </summary>
        /// <param name="errorMessage">Output: error message, indicating the paths that were checked</param>
        /// <returns>True if the file is found, otherwise false</returns>
        /// ReSharper disable once UnusedMember.Global
        public bool CertificateFileExists(out string errorMessage)
        {
            return mReader.CertificateFileExists(out errorMessage);
        }

        /// <summary>
        /// Clear the download queue
        /// </summary>
        public void ClearDownloadQueue()
        {
            mDownloadQueue.Clear();
            mDownloadedFiles.Clear();
        }

        /// <summary>
        /// Parses a path that contains the MyEMSL FileID tag
        /// </summary>
        /// <param name="filePath">Path to parse, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw@MyEMSLID_84327</param>
        /// <returns>MyEMSL File ID if successfully parsed, 0 if not present or a problem</returns>
        /// ReSharper disable once UnusedMember.Global
        public static long ExtractMyEMSLFileID(string filePath)
        {
            return ExtractMyEMSLFileID(filePath, out _);
        }

        /// <summary>
        /// Parses a path that contains the MyEMSL FileID tag
        /// </summary>
        /// <param name="filePath">Path to parse, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw@MyEMSLID_84327</param>
        /// <param name="newFilePath">Path with the MyEMSL FileID tag removed, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw</param>
        /// <returns>MyEMSL File ID if successfully parsed, 0 if not present or a problem</returns>
        public static long ExtractMyEMSLFileID(string filePath, out string newFilePath)
        {
            var charIndex = filePath.LastIndexOf(MYEMSL_FILE_ID_TAG, StringComparison.Ordinal);
            newFilePath = string.Copy(filePath);

            if (charIndex > 0)
            {
                newFilePath = filePath.Substring(0, charIndex);

                var myEmslFileIdText = filePath.Substring(charIndex + MYEMSL_FILE_ID_TAG.Length);

                if (long.TryParse(myEmslFileIdText, out var myEmslFileID))
                {
                    return myEmslFileID;
                }
            }

            return 0;
        }

        /// <summary>
        /// Looks for the given file, returning any matches as a list
        /// </summary>
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <returns>List of matching files</returns>
        public List<DatasetDirectoryOrFileInfo> FindFiles(string fileName)
        {
            var subdirectoryName = string.Empty;
            var datasetName = string.Empty;
            return FindFiles(fileName, subdirectoryName, datasetName, recurse: true);
        }

        /// <summary>
        /// Looks for the given file in all datasets added using AddDataset(), returning any matches as a list
        /// </summary>
        /// <remarks>subdirectoryName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subdirectoryName">Subdirectory in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <returns>List of matching files</returns>
        public List<DatasetDirectoryOrFileInfo> FindFiles(string fileName, string subdirectoryName)
        {
            var datasetName = string.Empty;
            return FindFiles(fileName, subdirectoryName, datasetName, recurse: true);
        }

        /// <summary>
        /// Looks for the given file, returning any matches as a list
        /// </summary>
        /// <remarks>subdirectoryName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subdirectoryName">Subdirectory in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="recurse">True to search all subdirectories; false to only search the root directory (or only subdirectoryName)</param>
        /// <param name="fileSplit">Set to True if fileName contains a list of file names (or file specs) separated by a semicolon</param>
        /// <returns>List of matching files</returns>
        public List<DatasetDirectoryOrFileInfo> FindFiles(string fileName, string subdirectoryName, bool recurse, bool fileSplit = false)
        {
            var datasetName = string.Empty;
            return FindFiles(fileName, subdirectoryName, datasetName, recurse, fileSplit);
        }

        /// <summary>
        /// Looks for the given file, returning any matches as a list
        /// </summary>
        /// <remarks>subdirectoryName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        /// ReSharper disable once UnusedMember.Global
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subdirectoryName">Subdirectory in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="datasetName">Dataset name filter</param>
        /// <returns>List of matching files</returns>
        public List<DatasetDirectoryOrFileInfo> FindFiles(string fileName, string subdirectoryName, string datasetName)
        {
            return FindFiles(fileName, subdirectoryName, datasetName, recurse: true);
        }

        /// <summary>
        /// Looks for the given file, returning any matches as a list
        /// </summary>
        /// <remarks>subdirectoryName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        /// <param name="fileName">
        /// File name to find; can contain a wildcard, e.g. *.zip
        /// Separate multiple values using a vertical bar, e.g. analysis.baf|ser
        /// </param>
        /// <param name="subdirectoryName">Subdirectory in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="datasetName">Dataset name filter</param>
        /// <param name="recurse">True to search all subdirectories; false to only search the root directory (or only subdirectoryName)</param>
        /// <param name="fileSplit">Set to True if fileName contains a list of file names (or file specs) separated by a semicolon</param>
        /// <returns>List of matching files</returns>
        public List<DatasetDirectoryOrFileInfo> FindFiles(string fileName, string subdirectoryName, string datasetName, bool recurse, bool fileSplit = false)
        {
            const int dataPackageID = 0;
            return FindFiles(fileName, subdirectoryName, datasetName, dataPackageID, recurse, fileSplit);
        }

        /// <summary>
        /// Looks for the given file, returning any matches as a list
        /// </summary>
        /// <remarks>subdirectoryName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        /// <param name="fileName">
        /// File name to find; can contain a wildcard, e.g. *.zip
        /// Separate multiple values using a vertical bar, e.g. analysis.baf|ser
        /// </param>
        /// <param name="subdirectoryName">Subdirectory in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="datasetName">Dataset name filter (blank to ignore)</param>
        /// <param name="dataPackageID">Data package ID filter (0 to ignore)</param>
        /// <param name="recurse">True to search all subdirectories; false to only search the root directory (or only subdirectoryName)</param>
        /// <param name="fileSplit">Set to True if fileName contains a list of file names (or file specs) separated by a semicolon</param>
        /// <returns>List of matching files</returns>
        public List<DatasetDirectoryOrFileInfo> FindFiles(
            string fileName,
            string subdirectoryName,
            string datasetName,
            int dataPackageID,
            bool recurse,
            bool fileSplit)
        {
            // Re-query the web service if the information is out-of-date
            RefreshInfoIfStale();

            var matches = new List<DatasetDirectoryOrFileInfo>();

            if (string.IsNullOrEmpty(fileName))
            {
                return matches;
            }

            if (mArchivedFiles.Count == 0)
            {
                return matches;
            }

            Regex reDirectory;
            List<string> subdirectoryPathParts;

            // A source directory of "." is treated as ""
            if (!string.IsNullOrEmpty(subdirectoryName) && !subdirectoryName.Equals("."))
            {
                // Assure that subdirectoryName has windows-style slashes (if it even has slashes)
                subdirectoryName = subdirectoryName.Replace("/", @"\");

                // If subdirectoryName does have multiple directory names, only the final directory can have wildcards
                subdirectoryPathParts = subdirectoryName.Split('\\').ToList();

                reDirectory = GetFileSearchRegEx(subdirectoryPathParts.Last());

                if (subdirectoryPathParts.Count > 0)
                {
                    subdirectoryPathParts.RemoveAt(subdirectoryPathParts.Count - 1);
                }
            }
            else
            {
                reDirectory = GetFileSearchRegEx("*");
                subdirectoryPathParts = new List<string>();
            }

            List<string> nameParts;

            if (fileSplit)
            {
                nameParts = fileName.Split(';').ToList();
            }
            else
            {
                nameParts = new List<string> { fileName };
            }

            foreach (var namePart in nameParts)
            {
                var fileMatcher = GetFileSearchRegEx(namePart);

                foreach (var archivedFile in mArchivedFiles)
                {
                    if (!string.IsNullOrWhiteSpace(datasetName) && !string.Equals(datasetName, archivedFile.Dataset, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (dataPackageID > 0 && archivedFile.DataPackageID != dataPackageID)
                    {
                        continue;
                    }

                    if (!fileMatcher.IsMatch(archivedFile.Filename))
                    {
                        continue;
                    }

                    var isMatch = true;

                    if (string.IsNullOrEmpty(subdirectoryName))
                    {
                        // Validate that the file resides in the appropriate directory
                        if (!recurse && archivedFile.RelativePathWindows.Contains("\\"))
                        {
                            // Invalid match
                            isMatch = false;
                        }
                    }
                    else
                    {
                        // Require a subdirectory match
                        isMatch = false;
                        if (archivedFile.RelativePathWindows.Contains("\\"))
                        {
                            var pathParts = archivedFile.RelativePathWindows.Split('\\').ToList();
                            for (var pathIndex = pathParts.Count - 2; pathIndex >= 0; pathIndex--)
                            {
                                if (reDirectory.IsMatch(pathParts[pathIndex]))
                                {
                                    isMatch = true;
                                    if (subdirectoryPathParts.Count > 0)
                                    {
                                        // Also require a match to the parent directories
                                        var comparisonIndex = subdirectoryPathParts.Count;

                                        for (var parentPathIndex = pathIndex - 1; parentPathIndex >= 0; parentPathIndex--)
                                        {
                                            comparisonIndex--;
                                            if (comparisonIndex < 0)
                                            {
                                                break;
                                            }

                                            if (!string.Equals(subdirectoryPathParts[comparisonIndex], pathParts[parentPathIndex], StringComparison.OrdinalIgnoreCase))
                                            {
                                                isMatch = false;
                                            }
                                        }
                                    }

                                    if (isMatch)
                                    {
                                        break;
                                    }
                                }

                                if (!recurse)
                                {
                                    break;
                                }
                            }
                        }
                    }

                    if (isMatch)
                    {
                        var newMatch = new DatasetDirectoryOrFileInfo(archivedFile.FileID, false, archivedFile);
                        matches.Add(newMatch);
                    }
                }
            }

            return matches;
        }

        /// <summary>
        /// Looks for the given directory, returning any matches as a list
        /// </summary>
        /// <param name="directoryName">Directory name to find; can contain a wildcard, e.g. SIC*</param>
        /// <returns>List of matching directories</returns>
        /// ReSharper disable once UnusedMember.Global
        public List<DatasetDirectoryOrFileInfo> FindDirectories(string directoryName)
        {
            var datasetName = string.Empty;

            return FindDirectories(directoryName, datasetName);
        }

        /// <summary>
        /// Looks for the given directory, returning any matches as a list
        /// </summary>
        /// <param name="directoryName">Directory name to find; can contain a wildcard, e.g. SIC*</param>
        /// <param name="datasetName">Dataset name filter</param>
        /// <returns>List of matching directories</returns>
        public List<DatasetDirectoryOrFileInfo> FindDirectories(string directoryName, string datasetName)
        {
            // Re-query the web service if the information is out-of-date
            RefreshInfoIfStale();

            var matches = new List<DatasetDirectoryOrFileInfo>();
            var matchPaths = new SortedSet<string>();

            if (string.IsNullOrEmpty(directoryName))
            {
                return matches;
            }

            var directoryMatcher = GetFileSearchRegEx(directoryName);

            foreach (var archivedFile in mArchivedFiles)
            {
                if (!string.IsNullOrWhiteSpace(datasetName))
                {
                    if (!string.Equals(datasetName, archivedFile.Dataset, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }
                }

                if (archivedFile.RelativePathWindows.IndexOf("\\", StringComparison.Ordinal) <= 0)
                {
                    continue;
                }

                var fileInfo = new FileInfo(archivedFile.RelativePathWindows);
                if (fileInfo.Directory == null)
                {
                    OnWarningEvent("Could not determine the directory of file " + archivedFile.RelativePathWindows);
                    continue;
                }

                if (!directoryMatcher.IsMatch(fileInfo.Directory.Name))
                {
                    continue;
                }

                var relativeDirectoryPath = string.Copy(archivedFile.RelativePathWindows);
                var charIndex = relativeDirectoryPath.LastIndexOf("\\", StringComparison.Ordinal);

                if (charIndex > 0)
                {
                    relativeDirectoryPath = relativeDirectoryPath.Substring(0, charIndex);
                }
                else
                {
                    // This shouldn't happen
                    throw new Exception("Forward slash not found in the relative file path; this code should not be reached");
                }

                if (matchPaths.Contains(relativeDirectoryPath))
                {
                    continue;
                }

                matchPaths.Add(relativeDirectoryPath);

                var pathParts = relativeDirectoryPath.Split('\\').ToList();
                var subDirPath = string.Empty;

                if (pathParts.Count > 1)
                {
                    subDirPath = pathParts[0];
                    for (var pathIndex = 1; pathIndex <= pathParts.Count - 2; pathIndex++)
                    {
                        subDirPath = subDirPath + "\\" + pathParts[pathIndex];
                    }
                    relativeDirectoryPath = pathParts.Last();
                    subDirPath = subDirPath.TrimEnd('\\');
                }

                const long fileID = 0;
                const bool isDirectory = true;
                var newMatch = new DatasetDirectoryOrFileInfo(fileID, isDirectory, new ArchivedFileInfo(archivedFile.Dataset, relativeDirectoryPath, subDirPath));

                matches.Add(newMatch);
            }

            return matches;
        }

        private Regex GetFileSearchRegEx(string name)
        {
            // ReSharper disable CommentTypo

            // Look for symbols reserved by RegEx
            // Replace them with escaped versions
            // For example, if name is "K00059_3-oxoacyl-[acyl-carrier_protein]_reductase_[EC_1_1_1_100].html"
            // Update it to be         "K00059_3-oxoacyl-\[acyl-carrier_protein]_reductase_\[EC_1_1_1_100]\.html\"

            // ReSharper restore CommentTypo

            var escapedName = mReplaceReservedRegExChars.Replace(name, @"\${Symbol}");

            var strSearchSpec = "^" + escapedName.Replace("*", ".*") + "$";

            return new Regex(strSearchSpec, RegexOptions.Compiled | RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// Retrieve queued files from MyEMSL
        /// </summary>
        /// <remarks>Returns False if the download queue is empty</remarks>
        /// <param name="downloadDirectoryPath">Target directory path (ignored for files defined in destFilePathOverride)</param>
        /// <param name="directoryLayout">Directory Layout (ignored for files defined in destFilePathOverride)</param>
        /// <returns>True if success, false if an error</returns>
        public bool ProcessDownloadQueue(string downloadDirectoryPath, Downloader.DownloadLayout directoryLayout)
        {
            mErrorMessages.Clear();
            mDownloadedFiles.Clear();

            var success = mDownloadQueue.ProcessDownloadQueue(downloadDirectoryPath, directoryLayout);

            if (success)
            {
                foreach (var downloadedFile in mDownloadQueue.DownloadedFiles)
                {
                    mDownloadedFiles.Add(downloadedFile.Key, downloadedFile.Value);
                }
            }

            return success;
        }

        /// <summary>
        /// Contact the metadata server to find the files associated with the datasets or data packages associated with this instance
        /// </summary>
        /// <returns>True if successful, false if an error</returns>
        public abstract bool RefreshInfo();

        /// <summary>
        /// Refresh the cached file info if over 5 minutes have elapsed
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        // ReSharper disable once UnusedMethodReturnValue.Local
        private bool RefreshInfoIfStale()
        {
            if (mCacheIsStale || DateTime.UtcNow.Subtract(mCacheDate).TotalMinutes >= CACHE_REFRESH_THRESHOLD_MINUTES)
            {
                return RefreshInfo();
            }

            return true;
        }

        /// <summary>
        /// File downloaded event
        /// </summary>
        public event EventHandler<FileDownloadedEventArgs> FileDownloadedEvent;

        /// <summary>
        /// MyEMSL Offline event
        /// </summary>
        public event StatusEventEventHandler MyEMSLOffline;

        private void Reader_MyEMSLOffline(string message)
        {
            if (MyEMSLOffline == null)
            {
                OnWarningEvent(message);
            }
            else
            {
                MyEMSLOffline?.Invoke(message);
            }
        }

        private void OnFileDownloadedEvent(object sender, FileDownloadedEventArgs e)
        {
            FileDownloadedEvent?.Invoke(sender, e);
        }
    }
}
