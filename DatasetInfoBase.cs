﻿using System;
using System.Diagnostics;
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
    public abstract class DatasetInfoBase : clsEventNotifier
    {

        #region "Constants"

        public const string MYEMSL_FILEID_TAG = "@MyEMSLID_";
        private const int CACHE_REFRESH_THRESHOLD_MINUTES = 5;

        #endregion

        #region "Module variables"

        protected readonly List<string> mErrorMessages;
        protected List<ArchivedFileInfo> mArchivedFiles;

        protected DateTime mCacheDate;
        protected bool mCacheIsStale;

        protected readonly Reader mReader;

        protected readonly DownloadQueue mDownloadQueue;

        private bool mUseTestInstance;

        /// <summary>
        /// The most recently downloaded files; keys are the full paths to the downloaded file, values are extended file info
        /// </summary>
        protected readonly Dictionary<string, ArchivedFileInfo> mDownloadedFiles;

        // Do not search for * or ? because we treat those as filename wildcards
        private readonly Regex mReplaceReservedRegExChars;

        #endregion

        #region "Properties"

        /// <summary>
        /// When true, will never download files using the cart mechanism
        /// </summary>
        /// <remarks>ForceDownloadViaCart takes precedence over DisableCart</remarks>
        public bool DisableCart { get; set; }

        /// <summary>
        /// The most recently downloaded files; keys are the full paths to the downloaded file, values are extended file info
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks>Keys are the full paths to the downloaded file, values are extended file info</remarks>
        // ReSharper disable once UnusedMember.Global
        public Dictionary<string, ArchivedFileInfo> DownloadedFiles => mDownloadQueue.DownloadedFiles;

        /// <summary>
        /// List of error messages
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks>The messages are cleared by when these functions are called: ProcessDownloadQueue and RefreshInfo</remarks>
        // ReSharper disable once UnusedMember.Global
        public List<string> ErrorMessages => mErrorMessages;

        /// <summary>
        /// MyEMSL IDs of files queued to be downloaded
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks>Keys are MyEMSL File IDs, values are struct udtFileToDownload</remarks>
        // ReSharper disable once UnusedMember.Global
        public Dictionary<long, DownloadQueue.udtFileToDownload> FilesToDownload => mDownloadQueue.FilesToDownload;

        /// <summary>
        /// When true, will always download files using the cart mechanism
        /// </summary>
        /// <remarks>ForceDownloadViaCart takes precedence over DisableCart</remarks>
        public bool ForceDownloadViaCart { get; set; }

        public bool ThrowErrors { get; set; }

        /// <summary>
        /// When False use https://my.emsl.pnl.gov/myemsl/elasticsearch/simple_items
        /// When True use  https://test0.my.emsl.pnl.gov/myemsl/search/simple/index.shtml
        /// </summary>
        public bool UseTestInstance
        {
            // ReSharper disable once UnusedMember.Global
            get
            {
                return mUseTestInstance;
            }
            set
            {
                mUseTestInstance = value;
                mDownloadQueue.UseTestInstance = value;
                mReader.UseTestInstance = value;
            }
        }

        #endregion

        protected DatasetInfoBase()
        {
            mErrorMessages = new List<string>();

            mReader = new Reader();
            RegisterEvents(mReader);

            mArchivedFiles = new List<ArchivedFileInfo>();

            mDownloadedFiles = new Dictionary<string, ArchivedFileInfo>(StringComparer.OrdinalIgnoreCase);

            mDownloadQueue = new DownloadQueue {
                ThrowErrors = ThrowErrors
            };

            RegisterEvents(mDownloadQueue);

            // Attach events
            mDownloadQueue.FileDownloadedEvent += OnFileDownloadedEvent;

            mReplaceReservedRegExChars = new Regex(@"(?<Symbol>[\^\$\.\|\+\(\)\[\{\\])", RegexOptions.Compiled);
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
        // ReSharper disable once MemberCanBeProtected.Global
        public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo, bool unzipRequired)
        {
            mDownloadQueue.AddFileToDownloadQueue(fileInfo, unzipRequired);
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="myEMSLFileID">MyEMSL File ID</param>
        /// <param name="fileInfo">Archive File Info</param>
        /// <param name="unzipRequired">
        /// True if the file will need to be unzipped after the download
        /// (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)
        /// </param>
        /// <remarks>fileInfo can be null if unzipRequired is false</remarks>
        // ReSharper disable once MemberCanBeProtected.Global
        public void AddFileToDownloadQueue(long myEMSLFileID, ArchivedFileInfo fileInfo, bool unzipRequired)
        {
            mDownloadQueue.AddFileToDownloadQueue(myEMSLFileID, fileInfo, unzipRequired);
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="myEMSLFileID">MyEMSL File ID</param>
        /// <param name="fileInfo">Archive File Info</param>
        /// <param name="unzipRequired">True if the file will need to be unzipped after the download (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)</param>
        /// <param name="destFilePath">Explicit destination file path</param>
        /// <remarks>fileInfo can be null if unzipRequired is false</remarks>
        // ReSharper disable once UnusedMember.Global
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
        /// <remarks></remarks>
        // ReSharper disable once UnusedMember.Global
        public static string AppendMyEMSLFileID(string filePath, long myEmslFileID)
        {
            return filePath + MYEMSL_FILEID_TAG + myEmslFileID.ToString(CultureInfo.InvariantCulture);
        }

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
        /// <remarks></remarks>
        // ReSharper disable once UnusedMember.Global
        public static long ExtractMyEMSLFileID(string filePath)
        {
            string newFilePath;
            return ExtractMyEMSLFileID(filePath, out newFilePath);
        }

        /// <summary>
        /// Parses a path that contains the MyEMSL FileID tag
        /// </summary>
        /// <param name="filePath">Path to parse, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw@MyEMSLID_84327</param>
        /// <param name="newFilePath">Path with the MyEMSL FileID tag removed, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw</param>
        /// <returns>MyEMSL File ID if successfully parsed, 0 if not present or a problem</returns>
        /// <remarks></remarks>
        public static long ExtractMyEMSLFileID(string filePath, out string newFilePath)
        {

            var charIndex = filePath.LastIndexOf(MYEMSL_FILEID_TAG, StringComparison.Ordinal);
            newFilePath = string.Copy(filePath);

            if (charIndex > 0)
            {
                newFilePath = filePath.Substring(0, charIndex);

                long myEmslFileID;
                var myEmslFileIdText = filePath.Substring(charIndex + MYEMSL_FILEID_TAG.Length);

                if (long.TryParse(myEmslFileIdText, out myEmslFileID))
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
        /// <remarks></remarks>
        public List<DatasetFolderOrFileInfo> FindFiles(string fileName)
        {
            var subFolderName = string.Empty;
            var datasetName = string.Empty;
            return FindFiles(fileName, subFolderName, datasetName, recurse: true);
        }

        /// <summary>
        /// Looks for the given file in all datasets added using AddDataset(), returning any matches as a list
        /// </summary>
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <returns>List of matching files</returns>
        /// <remarks>subFolderName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subFolderName)
        {
            var datasetName = string.Empty;
            return FindFiles(fileName, subFolderName, datasetName, recurse: true);
        }

        /// <summary>
        /// Looks for the given file, returning any matches as a list
        /// </summary>
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="recurse">True to search all subfolders; false to only search the root folder (or only subFolderName)</param>
        /// <param name="fileSplit">Set to True if fileName contains a list of file names (or file specs) separated by a semicolon</param>
        /// <returns>List of matching files</returns>
        /// <remarks>subFolderName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subFolderName, bool recurse, bool fileSplit = false)
        {
            var datasetName = string.Empty;
            return FindFiles(fileName, subFolderName, datasetName, recurse, fileSplit);
        }

        /// <summary>
        /// Looks for the given file, returning any matches as a list
        /// </summary>
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="datasetName">Dataset name filter</param>
        /// <returns>List of matching files</returns>
        /// <remarks>subFolderName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        // ReSharper disable once UnusedMember.Global
        public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subFolderName, string datasetName)
        {
            return FindFiles(fileName, subFolderName, datasetName, recurse: true);
        }

        /// <summary>
        /// Looks for the given file, returning any matches as a list
        /// </summary>
        /// <param name="fileName">
        /// File name to find; can contain a wildcard, e.g. *.zip
        /// Separate multiple values using a vertical bar, e.g. analysis.baf|ser
        /// </param>
        /// <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="datasetName">Dataset name filter</param>
        /// <param name="recurse">True to search all subfolders; false to only search the root folder (or only subFolderName)</param>
        /// <param name="fileSplit">Set to True if fileName contains a list of file names (or file specs) separated by a semicolon</param>
        /// <returns>List of matching files</returns>
        /// <remarks>subFolderName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subFolderName, string datasetName, bool recurse, bool fileSplit = false)
        {
            const int dataPackageID = 0;
            return FindFiles(fileName, subFolderName, datasetName, dataPackageID, recurse, fileSplit);
        }

        /// <summary>
        /// Looks for the given file, returning any matches as a list
        /// </summary>
        /// <param name="fileName">
        /// File name to find; can contain a wildcard, e.g. *.zip
        /// Separate multiple values using a vertical bar, e.g. analysis.baf|ser
        /// </param>
        /// <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="datasetName">Dataset name filter (blank to ignore)</param>
        /// <param name="dataPackageID">Data package ID filter (0 to ignore)</param>
        /// <param name="recurse">True to search all subfolders; false to only search the root folder (or only subFolderName)</param>
        /// <param name="fileSplit">Set to True if fileName contains a list of file names (or file specs) separated by a semicolon</param>
        /// <returns>List of matching files</returns>
        /// <remarks>subFolderName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        // ReSharper disable once MemberCanBeProtected.Global
        public List<DatasetFolderOrFileInfo> FindFiles(
            string fileName,
            string subFolderName,
            string datasetName,
            int dataPackageID,
            bool recurse,
            bool fileSplit)
        {

            // Re-query the web service if the information is out-of-date
            RefreshInfoIfStale();

            var lstMatches = new List<DatasetFolderOrFileInfo>();

            if (string.IsNullOrEmpty(fileName))
            {
                return lstMatches;
            }

            if (mArchivedFiles.Count == 0)
            {
                return lstMatches;
            }

            Regex reFolder;
            List<string> subFolderPathParts;

            if (!string.IsNullOrEmpty(subFolderName))
            {
                // Assure that subFolderName has windows-style slashes (if it even has slashes)
                subFolderName = subFolderName.Replace("/", @"\");

                // If subFolderName does have multiple foldernames, only the final folder can have wildcards
                subFolderPathParts = subFolderName.Split('\\').ToList();

                reFolder = GetFileSearchRegEx(subFolderPathParts.Last());

                if (subFolderPathParts.Count > 0)
                {
                    subFolderPathParts.RemoveAt(subFolderPathParts.Count - 1);
                }
            }
            else
            {
                reFolder = GetFileSearchRegEx("*");
                subFolderPathParts = new List<string>();
            }

            List<string> nameParts;

            if (fileSplit)
            {
                nameParts = fileName.Split(';').ToList();
            }
            else
            {
                nameParts = new List<string>() { fileName };
            }

            foreach (var namePart in nameParts)
            {
                var reFile = GetFileSearchRegEx(namePart);

                foreach (var archivedFile in mArchivedFiles)
                {
                    if (!string.IsNullOrWhiteSpace(datasetName))
                    {
                        if (!string.Equals(datasetName, archivedFile.Dataset, StringComparison.OrdinalIgnoreCase))
                            continue;
                    }

                    if (dataPackageID > 0)
                    {
                        if (archivedFile.DataPackageID != dataPackageID)
                            continue;
                    }

                    if (!reFile.IsMatch(archivedFile.Filename))
                    {
                        continue;
                    }

                    var isMatch = true;

                    if (string.IsNullOrEmpty(subFolderName))
                    {
                        // Validate that the file resides in the appropriate folder
                        if (!recurse && archivedFile.RelativePathWindows.Contains("\\"))
                        {
                            // Invalid match
                            isMatch = false;
                        }
                    }
                    else
                    {
                        // Require a subfolder match
                        isMatch = false;
                        if (archivedFile.RelativePathWindows.Contains("\\"))
                        {
                            var pathParts = archivedFile.RelativePathWindows.Split('\\').ToList();
                            for (var pathIndex = pathParts.Count - 2; pathIndex >= 0; pathIndex--)
                            {
                                if (reFolder.IsMatch(pathParts[pathIndex]))
                                {
                                    isMatch = true;
                                    if (subFolderPathParts.Count > 0)
                                    {
                                        // Also require a match to the parent folders
                                        var comparisonIndex = subFolderPathParts.Count;

                                        for (var parentPathIndex = pathIndex - 1; parentPathIndex >= 0; parentPathIndex--)
                                        {
                                            comparisonIndex--;
                                            if (comparisonIndex < 0)
                                                break;

                                            if (subFolderPathParts[comparisonIndex].ToLower() != pathParts[parentPathIndex].ToLower())
                                                isMatch = false;
                                        }

                                    }

                                    if (isMatch)
                                        break;
                                }

                                if (!recurse)
                                    break;
                            }
                        }
                    }

                    if (isMatch)
                    {
                        var newMatch = new DatasetFolderOrFileInfo(archivedFile.FileID, false, archivedFile);
                        lstMatches.Add(newMatch);
                    }
                }

            }

            return lstMatches;

        }

        /// <summary>
        /// Looks for the given folder, returning any matches as a list
        /// </summary>
        /// <param name="folderName">Folder name to find; can contain a wildcard, e.g. SIC*</param>
        /// <returns>List of matching folders</returns>
        /// <remarks></remarks>
        // ReSharper disable once UnusedMember.Global
        public List<DatasetFolderOrFileInfo> FindFolders(string folderName)
        {
            var datasetName = string.Empty;

            return FindFolders(folderName, datasetName);
        }

        /// <summary>
        /// Looks for the given folder, returning any matches as a list
        /// </summary>
        /// <param name="folderName">Folder name to find; can contain a wildcard, e.g. SIC*</param>
        /// <param name="datasetName">Dataset name filter</param>
        /// <returns>List of matching folders</returns>
        /// <remarks></remarks>
        public List<DatasetFolderOrFileInfo> FindFolders(string folderName, string datasetName)
        {

            // Re-query the web service if the information is out-of-date
            RefreshInfoIfStale();

            var lstMatches = new List<DatasetFolderOrFileInfo>();
            var lstMatchPaths = new SortedSet<string>();

            if (string.IsNullOrEmpty(folderName))
            {
                return lstMatches;
            }

            var reFolder = GetFileSearchRegEx(folderName);

            foreach (var archivedFile in mArchivedFiles)
            {
                if (!string.IsNullOrWhiteSpace(datasetName))
                {
                    if (!string.Equals(datasetName, archivedFile.Dataset, StringComparison.OrdinalIgnoreCase))
                        continue;
                }

                if (archivedFile.RelativePathWindows.IndexOf("\\", StringComparison.Ordinal) <= 0)
                {
                    continue;
                }

                var fiFile = new FileInfo(archivedFile.RelativePathWindows);
                Debug.Assert(fiFile.Directory != null, "fiFile.Directory != null");
                if (!reFolder.IsMatch(fiFile.Directory.Name))
                {
                    continue;
                }

                var relativeFolderPath = string.Copy(archivedFile.RelativePathWindows);
                var charIndex = relativeFolderPath.LastIndexOf("\\", StringComparison.Ordinal);

                if (charIndex > 0)
                {
                    relativeFolderPath = relativeFolderPath.Substring(0, charIndex);
                }
                else
                {
                    // This is a programming bug
                    // ReSharper disable once NotResolvedInText
                    throw new ArgumentOutOfRangeException("Forward slash not found in the relative file path; this code should not be reached");
                }

                if (lstMatchPaths.Contains(relativeFolderPath))
                {
                    continue;
                }

                lstMatchPaths.Add(relativeFolderPath);

                var pathParts = relativeFolderPath.Split('\\').ToList();
                var subDirPath = string.Empty;

                if (pathParts.Count > 1)
                {
                    subDirPath = pathParts[0];
                    for (var pathIndex = 1; pathIndex <= pathParts.Count - 2; pathIndex++)
                    {
                        subDirPath = subDirPath + "\\" + pathParts[pathIndex];
                    }
                    relativeFolderPath = pathParts.Last();
                    subDirPath = subDirPath.TrimEnd('\\');
                }

                const long fileID = 0;
                const bool isFolder = true;
                var newMatch = new DatasetFolderOrFileInfo(fileID, isFolder, new ArchivedFileInfo(archivedFile.Dataset, relativeFolderPath, subDirPath));

                lstMatches.Add(newMatch);
            }

            return lstMatches;

        }

        private Regex GetFileSearchRegEx(string name)
        {
            // Look for symbols reserved by RegEx
            // Replace them with escaped versions
            // For example, if name is "K00059_3-oxoacyl-[acyl-carrier_protein]_reductase_[EC_1_1_1_100].html"
            // Update it to be         "K00059_3-oxoacyl-\[acyl-carrier_protein]_reductase_\[EC_1_1_1_100]\.html\"

            var escapedName = mReplaceReservedRegExChars.Replace(name, @"\${Symbol}");

            var strSearchSpec = "^" + escapedName.Replace("*", ".*") + "$";

            return new Regex(strSearchSpec, RegexOptions.Compiled | RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// Retrieve queued files from MyEMSL
        /// </summary>
        /// <param name="downloadFolderPath">Target folder path (ignored for files defined in destFilePathOverride)</param>
        /// <param name="folderLayout">Folder Layout (ignored for files defined in destFilePathOverride)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Returns False if the download queue is empty</remarks>
        public bool ProcessDownloadQueue(string downloadFolderPath, Downloader.DownloadFolderLayout folderLayout)
        {

            mErrorMessages.Clear();
            mDownloadedFiles.Clear();

            var success = mDownloadQueue.ProcessDownloadQueue(downloadFolderPath, folderLayout, DisableCart, ForceDownloadViaCart);

            if (success)
            {
                foreach (var downloadedFile in mDownloadQueue.DownloadedFiles)
                    mDownloadedFiles.Add(downloadedFile.Key, downloadedFile.Value);
            }

            return success;

        }

        // ReSharper disable once MemberCanBeProtected.Global
        public abstract bool RefreshInfo();

        /// <summary>
        /// Refresh the cached file info if over 5 minutes have elapsed
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        // ReSharper disable once UnusedMethodReturnValue.Local
        private bool RefreshInfoIfStale()
        {
            if (mCacheIsStale || DateTime.UtcNow.Subtract(mCacheDate).TotalMinutes >= CACHE_REFRESH_THRESHOLD_MINUTES)
                return RefreshInfo();

            return true;
        }

        #region "Events"

        // ReSharper disable once EventNeverSubscribedTo.Global
        public event FileDownloadedEventHandler FileDownloadedEvent;

        #endregion

        #region "Event handlers"

        private void OnFileDownloadedEvent(object sender, FileDownloadedEventArgs e)
        {
            FileDownloadedEvent?.Invoke(sender, e);
        }

        #endregion

    }
}
