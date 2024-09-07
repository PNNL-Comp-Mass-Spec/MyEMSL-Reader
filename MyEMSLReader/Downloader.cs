using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Pacifica.Core;
using ICSharpCode.SharpZipLib.Tar;
using System.Net.Http;
using System.Text.RegularExpressions;

namespace MyEMSLReader
{
    /// <summary>
    /// This class contacts MyEMSL to download the desired files, which must be specified by MyEMSL File ID values
    /// Use the Reader class to find files and determine File IDs
    /// </summary>
    /// <remarks>Written by Matthew Monroe for PNNL in August 2013</remarks>
    public class Downloader : MyEMSLBase
    {
        // Ignore Spelling: dest, downloader

        // ReSharper disable UseCollectionExpression

        /// <summary>
        /// Download layout
        /// </summary>
        public enum DownloadLayout
        {
            /// <summary>
            /// Flat, no subdirectories
            /// </summary>
            FlatNoSubdirectories = 0,

            /// <summary>
            /// Single dataset
            /// </summary>
            SingleDataset = 1,

            /// <summary>
            /// Separate datasets, each in their own subdirectory
            /// </summary>
            DatasetNameAndSubdirectories = 2,

            /// <summary>
            /// Separate datasets, arranged by Year_Quarter\DatasetName
            /// </summary>
            InstrumentYearQuarterDataset = 3
        }

        /// <summary>
        /// File overwrite behavior
        /// </summary>
        public enum Overwrite
        {
            /// <summary>
            /// Only overwrite if the downloaded file is different
            /// </summary>
            /// <remarks>
            /// This mode will compute a SHA-1 hash of the target file and only overwrite the target if the hash values differ
            /// </remarks>
            IfChanged = 0,

            /// <summary>
            /// Always overwrite
            /// </summary>
            Always = 1,

            /// <summary>
            /// Never overwrite
            /// </summary>
            Never = 2
        }

        /// <summary>
        /// The most recently downloaded files.  Keys are the full path to the downloaded file; values are extended file info
        /// </summary>
        public Dictionary<string, ArchivedFileInfo> DownloadedFiles
        {
            get;
        }

        /// <summary>
        /// When True, if downloading multiple versions of the same file, include the FileID in the filename
        /// When False, if downloading multiple versions of the same file, will only keep one of the versions (order is not defined)
        /// </summary>
        /// <remarks>Default is False</remarks>
        public bool IncludeAllRevisions
        {
            get;
            set;
        }

        /// <summary>
        /// Overwrite mode (IfChanged, Always, or Never)
        /// </summary>
        /// <remarks>Default is IfChanged</remarks>
        public Overwrite OverwriteMode
        {
            get;
            set;
        }

        /// <summary>
        /// Percent complete (value between 0 and 100)
        /// </summary>
        public double PercentComplete
        {
            get;
            private set;
        }

        private bool mUseTestInstance;

        /// <summary>
        /// When False use https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/12345
        /// When True use  https://metadatadev.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/12345
        /// </summary>
        public bool UseTestInstance
        {
            get => mUseTestInstance;
            set
            {
                mUseTestInstance = value;

                if (mPacificaConfig.UseTestInstance != value)
                {
                    mPacificaConfig.UseTestInstance = value;
                }
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public Downloader()
        {
            IncludeAllRevisions = false;
            ThrowErrors = true;
            OverwriteMode = Overwrite.IfChanged;
            DownloadedFiles = new Dictionary<string, ArchivedFileInfo>(StringComparer.OrdinalIgnoreCase);

            EasyHttp.MyEMSLOffline += EasyHttp_MyEMSLOffline;
            EasyHttp.ErrorEvent += OnErrorEvent;

            ResetStatus();
        }

        /// <summary>
        /// Download queued files
        /// </summary>
        /// <param name="filesToDownload">MyEMSL Files to download; keys are MyEMSL File IDs and values are ArchivedFileInfo objects</param>
        /// <param name="downloadDirectoryPath">Target directory path (ignored for files defined in destFilePathOverride)</param>
        /// <param name="directoryLayout">Directory Layout (ignored for files defined in destFilePathOverride)</param>
        /// <returns>True if success, false if an error</returns>
        public bool DownloadFiles(
            Dictionary<long, ArchivedFileInfo> filesToDownload,
            string downloadDirectoryPath,
            DownloadLayout directoryLayout = DownloadLayout.SingleDataset)
        {
            var destFilePathOverride = new Dictionary<long, string>();
            return DownloadFiles(filesToDownload, destFilePathOverride, downloadDirectoryPath, directoryLayout);
        }

        /// <summary>
        /// Download files in filesToDownload
        /// </summary>
        /// <remarks>destFilePathOverride is not required and can be empty; it can also have values for just some of the files in filesToDownload</remarks>
        /// <param name="filesToDownload">MyEMSL Files to download; keys are MyEMSL File IDs and values are ArchivedFileInfo objects</param>
        /// <param name="destFilePathOverride">Dictionary where keys are FileIDs and values are the explicit destination path to use</param>
        /// <param name="downloadDirectoryPath">Target directory path (ignored for files defined in destFilePathOverride)</param>
        /// <param name="directoryLayout">Directory Layout (ignored for files defined in destFilePathOverride)</param>
        /// <returns>True if success, false if an error</returns>
        public bool DownloadFiles(
            Dictionary<long, ArchivedFileInfo> filesToDownload,
            Dictionary<long, string> destFilePathOverride,
            string downloadDirectoryPath,
            DownloadLayout directoryLayout = DownloadLayout.SingleDataset)
        {
            ResetStatus();

            if (mPacificaConfig.UseTestInstance != UseTestInstance)
            {
                mPacificaConfig.UseTestInstance = UseTestInstance;
            }

            try
            {
                // The following Callback allows us to access the MyEMSL server even if the certificate is expired or untrusted
                // For more info, see comments in Reader.RunElasticSearchQuery()
                ServicePointManager.ServerCertificateValidationCallback ??= ValidateRemoteCertificate;

                if (filesToDownload == null || filesToDownload.Count == 0)
                {
                    ReportError("File download dictionary is empty; nothing to download");
                    return false;
                }

                if (directoryLayout == DownloadLayout.SingleDataset)
                {
                    // Assure that the requested files all have the same dataset id
                    // If downloading files for a data package, datasetIDs will be 0
                    var datasetIDs = GetUniqueDatasetIDList(filesToDownload);

                    if (datasetIDs.Count > 1)
                    {
                        // Look for conflicts
                        var outputFilePaths = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                        foreach (var archiveFile in filesToDownload.Values)
                        {
                            if (outputFilePaths.Contains(archiveFile.RelativePathWindows))
                            {
                                // File conflicts
                                ReportMessage("Auto-changing directory layout to 'DatasetNameAndSubdirectories' since the files to download come from more than one dataset");
                                directoryLayout = DownloadLayout.DatasetNameAndSubdirectories;
                                break;
                            }

                            outputFilePaths.Add(archiveFile.RelativePathWindows);
                        }
                    }
                }

                var cookieJar = new CookieContainer();

                if (string.IsNullOrWhiteSpace(downloadDirectoryPath))
                {
                    downloadDirectoryPath = ".";
                }

                var downloadDirectory = new DirectoryInfo(downloadDirectoryPath);

                // Download the files
                // Keys in this dictionary are FileIDs, values are relative file paths
                var filesDownloaded = DownloadFilesDirectly(
                    filesToDownload, cookieJar, destFilePathOverride,
                    downloadDirectory, directoryLayout, out var bytesDownloaded);

                // Create a list of the files that remain (files that could not be downloaded directly)
                var filesNotDownloaded = new Dictionary<long, ArchivedFileInfo>();

                foreach (var archivedFileInfo in filesToDownload)
                {
                    var fileID = archivedFileInfo.Key;
                    var archiveFile = archivedFileInfo.Value;

                    if (filesDownloaded.ContainsKey(archivedFileInfo.Key))
                    {
                        continue;
                    }

                    var targetFile = GetTargetFile(
                        downloadDirectory, directoryLayout,
                        archiveFile, destFilePathOverride);

                    if (targetFile == null)
                    {
                        continue;
                    }

                    // Confirm one more time that we need to download the file
                    var downloadFile = IsDownloadRequired(archiveFile, targetFile, reportMessage: false);

                    if (downloadFile)
                    {
                        filesNotDownloaded.Add(fileID, archiveFile);
                    }
                }

                OnDebugEvent("Downloaded {0:F1} MB total", bytesDownloaded / 1024.0 / 1024);

                if (filesNotDownloaded.Count == 0)
                {
                    // All of the files have been downloaded (or already exist and have matching a matching SHA-1 hash)
                    return true;
                }

                ReportError(filesNotDownloaded.Count + " files(s) could not be downloaded");

                return false;
            }
            catch (Exception ex)
            {
                if (string.IsNullOrWhiteSpace(ErrorMessage))
                {
                    ReportError("Error in MyEMSLReader.Downloader.DownloadFiles", ex);
                }
                else if (ThrowErrors)
                {
                    throw;
                }
            }

            return false;
        }

        /// <summary>
        /// Possibly add a special prefix to work with files whose paths are more than 255 characters long
        /// See https://msdn.microsoft.com/en-us/library/aa365247(v=vs.85).aspx#maxpath
        /// </summary>
        /// <remarks>This only works if the path is rooted</remarks>
        /// <param name="fileOrDirectoryPath"></param>
        /// <returns>Updated path</returns>
        private string AddLongPathCode(string fileOrDirectoryPath)
        {
            if (fileOrDirectoryPath.Length < PRISM.NativeIOFileTools.FILE_PATH_LENGTH_THRESHOLD || fileOrDirectoryPath.StartsWith(@"\\?\"))
            {
                return fileOrDirectoryPath;
            }

            if (!Path.IsPathRooted(fileOrDirectoryPath))
            {
                throw new PathTooLongException(
                    "Target file path is over 255 characters long and is a relative path; " +
                    "cannot work with path " + fileOrDirectoryPath);
            }

            return @"\\?\" + fileOrDirectoryPath;
        }

        private long ComputeTotalBytes(IReadOnlyDictionary<long, ArchivedFileInfo> files)
        {
            return ComputeTotalBytes(files.Values);
        }

        private long ComputeTotalBytes(IEnumerable<ArchivedFileInfo> files)
        {
            var bytesToDownload = files.Sum(archiveFile => archiveFile.FileSizeBytes);
            return bytesToDownload;
        }

        private string ConstructDownloadFilePath(DownloadLayout directoryLayout, ArchivedFileInfo archiveFile)
        {
            string downloadFilePath;
            switch (directoryLayout)
            {
                case DownloadLayout.FlatNoSubdirectories:
                    downloadFilePath = archiveFile.Filename;
                    break;
                case DownloadLayout.SingleDataset:
                    downloadFilePath = archiveFile.RelativePathWindows;
                    break;
                case DownloadLayout.DatasetNameAndSubdirectories:
                    downloadFilePath = Path.Combine(archiveFile.Dataset, archiveFile.RelativePathWindows);
                    break;
                case DownloadLayout.InstrumentYearQuarterDataset:
                    downloadFilePath = archiveFile.PathWithInstrumentAndDatasetWindows;
                    break;
                default:
                    ReportError("Unrecognized DownloadLayout mode: " + directoryLayout.ToString());
                    downloadFilePath = Path.Combine(archiveFile.Dataset, archiveFile.RelativePathWindows);
                    break;
            }

            if (downloadFilePath.IndexOf("/", StringComparison.Ordinal) > 0)
            {
                downloadFilePath = downloadFilePath.Replace('/', Path.DirectorySeparatorChar);
            }

            return downloadFilePath;
        }

        private bool DownloadFile(
            string URL,
            CookieContainer cookieJar,
            int maxAttempts,
            string downloadFilePath,
            out Exception mostRecentException,
            out bool fileInUseByOtherProcess)
        {
            mostRecentException = null;
            fileInUseByOtherProcess = false;

            if (!ValidateCertFile("DownloadFile"))
            {
                return false;
            }

            var timeoutSeconds = 100;
            var attempts = 1;

            if (maxAttempts < 1)
            {
                maxAttempts = 1;
            }

            var success = false;
            var triedGC = false;

            // Use the special prefix \\?\ when working with files whose paths are 260 characters or longer
            // See https://msdn.microsoft.com/en-us/library/aa365247(v=vs.85).aspx#maxpath

            var downloadPathToUse = Utilities.PossiblyConvertToLongPath(downloadFilePath);

            while (!success && attempts <= maxAttempts)
            {
                try
                {
                    attempts++;
                    success = EasyHttp.GetFile(
                        mPacificaConfig, URL, cookieJar,
                        out _,
                        downloadPathToUse, timeoutSeconds);

                    if (!success)
                    {
                        break;
                    }
                }
                catch (IOException ex)
                {
                    // This exception occurs for errors "cannot access the file '...' because it is being used by another process."
                    if (!triedGC)
                    {
                        GarbageCollectNow();
                        triedGC = true;
                    }
                    else
                    {
                        mostRecentException = ex;
                        fileInUseByOtherProcess = true;
                        success = false;
                        break;
                    }
                }
                catch (Exception ex)
                {
                    var responseStatusCode = HttpStatusCode.OK;

                    if (ex.InnerException is HttpRequestException hrex)
                    {
                        if (hrex.InnerException is WebException wex && wex.Response != null)
                        {
                            responseStatusCode = ((HttpWebResponse)wex.Response).StatusCode;
                        }

#if NET5_0_OR_GREATER
                        if (hrex.StatusCode.HasValue)
                        {
                            responseStatusCode = hrex.StatusCode.Value;
                        }
#endif
                    }

                    if (responseStatusCode == HttpStatusCode.ServiceUnavailable)
                    {
                        success = false;
                        mostRecentException = ex.InnerException;
                    }
                    else if (attempts >= maxAttempts)
                    {
                        success = false;
                    }
                    else
                    {
                        // Wait 2 seconds, then retry
                        OnWarningEvent("Exception in DownloadFile on attempt " + attempts + ": " + ex.Message);
                        Thread.Sleep(2000);
                        timeoutSeconds = IncreaseTimeout(timeoutSeconds);
                    }
                }
            }

            return success;
        }

        /// <summary>
        /// Download files one at-a-time
        /// </summary>
        /// <param name="filesToDownload"></param>
        /// <param name="cookieJar"></param>
        /// <param name="destFilePathOverride"></param>
        /// <param name="downloadDirectory"></param>
        /// <param name="directoryLayout"></param>
        /// <param name="bytesDownloaded"></param>
        private Dictionary<long, string> DownloadFilesDirectly(
            IReadOnlyDictionary<long, ArchivedFileInfo> filesToDownload,
            CookieContainer cookieJar,
            IReadOnlyDictionary<long, string> destFilePathOverride,
            FileSystemInfo downloadDirectory,
            DownloadLayout directoryLayout,
            out long bytesDownloaded)
        {
            var filesDownloaded = new Dictionary<long, string>();
            bytesDownloaded = 0;

            try
            {
                // Determine total amount of data to be downloaded
                var bytesToDownload = ComputeTotalBytes(filesToDownload);

                // Construct a mapping between unique file hash and target MyEMSL File IDs
                // Typically there will only be one MyEMSL file per file hash, but if downloading a dataset with
                // lots of similar jobs, or a set of datasets, we can have files with the same hash but different
                // destination paths

                // Keys in this dictionary are file hashes, values are the hash type (typically sha1)
                var fileHashToTypeMap = new Dictionary<string, string>();

                // Keys in this dictionary are file hashes, values are MyEMSL File IDs
                var fileHashToIdMap = new Dictionary<string, SortedSet<long>>();

                // This dictionary tracks the number of copies of each target file that will be downloaded
                // Typically each target file will only have one instance, but if IncludeAllRevisions is true, a file could have multiple versions
                // Keys are file paths, values are counts of each file
                var targetFileCounts = new Dictionary<string, int>();

                foreach (var archivedFileInfo in filesToDownload)
                {
                    var fileHash = archivedFileInfo.Value.Hash;

                    if (fileHashToIdMap.TryGetValue(fileHash, out var fileIDs))
                    {
                        fileIDs.Add(archivedFileInfo.Key);
                    }
                    else
                    {
                        fileIDs = new SortedSet<long>
                        {
                            archivedFileInfo.Key
                        };

                        fileHashToIdMap.Add(fileHash, fileIDs);

                        fileHashToTypeMap.Add(fileHash, archivedFileInfo.Value.HashType);
                    }

                    var targetFile = GetTargetFile(
                        downloadDirectory, directoryLayout,
                        archivedFileInfo.Value, destFilePathOverride);

                    if (targetFileCounts.TryGetValue(targetFile.FullName, out var fileCount))
                    {
                        targetFileCounts[targetFile.FullName] = fileCount + 1;
                    }
                    else
                    {
                        targetFileCounts.Add(targetFile.FullName, 1);
                    }
                }

                foreach (var fileHashInfo in fileHashToIdMap)
                {
                    var fileHash = fileHashInfo.Key;
                    var fileIDs = fileHashInfo.Value;

                    // This will typically be sha1, but could be md5
                    var hashType = fileHashToTypeMap[fileHash];

                    // Construct the URL, for example:
                    // https://files.my.emsl.pnl.gov/files/sha1/08a0cffa75b79ac18e4f375a80e6a80d63784888
                    var URL = mPacificaConfig.FileServerUri + "/files/" + hashType + "/" + fileHash;

                    var firstArchiveFile = filesToDownload[fileIDs.First()];

                    var targetFileInitial = GetTargetFile(
                        downloadDirectory, directoryLayout,
                        firstArchiveFile, destFilePathOverride);

                    if (targetFileInitial == null)
                    {
                        continue;
                    }

                    var targetFile = GetTargetFileWithOptionalID(targetFileCounts, targetFileInitial, firstArchiveFile.FileID);

                    const int DEFAULT_MAX_ATTEMPTS = 5;
                    var fileInUseByOtherProcess = false;

                    var downloadFile = IsDownloadRequired(firstArchiveFile, targetFile, reportMessage: true);
                    bool fileRetrievedOrExists;

                    if (downloadFile)
                    {
                        const int maxAttempts = DEFAULT_MAX_ATTEMPTS;

                        var retrievalSuccess = DownloadFile(
                            URL, cookieJar, maxAttempts,
                            targetFile.FullName,
                            out var mostRecentException,
                            out fileInUseByOtherProcess);

                        if (retrievalSuccess)
                        {
                            filesDownloaded.Add(firstArchiveFile.FileID, firstArchiveFile.PathWithInstrumentAndDatasetWindows);

                            if (firstArchiveFile.FileLastWriteTime > DateTime.MinValue)
                            {
                                UpdateFileModificationTime(targetFile, firstArchiveFile.FileLastWriteTime);
                            }
                            else if (firstArchiveFile.SubmissionTimeValue > DateTime.MinValue)
                            {
                                UpdateFileModificationTime(targetFile, firstArchiveFile.SubmissionTimeValue);
                            }

                            fileRetrievedOrExists = true;
                        }
                        else
                        {
                            // Show the error at the console but do not throw an exception
                            if (mostRecentException == null)
                            {
                                ReportMessage("Failure downloading " + Path.GetFileName(targetFile.FullName) + ": unknown reason");
                            }
                            else
                            {
                                ReportMessage("Failure downloading " + Path.GetFileName(targetFile.FullName) + ": " + mostRecentException.Message);
                            }

                            fileRetrievedOrExists = targetFile.Exists;
                        }
                    }
                    else
                    {
                        fileRetrievedOrExists = true;
                    }

                    if (fileRetrievedOrExists && fileIDs.Count > 1)
                    {
                        // Copy the downloaded file to the additional local target file locations
                        DuplicateFile(
                            downloadDirectory, directoryLayout, destFilePathOverride,
                            targetFile, filesToDownload, fileIDs.Skip(1), filesDownloaded, targetFileCounts);
                    }

                    if (fileInUseByOtherProcess || !downloadFile)
                    {
                        // Download skipped
                        // Need to add to the downloaded files dictionary so that the file doesn't get downloaded via the .tar file mechanism
                        filesDownloaded.Add(firstArchiveFile.FileID, firstArchiveFile.PathWithInstrumentAndDatasetWindows);
                    }

                    if (!DownloadedFiles.ContainsKey(targetFile.FullName))
                    {
                        DownloadedFiles.Add(targetFile.FullName, firstArchiveFile);
                    }

                    bytesDownloaded += firstArchiveFile.FileSizeBytes;
                    UpdateProgress(bytesDownloaded, bytesToDownload);
                }
            }
            catch (Exception ex)
            {
                ReportError("Exception in DownloadFilesDirectly", ex);
                return new Dictionary<long, string>();
            }

            return filesDownloaded;
        }

        [Obsolete("Valid, but unused")]
        // ReSharper disable once UnusedMember.Local
        private bool DownloadAndExtractTarFile(
            CookieContainer cookieJar,
            IReadOnlyCollection<ArchivedFileInfo> filesInArchive,
            long bytesDownloaded,
            IReadOnlyDictionary<long, string> destFilePathOverride,
            FileSystemInfo downloadDirectory,
            DownloadLayout directoryLayout,
            string tarFileURL,
            int timeoutSeconds = 100)
        {
            return Task.Run(async () => await DownloadAndExtractTarFileAsync(cookieJar, filesInArchive, bytesDownloaded,
                    destFilePathOverride, downloadDirectory, directoryLayout, tarFileURL, timeoutSeconds)).GetAwaiter()
                .GetResult();
        }

        [Obsolete("Valid, but unused")]
        // ReSharper disable once UnusedMember.Local
        private async Task<bool> DownloadAndExtractTarFileAsync(
            CookieContainer cookieJar,
            IReadOnlyCollection<ArchivedFileInfo> filesInArchive,
            long bytesDownloaded,
            IReadOnlyDictionary<long, string> destFilePathOverride,
            FileSystemInfo downloadDirectory,
            DownloadLayout directoryLayout,
            string tarFileURL,
            int timeoutSeconds = 100)
        {
            if (!ValidateCertFile("DownloadAndExtractTarFile"))
            {
                return false;
            }

            try
            {
                // Prepare the client object
                using var client = EasyHttp.InitializeClient(mPacificaConfig, tarFileURL, ref cookieJar, ref timeoutSeconds, null);

                var bytesToDownload = ComputeTotalBytes(filesInArchive);

                // Receive response
                using var response = await client.GetAsync("", HttpCompletionOption.ResponseHeadersRead);

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    // Download the file and extract the files as the file is downloaded
                    // This way, the .tar file is never actually created on a local hard drive
                    // Code modeled after https://github.com/icsharpcode/SharpZipLib/wiki/GZip-and-Tar-Samples

                    using var receiveStream = await response.Content.ReadAsStreamAsync();

                    var tarIn = new TarInputStream(receiveStream);

                    while (true)
                    {
                        var tarEntry = tarIn.GetNextEntry();

                        if (tarEntry == null)
                            break;

                        if (tarEntry.IsDirectory)
                        {
                            continue;
                        }

                        var sourceFileName = tarEntry.Name;

                        /*
                         * Deprecated with the switch to SharpZipLib v1.1 from NuGet
                         *
                        // Long filenames (over 100 characters) will have part of their name in tarEntry.Name and part of it in tarEntry.Prefix
                        // Check for this
                        if (!string.IsNullOrEmpty(tarEntry.Prefix))
                            sourceFileName = tarEntry.Prefix + '/' + sourceFileName;
                         *
                         */

                        // Convert the Linux forward slashes in the filenames to windows backslashes
                        sourceFileName = sourceFileName.Replace('/', Path.DirectorySeparatorChar);

                        // The Filename of the tar entry used to start with a directory name that is a MyEMSL FileID
                        // As of March 2016 that is no longer the case
                        var charIndex = sourceFileName.IndexOf(Path.DirectorySeparatorChar);

                        long fileID = 0;
                        var fileIdFound = true;

                        // The default output file path is the relative path of the file in the .tar file
                        // This will get changed below once the MyEMSL FileID is known

                        // Make sure the path doesn't start with a backslash
                        var downloadFilePath = sourceFileName.TrimStart('\\');
                        var originalFileSubmissionTime = DateTime.MinValue;
                        ArchivedFileInfo archiveFile = null;

                        if (charIndex < 1)
                        {
                            // ReportMessage("Warning, .tar file entry does not contain a backslash; " + "unable to validate the file or customize the output path: " + sourceFile);
                            fileIdFound = false;
                        }

                        if (fileIdFound)
                        {
                            var fileIDText = sourceFileName.Substring(0, charIndex);

                            if (!long.TryParse(fileIDText, out fileID))
                            {
                                ReportMessage("Warning, .tar file entry does not contain a MyEMSL FileID value; " +
                                              "unable to validate the file or customize the output path: " + sourceFileName);
                                fileIdFound = false;
                            }
                        }

                        if (fileIdFound)
                        {
                            // Lookup fileID in filesInArchive
                            var archiveFileLookup = GetArchivedFileByID(filesInArchive, fileID);

                            if (archiveFileLookup.Count == 0)
                            {
                                ReportMessage("Warning, MyEMSL FileID '" + fileID + "' was not recognized; " +
                                              "unable to validate the file or customize the output path: " + sourceFileName);
                                fileIdFound = false;
                            }
                            else
                            {
                                // Confirm that the name of the file in the .Tar file matches the expected file name
                                // Names in the tar file will be limited to 255 characters (including any preceding parent directory names) so we should not compare the full name
                                // Furthermore, the primary filename is limited to 100 characters, so it too could be truncated

                                archiveFile = archiveFileLookup[0];

                                var sourceFile = new FileInfo(sourceFileName);

                                if (!archiveFile.Filename.StartsWith(sourceFile.Name, StringComparison.OrdinalIgnoreCase))
                                {
                                    ReportMessage("Warning, name conflict; filename in .tar file is " + sourceFile.Name +
                                                  " but expected filename is " + archiveFile.Filename);
                                }

                                // Define the local file path
                                downloadFilePath = ConstructDownloadFilePath(directoryLayout, archiveFile);
                                downloadFilePath = Path.Combine(downloadDirectory.FullName, downloadFilePath);

                                if (destFilePathOverride.TryGetValue(archiveFile.FileID, out var filePathOverride))
                                {
                                    if (!string.IsNullOrEmpty(filePathOverride))
                                    {
                                        downloadFilePath = filePathOverride;
                                    }
                                }

                                originalFileSubmissionTime = archiveFile.SubmissionTimeValue;
                            }
                        }

                        if (!fileIdFound)
                        {
                            sourceFileName = sourceFileName.TrimStart('\\');

                            switch (directoryLayout)
                            {
                                case DownloadLayout.FlatNoSubdirectories:
                                    downloadFilePath = Path.GetFileName(sourceFileName);
                                    break;
                                case DownloadLayout.SingleDataset:
                                    downloadFilePath = sourceFileName;
                                    break;
                                default:
                                    // Includes: DownloadLayout.DatasetNameAndSubdirectories
                                    // Includes: DownloadLayout.InstrumentYearQuarterDataset
                                    ReportMessage("Warning, due to the missing MyEMSL FileID the DownloadLayout cannot be honored");
                                    downloadFilePath = sourceFileName;
                                    break;
                            }

                            downloadFilePath = Path.Combine(downloadDirectory.FullName, downloadFilePath);

                            var subDirPath = Path.GetDirectoryName(sourceFileName);

                            if (string.IsNullOrEmpty(subDirPath))
                            {
                                subDirPath = string.Empty;
                            }
                            else
                            {
                                subDirPath = subDirPath.Replace(@"\", "/");
                            }

                            // Look for this file in filesInArchive
                            var archiveFileLookup = GetArchivedFileByPath(filesInArchive, sourceFileName);

                            if (archiveFileLookup.Count == 0)
                            {
                                ReportMessage("File path not recognized: " + sourceFileName);
                                archiveFile = new ArchivedFileInfo("UnknownDataset", Path.GetFileName(sourceFileName), subDirPath);
                            }
                            else
                            {
                                archiveFile = archiveFileLookup[0];
                                originalFileSubmissionTime = archiveFile.SubmissionTimeValue;
                            }
                        }

                        if (downloadFilePath.Length > 255)
                        {
                            downloadFilePath = AddLongPathCode(downloadFilePath);
                        }

                        // Create the target directory if necessary
                        var targetFile = new FileInfo(downloadFilePath);

                        if (targetFile.Directory == null)
                        {
                            OnErrorEvent("Cannot determine the parent directory of " + targetFile.FullName);
                            continue;
                        }

                        if (!targetFile.Directory.Exists)
                        {
                            targetFile.Directory.Create();
                        }

                        // Extract the file from the stream
                        using (var outStr = new FileStream(targetFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))
                        {
                            tarIn.CopyEntryContents(outStr);
                        }

                        targetFile.Refresh();

                        if (originalFileSubmissionTime > DateTime.MinValue)
                        {
                            UpdateFileModificationTime(targetFile, originalFileSubmissionTime);
                        }

                        if (archiveFile.FileSizeBytes == 0 && targetFile.Exists)
                        {
                            archiveFile.FileSizeBytes = targetFile.Length;
                        }

                        if (!DownloadedFiles.ContainsKey(downloadFilePath))
                        {
                            DownloadedFiles.Add(downloadFilePath, archiveFile);
                        }

                        bytesDownloaded += archiveFile.FileSizeBytes;
                        UpdateProgress(bytesDownloaded, bytesToDownload);
                    }
                }
                else
                {
                    throw new HttpRequestException("HTTP response code not OK in DownloadAndExtractTarFile: " + response.StatusCode + ", " + response.ReasonPhrase);
                }
            }
            catch (HttpRequestException ex)
            {
                throw new Exception("HTTP GET error", ex);
            }
            catch (TaskCanceledException ex)
            {
                throw new Exception("HTTP request or file download timed out", ex);
            }
            catch (OperationCanceledException ex)
            {
                throw new Exception($"HTTP request was cancelled?: {ex.CancellationToken.IsCancellationRequested}", ex);
            }

            return true;
        }

        private void DuplicateFile(
            FileSystemInfo downloadDirectory,
            DownloadLayout directoryLayout,
            IReadOnlyDictionary<long, string> destFilePathOverride,
            FileInfo fiSourceFile,
            IReadOnlyDictionary<long, ArchivedFileInfo> filesToDownload,
            IEnumerable<long> targetFileIDs,
            IDictionary<long, string> filesDownloaded,
            IReadOnlyDictionary<string, int> targetFileCounts)
        {
            foreach (var targetFileID in targetFileIDs)
            {
                var targetArchiveFile = filesToDownload[targetFileID];

                var targetFileInitial = GetTargetFile(
                        downloadDirectory, directoryLayout,
                        targetArchiveFile, destFilePathOverride);

                if (targetFileInitial == null)
                {
                    continue;
                }

                var targetFile = GetTargetFileWithOptionalID(targetFileCounts, targetFileInitial, targetArchiveFile.FileID);

                fiSourceFile.CopyTo(targetFile.FullName, true);

                filesDownloaded.Add(targetArchiveFile.FileID, targetArchiveFile.PathWithInstrumentAndDatasetWindows);

                UpdateFileModificationTime(targetFile, targetArchiveFile.SubmissionTimeValue);

                if (!DownloadedFiles.ContainsKey(targetFile.FullName))
                {
                    DownloadedFiles.Add(targetFile.FullName, targetArchiveFile);
                }
            }
        }

        private bool FileMatchesHash(FileInfo localFile, string Sha1HashExpected)
        {
            var fileMatchesHash = false;

            try
            {
                var actualSha1Hash = Utilities.GenerateSha1Hash(localFile);

                if (actualSha1Hash == Sha1HashExpected)
                {
                    fileMatchesHash = true;
                }
            }
            catch (Exception ex)
            {
                ReportError("Exception in FileChanged", ex);
                return false;
            }

            return fileMatchesHash;
        }

        private List<ArchivedFileInfo> GetArchivedFileByID(IEnumerable<ArchivedFileInfo> filesInArchive, long fileID)
        {
            var archiveFileLookup = (from item in filesInArchive
                                     where item.FileID == fileID
                                     select item).ToList();
            return archiveFileLookup;
        }

        private List<ArchivedFileInfo> GetArchivedFileByPath(IEnumerable<ArchivedFileInfo> filesInArchive, string filePath)
        {
            var archiveFileLookup = (from item in filesInArchive
                                     where string.Equals(item.RelativePathWindows, filePath, StringComparison.OrdinalIgnoreCase)
                                     select item).ToList();

            return archiveFileLookup;
        }

        private FileInfo GetTargetFile(
            FileSystemInfo downloadDirectory,
            DownloadLayout directoryLayout,
            ArchivedFileInfo archiveFile,
            IReadOnlyDictionary<long, string> destFilePathOverride)
        {
            var downloadFilePathRelative = ConstructDownloadFilePath(directoryLayout, archiveFile);

            string downloadFilePath;

            if (destFilePathOverride.TryGetValue(archiveFile.FileID, out var filePathOverride) &&
                !string.IsNullOrEmpty(filePathOverride))
            {
                downloadFilePath = filePathOverride;
            }
            else
            {
                downloadFilePath = Path.Combine(downloadDirectory.FullName, downloadFilePathRelative);
            }

            FileInfo fiTargetFile;

            // Use a special prefix to work with files whose paths are more than 255 characters long
            // See https://msdn.microsoft.com/en-us/library/aa365247(v=vs.85).aspx#maxpath
            // This only works if the path is rooted
            if (downloadFilePath.Length >= PRISM.NativeIOFileTools.FILE_PATH_LENGTH_THRESHOLD && !downloadFilePath.StartsWith(@"\\?\"))
            {
                if (!Path.IsPathRooted(downloadFilePath))
                {
                    OnErrorEvent("Target file path is over 255 characters long and is a relative path; " +
                                 "cannot create file " + downloadFilePath);
                    return null;
                }

                fiTargetFile = new FileInfo(AddLongPathCode(downloadFilePath));
            }
            else
            {
                fiTargetFile = new FileInfo(downloadFilePath);
            }

            if (fiTargetFile.Directory == null)
            {
                OnErrorEvent("Cannot determine the parent directory for the target file; skipping " + fiTargetFile.FullName);
                return null;
            }

            if (!fiTargetFile.Directory.Exists)
            {
                ReportMessage("Creating target directory: " + fiTargetFile.Directory.FullName);
                fiTargetFile.Directory.Create();
            }

            return fiTargetFile;
        }

        private FileInfo GetTargetFileWithOptionalID(IReadOnlyDictionary<string, int> targetFileCounts, FileInfo targetFile, long myEmslFileID)
        {
            if (!targetFileCounts.TryGetValue(targetFile.FullName, out var fileCount))
            {
                throw new KeyNotFoundException(string.Format("Dictionary targetFileCounts does not have key {0}", targetFile.FullName));
            }

            if (fileCount == 1 || targetFile.DirectoryName == null)
            {
                return targetFile;
            }

            var filePathWithFileID = Path.Combine(
                targetFile.DirectoryName,
                string.Format("{0}_FileID_{1}{2}",
                    Path.GetFileNameWithoutExtension(targetFile.Name),
                    myEmslFileID,
                    Path.GetExtension(targetFile.Name)));

            return new FileInfo(filePathWithFileID);
        }

        private List<int> GetUniqueDatasetIDList(IReadOnlyDictionary<long, ArchivedFileInfo> files)
        {
            return (from item in files
                    group item by item.Value.DatasetID into g
                    select g.Key).ToList();
        }

        /// <summary>
        /// Determines whether or not a file should be downloaded
        /// </summary>
        /// <param name="archiveFile"></param>
        /// <param name="targetFile"></param>
        /// <param name="reportMessage"></param>
        private bool IsDownloadRequired(
            ArchivedFileInfo archiveFile,
            FileInfo targetFile,
            bool reportMessage)
        {
            bool downloadFile;
            string message;

            targetFile.Refresh();

            if (!targetFile.Exists)
            {
                if (reportMessage)
                {
                    ReportMessage("... downloading " + targetFile.FullName);
                }
                return true;
            }

            switch (OverwriteMode)
            {
                case Overwrite.Always:
                    message = "... overwriting " + targetFile.FullName;
                    downloadFile = true;
                    break;
                case Overwrite.IfChanged:
                    if (string.IsNullOrEmpty(archiveFile.Sha1Hash))
                    {
                        message = "overwriting (SHA-1 hash missing) " + targetFile.FullName;
                        downloadFile = true;
                        break;
                    }

                    if (FileMatchesHash(targetFile, archiveFile.Sha1Hash))
                    {
                        message = "skipping (file unchanged) " + targetFile.FullName;
                        downloadFile = false;
                    }
                    else
                    {
                        message = "overwriting changed file " + targetFile.FullName;
                        downloadFile = true;
                    }
                    break;
                case Overwrite.Never:
                    message = "skipping (Overwrite disabled) " + targetFile.FullName;
                    downloadFile = false;
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Unrecognized OverwriteMode: " + OverwriteMode);
            }

            if (reportMessage)
            {
                ReportMessage("... " + message);
            }

            return downloadFile;
        }

        /// <summary>
        /// Reset the percent complete and clear the list of downloaded files
        /// </summary>
        protected sealed override void ResetStatus()
        {
            base.ResetStatus();
            PercentComplete = 0;
            DownloadedFiles.Clear();
        }

        private void UpdateFileModificationTime(FileSystemInfo targetFile, DateTime lastWriteTime)
        {
            // Update the file modification time
            targetFile.Refresh();

            if (targetFile.Exists)
            {
                targetFile.LastWriteTime = lastWriteTime;
            }
        }

        private void UpdateProgress(long bytesDownloaded, long bytesToDownload)
        {
            if (bytesToDownload > 0)
            {
                var percentComplete = bytesDownloaded / (float)bytesToDownload * 100;
                PercentComplete = Math.Round(percentComplete);

                OnProgressUpdate("Downloading data", percentComplete);
            }
        }

        private bool ValidateRemoteCertificate(object sender, X509Certificate cert, X509Chain chain, SslPolicyErrors policyErrors)
        {
            var success = Utilities.ValidateRemoteCertificate(cert, policyErrors, out var errorMessage);

            if (success)
            {
                if (!string.IsNullOrWhiteSpace(errorMessage))
                {
                    OnWarningEvent(errorMessage);
                }
                return true;
            }

            OnErrorEvent(errorMessage);
            return false;
        }
    }
}
