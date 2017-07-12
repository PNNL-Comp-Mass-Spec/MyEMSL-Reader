using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Pacifica.Core;
using ICSharpCode.SharpZipLib.Tar;

namespace MyEMSLReader
{
    /// <summary>
    /// This class contacts MyEMSL to download the desired files, which must be specified by MyEMSL File ID values
    /// Use the Reader class to find files and determine File IDs
    /// </summary>
    /// <remarks>Written by Matthew Monroe for PNNL in August 2013</remarks>
    public class Downloader : MyEMSLBase
    {
        #region "Constants"

        #endregion

        #region "Enums"

        public enum DownloadFolderLayout
        {
            FlatNoSubfolders = 0,
            SingleDataset = 1,
            DatasetNameAndSubFolders = 2,
            InstrumentYearQuarterDataset = 3
        }

        public enum Overwrite
        {
            IfChanged = 0,			// This mode will compute an MD5 hash of the target file and only overwrite the target if the hash values differ
            Always = 1,
            Never = 2
        }
        public enum CartState
        {
            NoCart,
            Unsubmitted,
            Building,
            Available,
            Expired,
            Admin,
            Unknown
        }

        #endregion

        #region "Properties"

        public CartState DownloadCartState
        {
            get;
            private set;
        }

        /// <summary>
        /// When true, will never download files using the cart mechanism
        /// </summary>
        /// <remarks>ForceDownloadViaCart takes precedence over DisableCart</remarks>
        public bool DisableCart { get; set; }

        /// <summary>
        /// The most recently downloaded files.  Keys are the full path to the downloaded file; values are extended file info
        /// </summary>
        public Dictionary<string, ArchivedFileInfo> DownloadedFiles
        {
            get;
        }

        /// <summary>
        /// When true, will always download files using the cart mechanism, which is likely slower if the file is not purged to tape
        /// </summary>
        public bool ForceDownloadViaCart
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

        #endregion

        #region "Member Variables"

        #endregion

        #region "Public methods"

        // Constructor
        public Downloader()
        {
            ThrowErrors = true;
            OverwriteMode = Overwrite.IfChanged;
            DownloadedFiles = new Dictionary<string, ArchivedFileInfo>(StringComparer.OrdinalIgnoreCase);

            ResetStatus();
        }

        /// <summary>
        /// Download queued files
        /// </summary>
        /// <param name="filesToDownload">MyEMSL Files to download; keys are MyEMSL File IDs and values are ArchivedFileInfo objects</param>
        /// <param name="downloadFolderPath">Target folder path (ignored for files defined in destFilePathOverride)</param>
        /// <param name="folderLayout">Folder Layout (ignored for files defined in destFilePathOverride)</param>
        /// <param name="maxMinutesToWait">Maximum minutes to wait (default is 1440 minutes = 24 hours)</param>
        /// <returns>True if success, false if an error</returns>
        public bool DownloadFiles(
            Dictionary<long, ArchivedFileInfo> filesToDownload,
            string downloadFolderPath,
            DownloadFolderLayout folderLayout = DownloadFolderLayout.SingleDataset,
            int maxMinutesToWait = 1440)
        {
            var destFilePathOverride = new Dictionary<long, string>();
            return DownloadFiles(filesToDownload, destFilePathOverride, downloadFolderPath, folderLayout, maxMinutesToWait);
        }

        /// <summary>
        /// Download files in lstFileIDs
        /// </summary>
        /// <param name="filesToDownload">MyEMSL Files to download; keys are MyEMSL File IDs and values are ArchivedFileInfo objects</param>
        /// <param name="destFilePathOverride">Dictionary where keys are FileIDs and values are the explicit destination path to use</param>
        /// <param name="downloadFolderPath">Target folder path (ignored for files defined in destFilePathOverride)</param>
        /// <param name="folderLayout">Folder Layout (ignored for files defined in destFilePathOverride)</param>
        /// <param name="maxMinutesToWait">Maximum minutes to wait (default is 1440 minutes = 24 hours)</param>
        /// <remarks>destFilePathOverride is not required and can be empty; it can also have values for just some of the files in lstFileIDs</remarks>
        /// <returns>True if success, false if an error</returns>
        public bool DownloadFiles(
            Dictionary<long, ArchivedFileInfo> filesToDownload,
            Dictionary<long, string> destFilePathOverride,
            string downloadFolderPath,
            DownloadFolderLayout folderLayout = DownloadFolderLayout.SingleDataset,
            int maxMinutesToWait = 1440)
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
                if (ServicePointManager.ServerCertificateValidationCallback == null)
                    ServicePointManager.ServerCertificateValidationCallback += Utilities.ValidateRemoteCertificate;

                if (filesToDownload == null || filesToDownload.Count == 0)
                {
                    ReportError("File download dictionary is empty; nothing to download");
                    return false;
                }

                if (folderLayout == DownloadFolderLayout.SingleDataset)
                {
                    // Assure that the requested files all have the same dataset id
                    var datasetIDs = GetUniqueDatasetIDList(filesToDownload);

                    if (datasetIDs.Count > 1)
                    {
                        // Look for conflicts
                        var lstOutputFilePaths = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
                        foreach (var archiveFile in filesToDownload.Values)
                        {
                            if (lstOutputFilePaths.Contains(archiveFile.RelativePathWindows))
                            {
                                // File conflicts
                                ReportMessage("Auto-changing folder layout to 'DatasetNameAndSubFolders' since the files to download come from more than one dataset");
                                folderLayout = DownloadFolderLayout.DatasetNameAndSubFolders;
                                break;
                            }

                            lstOutputFilePaths.Add(archiveFile.RelativePathWindows);
                        }

                    }
                }

                var cookieJar = new CookieContainer();

                if (string.IsNullOrWhiteSpace(downloadFolderPath))
                    downloadFolderPath = ".";

                var downloadFolder = new DirectoryInfo(downloadFolderPath);

                // Download the files
                // Keys in this dictionary are FileIDs, values are relative file paths
                var filesDownloaded = DownloadFilesDirectly(
                    filesToDownload, cookieJar, destFilePathOverride,
                    downloadFolder, folderLayout, out var bytesDownloaded);

                // Create a list of the files that remain (files that could not be downloaded directly)
                // These files will be downloaded via the cart mechanism
                var filesToDownloadViaCart = new Dictionary<long, ArchivedFileInfo>();

                foreach (var archivedFileInfo in filesToDownload)
                {
                    var fileID = archivedFileInfo.Key;
                    var archivefile = archivedFileInfo.Value;

                    if (filesDownloaded.ContainsKey(archivedFileInfo.Key))
                    {
                        continue;
                    }

                    var targetFile = GetTargetFile(
                       downloadFolder, folderLayout,
                       archivefile, destFilePathOverride);

                    if (targetFile == null)
                        continue;

                    // Confirm one more time that we need to download the file
                    var downloadFile = IsDownloadRequired(archivefile, targetFile, reportMessage: false);

                    if (downloadFile)
                    {
                        filesToDownloadViaCart.Add(fileID, archivefile);
                    }
                }

                if (filesToDownloadViaCart.Count == 0)
                {
                    // All of the files have been downloaded (or already exist and having matching a matching Sha-1 hash)
                    return true;
                }

                if (!ForceDownloadViaCart && DisableCart)
                {
                    ReportError(filesToDownloadViaCart.Count + " purged files(s) could not be downloaded because DisableCart=true");
                    return false;
                }

                var cartSuccess = DownloadFilesViaCart(
                    filesToDownloadViaCart, cookieJar, destFilePathOverride,
                    downloadFolder, folderLayout, out var bytesDownloadedViaCart);

                bytesDownloaded += bytesDownloadedViaCart;

                OnDebugEvent(string.Format("Downloaded {0:F1} MB total", bytesDownloaded / 1024.0 / 1024));

                return cartSuccess;

            }
            catch (Exception ex)
            {
                if (string.IsNullOrWhiteSpace(ErrorMessage))
                    ReportError("Error in MyEMSLReader.Downloader.Downloadfiles: " + ex.Message);
                else if (ThrowErrors)
                    throw;
            }

            return false;
        }

        #endregion

        #region "Private Methods"

        /// <summary>
        /// Possibly add a special prefix to work with files whose paths are more than 255 characters long
        /// See https://msdn.microsoft.com/en-us/library/aa365247(v=vs.85).aspx#maxpath
        /// </summary>
        /// <param name="fileOrFolderPath"></param>
        /// <returns>Updated path</returns>
        /// <remarks>This only works if the path is rooted</remarks>
        private string AddLongPathCode(string fileOrFolderPath)
        {
            if (fileOrFolderPath.Length <= 255 || fileOrFolderPath.StartsWith(@"\\?\"))
                return fileOrFolderPath;

            if (!Path.IsPathRooted(fileOrFolderPath))
            {
                throw new PathTooLongException(
                    "Target file path is over 255 characters long and is a relative path; " +
                    "cannot work with path " + fileOrFolderPath);
            }

            return @"\\?\" + fileOrFolderPath;
        }

        /// <summary>
        /// Append file metadata to a JSON cart file
        /// </summary>
        /// <param name="postData">Data to post to the cart server</param>
        /// <param name="fileMetadata">File metadata</param>
        /// <param name="appendComma">True to append a comma</param>
        private void AppendToCartPostData(
            StringBuilder postData,
            IEnumerable<KeyValuePair<string, string>> fileMetadata,
            bool appendComma)
        {
            postData.AppendLine("{");

            var optionalComma = appendComma ? "," : "";

            foreach (var item in fileMetadata)
            {
                postData.AppendLine(
                    string.Format("  \"{0}\":\"{1}\"{2}",
                    item.Key, item.Value, optionalComma));
            }

            postData.AppendLine("}");

        }

        private long ComputeTotalBytes(IReadOnlyDictionary<long, ArchivedFileInfo> dctFiles)
        {
            return ComputeTotalBytes(dctFiles.Values);
        }

        private long ComputeTotalBytes(IEnumerable<ArchivedFileInfo> dctFiles)
        {
            var bytesToDownload = dctFiles.Sum(archiveFile => archiveFile.FileSizeBytes);
            return bytesToDownload;
        }

        private string ConstructDownloadfilePath(DownloadFolderLayout folderLayout, ArchivedFileInfo archiveFile)
        {
            string downloadFilePath;
            switch (folderLayout)
            {
                case DownloadFolderLayout.FlatNoSubfolders:
                    downloadFilePath = archiveFile.Filename;
                    break;
                case DownloadFolderLayout.SingleDataset:
                    downloadFilePath = archiveFile.RelativePathWindows;
                    break;
                case DownloadFolderLayout.DatasetNameAndSubFolders:
                    downloadFilePath = Path.Combine(archiveFile.Dataset, archiveFile.RelativePathWindows);
                    break;
                case DownloadFolderLayout.InstrumentYearQuarterDataset:
                    downloadFilePath = archiveFile.PathWithInstrumentAndDatasetWindows;
                    break;
                default:
                    ReportError("Unrecognized DownloadFolderLayout mode: " + folderLayout.ToString());
                    downloadFilePath = Path.Combine(archiveFile.Dataset, archiveFile.RelativePathWindows);
                    break;
            }

            if (downloadFilePath.IndexOf("/", StringComparison.Ordinal) > 0)
                downloadFilePath = downloadFilePath.Replace('/', Path.DirectorySeparatorChar);

            return downloadFilePath;
        }

        /// <summary>
        /// Create a JSON file with the files to download
        /// </summary>
        /// <returns></returns>
        private StringBuilder CreateCartPostData(Dictionary<long, ArchivedFileInfo> filesToDownload)
        {
            // Example cart file contents:
            //   {
            //    "fileids": [
            //      {"id":"SpruceW_P19_15_22Jun17_Pippin_17-04-06_HighAbu_LCMS.png", "path":"DLS201707030948_Auto1468045/SpruceW_P19_15_22Jun17_Pippin_17-04-06_HighAbu_LCMS.png", "hashtype":"sha1", "hashsum":"08a0cffa75b79ac18e4f375a80e6a80d63784888"},
            //      {"id":"SpruceW_P19_15_22Jun17_Pippin_17-04-06_HighAbu_LCMS_zoom.png", "path":"DLS201707030948_Auto1468045/SpruceW_P19_15_22Jun17_Pippin_17-04-06_HighAbu_LCMS_zoom.png", "hashtype":"sha1", "hashsum":"2c3ea5561186b36291b2a4ddcf5e3f9d9b60fa2f"}
            //    ]
            //   }

            try
            {
                var postData = new StringBuilder();

                postData.AppendLine("{ \"fileids\": [");

                var fileNumber = 0;
                foreach (var file in filesToDownload)
                {
                    fileNumber++;

                    var fileMetadata = new List<KeyValuePair<string, string>> {
                        new KeyValuePair<string, string>("id", file.Value.Filename),
                        new KeyValuePair<string, string>("path", file.Value.RelativePathUnix),
                        new KeyValuePair<string, string>("hashtype", file.Value.HashType),
                        new KeyValuePair<string, string>("hashsum", file.Value.Hash)
                    };

                    var appendComma = fileNumber < filesToDownload.Count;
                    AppendToCartPostData(postData, fileMetadata, appendComma);

                }

                postData.AppendLine("] }");


                return postData;
            }
            catch (Exception ex)
            {
                ReportError("Exception in CreateCartPostData: " + ex.Message, ex);
                return new StringBuilder();
            }

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

            var timeoutSeconds = 100;
            var attempts = 1;

            if (maxAttempts < 1)
                maxAttempts = 1;

            var success = false;
            var triedGC = false;

            // Use a special prefix to work with files whose paths are more than 255 characters long
            // See https://msdn.microsoft.com/en-us/library/aa365247(v=vs.85).aspx#maxpath
            if (downloadFilePath.Length > 255 && !downloadFilePath.StartsWith(@"\\?\"))
                downloadFilePath = @"\\?\" + downloadFilePath;

            while (!success && attempts <= maxAttempts)
            {
                try
                {
                    attempts++;
                    success = EasyHttp.GetFile(
                        mPacificaConfig, URL, cookieJar,
                        out var responseStatusCode,
                        downloadFilePath, timeoutSeconds);

                    if (!success)
                        break;
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

                    var webException = ex.InnerException as WebException;
                    if (webException?.Response != null)
                    {
                        responseStatusCode = ((HttpWebResponse)webException.Response).StatusCode;
                    }

                    if (responseStatusCode == HttpStatusCode.ServiceUnavailable)
                    {
                        success = false;
                        mostRecentException = webException;
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
        /// <param name="downloadFolder"></param>
        /// <param name="folderLayout"></param>
        /// <param name="bytesDownloaded"></param>
        /// <returns></returns>
        private Dictionary<long, string> DownloadFilesDirectly(
            IReadOnlyDictionary<long, ArchivedFileInfo> filesToDownload,
            CookieContainer cookieJar,
            IReadOnlyDictionary<long, string> destFilePathOverride,
            FileSystemInfo downloadFolder,
            DownloadFolderLayout folderLayout,
            out long bytesDownloaded)
        {
            var filesDownloaded = new Dictionary<long, string>();
            bytesDownloaded = 0;

            if (ForceDownloadViaCart)
                return filesDownloaded;

            try
            {
                // Determine total amount of data to be downloaded
                var bytesToDownload = ComputeTotalBytes(filesToDownload);

                // Construct a mapping between unique file hash and target MyEMSL File IDs
                // Typically there will only be one MyEMSL file per file hash, but if downloading a dataset with
                // lots of similar jobs, or a set of datasets, we can have files with the same hash but different
                // destination paths

                var fileHashToTypeMap = new Dictionary<string, string>();
                var fileHashToIdMap = new Dictionary<string, SortedSet<long>>();

                foreach (var archivedFileInfo in filesToDownload)
                {
                    var fileHash = archivedFileInfo.Value.Hash;

                    if (fileHashToIdMap.TryGetValue(fileHash, out var fileIDs))
                    {
                        fileIDs.Add(archivedFileInfo.Key);
                        continue;
                    }

                    fileIDs = new SortedSet<long> {
                        archivedFileInfo.Key
                    };

                    fileHashToIdMap.Add(fileHash, fileIDs);

                    fileHashToTypeMap.Add(fileHash, archivedFileInfo.Value.HashType);
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

                    var targetFile = GetTargetFile(
                        downloadFolder, folderLayout,
                        firstArchiveFile, destFilePathOverride);

                    if (targetFile == null)
                        continue;

                    const int DEFAULT_MAX_ATTEMPTS = 5;
                    var fileInUseByOtherProcess = false;

                    var downloadFile = IsDownloadRequired(firstArchiveFile, targetFile, reportMessage: true);
                    bool fileRetrievedOrExists;

                    if (downloadFile)
                    {
                        var maxAttempts = DEFAULT_MAX_ATTEMPTS;

                        var retrievalSuccess = DownloadFile(
                            URL, cookieJar, maxAttempts,
                            targetFile.FullName,
                            out var mostRecentException,
                            out fileInUseByOtherProcess);

                        if (retrievalSuccess)
                        {
                            filesDownloaded.Add(firstArchiveFile.FileID, firstArchiveFile.PathWithInstrumentAndDatasetWindows);

                            if (firstArchiveFile.FileLastWriteTime > DateTime.MinValue)
                                UpdateFileModificationTime(targetFile, firstArchiveFile.FileLastWriteTime);
                            else if (firstArchiveFile.SubmissionTimeValue > DateTime.MinValue)
                                UpdateFileModificationTime(targetFile, firstArchiveFile.SubmissionTimeValue);

                            fileRetrievedOrExists = true;
                        }
                        else
                        {
                            // Show the error at the console but do not throw an exception
                            if (mostRecentException == null)
                                ReportMessage("Failure downloading " + Path.GetFileName(targetFile.FullName) + ": unknown reason");
                            else
                                ReportMessage("Failure downloading " + Path.GetFileName(targetFile.FullName) + ": " + mostRecentException.Message);

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
                            downloadFolder, folderLayout, destFilePathOverride,
                            targetFile, filesToDownload, fileIDs.Skip(1), filesDownloaded);
                    }

                    if (fileInUseByOtherProcess || !downloadFile)
                    {
                        // Download skipped
                        // Need to add to the downloaded files dictionary so that the file doesn't get downloaded via the .tar file mechanism
                        filesDownloaded.Add(firstArchiveFile.FileID, firstArchiveFile.PathWithInstrumentAndDatasetWindows);
                    }

                    if (!DownloadedFiles.ContainsKey(targetFile.FullName))
                        DownloadedFiles.Add(targetFile.FullName, firstArchiveFile);

                    bytesDownloaded += firstArchiveFile.FileSizeBytes;
                    UpdateProgress(bytesDownloaded, bytesToDownload);
                }
            }
            catch (Exception ex)
            {
                ReportError("Exception in DownloadFilesDirectly: " + ex.Message, ex);
                return new Dictionary<long, string>();
            }

            return filesDownloaded;
        }

        private bool DownloadFilesViaCart(
            Dictionary<long, ArchivedFileInfo> filesToDownload,
            CookieContainer cookieJar,
            Dictionary<long, string> destFilePathOverride,
            DirectoryInfo downloadFolder,
            DownloadFolderLayout folderLayout,
            out long bytesDownloaded)
        {

            bytesDownloaded = 0;

            ReportError(filesToDownload.Count + " purged files(s) could not be downloaded because the Cart Mechanism is not implemented in the Downloader class");

            try
            {

                // Create a JSON file with the files to download

                var postData = CreateCartPostData(filesToDownload);

                if (postData.Length == 0)
                    return false;

                // Post the JSON file to the cart server
                var success = PostCartData(postData.ToString(), cookieJar, out var cartId);

                if (!success)
                    return false;


                // Check cart status periodically
                // Examine the HEAD of http://cart.my.emsl.pnl.gov/7bf711b3-b736-43b0-9ac4-138d0ccfe8de

                // Retrieve the cart as a .tar file
                // http://cart.my.emsl.pnl.gov/7bf711b3-b736-43b0-9ac4-138d0ccfe8de?filename=my_cart.tar

                // See DownloadTarFileWithRetry();

                throw new NotImplementedException("Need to finish this code");

                return true;
            }
            catch (Exception ex)
            {
                ReportError("Exception in DownloadFilesViaCart: " + ex.Message, ex);
                return false;
            }


            /*
             * Old cart code
             *

            // Scan for the remaining files, thereby creating a ScrollID
            // We will also obtain a new authorization token, which will be associated with the ScrollID
            success = CreateScrollID(lstFilesRemaining, ref cookieJar, out authToken);
            if (!success)
            {
                if (string.IsNullOrWhiteSpace(ErrorMessage))
                    ReportError("Scroll ID is empty; cannot download files");
                return false;
            }

            // Create a cart
            var cartID = CreateCart(lstFilesRemaining, cookieJar, authToken);
            if (cartID <= 0)
            {
                if (string.IsNullOrWhiteSpace(ErrorMessage))
                    ReportError("Cart ID is 0; cannot download files");
                return false;
            }

            // Initialize .Tar File Creation
            success = InitializeCartCreation(cartID, cookieJar);
            if (!success)
            {
                if (string.IsNullOrWhiteSpace(ErrorMessage))
                    ReportError("Error initializing cart " + cartID);
                return false;
            }


            // Wait for the .Tar file to be created
            if (maxMinutesToWait < 2)
                maxMinutesToWait = 2;

            string tarFileURL;
            success = WaitForCartSuccess(cartID, cookieJar, maxMinutesToWait, out tarFileURL);
            if (!success)
            {
                if (string.IsNullOrWhiteSpace(ErrorMessage))
                    ReportError("Error waiting for cart " + cartID + " to become available");
                return false;
            }

            // Extract the files from the .tar file
            success = DownloadTarFileWithRetry(cookieJar, dctFiles.Keys.ToList(), bytesDownloaded, destFilePathOverride, downloadFolderPath, folderLayout, tarFileURL);

            *
            *
            */

            return false;

        }

        private bool DownloadTarFileWithRetry(
            CookieContainer cookieJar,
            List<ArchivedFileInfo> lstFilesInArchive,
            long bytesDownloaded,
            IReadOnlyDictionary<long, string> destFilePathOverride,
            FileSystemInfo downloadFolder,
            DownloadFolderLayout folderLayout,
            string tarFileURL)
        {
            var success = false;

            try
            {
                const int maxAttempts = 5;
                Exception mostRecentException = null;

                var timeoutSeconds = 100;
                var attempts = 0;

                while (!success && attempts <= maxAttempts)
                {
                    try
                    {
                        attempts++;
                        success = DownloadAndExtractTarFile(cookieJar, lstFilesInArchive, bytesDownloaded, destFilePathOverride, downloadFolder, folderLayout, tarFileURL, timeoutSeconds);

                        if (!success)
                            break;
                    }
                    catch (Exception ex)
                    {
                        mostRecentException = ex;
                        if (attempts >= maxAttempts)
                        {
                            success = false;
                        }
                        else
                        {
                            // Wait 2 seconds, then retry
                            OnWarningEvent("Exception in DownloadTarFileWithRetry on attempt " + attempts + ": " + ex.Message);
                            Thread.Sleep(2000);
                            timeoutSeconds = IncreaseTimeout(timeoutSeconds);
                        }
                    }
                }

                if (!success)
                {
                    if (mostRecentException == null)
                        ReportMessage("Failed to extract files from .tar file: unknown reason");
                    else
                        ReportMessage("Failed to extract files from .tar file: unknown reason: " + mostRecentException.Message);

                    return false;
                }

                ReportMessage("Successfully extracted files from .tar file at " + tarFileURL);
                UpdateProgress(1, 1);

            }
            catch (Exception ex)
            {
                ReportError("Exception in ExtractTarFile: " + ex.Message, ex);
                return false;
            }

            return true;

        }

        private bool DownloadAndExtractTarFile(
            CookieContainer cookieJar,
            List<ArchivedFileInfo> lstFilesInArchive,
            long bytesDownloaded,
            IReadOnlyDictionary<long, string> destFilePathOverride,
            FileSystemInfo downloadFolder,
            DownloadFolderLayout folderLayout,
            string tarFileURL,
            int timeoutSeconds = 100)
        {

            const double maxTimeoutHours = 24;
            NetworkCredential loginCredentials = null;

            // ReSharper disable once ExpressionIsAlwaysNull
            var request = EasyHttp.InitializeRequest(mPacificaConfig, tarFileURL, ref cookieJar, ref timeoutSeconds, loginCredentials, maxTimeoutHours);

            var bytesToDownload = ComputeTotalBytes(lstFilesInArchive);

            // Prepare the request object
            request.Method = "GET";
            request.PreAuthenticate = false;

            // Receive response
            HttpWebResponse response = null;
            try
            {
                request.Timeout = timeoutSeconds * 1000;
                response = (HttpWebResponse)request.GetResponse();

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    // Download the file and extract the files as the file is downloaded
                    // This way, the .tar file is never actually created on a local hard drive
                    // Code modelled after https://github.com/icsharpcode/SharpZipLib/wiki/GZip-and-Tar-Samples

                    var receiveStream = response.GetResponseStream();

                    var tarIn = new TarInputStream(receiveStream);
                    TarEntry tarEntry;
                    while ((tarEntry = tarIn.GetNextEntry()) != null)
                    {
                        if (tarEntry.IsDirectory)
                        {
                            continue;
                        }

                        var sourceFile = tarEntry.Name;

                        // Long filenames (over 100 characters) will have part of their name in tarEntry.Name and part of it in tarEntry.Prefix
                        // Check for this
                        if (!string.IsNullOrEmpty(tarEntry.Prefix))
                            sourceFile = tarEntry.Prefix + '/' + sourceFile;

                        // Convert the unix forward slashes in the filenames to windows backslashes
                        sourceFile = sourceFile.Replace('/', Path.DirectorySeparatorChar);

                        // The Filename of the tar entry used to start with a folder name that is a MyEMSL FileID
                        // As of March 2016 that is no longer the case
                        var charIndex = sourceFile.IndexOf(Path.DirectorySeparatorChar);

                        long fileID = 0;
                        var fileIdFound = true;

                        // The default output file path is the relative path of the file in the .tar file
                        // This will get changed below once the MyEMSL FileID is known

                        // Make sure the path doesn't start with a backslash
                        var downloadFilePath = sourceFile.TrimStart('\\');
                        var originalFileSubmissionTime = DateTime.MinValue;
                        ArchivedFileInfo archiveFile = null;

                        if (charIndex < 1)
                        {
                            /*
                            ReportMessage("Warning, .tar file entry does not contain a backslash; " +
                                          "unable to validate the file or customize the output path: " + sourceFile);
                            */
                            fileIdFound = false;
                        }

                        if (fileIdFound)
                        {
                            var fileIDText = sourceFile.Substring(0, charIndex);
                            if (!long.TryParse(fileIDText, out fileID))
                            {
                                ReportMessage("Warning, .tar file entry does not contain a MyEMSL FileID value; " +
                                              "unable to validate the file or customize the output path: " + sourceFile);
                                fileIdFound = false;
                            }
                        }

                        if (fileIdFound)
                        {
                            // Lookup fileID in dctFiles
                            var archiveFileLookup = GetArchivedFileByID(lstFilesInArchive, fileID);

                            if (archiveFileLookup.Count == 0)
                            {
                                ReportMessage("Warning, MyEMSL FileID '" + fileID + "' was not recognized; " +
                                              "unable to validate the file or customize the output path: " + sourceFile);
                                fileIdFound = false;
                            }
                            else
                            {
                                // Confirm that the name of the file in the .Tar file matches the expected file name
                                // Names in the tar file will be limited to 255 characters (including any preceding parent folder names) so we should not compare the full name
                                // Furthermore, the primary filename is limited to 100 characters, so it too could be truncated

                                archiveFile = archiveFileLookup.First();

                                var fiSourceFile = new FileInfo(sourceFile);
                                if (!archiveFile.Filename.ToLower().StartsWith(fiSourceFile.Name.ToLower()))
                                    ReportMessage("Warning, name conflict; filename in .tar file is " + fiSourceFile.Name +
                                                  " but expected filename is " + archiveFile.Filename);

                                // Define the local file path
                                downloadFilePath = ConstructDownloadfilePath(folderLayout, archiveFile);
                                downloadFilePath = Path.Combine(downloadFolder.FullName, downloadFilePath);

                                if (destFilePathOverride.TryGetValue(archiveFile.FileID, out var filePathOverride))
                                {
                                    if (!string.IsNullOrEmpty(filePathOverride))
                                        downloadFilePath = filePathOverride;
                                }

                                originalFileSubmissionTime = archiveFile.SubmissionTimeValue;

                            }
                        }

                        if (!fileIdFound)
                        {
                            sourceFile = sourceFile.TrimStart('\\');

                            switch (folderLayout)
                            {
                                case DownloadFolderLayout.FlatNoSubfolders:
                                    downloadFilePath = Path.GetFileName(sourceFile);
                                    break;
                                case DownloadFolderLayout.SingleDataset:
                                    downloadFilePath = sourceFile;
                                    break;
                                default:
                                    // Includes: DownloadFolderLayout.DatasetNameAndSubFolders
                                    // Includes: DownloadFolderLayout.InstrumentYearQuarterDataset
                                    ReportMessage("Warning, due to the missing MyEMSL FileID the DownloadFolderLayout cannot be honored");
                                    downloadFilePath = sourceFile;
                                    break;
                            }

                            downloadFilePath = Path.Combine(downloadFolder.FullName, downloadFilePath);

                            var subDirPath = Path.GetDirectoryName(sourceFile);
                            if (string.IsNullOrEmpty(subDirPath))
                                subDirPath = string.Empty;
                            else
                            {
                                subDirPath = subDirPath.Replace(@"\", "/");
                            }

                            // Look for this file in lstFilesInArchive
                            var archiveFileLookup = GetArchivedFileByPath(lstFilesInArchive, sourceFile);

                            if (archiveFileLookup.Count == 0)
                            {
                                ReportMessage("File path not recognized: " + sourceFile);
                                archiveFile = new ArchivedFileInfo("UnknownDataset", Path.GetFileName(sourceFile), subDirPath);
                            }
                            else
                            {
                                archiveFile = archiveFileLookup.First();
                                originalFileSubmissionTime = archiveFile.SubmissionTimeValue;
                            }

                        }

                        if (downloadFilePath.Length > 255)
                            downloadFilePath = AddLongPathCode(downloadFilePath);

                        // Create the target folder if necessary
                        var targetFile = new FileInfo(downloadFilePath);

                        if (targetFile.Directory == null)
                        {
                            OnErrorEvent("Cannot determine the parent directory of " + targetFile.FullName);
                            continue;
                        }

                        if (!targetFile.Directory.Exists)
                            targetFile.Directory.Create();

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
                            DownloadedFiles.Add(downloadFilePath, archiveFile);

                        bytesDownloaded += archiveFile.FileSizeBytes;
                        UpdateProgress(bytesDownloaded, bytesToDownload);
                    }

                }
                else
                {
                    throw new WebException("HTTP response code not OK in DownloadAndExtractTarFile: " + response.StatusCode + ", " + response.StatusDescription);
                }
            }
            catch (WebException ex)
            {
                var responseData = string.Empty;
                if (ex.Response != null)
                {
                    using (var sr = new StreamReader(ex.Response.GetResponseStream()))
                    {
                        const int maxLines = 20;
                        var linesRead = 0;
                        while (sr.Peek() > -1 && linesRead < maxLines)
                        {
                            responseData += sr.ReadLine() + Environment.NewLine;
                            linesRead++;
                        }
                    }
                }
                throw new Exception(responseData, ex);
            }
            finally
            {
                ((IDisposable)response)?.Dispose();
            }

            return true;
        }

        private void DuplicateFile(
            FileSystemInfo downloadFolder,
            DownloadFolderLayout folderLayout,
            IReadOnlyDictionary<long, string> destFilePathOverride,
            FileInfo fiSourceFile,
            IReadOnlyDictionary<long, ArchivedFileInfo> filesToDownload,
            IEnumerable<long> targetFileIDs,
            IDictionary<long, string> filesDownloaded)
        {

            foreach (var targetFileID in targetFileIDs)
            {
                var targetArchiveFile = filesToDownload[targetFileID];

                var targetFile = GetTargetFile(
                        downloadFolder, folderLayout,
                        targetArchiveFile, destFilePathOverride);

                if (targetFile == null)
                    continue;

                fiSourceFile.CopyTo(targetFile.FullName, true);

                filesDownloaded.Add(targetArchiveFile.FileID, targetArchiveFile.PathWithInstrumentAndDatasetWindows);

                UpdateFileModificationTime(targetFile, targetArchiveFile.SubmissionTimeValue);

                if (!DownloadedFiles.ContainsKey(targetFile.FullName))
                    DownloadedFiles.Add(targetFile.FullName, targetArchiveFile);
            }

        }

        private bool FileMatchesHash(string localFilePath, string Sha1HashExpected)
        {
            var fileMatchesHash = false;

            try
            {
                var actualSha1Hash = Utilities.GenerateSha1Hash(localFilePath);

                if (actualSha1Hash == Sha1HashExpected)
                    fileMatchesHash = true;
            }
            catch (Exception ex)
            {
                ReportError("Exception in FileChanged: " + ex.Message, ex);
                return false;
            }

            return fileMatchesHash;
        }

        private List<ArchivedFileInfo> GetArchivedFileByID(List<ArchivedFileInfo> lstFilesInArchive, long fileID)
        {
            var archiveFileLookup = (from item in lstFilesInArchive
                                     where item.FileID == fileID
                                     select item).ToList();
            return archiveFileLookup;
        }

        private List<ArchivedFileInfo> GetArchivedFileByPath(List<ArchivedFileInfo> lstFilesInArchive, string filePath)
        {
            var archiveFileLookup = (from item in lstFilesInArchive
                                     where string.Equals(item.RelativePathWindows, filePath, StringComparison.InvariantCultureIgnoreCase)
                                     select item).ToList();

            return archiveFileLookup;
        }

        private FileInfo GetTargetFile(
            FileSystemInfo downloadFolder,
            DownloadFolderLayout folderLayout,
            ArchivedFileInfo archiveFile,
            IReadOnlyDictionary<long, string> destFilePathOverride)
        {

            var downloadFilePathRelative = ConstructDownloadfilePath(folderLayout, archiveFile);

            string downloadFilePath;

            if (destFilePathOverride.TryGetValue(archiveFile.FileID, out var filePathOverride) &&
                !string.IsNullOrEmpty(filePathOverride))
            {
                downloadFilePath = filePathOverride;
            }
            else
            {
                downloadFilePath = Path.Combine(downloadFolder.FullName, downloadFilePathRelative);
            }

            FileInfo fiTargetFile;

            // Use a special prefix to work with files whose paths are more than 255 characters long
            // See https://msdn.microsoft.com/en-us/library/aa365247(v=vs.85).aspx#maxpath
            // This only works if the path is rooted
            if (downloadFilePath.Length > 255 && !downloadFilePath.StartsWith(@"\\?\"))
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
                ReportMessage("Creating target folder: " + fiTargetFile.Directory.FullName);
                fiTargetFile.Directory.Create();
            }

            return fiTargetFile;
        }

        private List<int> GetUniqueDatasetIDList(IReadOnlyDictionary<long, ArchivedFileInfo> dctFiles)
        {
            var datasetIDs = (from item in dctFiles
                              group item by item.Value.DatasetID into g
                              select g.Key).ToList();
            return datasetIDs;
        }

        /// <summary>
        /// Determines whether or not a file should be downloaded
        /// </summary>
        /// <param name="archiveFile"></param>
        /// <param name="targetFile"></param>
        /// <param name="reportMessage"></param>
        /// <returns></returns>
        private bool IsDownloadRequired(
            ArchivedFileInfo archiveFile,
            FileSystemInfo targetFile,
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
                        message = "overwriting (Sha1 hash missing) " + targetFile.FullName;
                        downloadFile = true;
                        break;
                    }

                    if (FileMatchesHash(targetFile.FullName, archiveFile.Sha1Hash))
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
        /// Post the JSON data to the cart server
        /// </summary>
        /// <param name="postData"></param>
        /// <param name="cookieJar"></param>
        /// <param name="cartId"></param>
        /// <returns>True if success, false if an error</returns>
        private bool PostCartData(string postData, CookieContainer cookieJar, out Guid cartId)
        {
            cartId = new Guid();

            try
            {

                // Construct the URL to post the cart file to, for example
                // https://cart.my.emsl.pnl.gov/7bf711b3-b736-43b0-9ac4-138d0ccfe8de
                var URL = mPacificaConfig.CartServerUri + "/" + cartId;

                const int maxAttempts = 2;
                var timeoutSeconds = 2;
                var attempts = 0;
                var success = false;

                while (!success && attempts <= maxAttempts)
                {
                    try
                    {
                        attempts++;


                        const bool allowEmptyResponseData = false;

                        success = SendHTTPRequestWithRetry(
                            URL, cookieJar, postData,
                            EasyHttp.HttpMethod.Post,
                            maxAttempts,
                            allowEmptyResponseData,
                            out var xmlString,
                            out var mostRecentException);

                        if (!success)
                            break;
                    }
                    catch (Exception ex)
                    {
                        var responseStatusCode = HttpStatusCode.OK;

                        var webException = ex.InnerException as WebException;
                        if (webException != null)
                        {
                            responseStatusCode = ((HttpWebResponse)webException.Response).StatusCode;
                        }

                        if (responseStatusCode == HttpStatusCode.ServiceUnavailable)
                        {
                            success = false;
                        }
                        else if (attempts >= maxAttempts)
                        {
                            success = false;
                        }
                        else
                        {
                            // Wait 2 seconds, then retry
                            OnWarningEvent("Exception in PostCartFile on attempt " + attempts + ": " + ex.Message);
                            Thread.Sleep(2000);
                            timeoutSeconds = IncreaseTimeout(timeoutSeconds);
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                ReportError("Exception in PostCartFile: " + ex.Message, ex);
                return false;
            }

        }

        protected sealed override void ResetStatus()
        {
            base.ResetStatus();
            DownloadCartState = CartState.NoCart;
            PercentComplete = 0;
            DownloadedFiles.Clear();
        }

        [Obsolete("Obsolete in June 2017")]
        private WebHeaderCollection SendHeadRequestWithRetry(
            string URL,
            CookieContainer cookieJar,
            int maxAttempts,
            out HttpStatusCode responseStatusCode,
            out Exception mostRecentException)
        {

            mostRecentException = null;

            // The following Callback allows us to access the MyEMSL server even if the certificate is expired or untrusted
            // For more info, see comments in Reader.RunElasticSearchQuery()
            if (ServicePointManager.ServerCertificateValidationCallback == null)
                ServicePointManager.ServerCertificateValidationCallback += Utilities.ValidateRemoteCertificate;
            var responseHeaders = new WebHeaderCollection();

            var timeoutSeconds = 2;
            var attempts = 0;
            var success = false;
            responseStatusCode = HttpStatusCode.NotFound;

            while (!success && attempts <= maxAttempts)
            {
                try
                {
                    attempts++;
                    responseHeaders = EasyHttp.GetHeaders(
                        mPacificaConfig, URL, cookieJar,
                        out responseStatusCode, timeoutSeconds);

                    if (responseHeaders == null || responseHeaders.Count == 0)
                    {
                        OnWarningEvent("Empty headers in SendHeadRequestWithRetry on attempt " + attempts);
                        timeoutSeconds = IncreaseTimeout(timeoutSeconds);
                    }
                    else
                        success = true;
                }
                catch (Exception ex)
                {
                    mostRecentException = ex;

                    if (responseStatusCode == HttpStatusCode.ServiceUnavailable)
                        // File is not locked; no point in retrying the head request.
                        break;

                    if (attempts <= maxAttempts)
                    {
                        // Wait 2 seconds, then retry
                        OnWarningEvent("Exception in SendHeadRequestWithRetry on attempt " + attempts + ": " + ex.Message);
                        Thread.Sleep(2000);
                        timeoutSeconds = IncreaseTimeout(timeoutSeconds);
                    }
                }
            }

            return responseHeaders;
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

        #endregion
    }
}
