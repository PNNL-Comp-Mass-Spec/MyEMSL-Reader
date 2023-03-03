using System;
using System.IO;
using System.Collections.Generic;
using PRISM;

namespace MyEMSLReader
{
    /// <summary>
    /// Download queue
    /// </summary>
    public class DownloadQueue : EventNotifier
    {
        /// <summary>
        /// File download info
        /// </summary>
        public struct FileDownloadInfo
        {
            /// <summary>
            /// Archived file info
            /// </summary>
            public ArchivedFileInfo FileInfo;

            /// <summary>
            /// True if the file should be unzipped after downloading
            /// </summary>
            public bool UnzipRequired;

            /// <summary>
            /// Destination file path
            /// </summary>
            public string DestFilePath;
        }

        /// <summary>
        /// File downloaded event
        /// </summary>
        public event EventHandler<FileDownloadedEventArgs> FileDownloadedEvent;

        /// <summary>
        /// Keys are MyEMSL File IDs, values are instances of FileDownloadInfo
        /// </summary>
        public Dictionary<long, FileDownloadInfo> FilesToDownload
        {
            get;
        }

        /// <summary>
        /// Keys are the full paths to the downloaded file, values are extended file info
        /// </summary>
        public Dictionary<string, ArchivedFileInfo> DownloadedFiles
        {
            get;
            private set;
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
        /// When true, raise a DebugEvent prior to contacting the metadata server
        /// </summary>
        public bool ThrowErrors { get; set; }

        /// <summary>
        /// When False use https://my.emsl.pnl.gov/myemsl/elasticsearch/simple_items
        /// When True use  https://test0.my.emsl.pnl.gov/myemsl/search/simple/index.shtml
        /// </summary>
        public bool UseTestInstance { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public DownloadQueue()
        {
            FilesToDownload = new Dictionary<long, FileDownloadInfo>();
            DownloadedFiles = new Dictionary<string, ArchivedFileInfo>();
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="fileInfo">Archive File Info</param>
        // ReSharper disable once UnusedMember.Global
        public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo)
        {
            AddFileToDownloadQueue(fileInfo.FileID, fileInfo, unzipRequired: false, destFilePath: string.Empty);
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="fileInfo">Archive File Info</param>
        /// <param name="unzipRequired">True if the file will need to be unzipped after the download (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)</param>
        public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo, bool unzipRequired)
        {
            var destFilePath = string.Empty;
            AddFileToDownloadQueue(fileInfo.FileID, fileInfo, unzipRequired, destFilePath);
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <remarks>fileInfo can be null if unzipRequired is false</remarks>
        /// <param name="myEMSLFileID">MyEMSL File ID</param>
        /// <param name="fileInfo">Archive File Info</param>
        /// <param name="unzipRequired">True if the file will need to be unzipped after the download (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)</param>
        public void AddFileToDownloadQueue(long myEMSLFileID, ArchivedFileInfo fileInfo, bool unzipRequired)
        {
            var destFilePath = string.Empty;
            AddFileToDownloadQueue(myEMSLFileID, fileInfo, unzipRequired, destFilePath);
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <remarks>fileInfo can be null if unzipRequired is false</remarks>
        /// <param name="myEMSLFileID">MyEMSL File ID</param>
        /// <param name="fileInfo">Archive File Info</param>
        /// <param name="unzipRequired">True if the file will need to be unzipped after the download (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)</param>
        /// <param name="destFilePath">Explicit destination file path</param>
        public void AddFileToDownloadQueue(long myEMSLFileID, ArchivedFileInfo fileInfo, bool unzipRequired, string destFilePath)
        {
            if (FilesToDownload.ContainsKey(myEMSLFileID))
                return;

            if (string.IsNullOrWhiteSpace(destFilePath))
                destFilePath = string.Empty;

            var newFile = new FileDownloadInfo
            {
                UnzipRequired = unzipRequired,
                FileInfo = fileInfo,
                DestFilePath = destFilePath
            };

            if (newFile.UnzipRequired && fileInfo == null)
            {
                var message = "Cannot queue file " + myEMSLFileID + " for download because the UnzipRequired flag was set, but the ArchivedFileInfo parameter is null";
                Console.WriteLine(message);
                throw new InvalidDataException(message);
            }

            FilesToDownload.Add(myEMSLFileID, newFile);
        }

        /// <summary>
        /// Clear the download queue
        /// </summary>
        public void Clear()
        {
            FilesToDownload.Clear();
        }

        /// <summary>
        /// Retrieve queued files from MyEMSL
        /// </summary>
        /// <remarks>Returns False if the download queue is empty</remarks>
        /// <param name="downloadDirectoryPath">Target directory path (ignored for files defined in destFilePathOverride)</param>
        /// <param name="directoryLayout">Directory Layout (ignored for files defined in destFilePathOverride)</param>
        /// <returns>True if success, false if an error</returns>
        public bool ProcessDownloadQueue(
            string downloadDirectoryPath,
            Downloader.DownloadLayout directoryLayout)
        {
            if (FilesToDownload.Count == 0)
            {
                OnErrorEvent("Download queue is empty; nothing to download (ProcessDownloadQueue)");
                return false;
            }

            try
            {
                var downloader = new Downloader();
                RegisterEvents(downloader);

                downloader.IncludeAllRevisions = IncludeAllRevisions;
                downloader.ThrowErrors = ThrowErrors;
                downloader.UseTestInstance = UseTestInstance;

                // Keys are MyEMSL File IDs, values are the full target path for the file to download
                var destFilePathOverride = new Dictionary<long, string>();

                // Keys are MyEMSL File IDs, values are ArchivedFileInfo objects
                var filesToDownload = new Dictionary<long, ArchivedFileInfo>();

                foreach (var fileToDownload in FilesToDownload)
                {
                    var archiveFileInfo = fileToDownload.Value.FileInfo;
                    if (string.IsNullOrWhiteSpace(archiveFileInfo.Sha1Hash))
                    {
                        OnWarningEvent("File does not have a SHA-1 hash; cannot download {0}, FileID {1}", archiveFileInfo.RelativePathWindows, fileToDownload.Key);
                        continue;
                    }

                    filesToDownload.Add(fileToDownload.Key, archiveFileInfo);

                    if (!string.IsNullOrEmpty(fileToDownload.Value.DestFilePath))
                    {
                        destFilePathOverride.Add(fileToDownload.Key, fileToDownload.Value.DestFilePath);
                    }
                }

                var success = downloader.DownloadFiles(filesToDownload, destFilePathOverride, downloadDirectoryPath, directoryLayout);

                if (success)
                {
                    DownloadedFiles = downloader.DownloadedFiles;

                    foreach (var file in FilesToDownload)
                    {
                        FileDownloadedEvent?.Invoke(this, new FileDownloadedEventArgs(downloadDirectoryPath, file.Value.FileInfo, file.Value.UnzipRequired));
                    }
                    FilesToDownload.Clear();
                }

                return success;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in MyEMSLReader.DownloadQueue.ProcessDownloadQueue: " + ex.Message, ex);
                return false;
            }
        }
    }
}
