using System;
using System.IO;
using System.Collections.Generic;
using PRISM;

namespace MyEMSLReader
{
    public class DownloadQueue : EventNotifier
    {
        // Ignore Spelling: struct, Args, dest

        #region "Structures"

        public struct FileDownloadInfo
        {
            public ArchivedFileInfo FileInfo;
            public bool UnzipRequired;
            public string DestFilePath;
        }

        #endregion

        #region "Module variables"

        #endregion

        #region "Events"

        public event EventHandler<FileDownloadedEventArgs> FileDownloadedEvent;

        #endregion

        #region "Properties"

        /// <summary>
        /// Keys are MyEMSL File IDs, values are struct FileDownloadInfo
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

        public bool ThrowErrors { get; set; }

        /// <summary>
        /// When False use https://my.emsl.pnl.gov/myemsl/elasticsearch/simple_items
        /// When True use  https://test0.my.emsl.pnl.gov/myemsl/search/simple/index.shtml
        /// </summary>
        public bool UseTestInstance { get; set; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks></remarks>
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
        /// <param name="myEMSLFileID">MyEMSL File ID</param>
        /// <param name="fileInfo">Archive File Info</param>
        /// <param name="unzipRequired">True if the file will need to be unzipped after the download (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)</param>
        /// <remarks>fileInfo can be null if unzipRequired is false</remarks>
        public void AddFileToDownloadQueue(long myEMSLFileID, ArchivedFileInfo fileInfo, bool unzipRequired)
        {
            var destFilePath = string.Empty;
            AddFileToDownloadQueue(myEMSLFileID, fileInfo, unzipRequired, destFilePath);
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="myEMSLFileID">MyEMSL File ID</param>
        /// <param name="fileInfo">Archive File Info</param>
        /// <param name="unzipRequired">True if the file will need to be unzipped after the download (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)</param>
        /// <param name="destFilePath">Explicit destination file path</param>
        /// <remarks>fileInfo can be null if unzipRequired is false</remarks>
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
        /// <param name="downloadDirectoryPath">Target directory path (ignored for files defined in destFilePathOverride)</param>
        /// <param name="directoryLayout">Directory Layout (ignored for files defined in destFilePathOverride)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Returns False if the download queue is empty</remarks>
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
                        OnWarningEvent(string.Format(
                                           "File does not have a SHA-1 hash; cannot download {0}, FileID {1}",
                                           archiveFileInfo.RelativePathWindows, fileToDownload.Key));
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
