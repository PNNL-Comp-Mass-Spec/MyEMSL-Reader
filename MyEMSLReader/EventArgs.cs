using System;

namespace MyEMSLReader
{
    /// <summary>
    /// File downloaded event arguments
    /// </summary>
    public class FileDownloadedEventArgs : EventArgs
    {
        /// <summary>
        /// Download directory path
        /// </summary>
        public readonly string DownloadDirectoryPath;

        /// <summary>
        /// Archived file info
        /// </summary>
        public readonly ArchivedFileInfo ArchivedFile;

        /// <summary>
        /// True if the file needs to unzipped after downloading
        /// </summary>
        public readonly bool UnzipRequired;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="downloadDirectoryPath"></param>
        /// <param name="archivedFile"></param>
        /// <param name="unzipRequired"></param>
        public FileDownloadedEventArgs(string downloadDirectoryPath, ArchivedFileInfo archivedFile, bool unzipRequired)
        {
            DownloadDirectoryPath = downloadDirectoryPath;
            ArchivedFile = archivedFile;
            UnzipRequired = unzipRequired;
        }
    }
}
