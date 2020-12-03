using System;

namespace MyEMSLReader
{
    public class FileDownloadedEventArgs : EventArgs
    {
        public readonly string DownloadDirectoryPath;

        public readonly ArchivedFileInfo ArchivedFile;

        public readonly bool UnzipRequired;

        public FileDownloadedEventArgs(string downloadDirectoryPath, ArchivedFileInfo archivedFile, bool unzipRequired)
        {
            DownloadDirectoryPath = downloadDirectoryPath;
            ArchivedFile = archivedFile;
            UnzipRequired = unzipRequired;
        }
    }
}
