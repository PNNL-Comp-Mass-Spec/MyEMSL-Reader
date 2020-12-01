using System;

namespace MyEMSLReader
{

    public delegate void FileDownloadedEventHandler(object sender, FileDownloadedEventArgs e);

    public class FileDownloadedEventArgs : EventArgs
    {
        public readonly string DownloadDirectoryPath;

        [Obsolete("Use DownloadDirectoryPath")]
        public string DownloadFolderPath => DownloadDirectoryPath;

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
