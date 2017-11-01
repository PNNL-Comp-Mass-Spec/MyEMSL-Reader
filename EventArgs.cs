using System;

namespace MyEMSLReader
{

    public delegate void FileDownloadedEventHandler(object sender, FileDownloadedEventArgs e);

    public class FileDownloadedEventArgs : EventArgs
    {
        public readonly string DownloadFolderPath;

        public readonly ArchivedFileInfo ArchivedFile;

        public readonly bool UnzipRequired;

        public FileDownloadedEventArgs(string downloadFolderPath, ArchivedFileInfo archivedFile, bool unzipRequired)
        {
            DownloadFolderPath = downloadFolderPath;
            ArchivedFile = archivedFile;
            UnzipRequired = unzipRequired;
        }
    }

}
