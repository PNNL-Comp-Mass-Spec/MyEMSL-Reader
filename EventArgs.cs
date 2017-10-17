using System;

namespace MyEMSLReader
{

    public delegate void FileDownloadedEventHandler(object sender, FileDownloadedEventArgs e);

    public class FileDownloadedEventArgs : EventArgs
    {
        // ReSharper disable once NotAccessedField.Global
        public readonly string DownloadFolderPath;

        // ReSharper disable once NotAccessedField.Global
        public readonly ArchivedFileInfo ArchivedFile;

        // ReSharper disable once NotAccessedField.Global
        public readonly bool UnzipRequired;

        public FileDownloadedEventArgs(string downloadFolderPath, ArchivedFileInfo archivedFile, bool unzipRequired)
        {
            DownloadFolderPath = downloadFolderPath;
            ArchivedFile = archivedFile;
            UnzipRequired = unzipRequired;
        }
    }

}
