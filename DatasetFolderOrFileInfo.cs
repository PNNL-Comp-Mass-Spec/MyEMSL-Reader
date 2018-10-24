using System;

namespace MyEMSLReader
{
    public class DatasetFolderOrFileInfo
    {
        /// <summary>
        /// The UTC datetime that the info was cached in memory
        /// </summary>
        public DateTime CacheDateUTC
        {
            get;
        }

        /// <summary>
        /// MyEMSL File ID
        /// </summary>
        /// <remarks>Will be 0 if this is a directory</remarks>
        public long FileID
        {
            get;
        }

        /// <summary>
        /// True if this entity is a directory in MyEMSL
        /// </summary>
        public bool IsDirectory
        {
            get;
        }

        /// <summary>
        /// File information
        /// </summary>
        public ArchivedFileInfo FileInfo
        {
            get;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileID"></param>
        /// <param name="isDirectory"></param>
        /// <param name="fileInfo"></param>
        public DatasetFolderOrFileInfo(long fileID, bool isFolder, ArchivedFileInfo fileInfo)
        {
            CacheDateUTC = DateTime.UtcNow;
            FileID = fileID;
            IsDirectory = isDirectory;
            FileInfo = fileInfo;
        }

        public override string ToString()
        {
            return "FileID " + FileID + "; " + FileInfo;
        }
    }
}
