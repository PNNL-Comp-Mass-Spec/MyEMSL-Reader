using System;

namespace MyEMSLReader
{
    public class DatasetDirectoryOrFileInfo
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
        public DatasetDirectoryOrFileInfo(long fileID, bool isDirectory, ArchivedFileInfo fileInfo)
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

    [Obsolete("Use DatasetDirectoryOrFileInfo")]
    public class DatasetFolderOrFileInfo : DatasetDirectoryOrFileInfo
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileID"></param>
        /// <param name="isFolder"></param>
        /// <param name="fileInfo"></param>
        public DatasetFolderOrFileInfo(long fileID, bool isFolder, ArchivedFileInfo fileInfo) : base(fileID, isFolder, fileInfo)
        {
        }

        public bool IsFolder => IsDirectory;
    }
}
