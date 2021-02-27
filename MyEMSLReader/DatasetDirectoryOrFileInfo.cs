using System;

namespace MyEMSLReader
{
    /// <summary>
    /// Dataset directory or file info
    /// </summary>
    public class DatasetDirectoryOrFileInfo
    {
        /// <summary>
        /// The UTC timestamp that the info was cached in memory
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

        /// <summary>
        /// Return the MyEMSL file ID
        /// </summary>
        public override string ToString()
        {
            return "FileID " + FileID + "; " + FileInfo;
        }
    }
}
