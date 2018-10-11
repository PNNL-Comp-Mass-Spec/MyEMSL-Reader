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
        /// <remarks>Will be 0 if this is a folder</remarks>
        public long FileID
        {
            get;
        }

        /// <summary>
        /// True if this entity is a folder in MyEMSL
        /// </summary>
        public bool IsFolder
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
        /// <param name="isFolder"></param>
        /// <param name="fileInfo"></param>
        public DatasetFolderOrFileInfo(long fileID, bool isFolder, ArchivedFileInfo fileInfo)
        {
            CacheDateUTC = DateTime.UtcNow;
            FileID = fileID;
            IsFolder = isFolder;
            FileInfo = fileInfo;
        }

        public override string ToString()
        {
            return "FileID " + FileID + "; " + FileInfo;
        }
    }
}
