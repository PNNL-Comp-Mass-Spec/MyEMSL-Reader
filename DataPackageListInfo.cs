using System;
using System.Collections.Generic;
using System.Linq;

namespace MyEMSLReader
{
    /// <summary>
    /// Tracks the file info for multiple data packages (by dataset ID)
    /// </summary>
    public class DataPackageListInfo : DatasetInfoBase
    {
        #region "Module variables"

        /// <summary>
        /// Keys are data package IDs, values are the optional Subdirectory name to filter on for the given data package
        /// </summary>
        private readonly Dictionary<int, string> mDataPackagesAndSubDirs;

        #endregion

        #region "Properties"

        /// <summary>
        /// Dataset IDs
        /// </summary>
        public List<int> DataPackageIDs => mDataPackagesAndSubDirs.Keys.ToList();

        /// <summary>
        /// Keys are data package IDs, values are the optional Subdirectory name to filter on for the given data package
        /// </summary>
        public Dictionary<int, string> DataPackagesAndSubDirs => mDataPackagesAndSubDirs;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public DataPackageListInfo()
        {
            mDataPackagesAndSubDirs = new Dictionary<int, string>();
        }

        /// <summary>
        /// Add a data package ID to search for in MyEMSL
        /// </summary>
        /// <param name="dataPackageID">Data Package ID ID</param>
        public void AddDataPackage(int dataPackageID)
        {
            AddDataPackage(dataPackageID, string.Empty);
        }

        /// <summary>
        /// Add a dataset ID to search for in MyEMSL
        /// </summary>
        /// <param name="dataPackageID">Data Package ID</param>
        /// <param name="subDir">Subdirectory name to filter on</param>
        public void AddDataPackage(int dataPackageID, string subDir)
        {
            if (string.IsNullOrWhiteSpace(subDir))
                subDir = string.Empty;

            if (mDataPackagesAndSubDirs.Keys.Contains(dataPackageID))
                mDataPackagesAndSubDirs[dataPackageID] = subDir;
            else
            {
                mDataPackagesAndSubDirs.Add(dataPackageID, subDir);
                mCacheIsStale = true;
            }
        }

        public void Clear()
        {
            mDataPackagesAndSubDirs.Clear();
            mCacheIsStale = true;
        }

        public bool ContainsDataPackage(int dataPackageID)
        {
            return mDataPackagesAndSubDirs.ContainsKey(dataPackageID);
        }

        /// <summary>
        /// Looks for the given file, returning any matches as a list (searches this directory and subdirectories)
        /// </summary>
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subdirectoryName">Subdirectory in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="dataPackageID">Data Package ID filter</param>
        /// <returns>List of matching files</returns>
        public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subFolderName, int dataPackageID)
        /// <remarks>subdirectoryName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        // ReSharper disable once UnusedMember.Global
        {
            return FindFiles(fileName, subdirectoryName, dataPackageID, recurse: true);
        }

        /// <summary>
        /// Looks for the given file, returning any matches as a list
        /// </summary>
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subdirectoryName">Subdirectory in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="dataPackageID">Data Package ID filter</param>
        /// <param name="recurse">True to search all subdirectories; false to only search the root directory (or only subdirectoryName)</param>
        /// <param name="fileSplit">Set to True if fileName contains a list of file names (or file specs) separated by a semicolon</param>
        /// <returns>List of matching files</returns>
        public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subFolderName, int dataPackageID, bool recurse, bool fileSplit = false)
        /// <remarks>subdirectoryName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        {
            var datasetName = string.Empty;

            return FindFiles(fileName, subdirectoryName, datasetName, dataPackageID, recurse, fileSplit);
        }

        /// <summary>
        /// Contact the metadata server to find the files associated with the data packages added via AddDataPackage
        /// </summary>
        /// <returns>True if successful, false if an error</returns>
        public override bool RefreshInfo()
        {

            try
            {
                mErrorMessages.Clear();

                mArchivedFiles = mReader.FindFilesByDataPackageID(mDataPackagesAndSubDirs, recurse: true);
                mCacheDate = DateTime.UtcNow;
                mCacheIsStale = false;

                if (mArchivedFiles.Count == 0)
                {
                    if (mErrorMessages.Count == 0)
                        return true;

                    return false;
                }

                return true;

            }
            catch (Exception ex)
            {
                var msg = "Error in MyEMSLReader.DatasetPackageListInfo.RefreshInfo: " + ex.Message;
                OnErrorEvent(msg, ex);
                mErrorMessages.Add(msg);
                return false;
            }
        }

    }
}
