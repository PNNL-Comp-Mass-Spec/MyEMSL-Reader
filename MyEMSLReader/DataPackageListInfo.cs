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
        #region "Properties"

        /// <summary>
        /// Dataset IDs
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public List<int> DataPackageIDs => DataPackagesAndSubDirs.Keys.ToList();

        /// <summary>
        /// Keys are data package IDs, values are the optional Subdirectory name to filter on for the given data package
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public Dictionary<int, string> DataPackagesAndSubDirs { get; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public DataPackageListInfo()
        {
            DataPackagesAndSubDirs = new Dictionary<int, string>();
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
            {
                subDir = string.Empty;
            }

            if (DataPackagesAndSubDirs.Keys.Contains(dataPackageID))
            {
                DataPackagesAndSubDirs[dataPackageID] = subDir;
            }
            else
            {
                DataPackagesAndSubDirs.Add(dataPackageID, subDir);
                mCacheIsStale = true;
            }
        }

        // ReSharper disable once UnusedMember.Global
        public void Clear()
        {
            DataPackagesAndSubDirs.Clear();
            mCacheIsStale = true;
        }

        // ReSharper disable once UnusedMember.Global
        public bool ContainsDataPackage(int dataPackageID)
        {
            return DataPackagesAndSubDirs.ContainsKey(dataPackageID);
        }

        /// <summary>
        /// Looks for the given file, returning any matches as a list (searches this directory and subdirectories)
        /// </summary>
        /// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subdirectoryName">Subdirectory in which the file must reside; can contain a wildcard, e.g. SIC*</param>
        /// <param name="dataPackageID">Data Package ID filter</param>
        /// <returns>List of matching files</returns>
        /// <remarks>subdirectoryName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        // ReSharper disable once UnusedMember.Global
        public List<DatasetDirectoryOrFileInfo> FindFiles(string fileName, string subdirectoryName, int dataPackageID)
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
        /// <remarks>subdirectoryName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
        public List<DatasetDirectoryOrFileInfo> FindFiles(string fileName, string subdirectoryName, int dataPackageID, bool recurse, bool fileSplit = false)
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

                mArchivedFiles = mReader.FindFilesByDataPackageID(DataPackagesAndSubDirs, recurse: true);
                mCacheDate = DateTime.UtcNow;
                mCacheIsStale = false;

                if (mArchivedFiles.Count == 0)
                {
                    if (mErrorMessages.Count == 0)
                    {
                        return true;
                    }

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