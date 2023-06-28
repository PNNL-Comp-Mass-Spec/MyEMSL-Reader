using System;
using System.Collections.Generic;
using System.Linq;

// ReSharper disable UnusedMember.Global

namespace MyEMSLReader
{
    /// <summary>
    /// Tracks the dataset info for multiple datasets (by dataset ID)
    /// </summary>
    public class DatasetListInfoByID : DatasetInfoBase
    {
        // Ignore Spelling: Dirs

        /// <summary>
        /// Dataset IDs
        /// </summary>
        public List<int> DatasetIDs => DatasetsAndSubDirs.Keys.ToList();

        /// <summary>
        /// Keys are dataset IDs, values are the optional Subdirectory name to filter on for the given dataset
        /// </summary>
        public Dictionary<int, string> DatasetsAndSubDirs { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public DatasetListInfoByID()
        {
            DatasetsAndSubDirs = new Dictionary<int, string>();
        }

        /// <summary>
        /// Add a dataset ID to search for in MyEMSL
        /// </summary>
        /// <param name="datasetID">Dataset ID</param>
        public void AddDataset(int datasetID)
        {
            AddDataset(datasetID, string.Empty);
        }

        /// <summary>
        /// Add a dataset ID to search for in MyEMSL
        /// </summary>
        /// <param name="datasetID">Dataset ID</param>
        /// <param name="subDir">Subdirectory name to filter on</param>
        public void AddDataset(int datasetID, string subDir)
        {
            if (string.IsNullOrWhiteSpace(subDir))
            {
                subDir = string.Empty;
            }

            if (DatasetsAndSubDirs.Keys.Contains(datasetID))
            {
                DatasetsAndSubDirs[datasetID] = subDir;
            }
            else
            {
                DatasetsAndSubDirs.Add(datasetID, subDir);
                mCacheIsStale = true;
            }
        }

        /// <summary>
        /// Clear DatasetsAndSubDirs and set mCacheIsStale to true
        /// </summary>
        public void Clear()
        {
            DatasetsAndSubDirs.Clear();
            mCacheIsStale = true;
        }

        /// <summary>
        /// Return true if DatasetsAndSubDirs has the given dataset
        /// </summary>
        /// <param name="datasetID"></param>
        public bool ContainsDataset(int datasetID)
        {
            return DatasetsAndSubDirs.ContainsKey(datasetID);
        }

        /// <summary>
        /// Contact the metadata server to find the files associated with datasets added via AddDataset
        /// </summary>
        /// <returns>True if successful, false if an error</returns>
        public override bool RefreshInfo()
        {
            try
            {
                mErrorMessages.Clear();

                mArchivedFiles = mReader.FindFilesByDatasetID(DatasetsAndSubDirs, recurse: true);
                mCacheDate = DateTime.UtcNow;
                mCacheIsStale = false;

                if (mArchivedFiles.Count == 0)
                {
                    return mErrorMessages.Count == 0;
                }

                return true;
            }
            catch (Exception ex)
            {
                var msg = "Error in MyEMSLReader.DatasetListInfoByID.RefreshInfo: " + ex.Message;
                OnErrorEvent(msg, ex);
                mErrorMessages.Add(msg);
                return false;
            }
        }
    }
}
