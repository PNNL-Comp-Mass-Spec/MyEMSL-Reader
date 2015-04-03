using System;
using System.Collections.Generic;
using System.Linq;

namespace MyEMSLReader
{
    /// <summary>
    /// Tracks the dataset info for multiple datasets (by dataset name)
    /// </summary>
    public class DatasetListInfo : DatasetInfoBase
    {
        #region "Module variables"

        /// <summary>
        /// Keys are dataset names, values are the optional Subdirectory name (or names) to filter on for the given dataset
        /// </summary>
        protected Dictionary<string, SortedSet<string>> mDatasetsAndSubDirLists;

        #endregion

        #region "Properties"

        /// <summary>
        /// Dataset names
        /// </summary>
        public List<string> Datasets
        {
            get
            {
                return mDatasetsAndSubDirLists.Keys.ToList();
            }
        }

        /// <summary>
        /// Keys are dataset names, values are a list of subdirectory names to filter on for the given dataset (empty string means do not filter)
        /// </summary>
        public Dictionary<string, SortedSet<string>> DatasetsAndSubDirs
        {
            get
            {
                return mDatasetsAndSubDirLists;
            }
        }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public DatasetListInfo()
        {
            mDatasetsAndSubDirLists = new Dictionary<string, SortedSet<string>>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Add a dataset name to search for in MyEMSL
        /// </summary>
        /// <param name="datasetName">Dataset Name</param>
        /// <remarks>Call RefreshInfo to retrieve the MyEMSL-tracked files for the added datasets</remarks>
        public void AddDataset(string datasetName)
        {
            AddDataset(datasetName, string.Empty);
        }

        /// <summary>
        /// Add a dataset name to search for in MyEMSL
        /// </summary>
        /// <param name="datasetName">Dataset Name</param>
        /// <param name="subDir">Subdirectory name to filter on (exact match; cannot contain wildcards)</param>
        /// /// <remarks>Call RefreshInfo to retrieve the MyEMSL-tracked files for the added datasets</remarks>
        public void AddDataset(string datasetName, string subDir)
        {
            if (string.IsNullOrWhiteSpace(subDir))
                subDir = string.Empty;

            SortedSet<string> subDirsForDataset;

            if (mDatasetsAndSubDirLists.TryGetValue(datasetName, out subDirsForDataset))
            {
                if (!subDirsForDataset.Contains(subDir, StringComparer.OrdinalIgnoreCase))
                {
                    subDirsForDataset.Add(subDir);
                    mCacheIsStale = true;
                }
            }
            else
            {
                subDirsForDataset = new SortedSet<string> {
                    subDir
                };

                mDatasetsAndSubDirLists.Add(datasetName, subDirsForDataset);

                mCacheIsStale = true;
            }
        }

        public void Clear()
        {
            mDatasetsAndSubDirLists.Clear();
            mCacheIsStale = true;
        }

        public bool ContainsDataset(string datasetName)
        {
            return mDatasetsAndSubDirLists.ContainsKey(datasetName);
        }

        public override bool RefreshInfo()
        {

            try
            {
                mErrorMessages.Clear();

                mArchivedFiles = mReader.FindFilesByDatasetName(mDatasetsAndSubDirLists);
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
                mErrorMessages.Add("Error in MyEMSLReader.DatasetListInfo.RefreshInfo: " + ex.Message);
                return false;
            }
        }

    }
}
