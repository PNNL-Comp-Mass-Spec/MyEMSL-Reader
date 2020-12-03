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
        #region "Properties"

        /// <summary>
        /// Dataset names
        /// </summary>
        public List<string> Datasets => DatasetsAndSubDirs.Keys.ToList();

        /// <summary>
        /// Keys are dataset names, values are a list of subdirectory names to filter on for the given dataset (empty string means do not filter)
        /// </summary>
        public Dictionary<string, SortedSet<string>> DatasetsAndSubDirs { get; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public DatasetListInfo()
        {
            DatasetsAndSubDirs = new Dictionary<string, SortedSet<string>>(StringComparer.OrdinalIgnoreCase);
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
            {
                subDir = string.Empty;
            }

            if (DatasetsAndSubDirs.TryGetValue(datasetName, out var subDirsForDataset))
            {
                // ReSharper disable once PossibleUnintendedLinearSearchInSet
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

                DatasetsAndSubDirs.Add(datasetName, subDirsForDataset);

                mCacheIsStale = true;
            }
        }

        public void Clear()
        {
            DatasetsAndSubDirs.Clear();
            mCacheIsStale = true;
        }

        public bool ContainsDataset(string datasetName)
        {
            return DatasetsAndSubDirs.ContainsKey(datasetName);
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

                mArchivedFiles = mReader.FindFilesByDatasetName(DatasetsAndSubDirs);
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
                var msg = "Error in MyEMSLReader.DatasetListInfo.RefreshInfo: " + ex.Message;
                OnErrorEvent(msg, ex);
                mErrorMessages.Add(msg);
                return false;
            }
        }
    }
}
