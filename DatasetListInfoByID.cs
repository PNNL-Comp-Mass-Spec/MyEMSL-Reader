using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyEMSLReader
{
	/// <summary>
	/// Tracks the dataset info for multiple datasets (by dataset ID)
	/// </summary>
	public class DatasetListInfoByID : DatasetInfoBase
	{
		#region "Module variables"

		/// <summary>
		/// Keys are dataset IDs, values are the optional Subdirectory name to filter on for the given dataset
		/// </summary>
		protected Dictionary<int, string> mDatasetsAndSubDirs;

		#endregion

		#region "Properties"

		/// <summary>
		/// Dataset IDs
		/// </summary>
		public List<int> DatasetIDs
		{
			get
			{
				return mDatasetsAndSubDirs.Keys.ToList();
			}
		}

		/// <summary>
		/// Keys are dataset IDs, values are the optional Subdirectory name to filter on for the given dataset
		/// </summary>
		public Dictionary<int, string> DatasetsAndSubDirs
		{
			get
			{
				return mDatasetsAndSubDirs;
			}
		}

		#endregion

		/// <summary>
		/// Constructor
		/// </summary>
		public DatasetListInfoByID()
			: base()
		{
			mDatasetsAndSubDirs = new Dictionary<int, string>();
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
				subDir = string.Empty;

			if (mDatasetsAndSubDirs.Keys.Contains(datasetID))
				mDatasetsAndSubDirs[datasetID] = subDir;
			else
			{
				mDatasetsAndSubDirs.Add(datasetID, subDir);
				mCacheIsStale = true;
			}
		}

		public void Clear()
		{
			mDatasetsAndSubDirs.Clear();
			mCacheIsStale = true;
		}

		public bool ContainsDataset(int datasetID)
		{
			return mDatasetsAndSubDirs.ContainsKey(datasetID);
		}

		public override bool RefreshInfo()
		{

			try
			{
				mErrorMessages.Clear();

				var dctDatasetIDsAndSubDirs = new Dictionary<int, string>();
				foreach (var entry in mDatasetsAndSubDirs)
					dctDatasetIDsAndSubDirs.Add(entry.Key, entry.Value);

				bool recurse = true;

				mArchivedFiles = mReader.FindFilesByDatasetID(dctDatasetIDsAndSubDirs, recurse);
				mCacheDate = DateTime.UtcNow;
				mCacheIsStale = false;

				if (mArchivedFiles.Count == 0)
				{
					if (mErrorMessages.Count == 0)
						return true;
					else
						return false;
				}

				return true;

			}
			catch (Exception ex)
			{
				mErrorMessages.Add("Error in DatasetIDListInfo.RefreshInfo: " + ex.Message);
				return false;
			}
		}

	}
}
