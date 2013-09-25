using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyEMSLReader
{
	/// <summary>
	/// Tracks the dataset info for multiple datasets (by dataset name)
	/// </summary>
	public class DatasetListInfo : DatasetInfoBase
	{
		#region "Module variables"

		/// <summary>
		/// Keys are dataset names, values are the optional Subdirectory name to filter on for the given dataset
		/// </summary>
		protected Dictionary<string, string> mDatasetsAndSubDirs;
	
		#endregion

		#region "Properties"

		/// <summary>
		/// Dataset names
		/// </summary>
		public List<string> Datasets
		{
			get
			{
				return mDatasetsAndSubDirs.Keys.ToList();
			}
		}

		/// <summary>
		/// Keys are dataset names, values are the optional Subdirectory name to filter on for the given dataset
		/// </summary>
		public Dictionary<string, string> DatasetsAndSubDirs
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
		public DatasetListInfo()
			: base()
		{
			mDatasetsAndSubDirs = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);			
		}

		/// <summary>
		/// Add a dataset name to search for in MyEMSL
		/// </summary>
		/// <param name="datasetName">Dataset Name</param>
		public void AddDataset(string datasetName)
		{
			AddDataset(datasetName, string.Empty);
		}

		/// <summary>
		/// Add a dataset name to search for in MyEMSL
		/// </summary>
		/// <param name="datasetName">Dataset Name</param>
		/// <param name="subDir">Subdirectory name to filter on</param>
		public void AddDataset(string datasetName, string subDir)
		{
			if (string.IsNullOrWhiteSpace(subDir))
				subDir = string.Empty;

			if (mDatasetsAndSubDirs.Keys.Contains(datasetName))
				mDatasetsAndSubDirs[datasetName] = subDir;
			else
			{
				mDatasetsAndSubDirs.Add(datasetName, subDir);
				mCacheIsStale = true;
			}
		}

		public void Clear()
		{
			mDatasetsAndSubDirs.Clear();
			mCacheIsStale = true;
		}

		public bool ContainsDataset(string datasetName)
		{
			return mDatasetsAndSubDirs.ContainsKey(datasetName);
		}

		public override bool RefreshInfo()
		{

			try
			{
				mErrorMessages.Clear();

				mArchivedFiles = mReader.FindFilesByDatasetName(mDatasetsAndSubDirs);
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
				mErrorMessages.Add("Error in MyEMSLReader.DatasetListInfo.RefreshInfo: " + ex.Message);
				return false;
			}
		}
	
	}
}
