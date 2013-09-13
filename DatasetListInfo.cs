﻿using System;
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
		/// Keys are dataset names, values are the optional Subdirectory name to filter on for the given dataset
		/// </summary>
		public Dictionary<string, string> Datasets
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
				mDatasetsAndSubDirs.Add(datasetName, subDir);
		}

		public void Clear()
		{
			mDatasetsAndSubDirs.Clear();
		}

		public override bool RefreshInfo()
		{

			try
			{
				mErrorMessages.Clear();

				var dctDatasetsAndSubDirs = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
				foreach (var entry in mDatasetsAndSubDirs)
					dctDatasetsAndSubDirs.Add(entry.Key, entry.Value);

				bool recurse = true;

				mArchivedFiles = mReader.FindFilesByDatasetName(dctDatasetsAndSubDirs, recurse);
				mCacheDate = System.DateTime.UtcNow;

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
				mErrorMessages.Add("Error in MyEMSLReader.DatasetNameListInfo.RefreshInfo: " + ex.Message);
				return false;
			}
		}
	
	}
}