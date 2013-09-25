using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyEMSLReader
{
	/// <summary>
	/// Tracks the file info for multiple data packages (by dataset ID)
	/// </summary>
	public class DatasetPackageListInfo : DatasetInfoBase
	{
		#region "Module variables"

		/// <summary>
		/// Keys are data package IDs, values are the optional Subdirectory name to filter on for the given data package
		/// </summary>
		protected Dictionary<int, string> mDataPackagesAndSubDirs;

		#endregion

		#region "Properties"

		/// <summary>
		/// Dataset IDs
		/// </summary>
		public List<int> DataPackageIDs
		{
			get
			{
				return mDataPackagesAndSubDirs.Keys.ToList();
			}
		}

		/// <summary>
		/// Keys are data package IDs, values are the optional Subdirectory name to filter on for the given data package
		/// </summary>
		public Dictionary<int, string> DataPackagesAndSubDirs
		{
			get
			{
				return mDataPackagesAndSubDirs;
			}
		}

		#endregion

		/// <summary>
		/// Constructor
		/// </summary>
		public DatasetPackageListInfo()
			: base()
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
		/// <param name="dataPackageID">Data Package ID ID</param>
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
		/// Looks for the given file, returning any matches as a list
		/// </summary>
		/// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
		/// <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
		/// <param name="dataPackageID">Data Package ID filter</param>
		/// <returns>List of matching files</returns>
		public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subFolderName, int dataPackageID)
		{
			bool recurse = true;
			string datasetName = string.Empty;

			return FindFiles(fileName, subFolderName, datasetName, dataPackageID, recurse);
		}

		public override bool RefreshInfo()
		{

			try
			{
				mErrorMessages.Clear();

				bool recurse = true;

				mArchivedFiles = mReader.FindFilesByDataPackageID(mDataPackagesAndSubDirs, recurse);
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
				mErrorMessages.Add("Error in MyEMSLReader.DatasetPackageListInfo.RefreshInfo: " + ex.Message);
				return false;
			}
		}

	}
}
