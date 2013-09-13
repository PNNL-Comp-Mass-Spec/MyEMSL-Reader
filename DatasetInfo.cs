﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyEMSLReader
{
	/// <summary>
	/// Tracks the dataset info for a single dataset (by dataset name)
	/// </summary>
	public class DatasetInfo : DatasetInfoBase
	{

		#region "Module variables"

		protected string mDatasetName;
		protected int mDatasetID;

		#endregion

		#region "Properties"

		public int DatasetID
		{
			get
			{
				return mDatasetID;
			}
		}

		public string DatasetName
		{
			get
			{
				return mDatasetName;
			}
		}

		#endregion

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="datasetName"></param>
		/// <remarks></remarks>
		public DatasetInfo(string datasetName)
			: base()
		{

			mDatasetName = string.Empty;
			mDatasetID = 0;

			UpdateDatasetName(datasetName);
		}

		public void AddFileToDownloadQueue(Int64 myEMSLFileID)
		{
			AddFileToDownloadQueue(myEMSLFileID, false);
		}

		public void AddFileToDownloadQueue(Int64 myEMSLFileID, bool unzipRequired)
		{
			if (mDownloadQueue.FilesToDownload.ContainsKey(myEMSLFileID))
				return;

			var qArchivedFile = (from item in mDownloadedFiles where item.Value.FileID == myEMSLFileID select item.Value).ToList();

			if (qArchivedFile.Count == 0)
			{
				string message;
				if (unzipRequired)
				{
					message = "Cannot queue file " + myEMSLFileID + " for download because the UnzipRequired flag was set, but the MyEMSL FileID is not cached locally for dataset " + mDatasetName;
					Console.WriteLine(message);
					throw new FormatException(message);
				}

				message = "Queued file " + myEMSLFileID + " for download with null ArchivedFileInfo because the MyEMSL FileID is not cached locally for dataset " + mDatasetName;
				Console.WriteLine(message);

				OnErrorEvent(this, new MessageEventArgs(message));

				AddFileToDownloadQueue(myEMSLFileID, null, unzipRequired);
			}
			else
			{
				AddFileToDownloadQueue(myEMSLFileID, qArchivedFile.First(), unzipRequired);
			}

		}

		/// <summary>
		/// Refresh the cached file info
		/// </summary>
		/// <returns>True if success, false if an error</returns>
		/// <remarks></remarks>
		public override bool RefreshInfo()
		{
			return RefreshInfo(mDatasetName);
		}

		/// <summary>
		/// Refresh the cached file info
		/// </summary>
		/// <param name="strDatasetName">Dataset name to lookup</param>
		/// <returns>True if success, false if an error</returns>
		/// <remarks></remarks>
		public bool RefreshInfo(string strDatasetName)
		{

			try
			{
				mErrorMessages.Clear();

				if (strDatasetName != mDatasetName)
				{
					UpdateDatasetName(strDatasetName);
				}

				mArchivedFiles = mReader.FindFilesByDatasetName(mDatasetName);
				mCacheDate = System.DateTime.UtcNow;

				if (mArchivedFiles.Count == 0)
				{
					if (mErrorMessages.Count == 0)
						return true;
					else
						return false;
				}

				mDatasetID = mArchivedFiles.First().DatasetID;

				return true;

			}
			catch (Exception ex)
			{
				mErrorMessages.Add("Error in MyEMSLReader.DatasetInfo.RefreshInfo: " + ex.Message);
				return false;
			}

		}

		public void UpdateDatasetName(string datasetName)
		{
			if (datasetName != mDatasetName)
			{
				mDatasetName = datasetName;
				mDatasetID = 0;
				mArchivedFiles.Clear();

				var oneDay = new TimeSpan(1, 0, 0, 0);
				mCacheDate = System.DateTime.UtcNow.Subtract(oneDay);

			}
		}

	}
}