using System;
using System.Linq;

namespace MyEMSLReader
{
    /// <summary>
    /// Tracks the dataset info for a single dataset (by dataset name)
    /// </summary>
    [Obsolete("Valid, but unused")]
    public class DatasetInfo : DatasetInfoBase
    {

        #region "Module variables"

        private string mDatasetName;
        private int mDatasetID;

        #endregion

        #region "Properties"

        public int DatasetID => mDatasetID;

        public string DatasetName => mDatasetName;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName"></param>
        /// <remarks></remarks>
        public DatasetInfo(string datasetName)
        {

            mDatasetName = string.Empty;
            mDatasetID = 0;

            UpdateDatasetName(datasetName);
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="myEMSLFileID">MyEMSL file ID</param>
        public void AddFileToDownloadQueue(long myEMSLFileID)
        {
            AddFileToDownloadQueue(myEMSLFileID, false);
        }

        /// <summary>
        /// Queue a file to be downloaded
        /// </summary>
        /// <param name="myEMSLFileID">MyEMSL file ID</param>
        /// <param name="unzipRequired">
        /// True if the file will need to be unzipped after the download
        /// (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)
        /// </param>
        public void AddFileToDownloadQueue(long myEMSLFileID, bool unzipRequired)
        {
            if (mDownloadQueue.FilesToDownload.ContainsKey(myEMSLFileID))
                return;

            var qArchivedFile = (from item in mDownloadedFiles where item.Value.FileID == myEMSLFileID select item.Value).ToList();

            if (qArchivedFile.Count == 0)
            {
                string message;
                if (unzipRequired)
                {
                    message = "Cannot queue file " + myEMSLFileID + " for download because the UnzipRequired flag was set, " +
                              "but the MyEMSL FileID is not cached locally for dataset " + mDatasetName;
                    OnErrorEvent(message);
                    throw new FormatException(message);
                }

                message = "Queued file " + myEMSLFileID + " for download with null ArchivedFileInfo " +
                          "because the MyEMSL FileID is not cached locally for dataset " + mDatasetName;
                OnErrorEvent(message);

                AddFileToDownloadQueue(myEMSLFileID, null, unzipRequired: false);
            }
            else
            {
                AddFileToDownloadQueue(qArchivedFile.First(), unzipRequired);
            }

        }

        /// <summary>
        /// Contact the metadata server to find the files associated with this instance's dataset
        /// </summary>
        /// <returns>True if successful, false if an error</returns>
        public override bool RefreshInfo()
        {
            return RefreshInfo(mDatasetName);
        }

        /// <summary>
        /// Contact the metadata server to find the files associated with dataset datasetName
        /// </summary>
        /// <param name="datasetName">Dataset name to lookup</param>
        /// <returns>True if successful, false if an error</returns>
        public bool RefreshInfo(string datasetName)
        {

            try
            {
                mErrorMessages.Clear();

                if (datasetName != mDatasetName)
                {
                    UpdateDatasetName(datasetName);
                }

                mArchivedFiles = mReader.FindFilesByDatasetName(mDatasetName);
                mCacheDate = DateTime.UtcNow;
                mCacheIsStale = false;

                if (mArchivedFiles.Count == 0)
                {
                    if (mErrorMessages.Count == 0)
                        return true;

                    return false;
                }

                mDatasetID = mArchivedFiles.First().DatasetID;

                return true;

            }
            catch (Exception ex)
            {
                var msg = "Error in MyEMSLReader.DatasetInfo.RefreshInfo: " + ex.Message;
                OnErrorEvent(msg, ex);
                mErrorMessages.Add(msg);
                return false;
            }

        }

        /// <summary>
        /// Update the dataset name associated with this instance
        /// </summary>
        /// <param name="datasetName"></param>
        /// <remarks>Use RefreshInfo to find the files tracked by MyEMSL for this dataset</remarks>
        public void UpdateDatasetName(string datasetName)
        {
            if (datasetName != mDatasetName)
            {
                mDatasetName = datasetName;
                mDatasetID = 0;
                mArchivedFiles.Clear();

                mCacheIsStale = true;
            }
        }

    }
}
