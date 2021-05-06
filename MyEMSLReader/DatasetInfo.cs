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
        // Ignore Spelling: Args

        /// <summary>
        /// Dataset ID
        /// </summary>
        public int DatasetID { get; private set; }

        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName"></param>
        public DatasetInfo(string datasetName)
        {
            DatasetName = string.Empty;
            DatasetID = 0;

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
            {
                return;
            }

            var qArchivedFile = (from item in mDownloadedFiles where item.Value.FileID == myEMSLFileID select item.Value).ToList();

            if (qArchivedFile.Count == 0)
            {
                string message;
                if (unzipRequired)
                {
                    message = "Cannot queue file " + myEMSLFileID + " for download because the UnzipRequired flag was set, " +
                              "but the MyEMSL FileID is not cached locally for dataset " + DatasetName;
                    OnErrorEvent(message);
                    throw new FormatException(message);
                }

                message = "Queued file " + myEMSLFileID + " for download with null ArchivedFileInfo " +
                          "because the MyEMSL FileID is not cached locally for dataset " + DatasetName;
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
            return RefreshInfo(DatasetName);
        }

        /// <summary>
        /// Contact the metadata server to find the files associated with the given dataset
        /// </summary>
        /// <param name="datasetName">Dataset name to lookup</param>
        /// <returns>True if successful, false if an error</returns>
        public bool RefreshInfo(string datasetName)
        {
            try
            {
                mErrorMessages.Clear();

                if (datasetName != DatasetName)
                {
                    UpdateDatasetName(datasetName);
                }

                mArchivedFiles = mReader.FindFilesByDatasetName(DatasetName);
                mCacheDate = DateTime.UtcNow;
                mCacheIsStale = false;

                if (mArchivedFiles.Count == 0)
                {
                    return mErrorMessages.Count == 0;
                }

                DatasetID = mArchivedFiles.First().DatasetID;

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
            if (datasetName != DatasetName)
            {
                DatasetName = datasetName;
                DatasetID = 0;
                mArchivedFiles.Clear();

                mCacheIsStale = true;
            }
        }
    }
}
