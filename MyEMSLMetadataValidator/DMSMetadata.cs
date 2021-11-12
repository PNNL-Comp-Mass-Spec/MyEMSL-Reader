
using System;

namespace MyEMSLMetadataValidator
{
    internal class DMSMetadata
    {
        public int EntryID { get; set; }

        public int Job { get; set; }

        public int DatasetID { get; }

        public string Subdirectory { get; set; }

        public int FileCountNew { get; set; }

        public int FileCountUpdated { get; set; }

        public int FilesAddedOrUpdated => FileCountNew + FileCountUpdated;

        public long Bytes { get; set; }

        public int StatusNum { get; set; }

        public int TransactionID { get; set; }

        public DateTime Entered { get; set; }

        /// <summary>
        ///  Constructor
        /// </summary>
        public DMSMetadata(int datasetId)
        {
            DatasetID = datasetId;
        }

        public override string ToString()
        {
            if (string.IsNullOrWhiteSpace(Subdirectory))
                return string.Format("Entry_ID {0}, DatasetId {1}, no subdirectory", EntryID, DatasetID);
            else
                return string.Format("Entry_ID {0}, DatasetId {1}, Subdirectory {2}", EntryID, DatasetID, Subdirectory);
        }
    }
}
