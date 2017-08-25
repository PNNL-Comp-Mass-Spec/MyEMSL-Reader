
using System;

namespace MyEMSLMetadataValidator
{
    class DMSMetadata
    {
        public int EntryID { get; set; }

        public int Job { get; set; }

        public int DatasetID { get; private set; }

        public string Subfolder { get; set; }

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
            if (string.IsNullOrWhiteSpace(Subfolder))
                return string.Format("Entry_ID {0}, DatasetId {1}, no subfolder", EntryID, DatasetID);
            else
                return string.Format("Entry_ID {0}, DatasetId {1}, Subfolder {2}", EntryID, DatasetID, Subfolder);
        }
    }
}
