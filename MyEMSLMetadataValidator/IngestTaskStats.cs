using System.Collections.Generic;
using System.Linq;

namespace MyEMSLMetadataValidator
{
    class IngestTaskStats
    {
        /// <summary>
        /// Ingest Tasks asssociated with these stats
        /// </summary>
        public List<DMSMetadata> IngestTasks { get; }

        public int TotalFiles { get; set; }

        public long TotalBytes { get; set; }

        public IngestTaskStats(DMSMetadata ingestTask, int filesAddedOrUpdated, long bytes)
        {
            IngestTasks = new List<DMSMetadata> {ingestTask};
            TotalFiles = filesAddedOrUpdated;
            TotalBytes = bytes;
        }

        public override string ToString()
        {
            var sizeStats = string.Format("{0} files, {1:F1} KB", TotalFiles, TotalBytes);

            var firstIngest = IngestTasks.First();
            if (firstIngest == null)
                return sizeStats;

            return string.Format("Entry_ID {0}, Dataset {1}, {2}", firstIngest.EntryID, firstIngest.DatasetID, sizeStats);
        }
    }
}
