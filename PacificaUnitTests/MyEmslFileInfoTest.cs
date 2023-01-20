using System;
using Pacifica.Json;

namespace PacificaUnitTests
{
    public class MyEmslFileInfoTest : MyEMSLFileInfo
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="fileId"></param>
        /// <param name="hashSum"></param>
        public MyEmslFileInfoTest(string fileName, long fileId, string hashSum) : base(fileName, fileId, hashSum)
        {
        }

        /// <summary>
        /// Update times the file was created, updated, and optionally deleted in MyEMSL
        /// </summary>
        /// <param name="createdInMyEMSL"></param>
        /// <param name="updatedInMyEMSL"></param>
        /// <param name="deletedInMyEMSL"></param>
        public new void UpdateRemoteFileTimes(string createdInMyEMSL, string updatedInMyEMSL, string deletedInMyEMSL = "")
        {
            if (!string.IsNullOrWhiteSpace(createdInMyEMSL))
            {
                if (DateTime.TryParse(createdInMyEMSL, out var creationTime))
                {
                    Created = creationTime;
                }
            }

            if (!string.IsNullOrWhiteSpace(updatedInMyEMSL))
            {
                if (DateTime.TryParse(updatedInMyEMSL, out var updateTime))
                {
                    Updated = updateTime;
                }
            }

            if (!string.IsNullOrWhiteSpace(deletedInMyEMSL))
            {
                if (DateTime.TryParse(deletedInMyEMSL, out var deletionTime))
                {
                    Deleted = deletionTime;
                }
            }
        }

        /// <summary>
        /// Update file creation and modification times using the string values reported by MyEMSL
        /// </summary>
        /// <param name="creationTimeText"></param>
        /// <param name="lastWriteTimeText"></param>
        public new void UpdateSourceFileTimes(string creationTimeText, string lastWriteTimeText)
        {
            if (!string.IsNullOrWhiteSpace(creationTimeText))
            {
                if (DateTime.TryParse(creationTimeText, out var creationTime))
                {
                    FileCreationTime = creationTime;
                }
            }

            if (!string.IsNullOrWhiteSpace(lastWriteTimeText))
            {
                if (DateTime.TryParse(lastWriteTimeText, out var lastWriteTime))
                {
                    FileLastWriteTime = lastWriteTime;
                }
            }
        }
    }
}
