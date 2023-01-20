using System;
using System.ComponentModel;
using Newtonsoft.Json;

namespace Pacifica.Json
{
    /// <summary>
    /// Status report data from MyEMSL upload
    /// </summary>
    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public class MyEMSLTaskStatus
    {
        /// <summary>
        /// True if the data is valid (false for empty responses)
        /// </summary>
        public bool Valid => JobId > 0 && !string.IsNullOrWhiteSpace(State);

        /// <summary>
        /// MyEMSL Job ID
        /// </summary>
        [JsonProperty("job_id")]
        [DefaultValue(-1)]
        public long JobId { get; private set; }

        /// <summary>
        /// State: Usually 'OK' or 'FAILED'
        /// </summary>
        [JsonProperty("state")]
        public string State { get; private set; }

        /// <summary>
        /// Current upload task. Examples: 'UPLOADING', 'Policy Validation', 'ingest metadata', 'ingest files'
        /// </summary>
        [JsonProperty("task")]
        public string CurrentTask { get; private set; }

        /// <summary>
        /// Upload task percent complete
        /// </summary>
        [JsonProperty("task_percent")]
        public string PercentCompleteText { get; private set; }

        /// <summary>
        /// Upload task percent complete
        /// </summary>
        public double PercentComplete => double.TryParse(PercentCompleteText, out var percent) ? percent : 0;

        /// <summary>
        /// True when task is completed
        /// </summary>
        [JsonProperty("complete")]
        [DefaultValue("")]
        public bool Complete { get; private set; }

        /// <summary>
        /// Last report update time
        /// </summary>
        [JsonProperty("updated")]
        public DateTime? Updated { get; private set; }

        /// <summary>
        /// Task creation time
        /// </summary>
        [JsonProperty("created")]
        public DateTime Created { get; private set; }

        /// <summary>
        /// Task upload error message
        /// </summary>
        [JsonProperty("exception")]
        [DefaultValue("")]
        public string Exception { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public MyEMSLTaskStatus()
        {
            JobId = -1;
            State = string.Empty;
            CurrentTask = string.Empty;
            PercentCompleteText = string.Empty;
            Complete = false;
            Exception = string.Empty;
        }

        /// <summary>
        /// Constructor for testing purposes
        /// </summary>
        /// <param name="jobId"></param>
        /// <param name="state"></param>
        /// <param name="currentTask"></param>
        /// <param name="taskPercentString"></param>
        /// <param name="exception"></param>
        public MyEMSLTaskStatus(long jobId, string state, string currentTask, string taskPercentString, string exception)
        {
            JobId = jobId;
            State = state;
            CurrentTask = currentTask;
            PercentCompleteText = taskPercentString;
            Exception = exception;
        }
    }
}
