using System;
using System.ComponentModel;
using Newtonsoft.Json;

namespace Pacifica.Json
{
    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public class MyEMSLTaskStatus
    {
        public bool Valid => JobId > 0 && !string.IsNullOrWhiteSpace(State);

        [JsonProperty("job_id")]
        [DefaultValue(-1)]
        public long JobId { get; private set; }

        [JsonProperty("state")]
        public string State { get; private set; }

        [JsonProperty("task")]
        public string CurrentTask { get; private set; }

        [JsonProperty("task_percent")]
        public string PercentCompleteText { get; private set; }

        public double PercentComplete => double.TryParse(PercentCompleteText, out var percent) ? percent : 0;

        [JsonProperty("complete")]
        [DefaultValue("")]
        public bool Complete { get; private set; }

        [JsonProperty("updated")]
        public DateTime? Updated { get; private set; }

        [JsonProperty("created")]
        public DateTime Created { get; private set; }

        [JsonProperty("exception")]
        [DefaultValue("")]
        public string Exception { get; private set; }

        public MyEMSLTaskStatus()
        {
            JobId = -1;
            State = string.Empty;
            CurrentTask = string.Empty;
            PercentCompleteText = string.Empty;
            Complete = false;
            Exception = string.Empty;
        }

        //[JsonConstructor]
        //public MyEmslTaskStatus(long job_id, string state, string task)
        //{
        //    JobId = job_id;
        //    State = state;
        //    CurrentTask = task;
        //}

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
