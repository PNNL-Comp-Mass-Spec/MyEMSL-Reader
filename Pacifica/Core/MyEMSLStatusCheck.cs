﻿using PRISM;
using System;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using Pacifica.Json;

// ReSharper disable UnusedMember.Global

namespace Pacifica.Core
{
    /// <summary>
    /// Examine the status of a given ingest job
    /// </summary>
    /// <remarks>
    /// <para>
    /// First call GetIngestStatus then call IngestStepCompleted.
    /// This allows for just one web request, but the ability to examine the status of multiple steps
    /// </para>
    /// <para>
    /// This class is used by Capture Task Manager plugins Archive Verify and Archive Status Check
    /// </para>
    /// </remarks>
    public class MyEMSLStatusCheck : EventNotifier
    {
        // Ignore Spelling: ArgC, ctime, CysAlk, hashsum, hashtype, mam Frodo, mimetype, mtime, ok, pacifica, subdir

        private readonly Configuration mPacificaConfig;

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage
        {
            get;
            set;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public MyEMSLStatusCheck()
        {
            ErrorMessage = string.Empty;
            mPacificaConfig = new Configuration();

            EasyHttp.MyEMSLOffline += EasyHttp_MyEMSLOffline;

            EasyHttp.ErrorEvent += OnErrorEvent;
            EasyHttp.WarningEvent += OnWarningEvent;
        }

        /// <summary>
        /// Examine the task name and percent complete to determine the number of ingest steps that have been completed
        /// </summary>
        /// <param name="currentTask"></param>
        /// <param name="percentComplete">percent complete; typically meaningless (either 0 or 100)</param>
        /// <param name="ingestStepsCompletedOld"></param>
        // ReSharper disable once UnusedMember.Global
        public static byte DetermineIngestStepsCompleted(string currentTask, int percentComplete, byte ingestStepsCompletedOld)
        {
            if (percentComplete > 0)
            {
                // PercentComplete (between 0 and 100) is non-zero
                // Convert it to a number between 0 and 7 since historically there were 7 steps to the ingest process
                return IngestStepCompletionCount(percentComplete);
            }

            // Define the ingestStepsCompleted value based on the current task reported by the status page
            return currentTask.ToLower() switch
            {
                "open tar" => 2,
                "policy validation" => 3,
                "uploading" => 4,
                "ingest files" => 5,
                "ingest metadata" => 6,
                _ => ingestStepsCompletedOld
            };
        }

        /// <summary>
        /// Check whether a file exists in MyEMSL
        /// </summary>
        /// <remarks>Searches using Sha1HashHex, so could match a file in a different location than the specific path tracked by fileInfo</remarks>
        /// <param name="fileInfo">File info object</param>
        /// <returns>True if found, otherwise false</returns>
        // ReSharper disable once UnusedMember.Global
        public bool DoesFileExistInMyEMSL(FileInfoObject fileInfo)
        {
            var fileSHA1HashSum = fileInfo.Sha1HashHex;

            // Example URL
            // https://metadata.my.emsl.pnl.gov/files?hashsum=7b05677da8a6a5c8d033e56dd36ab5445ae44860
            var metadataURL = mPacificaConfig.MetadataServerUri + "/files?hashsum=" + fileSHA1HashSum;

            if (!ValidateCertFile("DoesFileExistInMyEMSL", out _))
            {
                return false;
            }

            var fileListJSON = EasyHttp.Send(mPacificaConfig, metadataURL, out var responseStatusCode);

            if (responseStatusCode.ToString() != "200")
            {
                return false;
            }

            // ReSharper disable CommentTypo

            // Example response for just one file (hashsum=0a7bcbcf4085abc41bdbd98724f3e5c567726c56)
            // [{"mimetype": "application/octet-stream", "updated": "2017-07-02T23:54:53", "name": "QC_Mam_16_01_125ng_HCD-3_30Jun17_Frodo_REP-17-06-01_msgfplus_syn_ProteinMods.txt", "created": "2017-07-02T23:54:53", "deleted": null, "size": 899907, "hashsum": "0a7bcbcf4085abc41bdbd98724f3e5c567726c56", "hashtype": "sha1", "subdir": "MSG201707021504_Auto1467864", "mtime": "2017-07-02T23:49:14", "_id": 15578789, "encoding": "UTF8", "transaction_id": 1302996, "ctime": "2017-07-02T23:53:28"}]

            // Example response for multiple files (hashsum=627ad3a8a1eaad358e0c89f8e5b7db1473f33278):
            // [{"mimetype": "None", "updated": "2017-06-30T03:08:36", "name": "MSGFDB_PartArgC_MetOx_StatCysAlk_20ppmParTol_ModDefs.txt", "created": "2017-06-30T03:08:36", "deleted": null, "size": 52, "hashsum": "627ad3a8a1eaad358e0c89f8e5b7db1473f33278", "hashtype": "sha1", "subdir": "MSG201405141726_Auto1058369", "mtime": "2014-05-14T18:00:53", "_id": 3694295, "encoding": "UTF-8", "transaction_id": 443104, "ctime": "2014-05-14T18:01:08"}, {"mimetype": "None", "updated": "2017-06-30T03:23:14", "name": "MSGFDB_PartAspN_MetOx_StatCysAlk_20ppmParTol_ModDefs.txt", "created": "2017-06-30T03:23:14", "deleted": null, "size": 52, "hashsum": "627ad3a8a1eaad358e0c89f8e5b7db1473f33278", "hashtype": "sha1", "subdir": "MSG201405141729_Auto1058370", "mtime": "2014-06-03T13:43:05", "_id": 3841932, "encoding": "UTF-8", "transaction_id": 457902, "ctime": "2014-06-03T13:43:09"}]

            // ReSharper restore CommentTypo

            var fileList = JsonTools.JsonToFileList(fileListJSON, metadataURL, "MyEMSLStatusCheck.DoesFileExistInMyEMSL", out var jsonError);

            if (fileList == null || !string.IsNullOrWhiteSpace(jsonError))
            {
                OnWarningEvent(jsonError);
                return false;
            }

            foreach (var fileObj in fileList)
            {
                if (string.Equals(fileObj.HashSum, fileSHA1HashSum))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Obtain the status returned by the given MyEMSL status page
        /// </summary>
        /// <param name="statusURI">
        /// URI to examine, e.g. https://ingestdms.my.emsl.pnl.gov/get_state?job_id=1300782
        /// </param>
        /// <param name="currentTask">Current task</param>
        /// <param name="percentComplete">Output: ingest process percent complete (value between 0 and 100)</param>
        /// <param name="lookupError">Output: true if an error occurs</param>
        /// <param name="errorMessage">Output: error message if lookupError is true</param>
        /// <returns>Status dictionary (empty dictionary if an error)</returns>
        public MyEMSLTaskStatus GetIngestStatus(
            string statusURI,
            out string currentTask,
            out int percentComplete,
            out bool lookupError,
            out string errorMessage)
        {
            lookupError = false;

            if (!ValidateCertFile("GetIngestStatus", out errorMessage))
            {
                currentTask = string.Empty;
                percentComplete = 0;
                lookupError = true;
                return new MyEMSLTaskStatus();
            }

            OnStatusEvent("Contacting " + statusURI);
            var startTime = DateTime.UtcNow;

            var statusResult = EasyHttp.SendViaThreadStart(mPacificaConfig, statusURI, out _);

            var elapsedSeconds = DateTime.UtcNow.Subtract(startTime).TotalSeconds;

            if (elapsedSeconds > 20)
            {
                OnStatusEvent("Result received after {0} seconds: {1}", (int)elapsedSeconds, statusResult);
            }
            else
            {
                OnDebugEvent("Result received: " + statusResult);
            }

            // Example contents of statusResult
            // (as returned by https://ingestdms.my.emsl.pnl.gov/get_state?job_id=2390349)

            // {"job_id": 104,     "state": "OK",     "task": "UPLOADING",         "task_percent": "0.00000"      }  (starting)
            // {"job_id": 104,     "state": "FAILED", "task": "Policy Validation", "task_percent": "0.00000"      }  (error)
            // {"job_id": 1300782, "state": "FAILED", "task": "ingest metadata",   "task_percent": "0.00000"      }  (error)
            // {"job_id": 1301499, "state": "FAILED", "task": "ingest files",      "task_percent": "0.00000"      }  (error)
            // {"job_id": 1300004, "state": "OK",     "task": "ingest metadata",   "task_percent": "100.00000"    }  (complete)
            // {"job_id": 1303430, "state": "OK",     "task": "ingest files",      "task_percent": "0.00000",  "updated": "2017-07-06 22:00:49",  "created": "2017-07-06 22:00:51", "exception": ""}
            // {"job_id": 2390349, "state": "OK",     "task": "ingest metadata",   "task_percent": "100.00000", "complete": true, "updated": "2021-12-02 03:00:18.072576", "created": "2021-12-02 03:00:04.512060", "exception": ""}

            try
            {
                if (string.Equals(statusResult, EasyHttp.REQUEST_TIMEOUT_RESPONSE))
                {
                    OnWarningEvent("Ingest status lookup timed out");
                    currentTask = string.Empty;
                    percentComplete = 0;
                    return new MyEMSLTaskStatus();
                }

                if (EasyHttp.IsResponseError(statusResult))
                {
                    OnWarningEvent("Ingest status error: " + statusResult);
                    currentTask = string.Empty;
                    percentComplete = 0;
                    return new MyEMSLTaskStatus();
                }

                var status = JsonTools.JsonToTaskStatus(statusResult);

                var state = status.State.ToLower();
                currentTask = status.CurrentTask;
                var exception = status.Exception;
                percentComplete = (int)status.PercentComplete;

                switch (state)
                {
                    case "ok":
                        if (string.IsNullOrWhiteSpace(exception))
                        {
                            OnDebugEvent("Archive state is OK for " + statusURI);
                        }
                        else
                        {
                            errorMessage = "Upload state is OK, but an exception was reported for task \"" + currentTask + "\"" +
                                           "; exception \"" + exception + "\"";

                            OnErrorEvent(errorMessage + "; see " + statusURI);
                        }
                        break;

                    case "failed":
                        errorMessage = "Upload failed, task \"" + currentTask + "\"";

                        if (string.IsNullOrWhiteSpace(exception))
                        {
                            OnErrorEvent("{0}; see {1}", errorMessage, statusURI);
                        }
                        else
                        {
                            OnErrorEvent("{0}; exception \"{1}\"; see {2}", errorMessage, exception, statusURI);

                            if (exception.IndexOf("ConnectionTimeout", StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                errorMessage += "; ConnectionTimeout exception";
                            }
                            else if (exception.IndexOf("Traceback", StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                // ReSharper disable CommentTypo

                                // Example Traceback as of January 2021:
                                // Traceback (most recent call last):\n  File "/opt/pacifica/lib64/python3.6/site-packages/pacifica/ingest/tasks.py", line 88, in ingest_files\n    ingest_obj.ingest()\n  File "/opt/pacifica/lib64/python3.6/site-packages/pacifica/ingest/tarutils.py", line 248, in ingest\n    ingest.upload_file_in_file(info, self.tar)\n  File "/opt/pacifica/lib64/python3.6/site-packages/pacifica/ingest/tarutils.py", line 79, in upload_file_in_file\n    size = int(ret_dict['total_bytes'])\nKeyError: 'total_bytes'\n\n'total_bytes'

                                // ReSharper restore CommentTypo

                                // Use a RegEx to remove unnecessary text that makes some downstream evaluation harder (like checks in stored procedures)
                                var exceptionUpdater = new Regex(@"Traceback \(most recent call last\):\s+File", RegexOptions.IgnoreCase);
                                var exceptionClean = exceptionUpdater.Replace(exception, "in");

                                var keyErrorRegex = new Regex(@"(KeyError:\s.+[^\n])", RegexOptions.IgnoreCase);
                                var keyError = keyErrorRegex.Match(exception).Value; // == string.Empty if no match found.

                                // Lazy match of a string with at least 75 characters, until (and excluding) a newline, tab, or two spaces
                                var exceptionTruncate = new Regex(@"((?:.|[\r\n]){50,}?)(?=(?:  )|[\r\n\t])");
                                var truncated = exceptionTruncate.Match(exceptionClean).Value;

                                // Don't add an exception message longer than 150 characters when using this logic.
                                var reportMessage = truncated + (" ... " + keyError).TrimEnd();
                                errorMessage += "; exception " + reportMessage.Substring(0, Math.Min(150, reportMessage.Length));
                            }
                            else
                            {
                                // Unrecognized, non-"traceback" exception; include the first 75 characters
                                if (exception.Length < 80)
                                {
                                    errorMessage += "; exception " + exception;
                                }
                                else
                                {
                                    errorMessage += "; exception " + exception.Substring(0, Math.Min(75, exception.Length)) + " ...";
                                }
                            }
                        }

                        break;

                    default:
                        if (state.Contains("error"))
                        {
                            OnErrorEvent("Status server is offline or having issues; cannot check " + statusURI);
                        }
                        else
                        {
                            OnErrorEvent("Unrecognized state " + state + " for " + statusURI);
                        }
                        break;
                }

                return status;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error parsing ingest status response", ex);
                currentTask = string.Empty;
                percentComplete = 0;
                return new MyEMSLTaskStatus();
            }
        }

        /// <summary>
        /// Extract the StatusNum (StatusID) from a status URI
        /// </summary>
        /// <param name="statusURI"></param>
        /// <returns>The status number, or 0 if an error</returns>
        // ReSharper disable once UnusedMember.Global
        public static int GetStatusNumFromURI(string statusURI)
        {
            // Check for a match to a URI of the form
            // https://ingestdms.my.emsl.pnl.gov/get_state?job_id=1302995

            var statusNumMatcher = new Regex(@"job_id=(\d+)", RegexOptions.IgnoreCase);

            var match = statusNumMatcher.Match(statusURI);

            if (match.Success)
            {
                var statusNum = int.Parse(match.Groups[1].Value);

                if (statusNum <= 0)
                {
                    throw new Exception("Status ID is 0 in StatusURI: " + statusURI);
                }

                return statusNum;
            }

            // Check for a match to a URI of the form
            // https://a4.my.emsl.pnl.gov/myemsl/cgi-bin/status/2381528/xml
            var legacyStatusNumMatcher = new Regex(@"(\d+)/xml", RegexOptions.IgnoreCase);

            var legacyMatch = legacyStatusNumMatcher.Match(statusURI);

            if (!legacyMatch.Success)
            {
                throw new Exception("Could not find Status ID in StatusURI: " + statusURI);
            }

            var legacyStatusNum = int.Parse(legacyMatch.Groups[1].Value);

            if (legacyStatusNum <= 0)
            {
                throw new Exception("Status ID is 0 in StatusURI: " + statusURI);
            }

            return legacyStatusNum;
        }

        /// <summary>
        /// Percent complete (value between 0 and 100)
        /// </summary>
        /// <remarks>Reports 7 when percentComplete is 100</remarks>
        /// <param name="percentComplete"></param>
        /// <returns>Number of steps completed</returns>
        private static byte IngestStepCompletionCount(int percentComplete)
        {
            // Convert the percent complete value to a number between 0 and 7
            // since historically there were 7 steps to the ingest process:
            // 1. Submitted        .tar file submitted
            // 2. Received         .tar file received
            // 3. Processing       .tar file being processed
            // 4. Verified         .tar file contents validated
            // 5. Stored           .tar file contents copied to Aurora
            // 6. Available        Visible via ElasticSearch
            // 7. Archived         Data copied to tape

            return (byte)(Math.Round(7 * (percentComplete / 100.0)));
        }

        /// <summary>
        /// Validate that the MyEMSL certificate file exists
        /// </summary>
        /// <param name="callingMethod">Calling method</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>True if the cert file is found, otherwise false</returns>
        private bool ValidateCertFile(string callingMethod, out string errorMessage)
        {
            var certificateFilePath = EasyHttp.ResolveCertFile(mPacificaConfig, callingMethod, out errorMessage);

            if (!string.IsNullOrWhiteSpace(certificateFilePath))
            {
                return true;
            }

            OnErrorEvent(errorMessage);
            return false;
        }

        private void EasyHttp_MyEMSLOffline(object sender, MessageEventArgs e)
        {
            OnWarningEvent("MyEMSL is offline; unable to retrieve data: " + e.Message);
        }
    }
}
