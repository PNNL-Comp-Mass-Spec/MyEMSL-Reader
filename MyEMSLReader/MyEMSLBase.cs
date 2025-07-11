using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Pacifica.Core;
using PRISM;
using Utilities = Pacifica.Core.Utilities;

namespace MyEMSLReader
{
    /// <summary>
    /// Base class for the Downloader and Reader classes
    /// </summary>
    public class MyEMSLBase : EventNotifier
    {
        // Ignore Spelling: downloader, Pacifica, svc

        /// <summary>
        /// When true, raise a DebugEvent prior to contacting the metadata server
        /// </summary>
        public bool ThrowErrors
        {
            get;
            set;
        }

        /// <summary>
        /// Pacifica configuration
        /// </summary>
        protected readonly Configuration mPacificaConfig;

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage
        {
            get;
            private set;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public MyEMSLBase()
        {
            mPacificaConfig = new Configuration();
        }

        /// <summary>
        /// Garbage collect now
        /// </summary>
        public static void GarbageCollectNow()
        {
            const int intMaxWaitTimeMSec = 1000;
            GarbageCollectNow(intMaxWaitTimeMSec);
        }

        /// <summary>
        /// Force the garbage collector to run
        /// </summary>
        /// <param name="intMaxWaitTimeMSec"></param>
        public static void GarbageCollectNow(int intMaxWaitTimeMSec)
        {
            const int THREAD_SLEEP_TIME_MSEC = 100;

            if (intMaxWaitTimeMSec < 100)
                intMaxWaitTimeMSec = 100;

            if (intMaxWaitTimeMSec > 5000)
                intMaxWaitTimeMSec = 5000;

            Thread.Sleep(100);

            try
            {
                var gcThread = new Thread(GarbageCollectWaitForGC);
                gcThread.Start();

                var intTotalThreadWaitTimeMsec = 0;

                while (gcThread.IsAlive && intTotalThreadWaitTimeMsec < intMaxWaitTimeMSec)
                {
                    Thread.Sleep(THREAD_SLEEP_TIME_MSEC);
                    intTotalThreadWaitTimeMsec += THREAD_SLEEP_TIME_MSEC;
                }

#if NET48
                if (gcThread.IsAlive)
                    gcThread.Abort();
#endif
            }
            catch
            {
                // Ignore errors here
            }
        }

        private static void GarbageCollectWaitForGC()
        {
            try
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
            catch
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Get the value for a field, using valueIfNull if the field is null
        /// </summary>
        /// <param name="reader">Reader</param>
        /// <param name="fieldName">Field name</param>
        /// <param name="valueIfNull">Integer to return if null</param>
        /// <param name="isNull">True if the value is null</param>
        /// <returns>Integer</returns>
        // ReSharper disable once UnusedMember.Global
        protected static int GetDbValue(IDataRecord reader, string fieldName, int valueIfNull, out bool isNull)
        {
            if (Convert.IsDBNull(reader[fieldName]))
            {
                isNull = true;
                return valueIfNull;
            }

            isNull = false;
            return (int)reader[fieldName];
        }

        /// <summary>
        /// Get the value for a field, using valueIfNull if the field is null
        /// </summary>
        /// <param name="reader">Reader</param>
        /// <param name="fieldName">Field name</param>
        /// <param name="valueIfNull">String to return if null</param>
        /// <param name="isNull">True if the value is null</param>
        /// <returns>String</returns>
        // ReSharper disable once UnusedMember.Global
        protected static string GetDbValue(IDataRecord reader, string fieldName, string valueIfNull, out bool isNull)
        {
            if (Convert.IsDBNull(reader[fieldName]))
            {
                isNull = true;
                return valueIfNull;
            }

            isNull = false;

            // Use .ToString() and not a string cast to allow for DateTime fields to convert to strings
            return reader[fieldName].ToString();
        }

        /// <summary>
        /// Create a SortedSet dictionary for a single dataset and its subdirectory
        /// </summary>
        /// <param name="datasetNameOrID"></param>
        /// <param name="subDir"></param>
        protected static Dictionary<string, SortedSet<string>> GetSingleItemSortedSetDictionary(string datasetNameOrID, string subDir)
        {
            var datasetsAndSubDirLists = new Dictionary<string, SortedSet<string>>(StringComparer.OrdinalIgnoreCase)
            {
                {datasetNameOrID, new SortedSet<string>(StringComparer.OrdinalIgnoreCase) { subDir } }
            };

            return datasetsAndSubDirLists;
        }

        /// <summary>
        /// Compute a longer timeout to use, first by doubling, but later by multiplying by 1.5
        /// </summary>
        /// <param name="timeoutSeconds"></param>
        protected static int IncreaseTimeout(int timeoutSeconds)
        {
            if (timeoutSeconds < 8)
            {
                timeoutSeconds *= 2;
            }
            else
            {
                timeoutSeconds = (int)(Math.Ceiling(timeoutSeconds * 1.5));
            }

            return timeoutSeconds;
        }

        /// <summary>
        /// Verify that svc-dms.pfx exists either in the same directory as Pacifica.core.dll, C:\DMS_Programs\client_certs, or at C:\client_certs\
        /// </summary>
        /// <param name="errorMessage">Output: error message, indicating the paths that were checked</param>
        /// <returns>True if the file is found, otherwise false</returns>
        public bool CertificateFileExists(out string errorMessage)
        {
            var certificateFilePath = EasyHttp.ResolveCertFile(mPacificaConfig, "CertificateFileExists", out errorMessage, out _);

            if (string.IsNullOrWhiteSpace(certificateFilePath))
            {
                return false;
            }

            errorMessage = string.Empty;
            return true;
        }

        /// <summary>
        /// Read the value for the given key in the dictionary
        /// </summary>
        /// <param name="dataDictionary"></param>
        /// <param name="keyName"></param>
        /// <param name="valueIfMissing"></param>
        /// <returns>The value if found, or valueIfMissing</returns>
        protected static string ReadDictionaryValue(Dictionary<string, object> dataDictionary, string keyName, string valueIfMissing)
        {
            if (dataDictionary.TryGetValue(keyName, out var value))
            {
                return value.ToString();
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Read the value for the given key in the dictionary
        /// </summary>
        /// <param name="dataDictionary"></param>
        /// <param name="keyName"></param>
        /// <param name="valueIfMissing"></param>
        /// <returns>The value if found, or valueIfMissing</returns>
        // ReSharper disable once UnusedMember.Global
        protected static bool ReadDictionaryValue(Dictionary<string, object> dataDictionary, string keyName, bool valueIfMissing)
        {
            var valueText = ReadDictionaryValue(dataDictionary, keyName, valueIfMissing.ToString());

            if (bool.TryParse(valueText, out var value))
                return value;

            return valueIfMissing;
        }

        /// <summary>
        /// Read the value for the given key in the dictionary
        /// </summary>
        /// <param name="dataDictionary"></param>
        /// <param name="keyName"></param>
        /// <param name="valueIfMissing"></param>
        /// <returns>The value if found, or valueIfMissing</returns>
        // ReSharper disable once UnusedMember.Global
        protected static long ReadDictionaryValue(Dictionary<string, object> dataDictionary, string keyName, long valueIfMissing)
        {
            var valueText = ReadDictionaryValue(dataDictionary, keyName, valueIfMissing.ToString(CultureInfo.InvariantCulture));

            if (long.TryParse(valueText, out var value))
                return value;

            return valueIfMissing;
        }

        /// <summary>
        /// Report an error.  Will throw an exception if this.ThrowErrors is true
        /// </summary>
        /// <param name="errorMessage">Error Message</param>
        protected void ReportError(string errorMessage)
        {
            ReportError(errorMessage, null);
        }

        /// <summary>
        /// Report an error.  Will throw an exception if this.ThrowErrors is true and allowThrowErrors is true
        /// </summary>
        /// <param name="errorMessage">Error Message</param>
        /// <param name="allowThrowErrors">True to throw errors as an exception if ThrowErrors is true</param>
        // ReSharper disable once UnusedMember.Global
        protected void ReportError(string errorMessage, bool allowThrowErrors)
        {
            ReportError(errorMessage, null, allowThrowErrors);
        }

        /// <summary>
        /// Report an error.  Will throw an exception if this.ThrowErrors is true and allowThrowErrors is true
        /// </summary>
        /// <param name="errorMessage">Error Message</param>
        /// <param name="ex">Exception</param>
        /// <param name="allowThrowErrors">True to throw errors as an exception if ThrowErrors is true</param>
        protected void ReportError(string errorMessage, Exception ex, bool allowThrowErrors = true)
        {
            if (!string.IsNullOrEmpty(ErrorMessage) && string.Equals(ErrorMessage, errorMessage, StringComparison.OrdinalIgnoreCase))
            {
                // Duplicate error message; do not fire ErrorEvent
            }
            else
            {
                ErrorMessage = errorMessage;

                OnErrorEvent(errorMessage, ex);
                Thread.Sleep(10);
            }

            if (allowThrowErrors && ThrowErrors)
            {
                if (ex == null)
                    throw new Exception(errorMessage);

                throw new Exception(errorMessage, ex);
            }
        }

        /// <summary>
        /// Raise event StatusEvent if the message is not empty
        /// </summary>
        /// <param name="message"></param>
        protected void ReportMessage(string message)
        {
            if (!string.IsNullOrEmpty(message))
                OnStatusEvent(message);
        }

        /// <summary>
        /// Raise event WarningEvent
        /// </summary>
        /// <param name="message"></param>
        protected void ReportWarning(string message)
        {
            OnWarningEvent(message);
        }

        /// <summary>
        /// Clear the error message
        /// </summary>
        protected virtual void ResetStatus()
        {
            ErrorMessage = string.Empty;
        }

        /// <summary>
        /// Look for the given key in the results dictionary
        /// </summary>
        /// <param name="results"></param>
        /// <param name="keyName"></param>
        /// <returns>List Dictionary of string, object if found, or empty dictionary list if not found or an error occurs</returns>
        // ReSharper disable once UnusedMember.Global
        protected List<Dictionary<string, object>> RetrieveDictionaryListByKey(Dictionary<string, object> results, string keyName)
        {
            if (!results.TryGetValue(keyName, out var value))
            {
                ReportError("MyEMSL elastic search did not have a '" + keyName + "' dictionary list");
                return new List<Dictionary<string, object>>();
            }

            try
            {
                return (List<Dictionary<string, object>>)value;
            }
            catch (Exception ex)
            {
                ReportError("Error converting the '" + keyName + "' array to a list object", ex);
                return new List<Dictionary<string, object>>();
            }
        }

        /// <summary>
        /// Look for the given key in the results dictionary
        /// </summary>
        /// <param name="results"></param>
        /// <param name="keyName"></param>
        /// <returns>(Dictionary of string, object if found, or empty dictionary if not found or an error occurs</returns>
        // ReSharper disable once UnusedMember.Global
        protected Dictionary<string, object> RetrieveDictionaryObjectByKey(Dictionary<string, object> results, string keyName)
        {
            if (!results.TryGetValue(keyName, out var value))
            {
                ReportError("MyEMSL elastic search did not have a '" + keyName + "' section");
                return new Dictionary<string, object>();
            }

            Dictionary<string, object> dictionaryValue;
            try
            {
                dictionaryValue = (Dictionary<string, object>)value;
            }
            catch (Exception ex)
            {
                ReportError("Error converting the '" + keyName + "' section to a dictionary object", ex);
                return new Dictionary<string, object>();
            }

            return dictionaryValue;
        }

        /// <summary>
        /// Send the HTTP request, retrying if an error
        /// </summary>
        /// <param name="URL"></param>
        /// <param name="cookieJar"></param>
        /// <param name="postData"></param>
        /// <param name="postMethod"></param>
        /// <param name="maxAttempts"></param>
        /// <param name="allowEmptyResponseData"></param>
        /// <param name="responseData"></param>
        /// <param name="mostRecentException"></param>
        /// <returns>True if success, false if an error</returns>
        // ReSharper disable once UnusedMember.Global
        protected bool SendHTTPRequestWithRetry(
            string URL, CookieContainer cookieJar,
            string postData, EasyHttp.HttpMethod postMethod,
            int maxAttempts,
            bool allowEmptyResponseData,
            out string responseData,
            out Exception mostRecentException
            )
        {
            mostRecentException = null;
            responseData = string.Empty;

            if (!ValidateCertFile("SendHTTPRequestWithRetry"))
            {
                return false;
            }

            var timeoutSeconds = 25;
            var attempts = 0;
            var retrievalSuccess = false;
            var responseStatusCode = HttpStatusCode.NotFound;

            while (!retrievalSuccess && attempts <= maxAttempts)
            {
                try
                {
                    attempts++;
                    responseData = EasyHttp.SendViaThreadStart(mPacificaConfig, URL, cookieJar, out responseStatusCode, postData, postMethod, timeoutSeconds);

                    if (allowEmptyResponseData && responseStatusCode == HttpStatusCode.OK)
                    {
                        retrievalSuccess = true;
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(responseData))
                        {
                            Console.WriteLine("Empty responseData in SendHTTPRequestWithRetry on attempt " + attempts);
                            timeoutSeconds = IncreaseTimeout(timeoutSeconds);
                        }
                        else
                        {
                            retrievalSuccess = true;
                        }
                    }
                }
                catch (Exception ex)
                {
                    mostRecentException = ex;

                    if (responseStatusCode == HttpStatusCode.Forbidden)
                    {
                        // Access denied; no point in retrying the request
                        ReportMessage("Warning, access forbidden to URL; likely a cookie problem " + URL);
                        break;
                    }

                    if (mostRecentException.Message.StartsWith("Aurora Offline"))
                    {
                        // Access denied; no point in retrying the request
                        ReportMessage("Aurora is offline; cannot query MyEMSL");
                        break;
                    }

                    if (attempts <= maxAttempts)
                    {
                        // Wait 2 seconds, then retry
                        Console.WriteLine("Exception in SendHTTPRequestWithRetry on attempt " + attempts + ": " + ex.Message);
                        Thread.Sleep(2000);
                        timeoutSeconds = IncreaseTimeout(timeoutSeconds);
                    }
                }
            }

            return retrievalSuccess;
        }

        /// <summary>
        /// Validate that the MyEMSL certificate file exists
        /// </summary>
        /// <param name="callingMethod">Calling method</param>
        /// <returns>True if the cert file is found, otherwise false</returns>
        protected bool ValidateCertFile(string callingMethod)
        {
            var certificateFilePath = EasyHttp.ResolveCertFile(mPacificaConfig, callingMethod, out var errorMessage);

            if (!string.IsNullOrWhiteSpace(certificateFilePath))
                return true;

            OnErrorEvent(errorMessage);
            return false;
        }

        /// <summary>
        /// MyEMSL offline event
        /// </summary>
        public event StatusEventEventHandler MyEMSLOffline;

        /// <summary>
        /// Raise event MyEMSLOffline
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        protected void EasyHttp_MyEMSLOffline(object sender, MessageEventArgs e)
        {
            if (MyEMSLOffline == null)
                OnWarningEvent("MyEMSL is offline; unable to retrieve data: " + e.Message);
            else
                MyEMSLOffline?.Invoke(e.Message);
        }
    }
}
