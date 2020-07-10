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
    public class MyEMSLBase : EventNotifier
    {

        #region "Enums"

        [Obsolete("Unused")]
        public enum SearchOperator
        {
            And = 0,
            Or = 1
        }

        public bool ThrowErrors
        {
            get;
            set;
        }

        #endregion

        #region "Member Variables"

        protected readonly Configuration mPacificaConfig;

        #endregion

        #region "Properties"

        public string ErrorMessage
        {
            get;
            private set;
        }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public MyEMSLBase()
        {
            mPacificaConfig = new Configuration();
        }

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
                if (gcThread.IsAlive)
                    gcThread.Abort();

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

        protected static Dictionary<string, SortedSet<string>> GetSingleItemSortedSetDictionary(string datasetNameOrID, string subDir)
        {
            var dctDatasetsAndSubDirLists = new Dictionary<string, SortedSet<string>>(StringComparer.OrdinalIgnoreCase)
            {
                {datasetNameOrID, new SortedSet<string>(StringComparer.OrdinalIgnoreCase) { subDir } }
            };

            return dctDatasetsAndSubDirLists;
        }

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
        /// Verify that svc-dms.pfx exists either in the same directory as Pacifica.core.dll or at C:\client_certs\
        /// </summary>
        /// <param name="errorMessage">Output: error message, indicating the paths that were checked</param>
        /// <returns>True if the file is found, otherwise false</returns>
        public bool CertificateFileExists(out string errorMessage)
        {
            var certificateFilePath = EasyHttp.ResolveCertFile(mPacificaConfig, "CertificateFileExists", out _);

            if (string.IsNullOrWhiteSpace(certificateFilePath))
            {
                errorMessage = "MyEMSL certificate file not found in the current directory or in " + Configuration.CLIENT_CERT_DIRECTORY;
                return false;
            }

            errorMessage = string.Empty;
            return true;
        }

        protected string ReadDictionaryValue(Dictionary<string, object> dctData, string keyName, string valueIfMissing)
        {
            if (dctData.TryGetValue(keyName, out var value))
            {
                return value.ToString();
            }

            return valueIfMissing;
        }

        // ReSharper disable once UnusedMember.Global
        protected bool ReadDictionaryValue(Dictionary<string, object> dctData, string keyName, bool valueIfMissing)
        {
            var valueText = ReadDictionaryValue(dctData, keyName, valueIfMissing.ToString());

            if (bool.TryParse(valueText, out var value))
                return value;

            return valueIfMissing;
        }

        // ReSharper disable once UnusedMember.Global
        protected long ReadDictionaryValue(Dictionary<string, object> dctData, string keyName, long valueIfMissing)
        {
            var valueText = ReadDictionaryValue(dctData, keyName, valueIfMissing.ToString(CultureInfo.InvariantCulture));

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

        protected void ReportMessage(string message)
        {
            if (!string.IsNullOrEmpty(message))
                OnStatusEvent(message);
        }

        protected void ReportWarning(string message)
        {
            OnWarningEvent(message);
        }

        protected virtual void ResetStatus()
        {
            ErrorMessage = string.Empty;
        }

        // ReSharper disable once UnusedMember.Global
        protected List<Dictionary<string, object>> RetrieveDictionaryListByKey(Dictionary<string, object> dctResults, string keyName)
        {
            if (!dctResults.TryGetValue(keyName, out var value))
            {
                ReportError("MyEMSL elastic search did not have a '" + keyName + "' dictionary list");
                return new List<Dictionary<string, object>>();
            }

            List<Dictionary<string, object>> dctList;

            try
            {
                dctList = (List<Dictionary<string, object>>)value;
            }
            catch (Exception ex)
            {
                ReportError("Error converting the '" + keyName + "' array to a list object", ex);
                return new List<Dictionary<string, object>>();
            }

            return dctList;
        }

        // ReSharper disable once UnusedMember.Global
        protected Dictionary<string, object> RetrieveDictionaryObjectByKey(Dictionary<string, object> dctResults, string keyName)
        {
            if (!dctResults.TryGetValue(keyName, out var value))
            {
                ReportError("MyEMSL elastic search did not have a '" + keyName + "' section");
                return new Dictionary<string, object>();
            }

            Dictionary<string, object> dctValue;
            try
            {
                dctValue = (Dictionary<string, object>)value;
            }
            catch (Exception ex)
            {
                ReportError("Error converting the '" + keyName + "' section to a dictionary object", ex);
                return new Dictionary<string, object>();
            }

            return dctValue;
        }

        protected bool SendHTTPRequestWithRetry(
            string URL, CookieContainer cookieJar,
            string postData, EasyHttp.HttpMethod postMethod,
            int maxAttempts,
            bool allowEmptyResponseData,
            out string responseData,
            out Exception mostRecentException
            )
        {

            // The following Callback allows us to access the MyEMSL server even if the certificate is expired or untrusted
            // For more info, see comments in Reader.RunElasticSearchQuery()
            if (ServicePointManager.ServerCertificateValidationCallback == null)
                ServicePointManager.ServerCertificateValidationCallback += ValidateRemoteCertificate;

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
                        retrievalSuccess = true;
                    else
                    {
                        if (string.IsNullOrEmpty(responseData))
                        {
                            Console.WriteLine("Empty responseData in SendHTTPRequestWithRetry on attempt " + attempts);
                            timeoutSeconds = IncreaseTimeout(timeoutSeconds);
                        }
                        else
                            retrievalSuccess = true;
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

        private bool ValidateRemoteCertificate(object sender, X509Certificate cert, X509Chain chain, SslPolicyErrors policyErrors)
        {
            var success = Utilities.ValidateRemoteCertificate(sender, cert, chain, policyErrors, out var errorMessage);
            if (success)
                return true;

            OnErrorEvent(errorMessage);
            return false;
        }

        #region "Events"

        public event StatusEventEventHandler MyEMSLOffline;

        protected void EasyHttp_MyEMSLOffline(object sender, MessageEventArgs e)
        {
            if (MyEMSLOffline == null)
                OnWarningEvent("MyEMSL is offline; unable to retrieve data: " + e.Message);
            else
                MyEMSLOffline?.Invoke(e.Message);
        }


        #endregion
    }

}
