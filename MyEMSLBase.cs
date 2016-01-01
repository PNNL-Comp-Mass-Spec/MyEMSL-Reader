using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Threading;
using Pacifica.Core;

namespace MyEMSLReader
{
	public class MyEMSLBase
	{

		#region "Enums"
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

		#region "Properties"

		public string ErrorMessage
		{
			get;
			protected set;
		}

		#endregion
	   
		public static void GarbageCollectNow()
		{
			const int intMaxWaitTimeMSec = 1000;
			GarbageCollectNow(intMaxWaitTimeMSec);
		}

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
			// ReSharper disable once EmptyGeneralCatchClause
			catch
			{
				// Ignore errors here
			}

		}

		protected static void GarbageCollectWaitForGC()
		{
			try
			{
				GC.Collect();
				GC.WaitForPendingFinalizers();
			}
			// ReSharper disable once EmptyGeneralCatchClause
			catch
			{
				// Ignore errors here
			}
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

		protected string ReadDictionaryValue(Dictionary<string, object> dctData, string keyName, string valueIfMissing)
		{
			object value;
			if (dctData.TryGetValue(keyName, out value))
			{
				return value.ToString();
			}

			return valueIfMissing;

		}

		protected bool ReadDictionaryValue(Dictionary<string, object> dctData, string keyName, bool valueIfMissing)
		{
			var valueText = ReadDictionaryValue(dctData, keyName, valueIfMissing.ToString());
			bool value;

			if (bool.TryParse(valueText, out value))
				return value;

			return valueIfMissing;
		}

		protected Int64 ReadDictionaryValue(Dictionary<string, object> dctData, string keyName, Int64 valueIfMissing)
		{
			var valueText = ReadDictionaryValue(dctData, keyName, valueIfMissing.ToString(CultureInfo.InvariantCulture));
			Int64 value;

			if (Int64.TryParse(valueText, out value))
				return value;

			return valueIfMissing;
		}
	
		protected void ReportError(string errorMessage)
		{
			ReportError(errorMessage, null);
		}

		/// <summary>
		/// Report an error.  Will throw an exception if this.ThrowErrors is true
		/// </summary>
		/// <param name="errorMessage"></param>
		/// <param name="ex"></param>
		protected void ReportError(string errorMessage, Exception ex)
		{
		    if (!string.IsNullOrEmpty(ErrorMessage) && String.Equals(ErrorMessage, errorMessage, StringComparison.OrdinalIgnoreCase))
		    {
                // Duplicate error message; do not fire ErrorEvent
		    }
		    else
		    {
		        ErrorMessage = errorMessage;

		        OnErrorMessage(new MessageEventArgs(errorMessage));
		        Thread.Sleep(10);
		    }

		    if (ThrowErrors)
			{
				if (ex == null)
					throw new Exception(errorMessage);
				
				throw new Exception(errorMessage, ex);
			}
		}

		protected void ReportMessage(string strMessage)
		{

			if (!string.IsNullOrEmpty(strMessage))
				OnMessage(new MessageEventArgs(strMessage));

		}

		protected virtual void ResetStatus()
		{
			ErrorMessage = string.Empty;
		}

			
		protected List<Dictionary<string, object>> RetrieveDictionaryListByKey(Dictionary<string, object> dctResults, string keyName)
		{
			object value;
			if (!dctResults.TryGetValue(keyName, out value))
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
				ReportError("Error converting the '" + keyName + "' array to a list object: " + ex.Message, ex);
				return new List<Dictionary<string, object>>();
			}

			return dctList;
		}

		protected Dictionary<string, object> RetrieveDictionaryObjectByKey(Dictionary<string, object> dctResults, string keyName)
		{
			object value;

			if (!dctResults.TryGetValue(keyName, out value))
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
				ReportError("Error converting the '" + keyName + "' section to a dictionary object: " + ex.Message, ex);
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
				ServicePointManager.ServerCertificateValidationCallback += Utilities.ValidateRemoteCertificate;

			mostRecentException = null;
			responseData = string.Empty;

			var timeoutSeconds = 25;
			var attempts = 0;
			var retrievalSuccess = false;
			var responseStatusCode = HttpStatusCode.NotFound;

			while (!retrievalSuccess && attempts <= maxAttempts)
			{
				try
				{
					attempts++;
					responseData = EasyHttp.Send(URL, cookieJar, out responseStatusCode, postData, postMethod, timeoutSeconds);

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

		protected bool ValidSearchResults(Dictionary<string, object> dctResults, out string errorMessage)
		{
			// Check for an error
			errorMessage = ReadDictionaryValue(dctResults, "error", "");
			if (!string.IsNullOrEmpty(errorMessage))
			{
				var charIndex = errorMessage.IndexOf("{");

				// Truncate the message after the first curly bracket
				if (charIndex > 0)
					errorMessage = errorMessage.Substring(0, charIndex);

				charIndex = errorMessage.IndexOf("; shardFailures");
				if (charIndex > 0)
					errorMessage = errorMessage.Substring(0, charIndex);

				return false;
			}

			var timedOut = ReadDictionaryValue(dctResults, "timed_out", false);

			if (timedOut)
			{
				errorMessage = "Elastic search reports a timeout error";
				return false;
			}

			return true;
		}

		#region "Events"

		public event MessageEventHandler ErrorEvent;
		public event MessageEventHandler MessageEvent;
		public event ProgressEventHandler ProgressEvent;
	
		#endregion

		#region "Event Handlers"

        protected void OnErrorMessage(MessageEventArgs e)
		{
			if (ErrorEvent != null)
				ErrorEvent(this, e);
		}

        protected void OnMessage(MessageEventArgs e)
		{
			if (MessageEvent != null)
				MessageEvent(this, e);
		}

        protected void OnProgressUpdate(ProgressEventArgs e)
		{
			if (ProgressEvent != null)
				ProgressEvent(this, e);
		}
		#endregion
	}

}
