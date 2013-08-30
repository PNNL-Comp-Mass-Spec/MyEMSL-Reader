using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Web;
using Pacifica.Core;

namespace MyEMSLReader
{
	public class MyEMSLBase
	{

		#region "Enums"
		internal enum SearchOperator
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

		protected bool ValidSearchResults(Dictionary<string, object> dctResults, out string errorMessage)
		{
			// Check for an error
			errorMessage = ReadDictionaryValue(dctResults, "error", "");
			if (!string.IsNullOrEmpty(errorMessage))
			{
				int charIndex = errorMessage.IndexOf("{");

				// Truncate the message after the first curly bracket
				if (charIndex > 0)
					errorMessage = errorMessage.Substring(0, charIndex);

				charIndex = errorMessage.IndexOf("; shardFailures");
				if (charIndex > 0)
					errorMessage = errorMessage.Substring(0, charIndex);

				return false;
			}

			bool timedOut = ReadDictionaryValue(dctResults, "timed_out", false);

			if (timedOut)
			{
				errorMessage = "Elastic search reports a timeout error";
				return false;
			}

			return true;
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
			string valueText = ReadDictionaryValue(dctData, keyName, valueIfMissing.ToString());
			bool value;

			if (bool.TryParse(valueText, out value))
				return value;

			return valueIfMissing;
		}

		protected long ReadDictionaryValue(Dictionary<string, object> dctData, string keyName, long valueIfMissing)
		{
			string valueText = ReadDictionaryValue(dctData, keyName, valueIfMissing.ToString());
			long value;

			if (long.TryParse(valueText, out value))
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
			this.ErrorMessage = errorMessage;

			OnErrorMessage(new MessageEventArgs(errorMessage));
			System.Threading.Thread.Sleep(10);

			if (this.ThrowErrors)
			{
				if (ex == null)
					throw new Exception(errorMessage);
				else
					throw new Exception(errorMessage, ex);
			}
		}

		protected void ReportMessage(string strMessage)
		{

			if (!string.IsNullOrEmpty(strMessage))
				OnMessage(new MessageEventArgs(strMessage));

		}

		protected void ResetStatus()
		{
			this.ErrorMessage = string.Empty;
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
			ref string responseData,
			out Exception mostRecentException
			)
		{

			mostRecentException = null;
			responseData = string.Empty;

			int timeoutSeconds = 2;
			int attempts = 0;
			bool retrievalSuccess = false;
			HttpStatusCode responseStatusCode = HttpStatusCode.NotFound;

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
							Console.WriteLine("Empty responseDate in SendHTTPRequestWithRetry on attempt " + attempts);
							timeoutSeconds = (int)(Math.Ceiling(timeoutSeconds * 1.5));
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
					
					if (attempts <= maxAttempts)
					{
						// Wait 2 seconds, then retry
						Console.WriteLine("Exception in SendHTTPRequestWithRetry on attempt " + attempts + ": " + ex.Message);
						System.Threading.Thread.Sleep(2000);
						timeoutSeconds = (int)(Math.Ceiling(timeoutSeconds * 1.5));
						continue;
					}
				}
			}

			return retrievalSuccess;
		}


		#region "Event Delegates and Classes"

		public event MessageEventHandler ErrorEvent;
		public event MessageEventHandler MessageEvent;
		public event ProgressEventHandler ProgressEvent;

		public delegate void MessageEventHandler(object sender, MessageEventArgs e);
		public delegate void ProgressEventHandler(object sender, ProgressEventArgs e);

		#endregion

		#region "Event Functions"

		public void OnErrorMessage(MessageEventArgs e)
		{
			if (ErrorEvent != null)
				ErrorEvent(this, e);
		}

		public void OnMessage(MessageEventArgs e)
		{
			if (MessageEvent != null)
				MessageEvent(this, e);
		}

		public void OnProgressUpdate(ProgressEventArgs e)
		{
			if (ProgressEvent != null)
				ProgressEvent(this, e);
		}
		#endregion
	}

}
