using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Pacifica.Core;

namespace MyEMSLReader
{
	public class Reader
	{
		#region "Constants"

		protected const string QUERY_SPEC_INSTRUMENT = "groups.omics.dms.instrument";
		protected const string QUERY_SPEC_DATASET_ID = "groups.omics.dms.dataset_id";
		protected const string QUERY_SPEC_DATASET_NAME = "groups.omics.dms.dataset";

		protected const string QUERY_SPEC_FILENAME = "filename";
		protected const string QUERY_SPEC_EUS_PROPOSAL = "proposals";
		protected const string QUERY_SPEC_ANALYSIS_TOOL = "extended_metadata.gov_pnnl_emsl_dms_analysisjob.tool.name.untouched";
		protected const string QUERY_SPEC_DATASET_NAME_ALT = "extended_metadata.gov_pnnl_emsl_dms_dataset.name.untouched";

		#endregion

		#region "Properties"

		public long LastSearchFileCountMatched
		{
			get;
			private set;
		}

		public int LastSearchFileCountReturned
		{
			get;
			private set;
		}

		/// <summary>
		/// Maximum number of files to return
		/// </summary>
		public int MaxFileCount
		{
			get;
			set;
		}

		#endregion

		// Constructor
		public Reader()
		{
			this.MaxFileCount = 1000;
		}

		public List<ArchivedFileInfo> FindFilesByDatasetID(int datasetID, string subDir)
		{
			// Find all files in MyEMSL for this dataset (by dataset ID)

			var dctSearchTerms = new Dictionary<string, string>();
			dctSearchTerms.Add(QUERY_SPEC_DATASET_ID, datasetID.ToString());

			List<ArchivedFileInfo> lstFiles = QueryElasticSearch(dctSearchTerms);

			if (string.IsNullOrWhiteSpace(subDir))
			{
				return lstFiles;
			}
			else
			{
				// Filter on subDir
				return FilterFilesBySubDir(lstFiles, subDir);
			}

		}

		public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName)
		{
			string subDir = "";
			string instrumentName = "";
			return FindFilesByDatasetName(datasetName, subDir, instrumentName);
		}

		public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName, string subDir)
		{
			string instrumentName = "";
			return FindFilesByDatasetName(datasetName, subDir, instrumentName);
		}

		public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName, string subDir, string instrumentName)
		{
			// Find all files in MyEMSL for this dataset (by dataset ID)

			var dctSearchTerms = new Dictionary<string, string>();
			dctSearchTerms.Add(QUERY_SPEC_DATASET_NAME, datasetName);

			if (!string.IsNullOrWhiteSpace(instrumentName))
			{
				dctSearchTerms.Add(QUERY_SPEC_INSTRUMENT, instrumentName);
			}

			List<ArchivedFileInfo> lstFiles = QueryElasticSearch(dctSearchTerms);

			if (string.IsNullOrWhiteSpace(subDir))
			{
				return lstFiles;
			}
			else
			{
				// Filter on subDir
				return FilterFilesBySubDir(lstFiles, subDir);
			}

		}
		private static List<ArchivedFileInfo> FilterFilesBySubDir(List<ArchivedFileInfo> lstFiles, string subDir)
		{
			var lstFilesFiltered = new List<ArchivedFileInfo>();

			foreach (var file in lstFiles)
			{
				if (!string.IsNullOrWhiteSpace(file.SubDirPath))
				{
					var diSubDir = new DirectoryInfo(file.SubDirPath);

					if (diSubDir.Name.ToLower() == subDir.ToLower())
					{
						lstFilesFiltered.Add(file);
					}
				}
			}

			return lstFilesFiltered;
		}


		private List<ArchivedFileInfo> ParseResults(string xmlString)
		{

			try
			{

				Dictionary<string, object> dctResults = Utilities.JsonToObject(xmlString);

				// Check for an error
				string errorMessage = ReadDictionaryValue(dctResults, "error", "");
				if (!string.IsNullOrEmpty(errorMessage))
				{
					int charIndex = errorMessage.IndexOf("{");

					// Truncate the message after the first curly bracket
					if (charIndex > 0)
						errorMessage = errorMessage.Substring(0, charIndex);

					throw new Exception("MyEMSL elastic search reported an error: " + errorMessage);
				}

				bool timedOut = ReadDictionaryValue(dctResults, "timed_out", false);

				if (timedOut)
					throw new TimeoutException("Elastic search reports a timeout error");

				// Extract out the hits section
				// An exception will be thrown if the section is missing
				var dctHits = RetrieveDictionaryObjectByKey(dctResults, "hits");

				this.LastSearchFileCountMatched = ReadDictionaryValue(dctHits, "total", -1);
				if (this.LastSearchFileCountMatched < 0)
				{
					throw new Exception("Hits section did not have the 'total' entry");
				}

				if (this.LastSearchFileCountMatched == 0)
					return new List<ArchivedFileInfo>();

				// Extract out the list of hits
				var dctFiles = RetrieveDictionaryListByKey(dctHits, "hits");

				// Enumerate the files in dctFiles
				var lstFiles = new List<ArchivedFileInfo>();

				foreach (var item in dctFiles)
				{
					long fileID = ReadDictionaryValue(item, "_id", 0);


					var dctFileInfo = RetrieveDictionaryObjectByKey(item, "_source");

					string instrumentName = ReadDictionaryValue(dctFileInfo, "groups.omics.dms.instrument", string.Empty);

					// The transaction ID is incremented for every new file
					// If two files have the exact same name and path, the newer one will have a larger transaction ID
					long transID = ReadDictionaryValue(dctFileInfo, "trans", 0);

					string submissionTime = ReadDictionaryValue(dctFileInfo, "stime", string.Empty);
					bool publicFile = ReadDictionaryValue(dctFileInfo, "aged", false);

					string fileName = ReadDictionaryValue(dctFileInfo, "filename", string.Empty);
					long fileSizeBytes = ReadDictionaryValue(dctFileInfo, "size", 0);
					string datasetName = ReadDictionaryValue(dctFileInfo, "groups.omics.dms.dataset", string.Empty);
					string datasetID = ReadDictionaryValue(dctFileInfo, "groups.omics.dms.dataset_id", string.Empty);
					string datasetYearQuarter = ReadDictionaryValue(dctFileInfo, "groups.omics.dms.date_code", string.Empty);
					string subDir = ReadDictionaryValue(dctFileInfo, "subdir", string.Empty);

					var dctHashInfo = RetrieveDictionaryObjectByKey(item, "hash");

					string fileSha1Hash = ReadDictionaryValue(dctFileInfo, "sha1", string.Empty);

				}
				return lstFiles;

			}
			catch (Exception ex)
			{
				Console.WriteLine("Error in MyEMSLReader.Reader.ParseResults: " + ex.Message);
				throw ex;
			}

		}

		protected string PossiblyQuoteString(string text)
		{
			if (text.Contains(" "))
				return '"' + text + '"';
			else
				return text;
		}

		/// <summary>
		/// Find files in MyEMSL matching the given search times
		/// </summary>
		/// <param name="dctSearchTerms">Query search terms</param>
		/// <returns></returns>
		protected List<ArchivedFileInfo> QueryElasticSearch(Dictionary<string, string> dctSearchTerms)
		{

			if (this.MaxFileCount < 1)
				this.MaxFileCount = 1;

			this.LastSearchFileCountMatched = 0;
			this.LastSearchFileCountReturned = 0;

			string xmlString = RunQuery(dctSearchTerms, this.MaxFileCount);

			// Parse the results
			List<ArchivedFileInfo> lstFiles = ParseResults(xmlString);

			this.LastSearchFileCountReturned = lstFiles.Count;

			return lstFiles;
		}

		protected string ReadDictionaryValue(Dictionary<string, object> dctData, string keyName, string valueIfMissing)
		{
			object value;
			if (dctData.TryGetValue(keyName, out value))
			{
				return (string)value;
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

		protected List<Dictionary<string, object>> RetrieveDictionaryListByKey(Dictionary<string, object> dctResults, string keyName)
		{
			object value;
			if (!dctResults.TryGetValue(keyName, out value))
			{
				throw new Exception("MyEMSL elastic search did not have a '" + keyName + "' dictionary list");
			}

			List<Dictionary<string, object>> dctList;

			try
			{
				dctList = (List<Dictionary<string, object>>)value;
			}
			catch (Exception ex)
			{
				throw new Exception("Error converting the '" + keyName + "' array to a list object: " + ex.Message, ex);
			}

			return dctList;
		}

		protected Dictionary<string, object> RetrieveDictionaryObjectByKey(Dictionary<string, object> dctResults, string keyName)
		{
			object value;

			if (!dctResults.TryGetValue(keyName, out value))
			{
				throw new Exception("MyEMSL elastic search did not have a '" + keyName + "' section");
			}

			Dictionary<string, object> dctValue;
			try
			{
				dctValue = (Dictionary<string, object>)value;
			}
			catch (Exception ex)
			{
				throw new Exception("Error converting the '" + keyName + "' section to a dictionary object: " + ex.Message, ex);
			}

			return dctValue;
		}

		private string RunQuery(Dictionary<string, string> dctSearchTerms, int maxFileCount)
		{

			/* Construct a JSON query of the form:
				{
					"query": {
						"bool": {
							"must": 
								{
									"query_string": {
										"default_operator": "AND", 
										"default_field": "_all", 
										"query": "groups.omics.dms.instrument:LTQ_4 groups.omics.dms.dataset_id:267771"
									}
								}            
						}
					}, 
					"from": 0, 
					"size": "999"
					}
				}
			 */

			try
			{
				var searchSpec = new Dictionary<string, string>();

				searchSpec.Add("default_operator", "AND");
				searchSpec.Add("default_field", "_all");

				StringBuilder queryTerms = new StringBuilder();
				foreach (var item in dctSearchTerms)
				{
					if (queryTerms.Length > 0)
						queryTerms.Append(" ");

					queryTerms.Append(item.Key + ":" + PossiblyQuoteString(item.Value));
				}
				searchSpec.Add("query", queryTerms.ToString());

				var queryStringSpec = new Dictionary<string, object>();
				queryStringSpec.Add("query_string", searchSpec);

				var mustSpec = new Dictionary<string, object>();
				mustSpec.Add("must", queryStringSpec);

				var boolSpec = new Dictionary<string, object>();
				boolSpec.Add("bool", mustSpec);

				var querySpec = new Dictionary<string, object>();
				querySpec.Add("query", boolSpec);

				querySpec.Add("from", 0);
				querySpec.Add("size", maxFileCount);


				string postData = Pacifica.Core.Utilities.ObjectToJson(querySpec);
				string URL = "http://my.emsl.pnl.gov/myemsl/elasticsearch/simple_items";

				string xmlString = string.Empty;
				bool retrievalSuccess = false;
				int retrievalAttempts = 0;
				int maxAttempts = 3;

				Exception mostRecentException = null;

				while (!retrievalSuccess && retrievalAttempts < maxAttempts)
				{
					try
					{
						retrievalAttempts++;
						xmlString = EasyHttp.Send(URL, postData, EasyHttp.HttpMethod.Post);
						if (!string.IsNullOrEmpty(xmlString))
						{
							retrievalSuccess = true;
						}
					}
					catch (Exception ex)
					{
						mostRecentException = ex;
						if (retrievalAttempts >= maxAttempts)
						{
							xmlString = string.Empty;
						}
						else
						{
							//wait 5 seconds, then retry
							System.Threading.Thread.Sleep(5000);
							continue;
						}
					}
				}

				if (string.IsNullOrEmpty(xmlString))
					throw new System.Net.WebException("No results returned from MyEMSL after " + maxAttempts + " attempts", mostRecentException);

				return xmlString;

			}
			catch (Exception ex)
			{
				Console.WriteLine("Error in MyEMSLReader.Reader.RunQuery: " + ex.Message);
				throw ex;
			}
		}

	}

}
