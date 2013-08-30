﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Pacifica.Core;

namespace MyEMSLReader
{
	/// <summary>
	/// This class contacts MyEMSL to find all of the files associated with the given dataset (by name or ID)
	/// Optionally filter on Instrument name to guarantee you are finding the desired files
	/// Optionally filter on Subdirectory name below th dataset folder to limit the search space
	/// </summary>
	/// <remarks>Written by Matthew Monroe for PNNL in August 2013</remarks>
	public class Reader : MyEMSLBase
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

		#region "Enums"

		internal enum ScanMode
		{
			SimpleSearch = 0,
			ObtainAuthToken = 1,		// Perform a scan, but also obtain an authorization token
			CreateScrollID = 2			// Create a scroll ID
		}

		#endregion

		#region "Properties"

		/// <summary>
		/// When True, then will include all revisions of files that were imported to MyEMSL multiple times
		/// When False, then only reports the newest version of a file
		/// </summary>
		/// <remarks>Default is False</remarks>
		public bool IncludeAllRevisions
		{
			get;
			set;
		}
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
		/// <remarks>Default is 5000</remarks>
		public int MaxFileCount
		{
			get;
			set;
		}

		#endregion

		#region "Public methods"

		// Constructor
		public Reader()
		{			
			this.MaxFileCount = 5000;
			this.IncludeAllRevisions = false;
			this.ThrowErrors = true;
			ResetStatus();
		}

		public List<ArchivedFileInfo> FindFilesByDatasetID(int datasetID)
		{
			string subDir = "";
			string instrumentName = "";
			bool recurse = true;
			return FindFilesByDatasetID(datasetID, subDir, recurse, instrumentName);
		}

		public List<ArchivedFileInfo> FindFilesByDatasetID(int datasetID, string subDir)
		{
			string instrumentName = "";
			bool recurse = true;
			return FindFilesByDatasetID(datasetID, subDir, recurse, instrumentName);
		}

		public List<ArchivedFileInfo> FindFilesByDatasetID(int datasetID, string subDir, bool recurse)
		{
			string instrumentName = "";
			return FindFilesByDatasetID(datasetID, subDir, recurse, instrumentName);
		}

		/// <summary>
		/// Find all files in MyEMSL for this dataset (by dataset ID)
		/// </summary>
		/// <param name="datasetID"></param>
		/// <param name="subDir"></param>
		/// <param name="instrumentName"></param>
		/// <returns></returns>
		public List<ArchivedFileInfo> FindFilesByDatasetID(int datasetID, string subDir, bool recurse, string instrumentName)
		{
			var dctSearchTerms = new List<KeyValuePair<string, string>>();
			dctSearchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATASET_ID, datasetID.ToString()));

			return FindFilesByDataset(subDir, recurse, instrumentName, dctSearchTerms);
		}

		public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName)
		{
			string subDir = "";
			string instrumentName = "";
			bool recurse = true;
			return FindFilesByDatasetName(datasetName, subDir, recurse, instrumentName);
		}

		public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName, string subDir)
		{
			string instrumentName = "";
			bool recurse = true;
			return FindFilesByDatasetName(datasetName, subDir, recurse, instrumentName);
		}

		public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName, string subDir, bool recurse)
		{
			string instrumentName = "";
			return FindFilesByDatasetName(datasetName, subDir, recurse, instrumentName);
		}

		/// <summary>
		/// Find all files in MyEMSL for this dataset (by dataset name)
		/// </summary>
		/// <param name="datasetName"></param>
		/// <param name="subDir"></param>
		/// <param name="instrumentName"></param>
		/// <returns></returns>
		public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName, string subDir, bool recurse, string instrumentName)
		{

			var dctSearchTerms = new List<KeyValuePair<string, string>>();
			dctSearchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATASET_NAME, datasetName));

			return FindFilesByDataset(subDir, recurse, instrumentName, dctSearchTerms);
		}

		#endregion

		#region "Protected Methods"


		protected List<ArchivedFileInfo> FilterFilesNoRecursion(List<ArchivedFileInfo> lstFiles, string subDir)
		{
			var lstFilesFiltered = new List<ArchivedFileInfo>();

			foreach (var file in lstFiles)
			{
				if (string.IsNullOrEmpty(subDir))
				{
					// Did not filter by sub directory
					// Only keep this file if file.SubDirPath is empty
					if (string.IsNullOrEmpty(file.SubDirPath))
						lstFilesFiltered.Add(file);
				}
				else
				{
					// Filtered by sub directory subDir
					// Confirm that this file resides in that sub directory (and not in a sub directory of subDir)

					if (file.SubDirPath.ToLower() == subDir.ToLower())
					{
						lstFilesFiltered.Add(file);
					}

				}
			}

			return lstFilesFiltered;
		}

		protected List<ArchivedFileInfo> FilterFilesBySubDir(List<ArchivedFileInfo> lstFiles, string subDir)
		{
			var lstFilesFiltered = new List<ArchivedFileInfo>();

			List<string> lstRequiredSubDirTree = subDir.Split(new char[] {'/', '\\'}).ToList<string>();

			foreach (var file in lstFiles)
			{
				if (!string.IsNullOrWhiteSpace(file.SubDirPath))
				{
					List<string> lstFileSubDirTree = file.SubDirPath.Split(new char[] { '/', '\\' }).ToList<string>();

					if (lstFileSubDirTree.Count >= lstRequiredSubDirTree.Count)
					{
						int matchCount = 0;
						for (int i = 0; i < lstRequiredSubDirTree.Count; i++)
						{
							if (lstFileSubDirTree[i].ToLower() == lstRequiredSubDirTree[i].ToLower())
								matchCount++;
						}

						if (matchCount == lstRequiredSubDirTree.Count)
						{
							lstFilesFiltered.Add(file);
						}
					}
					
				}
			}

			return lstFilesFiltered;
		}

		protected List<ArchivedFileInfo> FindFilesByDataset(string subDir, bool recurse, string instrumentName, List<KeyValuePair<string, string>> dctSearchTerms)
		{

			try
			{
				ResetStatus();

				// Make sure subDir has unix-style slashes
				if (string.IsNullOrEmpty(subDir))
					subDir = string.Empty;
				else
					subDir = subDir.Replace(@"\", "/");

				if (!string.IsNullOrWhiteSpace(instrumentName))
				{
					dctSearchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_INSTRUMENT, instrumentName));
				}

				List<ArchivedFileInfo> lstFiles = QueryElasticSearch(dctSearchTerms);

				if (!recurse)
				{
					// Filter the files to remove any not in the "root" folder
					lstFiles = FilterFilesNoRecursion(lstFiles, subDir);
				}

				if (!string.IsNullOrWhiteSpace(subDir))
				{
					// Filter on subDir
					lstFiles = FilterFilesBySubDir(lstFiles, subDir);					
				}

				return (from item in lstFiles orderby item.PathWithInstrumentAndDatasetWindows select item).ToList();

			}
			catch (Exception ex)
			{
				if (string.IsNullOrWhiteSpace(this.ErrorMessage))
					ReportError("Error in FindFilesByDataset: " + ex.Message);
				else if (this.ThrowErrors)
					throw ex;

				return new List<ArchivedFileInfo>();
			}
		}

		/// <summary>
		/// Parse the search results from Elastic Search to generate a list of files
		/// </summary>
		/// <param name="xmlString"></param>
		/// <param name="authToken">Output parameter: Authorization token (if available)</param>
		/// <returns></returns>
		internal List<ArchivedFileInfo> ParseResults(string xmlString, out string authToken)
		{

			authToken = string.Empty;

			try
			{

				Dictionary<string, object> dctResults = Utilities.JsonToObject(xmlString);

				// Check for an error
				string errorMessage;
				if (!ValidSearchResults(dctResults, out errorMessage))
				{
					ReportError("Error parsing search results: " + errorMessage);
					return new List<ArchivedFileInfo>();
				}

				// Read the EMSL Authorization Token (will be present if we used ScanMode.ObtainAuthToken
				authToken = ReadDictionaryValue(dctResults, "myemsl_auth_token", string.Empty);

				// Extract out the hits section
				// An exception will be thrown if the section is missing
				var dctHits = RetrieveDictionaryObjectByKey(dctResults, "hits");

				this.LastSearchFileCountMatched = ReadDictionaryValue(dctHits, "total", -1);
				if (this.LastSearchFileCountMatched < 0)
				{
					ReportError("Hits section did not have the 'total' entry");
					return new List<ArchivedFileInfo>();
				}

				if (this.LastSearchFileCountMatched == 0)
					return new List<ArchivedFileInfo>();

				// Extract out the list of hits
				var dctFiles = RetrieveDictionaryListByKey(dctHits, "hits");

				// Enumerate the files in dctFiles
				var lstFiles = new List<ArchivedFileInfo>();

				// This dictionary keeps track of the newest version of each unique file
				// Keys in this dictionary are relative file paths while indices are the item index in lstFiles
				var dctMostRecentVersionPointers = new Dictionary<string, int>(StringComparer.CurrentCultureIgnoreCase);

				int itemIndex = 0;
				foreach (var dctFile in dctFiles)
				{
					try
					{

						long fileID = ReadDictionaryValue(dctFile, "_id", 0);

						var dctFileInfo = RetrieveDictionaryObjectByKey(dctFile, "_source");

						string instrumentName = ReadDictionaryValue(dctFileInfo, "groups.omics.dms.instrument", string.Empty);

						// The transaction ID is incremented every time a group of files is submitted (aka one bundle)
						// All files submitted in the same .tar file will have the same transaction ID
						// If two files have the exact same name and path, the newer one will have a larger transaction ID
						long transID = ReadDictionaryValue(dctFileInfo, "trans", 0);

						string submissionTime = ReadDictionaryValue(dctFileInfo, "stime", string.Empty);
						bool publicFile = ReadDictionaryValue(dctFileInfo, "aged", false);

						string fileName = ReadDictionaryValue(dctFileInfo, "filename", string.Empty);
						long fileSizeBytes = ReadDictionaryValue(dctFileInfo, "size", 0);
						string datasetName = ReadDictionaryValue(dctFileInfo, "groups.omics.dms.dataset", string.Empty);
						int datasetID = (int)ReadDictionaryValue(dctFileInfo, "groups.omics.dms.dataset_id", 0);
						string datasetYearQuarter = ReadDictionaryValue(dctFileInfo, "groups.omics.dms.date_code", string.Empty);
						string subDir = ReadDictionaryValue(dctFileInfo, "subdir", string.Empty);

						var dctHashInfo = RetrieveDictionaryObjectByKey(dctFileInfo, "hash");

						string fileSha1Hash = ReadDictionaryValue(dctHashInfo, "sha1", string.Empty);

						var archiveFile = new ArchivedFileInfo(datasetName, fileName, subDir, fileID, instrumentName, datasetYearQuarter, dctFile);
						archiveFile.Sha1Hash = fileSha1Hash;
						archiveFile.FileSizeBytes = fileSizeBytes;

						archiveFile.TransactionID = transID;
						archiveFile.SubmissionTime = submissionTime;
						archiveFile.IsPublicFile = publicFile;
						archiveFile.DatasetID = datasetID;

						int existingIndex;

						if (dctMostRecentVersionPointers.TryGetValue(archiveFile.PathWithInstrumentAndDatasetWindows, out existingIndex))
						{
							// Found a duplicate file
							if (this.IncludeAllRevisions)
							{
								// Including all revisions of a file
								lstFiles.Add(archiveFile);

								if (lstFiles[existingIndex].TransactionID < archiveFile.TransactionID)
								{
									// This file is newer; update dctUniqueFiles
									dctMostRecentVersionPointers[archiveFile.PathWithInstrumentAndDatasetWindows] = lstFiles.Count - 1;
								}

							}
							else
							{
								if (lstFiles[existingIndex].TransactionID < archiveFile.TransactionID)
								{
									// This file is newer; replace the old file
									lstFiles[existingIndex] = archiveFile;
								}
							}

						}
						else
						{
							// This is a new file; add it to lstFiles and update dctUniqueFiles
							lstFiles.Add(archiveFile);
							dctMostRecentVersionPointers.Add(archiveFile.PathWithInstrumentAndDatasetWindows, lstFiles.Count - 1);
						}


					}
					catch (Exception ex)
					{
						Console.WriteLine("Error parsing item " + itemIndex + "; will be skipped: " + ex.Message);
					}

					itemIndex++;
				}

				return lstFiles;

			}
			catch (Exception ex)
			{
				ReportError("Error in MyEMSLReader.Reader.ParseResults: " + ex.Message, ex);
				return new List<ArchivedFileInfo>();
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
		protected List<ArchivedFileInfo> QueryElasticSearch(List<KeyValuePair<string, string>> dctSearchTerms)
		{

			try
			{

				if (this.MaxFileCount < 1)
					this.MaxFileCount = 1;

				this.LastSearchFileCountMatched = 0;
				this.LastSearchFileCountReturned = 0;

				string xmlString = RunQuery(dctSearchTerms, this.MaxFileCount);
				if (string.IsNullOrWhiteSpace(xmlString))
				{
					if (string.IsNullOrWhiteSpace(this.ErrorMessage))
						ReportError("RunQuery returned an empty xml result");
					this.LastSearchFileCountReturned = 0;
					return new List<ArchivedFileInfo>();
				}

				// Parse the results (note that authToken will always be empty because we used ScanMode.SimpleSearch)
				string authToken = string.Empty;
				List<ArchivedFileInfo> lstFiles = ParseResults(xmlString, out authToken);

				this.LastSearchFileCountReturned = lstFiles.Count;

				return lstFiles;
			}
			catch (Exception ex)
			{
				if (string.IsNullOrWhiteSpace(this.ErrorMessage))
					ReportError("Error in QueryElasticSearch: " + ex.Message);
				else if (this.ThrowErrors)
					throw ex;

				return new List<ArchivedFileInfo>();
			}

		}

		protected new void ResetStatus()
		{
			base.ResetStatus();
		}

		internal string RunQuery(List<KeyValuePair<string, string>> dctSearchTerms, int maxFileCount)
		{
			CookieContainer cookieJar = null;
			return RunQuery(dctSearchTerms, maxFileCount, SearchOperator.And, ScanMode.SimpleSearch, ref cookieJar);
		}

		/// <summary>
		/// Run an elastic search query against MyEMSL
		/// </summary>
		/// <param name="dctSearchTerms">Dictionary of terms to search for</param>
		/// <param name="maxFileCount">Maximum number of hits to return</param>
		/// <param name="logicalOperator">Whether to AND or OR the search terms together</param>
		/// <param name="scanMode">Scan mode (0=Simple Search, 1=Search, but obtain a myemsl_auth_token, 2 = Create a ScrollID)</param>
		/// <returns>XML response string</returns>
		/// <remarks>Be sure to call Logout() when scanMode is not 0 </remarks>
		internal string RunQuery(List<KeyValuePair<string, string>> dctSearchTerms, int maxFileCount, SearchOperator logicalOperator, ScanMode scanMode, ref CookieContainer cookieJar)
		{

			/* Construct a JSON query, for example:
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

				switch (logicalOperator)
				{
					case SearchOperator.And:
						searchSpec.Add("default_operator", "AND");
						break;
					case SearchOperator.Or:
						searchSpec.Add("default_operator", "OR");
						break;
					default:
						throw new ArgumentOutOfRangeException("Unrecognized value for logicalOperator: " + logicalOperator.ToString());
				}

				searchSpec.Add("default_field", "_all");

				StringBuilder queryTerms = new StringBuilder();
				foreach (var searchTerm in dctSearchTerms)
				{
					if (queryTerms.Length > 0)
						queryTerms.Append(" ");

					queryTerms.Append(searchTerm.Key + ":" + PossiblyQuoteString(searchTerm.Value));
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

				// Call the testauth service to obtain a cookie for this session
				string authURL = Configuration.TestAuthUri;
				Auth auth = new Auth(new Uri(authURL));

				if (cookieJar == null)
				{
					if (!auth.GetAuthCookies(out cookieJar))
					{
						ReportError("Auto-login to ingest.my.emsl.pnl.gov failed authentication");
						return string.Empty;
					}
				}

				string postData = Pacifica.Core.Utilities.ObjectToJson(querySpec);
				string URL = Configuration.ElasticSearchUri + "simple_items";

				if (scanMode == ScanMode.ObtainAuthToken)
				{
					URL += "?auth";
				}
				else if (scanMode == ScanMode.CreateScrollID)
				{
					URL += "?search_type=scan";
				}

				int maxAttempts = 4;
				string xmlString = string.Empty;
				Exception mostRecentException;
				bool allowEmptyResponseData = false;

				bool retrievalSuccess = SendHTTPRequestWithRetry(URL, cookieJar, postData, EasyHttp.HttpMethod.Post, maxAttempts, allowEmptyResponseData, ref xmlString, out mostRecentException);

				if (string.IsNullOrEmpty(xmlString))
				{
					ReportError("No results returned from MyEMSL after " + maxAttempts + " attempts", mostRecentException);
				}

				if (scanMode == ScanMode.SimpleSearch)
				{
					Utilities.Logout(cookieJar);
				}

				return xmlString;

			}
			catch (Exception ex)
			{
				ReportError("Error in MyEMSLReader.Reader.RunQuery: " + ex.Message, ex);
				return string.Empty;
			}


		}

		#endregion
	}

}
