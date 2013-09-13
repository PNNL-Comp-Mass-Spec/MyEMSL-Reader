using System;
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

		protected const string DATASET_ID_TAG = "#*#*#_DATASET_ID_";

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
		public Int64 LastSearchFileCountMatched
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

		/// <summary>
		/// Find all files in MyEMSL for one dataset (by dataset ID)
		/// </summary>
		/// <param name="datasetID">Dataset ID</param>
		/// <returns>List of matched files</returns>
		public List<ArchivedFileInfo> FindFilesByDatasetID(int datasetID)
		{
			string subDir = "";
			string instrumentName = "";
			bool recurse = true;
			return FindFilesByDatasetID(datasetID, subDir, recurse, instrumentName);
		}

		/// <summary>
		/// Find all files in MyEMSL for one dataset (by dataset ID)
		/// </summary>
		/// <param name="datasetID">Dataset ID</param>
		/// <param name="subDir">Subdirectory name to filter on</param>
		/// <returns>List of matched files</returns>
		public List<ArchivedFileInfo> FindFilesByDatasetID(int datasetID, string subDir)
		{
			string instrumentName = "";
			bool recurse = true;
			return FindFilesByDatasetID(datasetID, subDir, recurse, instrumentName);
		}

		/// <summary>
		/// Find all files in MyEMSL for one dataset (by dataset ID)
		/// </summary>
		/// <param name="datasetID">Dataset ID</param>
		/// <param name="subDir">Subdirectory name to filter on</param>
		/// <param name="recurse">True to recursively search for files</param>
		/// <returns>List of matched files</returns>
		public List<ArchivedFileInfo> FindFilesByDatasetID(int datasetID, string subDir, bool recurse)
		{
			string instrumentName = "";
			return FindFilesByDatasetID(datasetID, subDir, recurse, instrumentName);
		}

		/// <summary>
		/// Find all files in MyEMSL for one dataset (by dataset ID)
		/// </summary>
		/// <param name="datasetID">Dataset ID</param>
		/// <param name="subDir">Subdirectory name to filter on</param>
		/// <param name="recurse">True to recursively search for files</param>
		/// <param name="instrumentName">Instrument name to filter on; this can be used to double-check that the dataset ID corresponds to an expected instrument</param>
		/// <returns>List of matched files</returns>
		public List<ArchivedFileInfo> FindFilesByDatasetID(int datasetID, string subDir, bool recurse, string instrumentName)
		{
			var dctSearchTerms = new List<KeyValuePair<string, string>>();
			dctSearchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATASET_ID, datasetID.ToString()));

			var dctDatasetsAndSubDirs = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
			dctDatasetsAndSubDirs.Add(DATASET_ID_TAG + datasetID, subDir);

			return FindFilesByDataset(dctDatasetsAndSubDirs, recurse, instrumentName, dctSearchTerms);
		}

		/// <summary>
		/// Find all files in MyEMSL for a list of datasets (by dataset ID)
		/// </summary>
		/// <param name="dctDatasetsAndSubDirs">Keys are dataset ID, values are the optional Subdirectory name to filter on for the given dataset</param>
		/// <returns>List of matched files</returns>
		public List<ArchivedFileInfo> FindFilesByDatasetID(Dictionary<int, string> dctDatasetIDsAndSubDirs)
		{
			bool recurse = true;
			return FindFilesByDatasetID(dctDatasetIDsAndSubDirs, recurse);
		}

		/// <summary>
		/// Find all files in MyEMSL for a list of datasets (by dataset ID)
		/// </summary>
		/// <param name="dctDatasetsAndSubDirs">Keys are dataset names, values are the optional Subdirectory name to filter on for the given dataset</param>
		/// <param name="recurse">True to recursively search for files</param>
		/// <returns>List of matched files</returns>
		public List<ArchivedFileInfo> FindFilesByDatasetID(Dictionary<int, string> dctDatasetIDsAndSubDirs, bool recurse)
		{

			var dctSearchTerms = new List<KeyValuePair<string, string>>();
			var dctDatasetsAndSubDirs = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

			foreach (var datasetEntry in dctDatasetIDsAndSubDirs)
			{
				dctSearchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATASET_ID, datasetEntry.Key.ToString()));
				dctDatasetsAndSubDirs.Add(DATASET_ID_TAG + datasetEntry.Key, datasetEntry.Value);
			}

			string instrumentName = string.Empty;
			return FindFilesByDataset(dctDatasetsAndSubDirs, recurse, instrumentName, dctSearchTerms);
		}

		/// <summary>
		/// Find all files in MyEMSL for one dataset (by dataset name)
		/// </summary>
		/// <param name="datasetName">Dataset Name</param>
		/// <returns>List of matched files</returns>
		public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName)
		{
			string subDir = "";
			string instrumentName = "";
			bool recurse = true;
			return FindFilesByDatasetName(datasetName, subDir, recurse, instrumentName);
		}

		/// <summary>
		/// Find all files in MyEMSL for one dataset (by dataset name)
		/// </summary>
		/// <param name="datasetName">Dataset Name</param>
	    /// <param name="subDir">Subdirectory name to filter on</param>
		/// <returns>List of matched files</returns>
		public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName, string subDir)
		{
			string instrumentName = "";
			bool recurse = true;
			return FindFilesByDatasetName(datasetName, subDir, recurse, instrumentName);
		}

		/// <summary>
		/// Find all files in MyEMSL for one dataset (by dataset name)
		/// </summary>
		/// <param name="datasetName">Dataset Name</param>
		/// <param name="subDir">Subdirectory name to filter on</param>
		/// <param name="recurse">True to recursively search for files</param>
		/// <returns>List of matched files</returns>
		public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName, string subDir, bool recurse)
		{
			string instrumentName = "";
			return FindFilesByDatasetName(datasetName, subDir, recurse, instrumentName);
		}

		/// <summary>
		/// Find all files in MyEMSL for one dataset (by dataset name)
		/// </summary>
		/// <param name="datasetName">Dataset Name</param>
		/// <param name="subDir">Subdirectory name to filter on</param>
		/// <param name="recurse">True to recursively search for files</param>
		/// <param name="instrumentName">Instrument name to filter on; this can be used to double-check that the dataset ID corresponds to an expected instrument</param>
		/// <returns>List of matched files</returns>
		public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName, string subDir, bool recurse, string instrumentName)
		{

			var dctSearchTerms = new List<KeyValuePair<string, string>>();
			dctSearchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATASET_NAME, datasetName));

			var dctDatasetsAndSubDirs = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
			dctDatasetsAndSubDirs.Add(datasetName, subDir);

			return FindFilesByDataset(dctDatasetsAndSubDirs, recurse, instrumentName, dctSearchTerms);
		}


		/// <summary>
		/// Find all files in MyEMSL for a list of datasets (by dataset name)
		/// </summary>
		/// <param name="dctDatasetsAndSubDirs">Keys are dataset name, values are the optional Subdirectory name to filter on for the given dataset</param>
		/// <returns>List of matched files</returns>
		public List<ArchivedFileInfo> FindFilesByDatasetName(Dictionary<string, string> dctDatasetsAndSubDirs)
		{
			bool recurse = true;
			return FindFilesByDatasetName(dctDatasetsAndSubDirs, recurse);
		}

		/// <summary>
		/// Find all files in MyEMSL for a list of datasets (by dataset name)
		/// </summary>
		/// <param name="dctDatasetsAndSubDirs">Keys are dataset names, values are the optional Subdirectory name to filter on for the given dataset</param>
		/// <param name="recurse">True to recursively search for files</param>
		/// <returns>List of matched files</returns>
		public List<ArchivedFileInfo> FindFilesByDatasetName(Dictionary<string, string> dctDatasetsAndSubDirs, bool recurse)
		{

			var dctSearchTerms = new List<KeyValuePair<string, string>>();
			foreach (var dataset in dctDatasetsAndSubDirs)
			{
				dctSearchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATASET_NAME, dataset.Key));
			}

			string instrumentName = string.Empty;
			return FindFilesByDataset(dctDatasetsAndSubDirs, recurse, instrumentName, dctSearchTerms);
		}

		#endregion

		#region "Protected Methods"


		protected List<ArchivedFileInfo> FilterFilesNoRecursion(List<ArchivedFileInfo> lstFiles, Dictionary<string, string> dctDatasetsAndSubDirs)
		{
			var lstFilesFiltered = new List<ArchivedFileInfo>();
			bool usingDatasetIDs = false;

			if (dctDatasetsAndSubDirs.First().Key.StartsWith(DATASET_ID_TAG))
				usingDatasetIDs = true;

			string currentDataset = string.Empty;
			string currentSubDir = dctDatasetsAndSubDirs.First().Value;

			foreach (var file in lstFiles)
			{
				if (dctDatasetsAndSubDirs.Count > 1 && file.Dataset != currentDataset)
				{
					if (!LookupSubDirFilterByDataset(dctDatasetsAndSubDirs, file, usingDatasetIDs, out currentSubDir))
						continue;
					currentDataset = string.Copy(file.Dataset);
				}

				if (string.IsNullOrEmpty(currentSubDir))
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

					if (string.Equals(file.SubDirPath, currentSubDir, StringComparison.CurrentCultureIgnoreCase))
					{
						lstFilesFiltered.Add(file);
					}

				}
			}

			return lstFilesFiltered;
		}

		protected List<ArchivedFileInfo> FilterFilesBySubDir(List<ArchivedFileInfo> lstFiles, Dictionary<string, string> dctDatasetsAndSubDirs)
		{
			var lstFilesFiltered = new List<ArchivedFileInfo>();
			bool usingDatasetIDs = false;

			if (dctDatasetsAndSubDirs.First().Key.StartsWith(DATASET_ID_TAG))
				usingDatasetIDs = true;

			string currentDataset = string.Empty;
			string currentSubDir = dctDatasetsAndSubDirs.First().Value;

			foreach (var file in lstFiles)
			{
				
				if (dctDatasetsAndSubDirs.Count > 1 && file.Dataset != currentDataset)
				{
					if (!LookupSubDirFilterByDataset(dctDatasetsAndSubDirs, file, usingDatasetIDs, out currentSubDir))
						continue;				
					currentDataset = string.Copy(file.Dataset);
				}

				if (string.IsNullOrEmpty(currentSubDir))
				{
					lstFilesFiltered.Add(file);
					continue;
				}

				if (!string.IsNullOrWhiteSpace(file.SubDirPath))
				{

					List<string> lstRequiredSubDirTree = currentSubDir.Split(new char[] { '/', '\\' }).ToList<string>();

					List<string> lstFileSubDirTree = file.SubDirPath.Split(new char[] { '/', '\\' }).ToList<string>();

					if (lstFileSubDirTree.Count >= lstRequiredSubDirTree.Count)
					{
						int matchCount = 0;
						for (int i = 0; i < lstRequiredSubDirTree.Count; i++)
						{
							if (string.Equals(lstFileSubDirTree[i], lstRequiredSubDirTree[i], StringComparison.CurrentCultureIgnoreCase))
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

		/// <summary>
		/// Searches for files associated with one or more datasets
		/// </summary>
		/// <param name="dctDatasetsAndSubDirs">Keys are dataset names that correspond to the search terms, values are the optional Subdirectory name to filter on for the given dataset; if the keys start with DATASET_ID_TAG then they are DatasetIDs and not dataset names</param>
		/// <param name="recurse">True to recursively find files</param>
		/// <param name="instrumentName">Ignored if dctDatasetsAndSubDirs has more than one entry</param>
		/// <param name="dctSearchTerms">If datsetNameAndSubdir only has one entry, then the dataset name will be ignored (it cannot be blank, but it could be "dummy" or "unknown" or "0", etc.)</param>
		/// <returns></returns>
		protected List<ArchivedFileInfo> FindFilesByDataset(Dictionary<string, string> dctDatasetsAndSubDirs, bool recurse, string instrumentName, List<KeyValuePair<string, string>> dctSearchTerms)
		{

			try
			{
				ResetStatus();

				bool filterOnSubDir = false;

				// Make sure subDir entries have unix-style slashes
				var keys = new List<string>(dctDatasetsAndSubDirs.Keys);
				foreach (string key in keys)
				{
					string subDir = dctDatasetsAndSubDirs[key];

					if (string.IsNullOrEmpty(subDir))
						dctDatasetsAndSubDirs[key] = string.Empty;
					else
					{
						dctDatasetsAndSubDirs[key] = subDir.Replace(@"\", "/");
						filterOnSubDir = true;
					}
				}

				SearchOperator logicalOperator;

				if (dctDatasetsAndSubDirs.Count == 1)
				{
					if (!string.IsNullOrWhiteSpace(instrumentName))
					{
						dctSearchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_INSTRUMENT, instrumentName));
					}
					logicalOperator = SearchOperator.And;
				}
				else
				{
					logicalOperator = SearchOperator.Or;
				}

				List<ArchivedFileInfo> lstFiles = QueryElasticSearch(dctSearchTerms, logicalOperator);

				if (!recurse)
				{
					// Filter the files to remove any not in the "root" folder
					lstFiles = FilterFilesNoRecursion(lstFiles, dctDatasetsAndSubDirs);
				}

				if (filterOnSubDir)
				{
					// Filter on subDir
					lstFiles = FilterFilesBySubDir(lstFiles, dctDatasetsAndSubDirs);					
				}

				return (from item in lstFiles orderby item.PathWithInstrumentAndDatasetWindows select item).ToList();

			}
			catch (Exception ex)
			{
				if (string.IsNullOrWhiteSpace(this.ErrorMessage))
					ReportError("Error in MyEMSLReader.Reader.FindFilesByDataset: " + ex.Message);
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

						Int64 fileID = ReadDictionaryValue(dctFile, "_id", 0);

						var dctFileInfo = RetrieveDictionaryObjectByKey(dctFile, "_source");

						string instrumentName = ReadDictionaryValue(dctFileInfo, "groups.omics.dms.instrument", string.Empty);

						// The transaction ID is incremented every time a group of files is submitted (aka one bundle)
						// All files submitted in the same .tar file will have the same transaction ID
						// If two files have the exact same name and path, the newer one will have a larger transaction ID
						Int64 transID = ReadDictionaryValue(dctFileInfo, "trans", 0);

						string submissionTime = ReadDictionaryValue(dctFileInfo, "stime", string.Empty);
						bool publicFile = ReadDictionaryValue(dctFileInfo, "aged", false);

						string fileName = ReadDictionaryValue(dctFileInfo, "filename", string.Empty);
						Int64 fileSizeBytes = ReadDictionaryValue(dctFileInfo, "size", 0);
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

		protected bool LookupSubDirFilterByDataset(
			Dictionary<string, string> dctDatasetsAndSubDirs, 
			ArchivedFileInfo file, 
			bool usingDatasetIDs, 
			out string subDir)
		{
			bool success = true;

			if (usingDatasetIDs)
			{
				if (!dctDatasetsAndSubDirs.TryGetValue(DATASET_ID_TAG + file.DatasetID, out subDir))
				{
					OnErrorMessage(new MessageEventArgs("File " + file.FileID + " has an unrecognized dateset ID: " + file.DatasetID + "; skipping"));
					success = false;
				}
			}
			else
			{
				if (!dctDatasetsAndSubDirs.TryGetValue(file.Dataset, out subDir))
				{
					OnErrorMessage(new MessageEventArgs("File " + file.FileID + " has an unrecognized dateset name: " + file.Dataset + "; skipping"));
					success = false;
				}
			}

			return success;
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
		/// <param name="logicalOperator">Whether to AND or OR the search terms together</param>
		/// <returns></returns>
		protected List<ArchivedFileInfo> QueryElasticSearch(List<KeyValuePair<string, string>> dctSearchTerms, SearchOperator logicalOperator)
		{

			try
			{

				if (this.MaxFileCount < 1)
					this.MaxFileCount = 1;

				this.LastSearchFileCountMatched = 0;
				this.LastSearchFileCountReturned = 0;

				string xmlString = RunQuery(dctSearchTerms, this.MaxFileCount, logicalOperator);
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
					ReportError("Error in MyEMSLReader.Reader.QueryElasticSearch: " + ex.Message);
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

		internal string RunQuery(List<KeyValuePair<string, string>> dctSearchTerms, int maxFileCount, SearchOperator logicalOperator)
		{
			CookieContainer cookieJar = null;
			return RunQuery(dctSearchTerms, maxFileCount, logicalOperator, ScanMode.SimpleSearch, ref cookieJar);
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
										"query": "groups.omics.dms.instrument:LTQ_4 AND groups.omics.dms.dataset_id:267771"
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

				string operatorString;
				switch (logicalOperator)
				{
					case SearchOperator.And:
						operatorString = " AND ";
						break;
					case SearchOperator.Or:
						operatorString = " OR ";
						break;
					default:
						throw new ArgumentOutOfRangeException("Unrecognized value for logicalOperator: " + logicalOperator.ToString());
				}

				StringBuilder queryTerms = new StringBuilder();
				foreach (var searchTerm in dctSearchTerms)
				{
					if (queryTerms.Length > 0)
						queryTerms.Append(operatorString);

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
