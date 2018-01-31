using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Jayrock.Json.Conversion;
using Pacifica.Core;
using Utilities = Pacifica.Core.Utilities;

namespace MyEMSLReader
{
    /// <summary>
    /// This class contacts MyEMSL to find all of the files associated with the given dataset (by name or ID)
    ///   Optionally filter on Instrument name to guarantee you are finding the desired files
    ///   Optionally filter on Subdirectory name below the dataset folder to limit the search space
    /// Also supports searching by Data Package ID
    /// </summary>
    /// <remarks>Written by Matthew Monroe for PNNL in 2013.</remarks>
    public class Reader : MyEMSLBase
    {
        #region "Constants"

        // Obsolete: private const string QUERY_SPEC_INSTRUMENT = "omics.dms.instrument";

        private const string QUERY_SPEC_DATASET_ID = "omics.dms.dataset_id";

        private const string QUERY_SPEC_DATASET_NAME = "omics.dms.dataset_name";

        private const string QUERY_SPEC_DATA_PACKAGE_ID = "omics.dms.datapackage_id";

        // Unused: private const string QUERY_SPEC_DATA_PACKAGE_ID_ALT = "extended_metadata.gov_pnnl_emsl_dms_datapackage.id";

        // Unused: private const string QUERY_SPEC_FILENAME = "filename";
        // Unused: private const string QUERY_SPEC_EUS_PROPOSAL = "proposals";
        // Unused: private const string QUERY_SPEC_ANALYSIS_TOOL = "extended_metadata.gov_pnnl_emsl_dms_analysisjob.tool.name.untouched";
        // Unused: private const string QUERY_SPEC_DATASET_NAME_ALT = "extended_metadata.gov_pnnl_emsl_dms_dataset.name.untouched";

        private const string DATASET_ID_TAG = "#*#*#_DATASET_ID_";
        private const string DATA_PKG_ID_TAG = "#*#*#_DATA_PKG_ID_";

        private const string DEFAULT_DMS_CONNECTION_STRING = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI";

        #endregion

        #region "Enums"

        internal enum ScanMode
        {
            SimpleSearch = 0,
            ObtainAuthToken = 1,		// Perform a scan, but also obtain an authorization token
            CreateScrollID = 2			// Create a scroll ID
        }

        private enum SearchEntity
        {
            DatasetName = 0,
            DatasetID = 1,
            DataPackageID = 2
        }
        #endregion

        #region "Properties"

        public string DMSConnectionString { get; set; } = DEFAULT_DMS_CONNECTION_STRING;

        /// <summary>
        /// When True, will include all revisions of files that were imported to MyEMSL multiple times
        /// When False, only reports the newest version of a file
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

        /// <summary>
        /// When true, raise a DebugEvent prior to contacting the metadata server
        /// </summary>
        public bool ReportMetadataURLs { get; set; }

        /// <summary>
        /// When true, use Debug Events to show additional information
        /// </summary>
        public bool TraceMode { get; set; }

        private bool mUseTestInstance;

        /// <summary>
        /// When False use https://my.emsl.pnl.gov/myemsl/elasticsearch/simple_items
        /// When True use  https://test0.my.emsl.pnl.gov/myemsl/search/simple/index.shtml
        /// or if UseItemSearch=True use https://dev1.my.emsl.pnl.gov/myemsl/status/index.php/api/item_search
        /// </summary>
        public bool UseTestInstance
        {
            get => mUseTestInstance;
            set
            {
                mUseTestInstance = value;
                if (mPacificaConfig.UseTestInstance != value)
                {
                    mPacificaConfig.UseTestInstance = value;
                }

            }
        }

        #endregion

        #region "Public methods"

        // Constructor
        public Reader()
        {
            MaxFileCount = 5000;
            IncludeAllRevisions = false;
            ThrowErrors = true;

            EasyHttp.MyEMSLOffline += EasyHttp_MyEMSLOffline;

            ResetStatus();
        }

        /// <summary>
        /// Find all files in MyEMSL for one data package (by ID)
        /// </summary>
        /// <param name="dataPkgID">Data Package ID</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDataPackageID(int dataPkgID)
        {
            var subDir = string.Empty;
            return FindFilesByDataPackageID(dataPkgID, subDir, recurse: true);
        }


        /// <summary>
        /// Find all files in MyEMSL for one data package (by ID)
        /// </summary>
        /// <param name="dataPkgID">Data Package ID</param>
        /// <param name="subDir">Subdirectory name to filter on</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDataPackageID(int dataPkgID, string subDir)
        {
            return FindFilesByDataPackageID(dataPkgID, subDir, recurse: true);
        }

        /// <summary>
        /// Find all files in MyEMSL for one data package (by ID)
        /// </summary>
        /// <param name="dataPkgID">Data Package ID</param>
        /// <param name="subDir">Subdirectory name to filter on</param>
        /// <param name="recurse">True to recursively search for files</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDataPackageID(int dataPkgID, string subDir, bool recurse)
        {
            var dctSearchTerms = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(QUERY_SPEC_DATA_PACKAGE_ID, dataPkgID.ToString(CultureInfo.InvariantCulture))
            };

            var dctDatasetsAndSubDirLists = GetSingleItemSortedSetDictionary(DATA_PKG_ID_TAG + dataPkgID, subDir);

            // Do not filter by instrument name when searching for data packages
            var instrumentName = string.Empty;
            return FindFilesByDataset(dctDatasetsAndSubDirLists, recurse, instrumentName, dctSearchTerms);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of data packages (by data package ID)
        /// </summary>
        /// <param name="dctDataPkgIDsAndSubDirs">Keys are data package ID, values are the optional Subdirectory name to filter on for the given data package</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDataPackageID(Dictionary<int, string> dctDataPkgIDsAndSubDirs)
        {
            return FindFilesByDataPackageID(dctDataPkgIDsAndSubDirs, recurse: true);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of data packages (by data package ID)
        /// </summary>
        /// <param name="dctDataPkgIDsAndSubDirs">Keys are data package ID, values are the optional Subdirectory name to filter on for the given data package</param>
        /// <param name="recurse">True to recursively search for files</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDataPackageID(Dictionary<int, string> dctDataPkgIDsAndSubDirs, bool recurse)
        {

            var dctSearchTerms = new List<KeyValuePair<string, string>>();
            var dctDatasetsAndSubDirs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var datasetEntry in dctDataPkgIDsAndSubDirs)
            {
                dctSearchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATA_PACKAGE_ID, datasetEntry.Key.ToString(CultureInfo.InvariantCulture)));
                dctDatasetsAndSubDirs.Add(DATA_PKG_ID_TAG + datasetEntry.Key, datasetEntry.Value);
            }

            var dctDatasetsAndSubDirLists = ConvertDatasetSubDirDictToSubDirListDict(dctDatasetsAndSubDirs);

            // Do not filter by instrument name when searching for data packages
            var instrumentName = string.Empty;
            return FindFilesByDataset(dctDatasetsAndSubDirLists, recurse, instrumentName, dctSearchTerms);
        }

        /// <summary>
        /// Find all files in MyEMSL for one dataset (by dataset ID)
        /// </summary>
        /// <param name="datasetID">Dataset ID</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetID(int datasetID)
        {
            return FindFilesByDatasetID(datasetID, subDir: "", recurse: true, instrumentName: "");
        }

        /// <summary>
        /// Find all files in MyEMSL for one dataset (by dataset ID)
        /// </summary>
        /// <param name="datasetID">Dataset ID</param>
        /// <param name="subDir">Subdirectory name to filter on</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetID(int datasetID, string subDir)
        {
            return FindFilesByDatasetID(datasetID, subDir, recurse: true, instrumentName: "");
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
            var instrumentName = string.Empty;
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
            var dctSearchTerms = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(QUERY_SPEC_DATASET_ID, datasetID.ToString(CultureInfo.InvariantCulture))
            };

            var dctDatasetsAndSubDirLists = GetSingleItemSortedSetDictionary(DATASET_ID_TAG + datasetID, subDir);

            return FindFilesByDataset(dctDatasetsAndSubDirLists, recurse, instrumentName, dctSearchTerms);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of datasets (by dataset ID)
        /// </summary>
        /// <param name="dctDatasetIDsAndSubDirs">Keys are dataset ID, values are the optional Subdirectory name to filter on for the given dataset</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetID(Dictionary<int, string> dctDatasetIDsAndSubDirs)
        {
            return FindFilesByDatasetID(dctDatasetIDsAndSubDirs, recurse: true);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of datasets (by dataset ID)
        /// </summary>
        /// <param name="dctDatasetIDsAndSubDirs">Keys are dataset names, values are the optional Subdirectory name to filter on for the given dataset</param>
        /// <param name="recurse">True to recursively search for files</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetID(Dictionary<int, string> dctDatasetIDsAndSubDirs, bool recurse)
        {

            var dctSearchTerms = new List<KeyValuePair<string, string>>();
            var dctDatasetsAndSubDirs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var datasetEntry in dctDatasetIDsAndSubDirs)
            {
                dctSearchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATASET_ID, datasetEntry.Key.ToString(CultureInfo.InvariantCulture)));
                dctDatasetsAndSubDirs.Add(DATASET_ID_TAG + datasetEntry.Key, datasetEntry.Value);
            }

            var dctDatasetsAndSubDirLists = ConvertDatasetSubDirDictToSubDirListDict(dctDatasetsAndSubDirs);

            var instrumentName = string.Empty;
            return FindFilesByDataset(dctDatasetsAndSubDirLists, recurse, instrumentName, dctSearchTerms);
        }

        /// <summary>
        /// Find all files in MyEMSL for one dataset (by dataset name)
        /// </summary>
        /// <param name="datasetName">Dataset Name</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName)
        {
            return FindFilesByDatasetName(datasetName, subDir: "", recurse: true, instrumentName: "");
        }

        /// <summary>
        /// Find all files in MyEMSL for one dataset (by dataset name)
        /// </summary>
        /// <param name="datasetName">Dataset Name</param>
        /// <param name="subDir">Subdirectory name to filter on</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetName(string datasetName, string subDir)
        {
            return FindFilesByDatasetName(datasetName, subDir, recurse: true, instrumentName: "");
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
            var instrumentName = string.Empty;
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
            if (TraceMode)
                OnDebugEvent("Entering FindFilesByDatasetName");

            var dctSearchTerms = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(QUERY_SPEC_DATASET_NAME, datasetName)
            };

            var dctDatasetsAndSubDirLists = GetSingleItemSortedSetDictionary(datasetName, subDir);

            return FindFilesByDataset(dctDatasetsAndSubDirLists, recurse, instrumentName, dctSearchTerms);
        }


        /// <summary>
        /// Find all files in MyEMSL for a list of datasets (by dataset name)
        /// </summary>
        /// <param name="dctDatasetsAndSubDirs">Keys are dataset name, values are the optional Subdirectory name to filter on for the given dataset (exact match; cannot contain wildcards)</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetName(Dictionary<string, string> dctDatasetsAndSubDirs)
        {
            return FindFilesByDatasetName(dctDatasetsAndSubDirs, recurse: true);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of datasets (by dataset name)
        /// </summary>
        /// <param name="dctDatasetsAndSubDirs">
        /// Keys are dataset names,
        /// values are the optional Subdirectory name to filter on for the given dataset (exact match; cannot contain wildcards)
        /// </param>
        /// <param name="recurse">True to recursively search for files</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetName(
            Dictionary<string, string> dctDatasetsAndSubDirs,
            bool recurse)
        {
            var dctDatasetsAndSubDirLists = ConvertDatasetSubDirDictToSubDirListDict(dctDatasetsAndSubDirs);

            return FindFilesByDatasetName(dctDatasetsAndSubDirLists, recurse);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of datasets (by dataset name)
        /// </summary>
        /// <param name="dctDatasetsAndSubDirLists">
        /// Keys are dataset names,
        /// Values are a list of subdirectory names to filter on for the given dataset
        /// (exact match; cannot contain wildcards; empty list means do not filter by subdirectory name)
        /// </param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetName(
            Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists)
        {
            return FindFilesByDatasetName(dctDatasetsAndSubDirLists, recurse: true);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of datasets (by dataset name)
        /// </summary>
        /// <param name="dctDatasetsAndSubDirLists">
        /// Keys are dataset names,
        /// Values are a list of subdirectory names to filter on for the given dataset
        /// (exact match; cannot contain wildcards; empty list means do not filter by subdirectory name)
        /// </param>
        /// <param name="recurse">True to recursively search for files</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetName(
            Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists,
            bool recurse)
        {

            var dctSearchTerms = new List<KeyValuePair<string, string>>();
            foreach (var dataset in dctDatasetsAndSubDirLists)
            {
                if (string.IsNullOrWhiteSpace(dataset.Key))
                {
                    throw new Exception("Dataset name cannot be empty when finding files by dataset name");
                }
                dctSearchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATASET_NAME, dataset.Key));
            }

            var instrumentName = string.Empty;
            return FindFilesByDataset(dctDatasetsAndSubDirLists, recurse, instrumentName, dctSearchTerms);
        }

        #endregion

        #region "private Methods"

        /// <summary>
        /// Add a new file to the MyEMSL search results
        /// </summary>
        /// <param name="lstResults">MyEMSL search results</param>
        /// <param name="remoteFilePaths">Dictionary where keys are DatasetID_RemoteFilePath and values are the transaction ID for that file</param>
        /// <param name="remoteFile">
        /// Remote file info, where key is relative file path (Windows style paths) and values are file info details
        /// (multiple entries if multiple versions)</param>
        /// <param name="versionToAdd">Specific version of the remote file to add to lstResults</param>
        /// <param name="keepDuplicates">When true, keep duplicate versions of the same file (files with the same relative path, but different hash values)</param>
        private void AddFileToSearchResults(
            IList<ArchivedFileInfo> lstResults,
            IDictionary<string, ArchivedFileInfo> remoteFilePaths,
            KeyValuePair<string, List<ArchivedFileInfo>> remoteFile,
            ArchivedFileInfo versionToAdd,
            bool keepDuplicates)
        {
            var remoteFileKey = versionToAdd.DatasetID + "_" + remoteFile.Key;

            if (remoteFilePaths.TryGetValue(remoteFileKey, out var existingArchiveFile))
            {
                if (keepDuplicates)
                {
                    LastSearchFileCountReturned += 1;
                }
                else
                {
                    if (versionToAdd.TransactionID < existingArchiveFile.TransactionID)
                    {
                        // Duplicate found, and the transaction ID for versionToAdd is older than the transaction ID we're already tracking
                        return;
                    }

                    // Remove the current version stored in lstResults
                    for (var i = 0; i < lstResults.Count; i++)
                    {
                        if (lstResults[i].FileID != existingArchiveFile.FileID)
                            continue;

                        lstResults.RemoveAt(i);
                        break;
                    }
                }

                lstResults.Add(versionToAdd);
                remoteFilePaths[remoteFileKey] = versionToAdd;
            }
            else
            {
                lstResults.Add(versionToAdd);
                remoteFilePaths.Add(remoteFileKey, versionToAdd);
                LastSearchFileCountReturned += 1;
            }

        }

        /// <summary>
        /// Convert a dictionary of dataset names and SubDir names to a dictionary of dataset names and SubDir lists (with one item per list for each dataset)
        /// </summary>
        /// <param name="dctDatasetsAndSubDirs"></param>
        /// <returns>Dictionary where Keys are dataset names, and Values are a list of subdirectory names to filter on for the given dataset</returns>
        private Dictionary<string, SortedSet<string>> ConvertDatasetSubDirDictToSubDirListDict(
            Dictionary<string, string> dctDatasetsAndSubDirs)
        {

            var dctDatasetsAndSubDirLists = new Dictionary<string, SortedSet<string>>();
            foreach (var datasetSubDirCombo in dctDatasetsAndSubDirs)
            {
                ConvertDatasetSubDirAddToDictionary(dctDatasetsAndSubDirLists, datasetSubDirCombo.Key, datasetSubDirCombo.Value);
            }

            return dctDatasetsAndSubDirLists;

        }

        /// <summary>
        /// Add a dataset name / subDir combo to the given tracking dictionary
        /// </summary>
        /// <param name="dctDatasetsAndSubDirLists">
        /// Keys are dataset names,
        /// Values are a list of subdirectory names to filter on for the given dataset
        /// (exact match; cannot contain wildcards; empty list means do not filter by subdirectory name)
        /// </param>
        /// <param name="datasetName">Dataset Name</param>
        /// <param name="subDir">Subdirectory name to filter on (exact match; cannot contain wildcards)</param>
        private void ConvertDatasetSubDirAddToDictionary(
            IDictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists,
            string datasetName,
            string subDir)
        {
            if (string.IsNullOrWhiteSpace(subDir))
                subDir = string.Empty;

            if (dctDatasetsAndSubDirLists.TryGetValue(datasetName, out var subDirsForDataset))
            {
                if (!subDirsForDataset.Contains(subDir))
                {
                    subDirsForDataset.Add(subDir);
                }
            }
            else
            {
                subDirsForDataset = new SortedSet<string>(StringComparer.OrdinalIgnoreCase) {
                    subDir
                };

                dctDatasetsAndSubDirLists.Add(datasetName, subDirsForDataset);
            }

        }

        /// <summary>
        /// Return true if fileVersions has a file with the given hash
        /// </summary>
        /// <param name="fileVersions">List of files in MyEMSL</param>
        /// <param name="fileHash">Sha-1 hash to find</param>
        /// <returns>True if a match is found, otherwise false</returns>
        private bool FileHashExists(IEnumerable<ArchivedFileInfo> fileVersions, string fileHash)
        {
            return (from item in fileVersions where string.Equals(item.Sha1Hash, fileHash) select item).Any();
        }

        private List<ArchivedFileInfo> FilterFilesNoRecursion(
            List<ArchivedFileInfo> lstFiles,
            Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists)
        {
            var lstFilesFiltered = new List<ArchivedFileInfo>();
            var entityType = GetEntityType(dctDatasetsAndSubDirLists);

            var currentDataset = string.Empty;
            var currentSubDirList = dctDatasetsAndSubDirLists.First().Value;

            foreach (var file in lstFiles)
            {
                if (dctDatasetsAndSubDirLists.Count > 1 && file.Dataset != currentDataset)
                {
                    if (!LookupSubDirFilterByDataset(dctDatasetsAndSubDirLists, file, entityType, out currentSubDirList))
                        continue;
                    currentDataset = string.Copy(file.Dataset);
                }

                if (currentSubDirList.Count == 0)
                {
                    // Did not filter by sub directory
                    // Only keep this file if file.SubDirPath is empty
                    if (string.IsNullOrEmpty(file.SubDirPath))
                    {
                        lstFilesFiltered.Add(file);
                    }
                    continue;
                }

                // Filter by any sub directory in currentSubDirList
                // Confirm that this file resides in that sub directory (and not in a sub directory of subDir)

                foreach (var subDir in currentSubDirList)
                {
                    if (string.Equals(file.SubDirPath, subDir, StringComparison.OrdinalIgnoreCase))
                    {
                        lstFilesFiltered.Add(file);
                        break;
                    }
                }

            }

            return lstFilesFiltered;
        }

        private List<ArchivedFileInfo> FilterFilesByDatasetID(
            IEnumerable<ArchivedFileInfo> lstFiles,
            IEnumerable<string> lstDatasetIDTags)
        {
            var lstFilesFiltered = new List<ArchivedFileInfo>();
            var lstDatasetIDsSorted = new SortedSet<int>();

            foreach (var item in lstDatasetIDTags)
            {
                var datasetIDText = item.Replace(DATASET_ID_TAG, string.Empty);

                if (int.TryParse(datasetIDText, out var datasetID))
                {
                    lstDatasetIDsSorted.Add(datasetID);
                }
                else
                {
                    throw new InvalidOperationException("Error in FilterFilesByDatasetID: Search key not in the expected form of " + DATASET_ID_TAG + "123456");
                }

            }

            // Equivalent Linq expression:
            // return lstFiles.Where(file => lstDatasetIDsSorted.Contains(file.DatasetID) || file.DatasetID == 0).ToList();

            foreach (var file in lstFiles)
            {
                if (lstDatasetIDsSorted.Contains(file.DatasetID) || file.DatasetID == 0)
                {
                    lstFilesFiltered.Add(file);
                }
            }

            return lstFilesFiltered;
        }

        private List<ArchivedFileInfo> FilterFilesByDatasetName(
            IEnumerable<ArchivedFileInfo> lstFiles,
            IEnumerable<string> datasetNames)
        {
            var lstFilesFiltered = new List<ArchivedFileInfo>();
            var lstDatasetNamesSorted = new SortedSet<string>(datasetNames, StringComparer.OrdinalIgnoreCase);

            // Equivalent Linq expression:
            // return lstFiles.Where(file => lstDatasetNamesSorted.Contains(file.Dataset)).ToList();

            foreach (var file in lstFiles)
            {
                if (lstDatasetNamesSorted.Contains(file.Dataset))
                {
                    lstFilesFiltered.Add(file);
                }
            }

            return lstFilesFiltered;
        }

        private List<ArchivedFileInfo> FilterFilesBySubDir(
            List<ArchivedFileInfo> lstFiles,
            Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists)
        {
            var lstFilesFiltered = new List<ArchivedFileInfo>();
            var entityType = GetEntityType(dctDatasetsAndSubDirLists);

            var currentDataset = string.Empty;
            var currentSubDirList = dctDatasetsAndSubDirLists.First().Value;

            foreach (var file in lstFiles)
            {

                if (dctDatasetsAndSubDirLists.Count > 1 && file.Dataset != currentDataset)
                {
                    if (!LookupSubDirFilterByDataset(dctDatasetsAndSubDirLists, file, entityType, out currentSubDirList))
                        continue;
                    currentDataset = string.Copy(file.Dataset);
                }

                if (currentSubDirList.Count == 0)
                {
                    lstFilesFiltered.Add(file);
                    continue;
                }

                if (string.IsNullOrWhiteSpace(file.SubDirPath))
                {
                    // File is not in a subdirectory
                    // Only add it if currentSubDirList contains an empty string

                    if (currentSubDirList.Contains(string.Empty))
                    {
                        lstFilesFiltered.Add(file);
                    }
                    continue;
                }

                foreach (var subDir in currentSubDirList)
                {

                    var lstRequiredSubDirTree = subDir.Split('/', '\\').ToList();

                    var lstFileSubDirTree = file.SubDirPath.Split('/', '\\').ToList();

                    if (lstFileSubDirTree.Count >= lstRequiredSubDirTree.Count)
                    {
                        var matchCount = 0;
                        for (var i = 0; i < lstRequiredSubDirTree.Count; i++)
                        {
                            if (string.Equals(lstFileSubDirTree[i], lstRequiredSubDirTree[i], StringComparison.OrdinalIgnoreCase))
                                matchCount++;
                        }

                        if (matchCount == lstRequiredSubDirTree.Count)
                        {
                            lstFilesFiltered.Add(file);
                            break;
                        }
                    }
                }

            }

            return lstFilesFiltered;
        }

        /// <summary>
        /// Searches for files associated with one or more datasets
        /// </summary>
        /// <param name="dctDatasetsAndSubDirLists">
        /// Keys are dataset names (or DATASET_ID_TAG and DatasetID or DATA_PKG_ID_TAG and DataPkgID),
        /// Values are a list of subdirectory names to filter on for the given dataset
        /// (exact match; cannot contain wildcards; empty list means do not filter by subdirectory name)
        /// </param>
        /// <param name="recurse">True to recursively find files</param>
        /// <param name="instrumentName">Ignored if dctDatasetsAndSubDirLists has more than one entry</param>
        /// <param name="dctSearchTerms">
        /// Search terms as key/value pairs
        /// Should support the following, but only actually supports "omics.dms.dataset_id" and omics.dms.dataset_name at present
        ///   omics.dms.dataset_id
        ///   omics.dms.dataset_name
        ///   omics.dms.datapackage_id
        ///
        ///  If dctDatasetsAndSubDirLists only has one entry, the dataset name will be ignored and the value in dctSearchTerms will take precedence
        ///  (it cannot be blank, but it could be "dummy" or "unknown" or "0", etc.)
        /// </param>
        /// <returns>Files that were found</returns>
        /// <remarks>
        /// If the keys in dctDatasetsAndSubDirLists start with DATASET_ID_TAG then they are Dataset IDs and not dataset names
        /// If the keys in dctDatasetsAndSubDirLists start with DATA_PKG_ID_TAG then they are Data Package IDs and not dataset names
        /// </remarks>
        private List<ArchivedFileInfo> FindFilesByDataset(
            Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists,
            bool recurse,
            string instrumentName,
            IEnumerable<KeyValuePair<string, string>> dctSearchTerms)
        {

            if (TraceMode)
                OnDebugEvent("Entering FindFilesByDataset");

            try
            {
                ResetStatus();

                var filterOnSubDir = false;

                var dctDatasetsAndSubDirListsCleaned = new Dictionary<string, SortedSet<string>>(StringComparer.OrdinalIgnoreCase);

                // Make sure subDir entries have unix-style slashes
                foreach (var dataset in dctDatasetsAndSubDirLists)
                {
                    var subDirList = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                    if (dataset.Value != null)
                    {
                        foreach (var subDir in dataset.Value)
                        {
                            if (string.IsNullOrWhiteSpace(subDir))
                            {
                                continue;
                            }

                            var subDirToAdd = subDir.Replace(@"\", "/");
                            if (!subDirList.Contains(subDirToAdd))
                                subDirList.Add(subDirToAdd);

                            filterOnSubDir = true;
                        }
                    }

                    dctDatasetsAndSubDirListsCleaned.Add(dataset.Key, subDirList);
                }

                // Make sure that dctDatasetsAndSubDirListsCleaned does not contain a mix of datasets, dataset IDs, and data package IDs
                ValidateDatasetInfoDictionary(dctDatasetsAndSubDirListsCleaned);

                var lstResults = new List<ArchivedFileInfo>();

                // Keys in this dictionary are DatasetID_RemoteFilePath; values are the transaction ID for that file
                var remoteFilePaths = new Dictionary<string, ArchivedFileInfo>();

                LastSearchFileCountMatched = 0;
                LastSearchFileCountReturned = 0;

                var filterOnInstrument = !string.IsNullOrWhiteSpace(instrumentName);

                foreach (var searchTerm in dctSearchTerms)
                {

                    // Run the query against the Item Search service
                    // Returns a dictionary where keys are relative file paths (Windows style paths) and values are file info details (multiple entries if multiple versions)
                    var lstFilesToAdd = RunItemSearchQuery(searchTerm.Key, searchTerm.Value);

                    foreach (var remoteFile in lstFilesToAdd)
                    {

                        if (filterOnInstrument)
                        {
                            // Skip files that do not match this instrument
                            var fileInstrument = remoteFile.Value.First().Instrument;

                            if (!string.IsNullOrEmpty(fileInstrument) && !string.Equals(fileInstrument, instrumentName, StringComparison.OrdinalIgnoreCase))
                                continue;
                        }

                        if (IncludeAllRevisions)
                        {
                            foreach (var item in remoteFile.Value)
                            {
                                AddFileToSearchResults(lstResults, remoteFilePaths, remoteFile, item, true);
                            }
                        }
                        else
                        {
                            // Select the newest version of the item
                            var newestVersion = (from item in remoteFile.Value orderby item.TransactionID descending select item).First();

                            AddFileToSearchResults(lstResults, remoteFilePaths, remoteFile, newestVersion, false);
                        }

                    }

                }

                // Filter the results
                lstResults = FilterSearchResults(dctDatasetsAndSubDirListsCleaned, recurse, lstResults, filterOnSubDir);

                // Return the results, sorted by folder path and file name
                return (from item in lstResults orderby item.PathWithInstrumentAndDatasetWindows select item).ToList();

            }
            catch (Exception ex)
            {
                if (string.IsNullOrWhiteSpace(ErrorMessage))
                    ReportError("Error in MyEMSLReader.FindFilesByDataset: " + ex.Message, ex);
                else if (ThrowErrors)
                    throw;

                return new List<ArchivedFileInfo>();
            }
        }

        private List<ArchivedFileInfo> FilterSearchResults(
            Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists,
            bool recurse,
            List<ArchivedFileInfo> lstResults,
            bool filterOnSubDir)
        {

            if (dctDatasetsAndSubDirLists.Count > 0)
            {
                if (dctDatasetsAndSubDirLists.First().Key.StartsWith(DATASET_ID_TAG))
                {
                    // Filter the files to remove any that are not an exact match to the dataset ID
                    lstResults = FilterFilesByDatasetID(lstResults, dctDatasetsAndSubDirLists.Keys);
                }
                else if (dctDatasetsAndSubDirLists.First().Key.StartsWith(DATA_PKG_ID_TAG))
                {
                    // Files stored in MyEMSL for data packages do not have the data package ID associated with them (each file has DatasetID = 0)
                    // Thus, we cannot filter them and will instead need to trust the results returned by MyEMSL
                }
                else
                {
                    // Filter the files to remove any that are not an exact match to the dataset names in dctSearchTerms
                    lstResults = FilterFilesByDatasetName(lstResults, dctDatasetsAndSubDirLists.Keys);
                }
            }

            if (!recurse)
            {
                // Filter the files to remove any not in the "root" folder
                lstResults = FilterFilesNoRecursion(lstResults, dctDatasetsAndSubDirLists);
            }

            if (filterOnSubDir)
            {
                // Filter on subDir
                lstResults = FilterFilesBySubDir(lstResults, dctDatasetsAndSubDirLists);
            }

            return lstResults;
        }

        private SearchEntity GetEntityType(Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists)
        {
            var entityType = SearchEntity.DatasetName;

            if (dctDatasetsAndSubDirLists.First().Key.StartsWith(DATASET_ID_TAG))
                entityType = SearchEntity.DatasetID;
            else if (dctDatasetsAndSubDirLists.First().Key.StartsWith(DATA_PKG_ID_TAG))
                entityType = SearchEntity.DataPackageID;

            return entityType;
        }

        private int LookupDatasetIDByName(string datasetName, out string instrument, int retryCount = 2)
        {

            var queryString = string.Format(
                "SELECT ID, Instrument " +
                "FROM V_Dataset_Export " +
                "WHERE Dataset = '{0}'",
                datasetName);

            instrument = string.Empty;

            while (retryCount >= 0)
            {
                try
                {
                    if (TraceMode)
                        OnDebugEvent("Running query: " + queryString);

                    using (var connection = new SqlConnection(DMSConnectionString))
                    {
                        var command = new SqlCommand(queryString, connection);
                        connection.Open();

                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.HasRows && reader.Read())
                            {
                                instrument = GetDbValue(reader, "Instrument", "", out _);
                                var datasetId = GetDbValue(reader, "ID", 0, out _);
                                return datasetId;
                            }

                        }
                    }

                    OnWarningEvent(string.Format("Dataset {0} not found in DMS; connecting to {1}",
                        datasetName, DMSConnectionString));

                    return 0;
                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    var msg = string.Format("Exception looking up dataset name for Dataset {0}: {1}; " +
                                            "ConnectionString: {2}, RetryCount = {3}",
                                            datasetName, ex.Message, DMSConnectionString, retryCount);

                    ReportError(msg);

                    // Delay for 5 seconds before trying again
                    if (retryCount >= 0)
                        System.Threading.Thread.Sleep(5000);
                }

            } // while

            return 0;
        }

        private string LookupDatasetNameByID(int datasetID, out string instrument, int retryCount = 2)
        {

            var queryString = string.Format(
                "SELECT Dataset, Instrument " +
                "FROM V_Dataset_Export WHERE " +
                "ID = {0}",
                datasetID);

            instrument = string.Empty;

            while (retryCount >= 0)
            {
                try
                {

                    using (var connection = new SqlConnection(DMSConnectionString))
                    {
                        var command = new SqlCommand(queryString, connection);
                        connection.Open();

                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.HasRows && reader.Read())
                            {
                                instrument = GetDbValue(reader, "Instrument", "", out _);
                                var datasetName= GetDbValue(reader, "Dataset", string.Empty, out _);
                                return datasetName;
                            }

                        }
                    }

                    return string.Empty;
                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    var msg = string.Format("Exception looking up dataset name for Dataset ID {0}: {1}; " +
                                            "ConnectionString: {2}, RetryCount = {3}",
                                            datasetID, ex.Message, DMSConnectionString, retryCount);

                    ReportError(msg);

                    // Delay for 5 seconds before trying again
                    if (retryCount >= 0)
                        System.Threading.Thread.Sleep(5000);
                }

            } // while

            return string.Empty;
        }

        private bool LookupSubDirFilterByDataset(
            IReadOnlyDictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists,
            ArchivedFileInfo file,
            SearchEntity entityType,
            out SortedSet<string> subDirs)
        {
            var success = true;

            if (entityType == SearchEntity.DatasetID)
            {
                if (!dctDatasetsAndSubDirLists.TryGetValue(DATASET_ID_TAG + file.DatasetID, out subDirs))
                {
                    ReportError("File " + file.FileID + " has an unrecognized dateset ID: " + file.DatasetID +
                        "; skipping (MyEMSLReader.LookupSubDirFilterByDataset)");
                    success = false;
                }
            }
            else if (entityType == SearchEntity.DataPackageID)
            {
                if (!dctDatasetsAndSubDirLists.TryGetValue(DATA_PKG_ID_TAG + file.DataPackageID, out subDirs))
                {
                    ReportError("File " + file.FileID + " has an unrecognized data package ID: " + file.DataPackageID +
                        "; skipping (MyEMSLReader.LookupSubDirFilterByDataset)");
                    success = false;
                }
            }
            else
            {
                if (!dctDatasetsAndSubDirLists.TryGetValue(file.Dataset, out subDirs))
                {
                    ReportError("File " + file.FileID + " has an unrecognized dateset name: " + file.Dataset +
                        "; skipping (MyEMSLReader.LookupSubDirFilterByDataset)");
                    success = false;
                }
            }

            return success;
        }

        protected sealed override void ResetStatus()
        {
            base.ResetStatus();
        }

        /// <summary>
        /// Call the Item Search service to find the matching items
        /// </summary>
        /// <param name="searchKey">Key to search on</param>
        /// <param name="searchValue">Value to match</param>
        /// <param name="timeoutSeconds">Max time (in seconds) to wait for the item search query to finish</param>
        /// <returns>Dictionary where keys are relative file paths (Windows style paths); values are file info details</returns>
        /// <remarks>A given remote file could have multiple hash values if multiple versions of the file have been uploaded</remarks>
        internal Dictionary<string, List<ArchivedFileInfo>> RunItemSearchQuery(
            string searchKey,
            string searchValue,
            int timeoutSeconds = 300)
        {
            const int DUPLICATE_HASH_MESSAGES_TO_LOG = 5;

            if (TraceMode)
                OnDebugEvent("Entering RunItemSearchQuery");

            if (!ValidateCertFile("RunItemSearchQuery"))
            {
                return new Dictionary<string, List<ArchivedFileInfo>>();
            }

            // Keys in this dictionary are relative file paths (Windows style paths); values are file info details
            // A given remote file could have multiple hash values if multiple versions of the file have been uploaded
            var remoteFiles = new Dictionary<string, List<ArchivedFileInfo>>();

            string datasetName;
            int datasetOrDataPackageId;
            string instrument;

            bool checkingDataPackage;

            if (string.Equals(QUERY_SPEC_DATASET_ID, searchKey))
            {

                if (!int.TryParse(searchValue, out datasetOrDataPackageId))
                {
                    ReportError("Search value is not numeric: " + searchValue + "; expecting a dataset ID");
                }

                // Contact DMS to retrieve the dataset name for this dataset ID
                // This is a temporary fix until MyEMSL reports Dataset Name
                datasetName = LookupDatasetNameByID(datasetOrDataPackageId, out instrument);

                checkingDataPackage = false;

                if (TraceMode)
                    OnDebugEvent("Dataset ID " + datasetOrDataPackageId + " is " + datasetName);

            }
            else if (string.Equals(QUERY_SPEC_DATASET_NAME, searchKey))
            {
                datasetName = searchValue;

                // Contact DMS to retrieve the dataset name for this dataset ID
                //
                // This is required because https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_name/
                // only works for datasets added after July 1, 2017; compare/contrast:
                // Results:    https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_name/QC_pp_MCF-7_17_01_B_25JUN17_Frodo_REP-17-06-02
                // Results:    https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/597319
                //   vs.
                // No results: https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_name/QC_pp_MCF-7_17_01_2_16May17_Samwise_REP-17-04-01
                // Results:    https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/586916

                datasetOrDataPackageId = LookupDatasetIDByName(datasetName, out instrument);

                checkingDataPackage = false;

                if (TraceMode)
                    OnDebugEvent("Dataset " + datasetName + " has ID " + datasetOrDataPackageId);

                searchKey = QUERY_SPEC_DATASET_ID;
                searchValue = datasetOrDataPackageId.ToString();
            }
            else if (string.Equals(QUERY_SPEC_DATA_PACKAGE_ID, searchKey))
            {
                if (!int.TryParse(searchValue, out datasetOrDataPackageId))
                {
                    ReportError("Search value is not numeric: " + searchValue + "; expecting a DataPackage ID");
                }

                // Dataset name and Instrument name are blank for data packages
                datasetName = string.Empty;
                instrument = string.Empty;

                checkingDataPackage = true;

                if (TraceMode)
                    OnDebugEvent("Obtaining metadata for DataPackage ID " + datasetOrDataPackageId);
            }
            else
            {
                throw new NotSupportedException("MyEMSL metadata search only supports searching by dataset_id, dataset name, or data_package_id");
            }

            try
            {

                // Example URLs:
                // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/403490
                // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_name/QC_pp_MCF-7_17_01_B_25JUN17_Frodo_REP-17-06-02
                // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.datapackage_id/2819

                // Note that querying by dataset name only works for datasets ingested after July 1, 2017
                // For example, QC_pp_MCF-7_17_01_B_25JUN17_Frodo_REP-17-06-02 (shown above) works,
                // while the following gives no results by name but does give results by Dataset ID
                // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_name/CPTAC_CompRef_P32_TMT11_17_18Jun17_Samwise_REP-17-05-01
                // vs. https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/595858

                // Future:
                // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.experiment/6Apr15

                var metadataUrl = string.Format(mPacificaConfig.MetadataServerUri + "/fileinfo/files_for_keyvalue/{0}/{1}",
                    searchKey, searchValue);

                if (TraceMode || ReportMetadataURLs)
                    OnDebugEvent("Contacting " + metadataUrl);

                // Retrieve a list of files already in MyEMSL for this dataset
                // Run the search in a separate thread so that we can abort the search if it takes too long

                var responseData = new WebResponseData();

                var task = Task.Factory.StartNew(() => SendWebRequest(metadataUrl, responseData));

                var success = task.Wait(timeoutSeconds * 1000);

                if (!success)
                {
                    var msg = string.Format("MyEMSL item search query timed out after {0} seconds", timeoutSeconds);
                    ReportError(msg);
                    return remoteFiles;
                }

                var fileInfoListJSON = responseData.ResponseText;

                if (string.IsNullOrEmpty(fileInfoListJSON))
                {
                    var msg = "No results returned from MyEMSL (MyEMSLReader.RunItemSearchQuery)";
                    ReportError(msg);
                    return remoteFiles;
                }

                if (TraceMode)
                {
                    var previewLength = Math.Min(fileInfoListJSON.Length, 75);
                    OnDebugEvent("Response received, convert to a dictionary: " + fileInfoListJSON.Substring(0, previewLength));
                }

                // Convert the response to a dictionary
                var jsa = (Jayrock.Json.JsonArray)JsonConvert.Import(fileInfoListJSON);
                var remoteFileInfoList = Utilities.JsonArrayToDictionaryList(jsa);

                var duplicateHashCount = 0;

                // Note that two files in the same directory could have the same hash value (but different names),
                // so we cannot simply compare file hashes

                foreach (var fileObj in remoteFileInfoList)
                {
                    var fileName = Utilities.GetDictionaryValue(fileObj, "name");
                    var fileId = Utilities.GetDictionaryValue(fileObj, "_id", 0);
                    var fileHash = Utilities.GetDictionaryValue(fileObj, "hashsum");
                    var subFolder = Utilities.GetDictionaryValue(fileObj, "subdir");

                    // Windows style path
                    var relativeFilePath = Path.Combine(subFolder, fileName);

                    if (remoteFiles.TryGetValue(relativeFilePath, out var fileVersions))
                    {
                        // Make sure that fileVersions doesn't already have a version of this file with this specific Sha-1 hash
                        // This is a rare occurrence and should only happen due to metadata ingest logic issues

                        if (FileHashExists(fileVersions, fileHash))
                        {
                            if (string.Equals(fileHash, "none", StringComparison.InvariantCultureIgnoreCase))
                            {
                                // Do not log a warning; just silently ignore it
                                // Example of a dataset with hash values of "None" is test dataset SWT_LCQData_300
                                // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/54007
                                continue;
                            }

                            duplicateHashCount++;
                            if (duplicateHashCount <= DUPLICATE_HASH_MESSAGES_TO_LOG)
                            {
                                // This warning is logged as a debug event since it's not a critical error
                                OnDebugEvent(string.Format(
                                    "Remote file listing reports the same file with the same hash more than once;\n" +
                                    "  ignoring duplicate hash {0} for {1}", fileHash, relativeFilePath));
                            }
                            continue;
                        }
                    }
                    else
                    {
                        // Add the file to fileVersions
                        fileVersions = new List<ArchivedFileInfo>();
                        remoteFiles.Add(relativeFilePath, fileVersions);
                    }

                    var remoteFileInfo = new ArchivedFileInfo(datasetName, fileName, subFolder, fileId)
                    {
                        DatasetYearQuarter = string.Empty,
                        FileSizeBytes = Utilities.GetDictionaryValue(fileObj, "size", 0),
                        Hash = fileHash,
                        HashType = Utilities.GetDictionaryValue(fileObj, "hashtype"),
                        Instrument = instrument,
                        SubmissionTime = Utilities.GetDictionaryValue(fileObj, "created"),
                        TransactionID = Utilities.GetDictionaryValue(fileObj, "transaction_id", 0)
                    };

                    if (checkingDataPackage)
                    {
                        remoteFileInfo.DatasetID = 0;
                        remoteFileInfo.DataPackageID = datasetOrDataPackageId;
                    }
                    else
                    {
                        remoteFileInfo.DatasetID = datasetOrDataPackageId;
                        remoteFileInfo.DataPackageID = 0;
                    }

                    var creationTime = Utilities.GetDictionaryValue(fileObj, "ctime");
                    var lastWriteTime = Utilities.GetDictionaryValue(fileObj, "mtime");

                    remoteFileInfo.UpdateSourceFileTimes(creationTime, lastWriteTime);

                    fileVersions.Add(remoteFileInfo);

                }

                if (duplicateHashCount > DUPLICATE_HASH_MESSAGES_TO_LOG)
                {
                    OnDebugEvent(string.Format("Duplicate hash value found for {0} files in MyEMSL", duplicateHashCount));
                }

                return remoteFiles;

            }
            catch (Exception ex)
            {
                ReportError("Error in MyEMSLReader.RunItemSearchQuery: " + ex.Message, ex);
                return remoteFiles;
            }

        }

        private void SendWebRequest(string metadataUrl, WebResponseData responseData)
        {
            var response = EasyHttp.Send(mPacificaConfig, metadataUrl, out var responseStatusCode);
            responseData.ResponseText = response;
            responseData.ResponseStatusCode = responseStatusCode;
        }

        /// <summary>
        /// Examines the keys in dctDatasetsAndSubDirLists to make sure that they are not a mix of datasets, dataset IDs, and data package IDs
        /// </summary>
        /// <param name="dctDatasetsAndSubDirLists">Dictionary to examine</param>
        private static void ValidateDatasetInfoDictionary(Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists)
        {

            var tagCountDatasetID = 0;
            var tagCountDataPkgID = 0;
            var tagCountDatasetName = 0;

            foreach (var item in dctDatasetsAndSubDirLists.Keys)
            {
                if (item.StartsWith(DATASET_ID_TAG))
                {
                    tagCountDatasetID++;
                }
                else if (item.StartsWith(DATA_PKG_ID_TAG))
                {
                    tagCountDataPkgID++;
                }
                else
                {
                    tagCountDatasetName++;
                }
            }

            if (tagCountDataPkgID > 0 && tagCountDatasetID > 0)
            {
                throw new Exception("Coding error: dctDatasetsAndSubDirLists has both data package ID and dataset ID entries");
            }

            if (tagCountDataPkgID > 0 && tagCountDatasetName > 0)
            {
                throw new Exception("Coding error: dctDatasetsAndSubDirLists has both data package ID and dataset name entries");
            }

            if (tagCountDatasetID > 0 && tagCountDatasetName > 0)
            {
                throw new Exception("Coding error: dctDatasetsAndSubDirLists has both dataset ID and dataset name entries");
            }
        }

        #endregion

    }

}
