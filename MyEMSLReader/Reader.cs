using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using Pacifica.Core;
using Pacifica.Json;
using PRISMDatabaseUtils;

namespace MyEMSLReader
{
    /// <summary>
    /// This class contacts MyEMSL to find the files associated with the given dataset (by name or ID)
    /// Optionally filter on Instrument name to guarantee you are finding the desired files
    /// Optionally filter on Subdirectory name below the dataset directory to limit the search space
    /// Also supports searching by Data Package ID
    /// </summary>
    /// <remarks>Written by Matthew Monroe for PNNL in 2013.</remarks>
    public class Reader : MyEMSLBase
    {
        // Ignore Spelling: ctime, Dirs, DMS, fileinfo, Frodo, hashsum, hashtype, Json, keyvalue, Linq, mtime, subdir, wildcards

        // ReSharper disable UseCollectionExpression

        private const string QUERY_SPEC_DATASET_ID = "omics.dms.dataset_id";

        private const string QUERY_SPEC_DATASET_NAME = "omics.dms.dataset_name";

        private const string QUERY_SPEC_DATA_PACKAGE_ID = "omics.dms.datapackage_id";

        private const string DATASET_ID_TAG = "#*#*#_DATASET_ID_";
        private const string DATA_PKG_ID_TAG = "#*#*#_DATA_PKG_ID_";

        /// <summary>
        /// Default DMS connection string
        /// </summary>
        public const string DEFAULT_DMS_CONNECTION_STRING = "Host=prismdb2.emsl.pnl.gov;Database=dms;Username=dmsreader;Application Name=MyEMSLReader";

        private enum SearchEntity
        {
            DatasetName = 0,
            DatasetID = 1,
            DataPackageID = 2
        }

        /// <summary>
        /// DMS Connection string
        /// </summary>
        public string DMSConnectionString { get; set; } = DEFAULT_DMS_CONNECTION_STRING;

        /// <summary>
        /// When True, include all revisions of files that were imported to MyEMSL multiple times
        /// When False, only reports the newest version of a file
        /// </summary>
        /// <remarks>Default is False</remarks>
        public bool IncludeAllRevisions
        {
            get;
            set;
        }

        /// <summary>
        /// File count returned by the most recent search
        /// </summary>
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

        /// <summary>
        /// Constructor
        /// </summary>
        public Reader()
        {
            MaxFileCount = 5000;
            IncludeAllRevisions = false;
            ThrowErrors = true;

            EasyHttp.MyEMSLOffline += EasyHttp_MyEMSLOffline;
            EasyHttp.ErrorEvent += OnErrorEvent;

            ResetStatus();
        }

        /// <summary>
        /// Find all files in MyEMSL for one data package (by ID)
        /// </summary>
        /// <param name="dataPkgID">Data Package ID</param>
        /// <returns>List of matched files</returns>
        // ReSharper disable once UnusedMember.Global
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
            var searchTerms = new List<KeyValuePair<string, string>>
            {
                new(QUERY_SPEC_DATA_PACKAGE_ID, dataPkgID.ToString(CultureInfo.InvariantCulture))
            };

            var datasetsAndSubDirLists = GetSingleItemSortedSetDictionary(DATA_PKG_ID_TAG + dataPkgID, subDir);

            // Do not filter by instrument name when searching for data packages
            var instrumentName = string.Empty;
            return FindFilesByDataset(datasetsAndSubDirLists, recurse, instrumentName, searchTerms);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of data packages (by data package ID)
        /// </summary>
        /// <param name="dataPkgIDsAndSubDirs">Keys are data package ID, values are the optional Subdirectory name to filter on for the given data package</param>
        /// <returns>List of matched files</returns>
        // ReSharper disable once UnusedMember.Global
        public List<ArchivedFileInfo> FindFilesByDataPackageID(Dictionary<int, string> dataPkgIDsAndSubDirs)
        {
            return FindFilesByDataPackageID(dataPkgIDsAndSubDirs, recurse: true);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of data packages (by data package ID)
        /// </summary>
        /// <param name="dataPkgIDsAndSubDirs">Keys are data package ID, values are the optional Subdirectory name to filter on for the given data package</param>
        /// <param name="recurse">True to recursively search for files</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDataPackageID(Dictionary<int, string> dataPkgIDsAndSubDirs, bool recurse)
        {
            var searchTerms = new List<KeyValuePair<string, string>>();
            var datasetsAndSubDirs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var datasetEntry in dataPkgIDsAndSubDirs)
            {
                searchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATA_PACKAGE_ID, datasetEntry.Key.ToString(CultureInfo.InvariantCulture)));
                datasetsAndSubDirs.Add(DATA_PKG_ID_TAG + datasetEntry.Key, datasetEntry.Value);
            }

            var datasetsAndSubDirLists = ConvertDatasetSubDirDictToSubDirListDict(datasetsAndSubDirs);

            // Do not filter by instrument name when searching for data packages
            var instrumentName = string.Empty;
            return FindFilesByDataset(datasetsAndSubDirLists, recurse, instrumentName, searchTerms);
        }

        /// <summary>
        /// Find all files in MyEMSL for one dataset (by dataset ID)
        /// </summary>
        /// <param name="datasetID">Dataset ID</param>
        /// <returns>List of matched files</returns>
        // ReSharper disable once UnusedMember.Global
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
        // ReSharper disable once UnusedMember.Global
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
        // ReSharper disable once UnusedMember.Global
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
            var searchTerms = new List<KeyValuePair<string, string>>
            {
                new(QUERY_SPEC_DATASET_ID, datasetID.ToString(CultureInfo.InvariantCulture))
            };

            var datasetsAndSubDirLists = GetSingleItemSortedSetDictionary(DATASET_ID_TAG + datasetID, subDir);

            return FindFilesByDataset(datasetsAndSubDirLists, recurse, instrumentName, searchTerms);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of datasets (by dataset ID)
        /// </summary>
        /// <param name="datasetIDsAndSubDirs">Keys are dataset ID, values are the optional Subdirectory name to filter on for the given dataset</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetID(Dictionary<int, string> datasetIDsAndSubDirs)
        {
            return FindFilesByDatasetID(datasetIDsAndSubDirs, recurse: true);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of datasets (by dataset ID)
        /// </summary>
        /// <param name="datasetIDsAndSubDirs">Keys are dataset names, values are the optional Subdirectory name to filter on for the given dataset</param>
        /// <param name="recurse">True to recursively search for files</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetID(Dictionary<int, string> datasetIDsAndSubDirs, bool recurse)
        {
            var searchTerms = new List<KeyValuePair<string, string>>();
            var datasetsAndSubDirs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var datasetEntry in datasetIDsAndSubDirs)
            {
                searchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATASET_ID, datasetEntry.Key.ToString(CultureInfo.InvariantCulture)));
                datasetsAndSubDirs.Add(DATASET_ID_TAG + datasetEntry.Key, datasetEntry.Value);
            }

            var datasetsAndSubDirLists = ConvertDatasetSubDirDictToSubDirListDict(datasetsAndSubDirs);

            var instrumentName = string.Empty;
            return FindFilesByDataset(datasetsAndSubDirLists, recurse, instrumentName, searchTerms);
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
        // ReSharper disable once UnusedMember.Global
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
            {
                OnDebugEvent("Entering FindFilesByDatasetName");
            }

            var searchTerms = new List<KeyValuePair<string, string>>
            {
                new(QUERY_SPEC_DATASET_NAME, datasetName)
            };

            var datasetsAndSubDirLists = GetSingleItemSortedSetDictionary(datasetName, subDir);

            return FindFilesByDataset(datasetsAndSubDirLists, recurse, instrumentName, searchTerms);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of datasets (by dataset name)
        /// </summary>
        /// <param name="datasetsAndSubDirs">Keys are dataset name, values are the optional Subdirectory name to filter on for the given dataset (exact match; cannot contain wildcards)</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetName(Dictionary<string, string> datasetsAndSubDirs)
        {
            return FindFilesByDatasetName(datasetsAndSubDirs, recurse: true);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of datasets (by dataset name)
        /// </summary>
        /// <param name="datasetsAndSubDirs">
        /// Keys are dataset names,
        /// values are the optional Subdirectory name to filter on for the given dataset (exact match; cannot contain wildcards)
        /// </param>
        /// <param name="recurse">True to recursively search for files</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetName(
            Dictionary<string, string> datasetsAndSubDirs,
            bool recurse)
        {
            var datasetsAndSubDirLists = ConvertDatasetSubDirDictToSubDirListDict(datasetsAndSubDirs);

            return FindFilesByDatasetName(datasetsAndSubDirLists, recurse);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of datasets (by dataset name)
        /// </summary>
        /// <param name="datasetsAndSubDirLists">
        /// Keys are dataset names,
        /// Values are a list of subdirectory names to filter on for the given dataset
        /// (exact match; cannot contain wildcards; empty list means do not filter by subdirectory name)
        /// </param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetName(
            Dictionary<string, SortedSet<string>> datasetsAndSubDirLists)
        {
            return FindFilesByDatasetName(datasetsAndSubDirLists, recurse: true);
        }

        /// <summary>
        /// Find all files in MyEMSL for a list of datasets (by dataset name)
        /// </summary>
        /// <param name="datasetsAndSubDirLists">
        /// Keys are dataset names,
        /// Values are a list of subdirectory names to filter on for the given dataset
        /// (exact match; cannot contain wildcards; empty list means do not filter by subdirectory name)
        /// </param>
        /// <param name="recurse">True to recursively search for files</param>
        /// <returns>List of matched files</returns>
        public List<ArchivedFileInfo> FindFilesByDatasetName(
            Dictionary<string, SortedSet<string>> datasetsAndSubDirLists,
            bool recurse)
        {
            var searchTerms = new List<KeyValuePair<string, string>>();

            foreach (var dataset in datasetsAndSubDirLists)
            {
                if (string.IsNullOrWhiteSpace(dataset.Key))
                {
                    throw new Exception("Dataset name cannot be empty when finding files by dataset name");
                }
                searchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATASET_NAME, dataset.Key));
            }

            var instrumentName = string.Empty;
            return FindFilesByDataset(datasetsAndSubDirLists, recurse, instrumentName, searchTerms);
        }

        /// <summary>
        /// Add a new file to the MyEMSL search results
        /// </summary>
        /// <param name="searchResults">MyEMSL search results</param>
        /// <param name="remoteFilePaths">Dictionary where keys are DatasetID_RemoteFilePath and values are the transaction ID for that file</param>
        /// <param name="remoteFile">
        /// Remote file info, where key is relative file path (Windows style paths) and values are file info details
        /// (multiple entries if multiple versions)</param>
        /// <param name="versionToAdd">Specific version of the remote file to add to searchResults</param>
        /// <param name="keepDuplicates">When true, keep duplicate versions of the same file (files with the same relative path, but different hash values)</param>
        private void AddFileToSearchResults(
            IList<ArchivedFileInfo> searchResults,
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
                    LastSearchFileCountReturned++;
                }
                else
                {
                    if (versionToAdd.TransactionID < existingArchiveFile.TransactionID)
                    {
                        // Duplicate found, and the transaction ID for versionToAdd is older than the transaction ID we're already tracking
                        return;
                    }

                    // Remove the current version stored in searchResults
                    for (var i = 0; i < searchResults.Count; i++)
                    {
                        if (searchResults[i].FileID != existingArchiveFile.FileID)
                        {
                            continue;
                        }

                        searchResults.RemoveAt(i);
                        break;
                    }
                }

                searchResults.Add(versionToAdd);
                remoteFilePaths[remoteFileKey] = versionToAdd;
            }
            else
            {
                searchResults.Add(versionToAdd);
                remoteFilePaths.Add(remoteFileKey, versionToAdd);
                LastSearchFileCountReturned++;
            }
        }

        /// <summary>
        /// Convert a dictionary of dataset names and SubDir names to a dictionary of dataset names and SubDir lists (with one item per list for each dataset)
        /// </summary>
        /// <param name="datasetsAndSubDirs"></param>
        /// <returns>Dictionary where Keys are dataset names, and Values are a list of subdirectory names to filter on for the given dataset</returns>
        private static Dictionary<string, SortedSet<string>> ConvertDatasetSubDirDictToSubDirListDict(
            Dictionary<string, string> datasetsAndSubDirs)
        {
            var datasetsAndSubDirLists = new Dictionary<string, SortedSet<string>>();

            foreach (var datasetSubDirCombo in datasetsAndSubDirs)
            {
                ConvertDatasetSubDirAddToDictionary(datasetsAndSubDirLists, datasetSubDirCombo.Key, datasetSubDirCombo.Value);
            }

            return datasetsAndSubDirLists;
        }

        /// <summary>
        /// Add a dataset name / subDir combo to the given tracking dictionary
        /// </summary>
        /// <param name="datasetsAndSubDirLists">
        /// Keys are dataset names,
        /// Values are a list of subdirectory names to filter on for the given dataset
        /// (exact match; cannot contain wildcards; empty list means do not filter by subdirectory name)
        /// </param>
        /// <param name="datasetName">Dataset Name</param>
        /// <param name="subDir">Subdirectory name to filter on (exact match; cannot contain wildcards)</param>
            IDictionary<string, SortedSet<string>> datasetsAndSubDirLists,
        private static void ConvertDatasetSubDirAddToDictionary(
            string datasetName,
            string subDir)
        {
            if (string.IsNullOrWhiteSpace(subDir))
            {
                subDir = string.Empty;
            }

            if (datasetsAndSubDirLists.TryGetValue(datasetName, out var subDirsForDataset))
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

                datasetsAndSubDirLists.Add(datasetName, subDirsForDataset);
            }
        }

        /// <summary>
        /// Return true if fileVersions has a file with the given hash
        /// </summary>
        /// <param name="fileVersions">List of files in MyEMSL</param>
        /// <param name="fileHash">SHA-1 hash to find</param>
        /// <returns>True if a match is found, otherwise false</returns>
        private static bool FileHashExists(IEnumerable<ArchivedFileInfo> fileVersions, string fileHash)
        {
            return (from item in fileVersions where string.Equals(item.Sha1Hash, fileHash) select item).Any();
        }

        private List<ArchivedFileInfo> FilterFilesNoRecursion(
            IEnumerable<ArchivedFileInfo> searchResults,
            Dictionary<string, SortedSet<string>> datasetsAndSubDirLists)
        {
            var filteredSearchResults = new List<ArchivedFileInfo>();
            var entityType = GetEntityType(datasetsAndSubDirLists);

            var currentDataset = string.Empty;
            var currentSubDirList = datasetsAndSubDirLists.First().Value;

            foreach (var file in searchResults)
            {
                if (datasetsAndSubDirLists.Count > 1 && file.Dataset != currentDataset)
                {
                    if (!LookupSubDirFilterByDataset(datasetsAndSubDirLists, file, entityType, out currentSubDirList))
                    {
                        continue;
                    }

                    currentDataset = file.Dataset;
                }

                if (currentSubDirList.Count == 0)
                {
                    // Did not filter by sub directory
                    // Only keep this file if file.SubDirPath is empty
                    if (string.IsNullOrEmpty(file.SubDirPath))
                    {
                        filteredSearchResults.Add(file);
                    }
                    continue;
                }

                // Filter by any sub directory in currentSubDirList
                // Confirm that this file resides in that sub directory (and not in a sub directory of subDir)

                foreach (var subDir in currentSubDirList)
                {
                    if (string.Equals(file.SubDirPath, subDir, StringComparison.OrdinalIgnoreCase))
                    {
                        filteredSearchResults.Add(file);
                        break;
                    }
                }
            }

            return filteredSearchResults;
        }

        private static List<ArchivedFileInfo> FilterFilesByDatasetID(
            IEnumerable<ArchivedFileInfo> searchResults,
            IEnumerable<string> datasetIdTags)
        {
            var filteredSearchResults = new List<ArchivedFileInfo>();
            var sortedDatasetIDs = new SortedSet<int>();

            foreach (var item in datasetIdTags)
            {
                var datasetIDText = item.Replace(DATASET_ID_TAG, string.Empty);

                if (int.TryParse(datasetIDText, out var datasetID))
                {
                    sortedDatasetIDs.Add(datasetID);
                }
                else
                {
                    throw new InvalidOperationException("Error in FilterFilesByDatasetID: Search key not in the expected form of " + DATASET_ID_TAG + "123456");
                }
            }

            // Equivalent Linq expression:
            // return searchResults.Where(file => sortedDatasetIDs.Contains(file.DatasetID) || file.DatasetID == 0).ToList();

            foreach (var file in searchResults)
            {
                if (sortedDatasetIDs.Contains(file.DatasetID) || file.DatasetID == 0)
                {
                    filteredSearchResults.Add(file);
                }
            }

            return filteredSearchResults;
        }

        private static List<ArchivedFileInfo> FilterFilesByDatasetName(
            IEnumerable<ArchivedFileInfo> searchResults,
            IEnumerable<string> datasetNames)
        {
            var filteredSearchResults = new List<ArchivedFileInfo>();
            var datasetNamesSorted = new SortedSet<string>(datasetNames, StringComparer.OrdinalIgnoreCase);

            // Equivalent Linq expression:
            // return searchResults.Where(file => datasetNamesSorted.Contains(file.Dataset)).ToList();

            foreach (var file in searchResults)
            {
                if (datasetNamesSorted.Contains(file.Dataset))
                {
                    filteredSearchResults.Add(file);
                }
            }

            return filteredSearchResults;
        }

        private List<ArchivedFileInfo> FilterFilesBySubDir(
            IEnumerable<ArchivedFileInfo> searchResults,
            Dictionary<string, SortedSet<string>> datasetsAndSubDirs)
        {
            var filteredSearchResults = new List<ArchivedFileInfo>();
            var entityType = GetEntityType(datasetsAndSubDirs);

            var currentDataset = string.Empty;
            var currentSubDirList = datasetsAndSubDirs.First().Value;

            foreach (var file in searchResults)
            {
                if (datasetsAndSubDirs.Count > 1 && file.Dataset != currentDataset)
                {
                    if (!LookupSubDirFilterByDataset(datasetsAndSubDirs, file, entityType, out currentSubDirList))
                    {
                        continue;
                    }

                    currentDataset = file.Dataset;
                }

                if (currentSubDirList.Count == 0)
                {
                    filteredSearchResults.Add(file);
                    continue;
                }

                if (string.IsNullOrWhiteSpace(file.SubDirPath))
                {
                    // File is not in a subdirectory
                    // Only add it if currentSubDirList contains an empty string

                    if (currentSubDirList.Contains(string.Empty))
                    {
                        filteredSearchResults.Add(file);
                    }
                    continue;
                }

                foreach (var subDir in currentSubDirList)
                {
                    var requiredSubDirTree = subDir.Split('/', '\\').ToList();

                    var fileSubDirTree = file.SubDirPath.Split('/', '\\').ToList();

                    if (fileSubDirTree.Count >= requiredSubDirTree.Count)
                    {
                        var matchCount = 0;

                        for (var i = 0; i < requiredSubDirTree.Count; i++)
                        {
                            if (string.Equals(fileSubDirTree[i], requiredSubDirTree[i], StringComparison.OrdinalIgnoreCase))
                            {
                                matchCount++;
                            }
                        }

                        if (matchCount == requiredSubDirTree.Count)
                        {
                            filteredSearchResults.Add(file);
                            break;
                        }
                    }
                }
            }

            return filteredSearchResults;
        }

        /// <summary>
        /// Searches for files associated with one or more datasets
        /// </summary>
        /// <remarks>
        /// If the keys in datasetsAndSubDirLists start with DATASET_ID_TAG then they are Dataset IDs and not dataset names
        /// If the keys in datasetsAndSubDirLists start with DATA_PKG_ID_TAG then they are Data Package IDs and not dataset names
        /// </remarks>
        /// <param name="datasetsAndSubDirs">
        /// Keys are dataset names (or DATASET_ID_TAG and DatasetID or DATA_PKG_ID_TAG and DataPkgID),
        /// Values are a list of subdirectory names to filter on for the given dataset
        /// (exact match; cannot contain wildcards; empty list means do not filter by subdirectory name)
        /// </param>
        /// <param name="recurse">True to recursively find files</param>
        /// <param name="instrumentName">Ignored if datasetsAndSubDirLists has more than one entry</param>
        /// <param name="searchTerms">
        /// Search terms as key/value pairs
        /// Should support the following, but only actually supports "omics.dms.dataset_id" and omics.dms.dataset_name at present
        ///   omics.dms.dataset_id
        ///   omics.dms.dataset_name
        ///   omics.dms.datapackage_id
        ///
        ///  If datasetsAndSubDirLists only has one entry, the dataset name will be ignored and the value in searchTerms will take precedence
        ///  (it cannot be blank, but it could be "dummy" or "unknown" or "0", etc.)
        /// </param>
        /// <returns>Files that were found</returns>
        private List<ArchivedFileInfo> FindFilesByDataset(
            Dictionary<string, SortedSet<string>> datasetsAndSubDirs,
            bool recurse,
            string instrumentName,
            IEnumerable<KeyValuePair<string, string>> searchTerms)
        {
            if (TraceMode)
            {
                OnDebugEvent("Entering FindFilesByDataset");
            }

            try
            {
                ResetStatus();

                var filterOnSubDir = false;

                var datasetsAndSubdirectoriesCleaned = new Dictionary<string, SortedSet<string>>(StringComparer.OrdinalIgnoreCase);

                // Make sure subDir entries have Linux-style slashes
                foreach (var dataset in datasetsAndSubDirs)
                {
                    var subDirList = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                    if (dataset.Value != null)
                    {
                        var includeBaseDirectory = false;

                        foreach (var subDir in dataset.Value)
                        {
                            if (string.IsNullOrWhiteSpace(subDir))
                            {
                                includeBaseDirectory = true;
                                continue;
                            }

                            var subDirToAdd = subDir.Replace(@"\", "/");

                            if (!subDirList.Contains(subDirToAdd))
                            {
                                subDirList.Add(subDirToAdd);
                            }

                            filterOnSubDir = true;
                        }

                        if (filterOnSubDir && includeBaseDirectory)
                        {
                            subDirList.Add(string.Empty);
                        }
                    }

                    datasetsAndSubdirectoriesCleaned.Add(dataset.Key, subDirList);
                }

                // Make sure that datasetsAndSubdirectoriesCleaned does not contain a mix of datasets, dataset IDs, and data package IDs
                ValidateDatasetInfoDictionary(datasetsAndSubdirectoriesCleaned);

                var searchResults = new List<ArchivedFileInfo>();

                // Keys in this dictionary are DatasetID_RemoteFilePath; values are the transaction ID for that file
                var remoteFilePaths = new Dictionary<string, ArchivedFileInfo>();

                LastSearchFileCountReturned = 0;

                var filterOnInstrument = !string.IsNullOrWhiteSpace(instrumentName);

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(DMSConnectionString, "MyEMSLReader");

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse);
                RegisterEvents(dbTools);

                foreach (var searchTerm in searchTerms)
                {
                    // Run the query against the Item Search service
                    // Returns a dictionary where keys are relative file paths (Windows style paths) and values are file info details (multiple entries if multiple versions)
                    var filesToAdd = RunItemSearchQuery(dbTools, searchTerm.Key, searchTerm.Value);

                    foreach (var remoteFile in filesToAdd)
                    {
                        if (remoteFile.Value.Count == 0)
                            continue;

                        if (filterOnInstrument)
                        {
                            // Skip files that do not match this instrument
                            var fileInstrument = remoteFile.Value[0].Instrument;

                            if (!string.IsNullOrEmpty(fileInstrument) && !string.Equals(fileInstrument, instrumentName, StringComparison.OrdinalIgnoreCase))
                            {
                                continue;
                            }
                        }

                        if (IncludeAllRevisions)
                        {
                            foreach (var item in remoteFile.Value)
                            {
                                AddFileToSearchResults(searchResults, remoteFilePaths, remoteFile, item, true);
                            }
                        }
                        else
                        {
                            // Select the newest version of the item
                            var newestVersion = (from item in remoteFile.Value orderby item.TransactionID descending select item).First();

                            AddFileToSearchResults(searchResults, remoteFilePaths, remoteFile, newestVersion, false);
                        }
                    }
                }

                // Filter the results
                var filteredSearchResults = FilterSearchResults(datasetsAndSubdirectoriesCleaned, recurse, searchResults, filterOnSubDir);

                var sortedResults = new List<ArchivedFileInfo>();

                // Sort the results by directory path and file name, but assure that files in subdirectories are listed second
                sortedResults.AddRange(from item in filteredSearchResults
                                       where !item.RelativePathWindows.Contains("\\")
                                       orderby item.RelativePathWindows
                                       select item);

                // Then show the files that are in a subdirectory
                sortedResults.AddRange(from item in filteredSearchResults
                                       where item.RelativePathWindows.Contains("\\")
                                       orderby item.RelativePathWindows
                                       select item);

                return sortedResults;
            }
            catch (Exception ex)
            {
                if (string.IsNullOrWhiteSpace(ErrorMessage))
                {
                    ReportError("Error in MyEMSLReader.FindFilesByDataset", ex);
                }
                else if (ThrowErrors)
                {
                    throw;
                }

                return new List<ArchivedFileInfo>();
            }
        }

        private List<ArchivedFileInfo> FilterSearchResults(
            Dictionary<string, SortedSet<string>> datasetsAndSubDirs,
            bool recurse,
            IEnumerable<ArchivedFileInfo> searchResults,
            bool filterOnSubDir)
        {
            List<ArchivedFileInfo> filteredSearchResults;

            if (datasetsAndSubDirs.Count > 0)
            {
                if (datasetsAndSubDirs.First().Key.StartsWith(DATASET_ID_TAG))
                {
                    // Filter the files to remove any that are not an exact match to the dataset ID
                    filteredSearchResults = FilterFilesByDatasetID(searchResults, datasetsAndSubDirs.Keys);
                }
                else if (datasetsAndSubDirs.First().Key.StartsWith(DATA_PKG_ID_TAG))
                {
                    // Files stored in MyEMSL for data packages do not have the data package ID associated with them (each file has DatasetID = 0)
                    // Thus, we cannot filter them and will instead need to trust the results returned by MyEMSL
                    filteredSearchResults = new List<ArchivedFileInfo>();
                    filteredSearchResults.AddRange(searchResults);
                }
                else
                {
                    // Filter the files to remove any that are not an exact match to the dataset names in searchTerms
                    filteredSearchResults = FilterFilesByDatasetName(searchResults, datasetsAndSubDirs.Keys);
                }
            }
            else
            {
                filteredSearchResults = new List<ArchivedFileInfo>();
                filteredSearchResults.AddRange(searchResults);
            }

            if (!recurse)
            {
                // Filter the files to remove any not in the "root" directory
                filteredSearchResults = FilterFilesNoRecursion(filteredSearchResults, datasetsAndSubDirs);
            }

            if (filterOnSubDir)
            {
                // Filter on subDir
                filteredSearchResults = FilterFilesBySubDir(filteredSearchResults, datasetsAndSubDirs);
            }

            return filteredSearchResults;
        }

        private static SearchEntity GetEntityType(Dictionary<string, SortedSet<string>> datasetsAndSubDirLists)
        {
            if (datasetsAndSubDirLists.First().Key.StartsWith(DATASET_ID_TAG))
            {
                return SearchEntity.DatasetID;
            }

            if (datasetsAndSubDirLists.First().Key.StartsWith(DATA_PKG_ID_TAG))
            {
                return SearchEntity.DataPackageID;
            }

            return SearchEntity.DatasetName;
        }

        private int LookupDatasetIDByName(IDBTools dbTools, string datasetName, out string instrument, int retryCount = 2)
        {
            var queryString = string.Format(
                "SELECT id, instrument " +
                "FROM v_dataset_export " +
                "WHERE dataset = '{0}'",
                datasetName);

            instrument = string.Empty;

            var success = dbTools.GetQueryResultsDataTable(queryString, out var table, retryCount);

            if (success)
            {
                foreach (DataRow row in table.Rows)
                {
                    instrument = row["instrument"].CastDBVal(string.Empty);
                    return row["id"].CastDBVal(0);
                }
            }

            OnWarningEvent("Dataset {0} not found in DMS (using {1})", datasetName, dbTools.ConnectStr);

            return 0;
        }

        private static string LookupDatasetNameByID(IDBTools dbTools, int datasetID, out string instrument, int retryCount = 2)
        {
            var queryString = string.Format(
                "SELECT dataset, instrument " +
                "FROM v_dataset_export " +
                "WHERE id = {0}",
                datasetID);

            instrument = string.Empty;

            var success = dbTools.GetQueryResultsDataTable(queryString, out var table, retryCount);

            if (success)
            {
                foreach (DataRow row in table.Rows)
                {
                    instrument = row["instrument"].CastDBVal(string.Empty);
                    return row["dataset"].CastDBVal(string.Empty);
                }
            }

            return string.Empty;
        }

        private bool LookupSubDirFilterByDataset(
            IReadOnlyDictionary<string, SortedSet<string>> datasetsAndSubDirLists,
            ArchivedFileInfo file,
            SearchEntity entityType,
            out SortedSet<string> subDirs)
        {
            var success = true;

            if (entityType == SearchEntity.DatasetID)
            {
                if (!datasetsAndSubDirLists.TryGetValue(DATASET_ID_TAG + file.DatasetID, out subDirs))
                {
                    ReportError(
                        "File " + file.FileID + " has an unrecognized dataset ID: " + file.DatasetID +
                        "; skipping (MyEMSLReader.LookupSubDirFilterByDataset)");
                    success = false;
                }
            }
            else if (entityType == SearchEntity.DataPackageID)
            {
                if (!datasetsAndSubDirLists.TryGetValue(DATA_PKG_ID_TAG + file.DataPackageID, out subDirs))
                {
                    ReportError(
                        "File " + file.FileID + " has an unrecognized data package ID: " + file.DataPackageID +
                        "; skipping (MyEMSLReader.LookupSubDirFilterByDataset)");
                    success = false;
                }
            }
            else
            {
                if (!datasetsAndSubDirLists.TryGetValue(file.Dataset, out subDirs))
                {
                    ReportError(
                        "File " + file.FileID + " has an unrecognized dataset name: " + file.Dataset +
                        "; skipping (MyEMSLReader.LookupSubDirFilterByDataset)");
                    success = false;
                }
            }

            return success;
        }

        /// <summary>
        /// Clear the error message tracked by the base class
        /// </summary>
        protected sealed override void ResetStatus()
        {
            base.ResetStatus();
        }

        /// <summary>
        /// Call the Item Search service to find the matching items
        /// </summary>
        /// <remarks>A given remote file could have multiple hash values if multiple versions of the file have been uploaded</remarks>
        /// <param name="dbTools">Key to search on</param>
        /// <param name="searchKey">Key to search on</param>
        /// <param name="searchValue">Value to match</param>
        /// <param name="timeoutSeconds">Max time (in seconds) to wait for the item search query to finish</param>
        /// <returns>Dictionary where keys are relative file paths (Windows style paths); values are file info details</returns>
        internal Dictionary<string, List<ArchivedFileInfo>> RunItemSearchQuery(
            IDBTools dbTools,
            string searchKey,
            string searchValue,
            int timeoutSeconds = 300)
        {
            const int DUPLICATE_HASH_MESSAGES_TO_LOG = 5;

            if (TraceMode)
            {
                OnDebugEvent("Entering RunItemSearchQuery");
            }

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
                    ReportWarning("Dataset ID should be an integer, not: " + searchValue);
                    return remoteFiles;
                }

                if (datasetOrDataPackageId == 0)
                {
                    ReportWarning("Dataset ID is 0, not contacting MyEMSL");
                    return remoteFiles;
                }

                // Contact DMS to retrieve the dataset name for this dataset ID
                datasetName = LookupDatasetNameByID(dbTools, datasetOrDataPackageId, out instrument);

                checkingDataPackage = false;

                if (TraceMode)
                {
                    OnDebugEvent("Dataset ID " + datasetOrDataPackageId + " is " + datasetName);
                }
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

                datasetOrDataPackageId = LookupDatasetIDByName(dbTools, datasetName, out instrument);

                if (datasetOrDataPackageId == 0)
                {
                    ReportWarning("Dataset ID is 0, not contacting MyEMSL");
                    return remoteFiles;
                }

                checkingDataPackage = false;

                if (TraceMode)
                {
                    OnDebugEvent("Dataset " + datasetName + " has ID " + datasetOrDataPackageId);
                }

                searchKey = QUERY_SPEC_DATASET_ID;
                searchValue = datasetOrDataPackageId.ToString();
            }
            else if (string.Equals(QUERY_SPEC_DATA_PACKAGE_ID, searchKey))
            {
                if (!int.TryParse(searchValue, out datasetOrDataPackageId))
                {
                    ReportWarning("Data Package ID should be an integer, not: " + searchValue);
                    return remoteFiles;
                }

                if (datasetOrDataPackageId == 0)
                {
                    ReportWarning("Data Package ID is 0, not contacting MyEMSL");
                    return remoteFiles;
                }

                // Dataset name and Instrument name are blank for data packages
                datasetName = string.Empty;
                instrument = string.Empty;

                checkingDataPackage = true;

                if (TraceMode)
                {
                    OnDebugEvent("Obtaining metadata for DataPackage ID " + datasetOrDataPackageId);
                }
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
                {
                    OnDebugEvent("Contacting " + metadataUrl);
                }

                // Retrieve a list of files already in MyEMSL for this dataset

                var responseData = EasyHttp.SendViaThreadStart(mPacificaConfig, metadataUrl, out var responseStatusCode);

                if (responseStatusCode == HttpStatusCode.RequestTimeout)
                {
                    var errMsg = string.Format("MyEMSL item search query timed out after {0} seconds", timeoutSeconds);
                    ReportError(errMsg);
                    return remoteFiles;
                }

                var fileInfoListJSON = responseData;

                if (string.IsNullOrEmpty(fileInfoListJSON))
                {
                    const string errMsg = "No results returned from MyEMSL (MyEMSLReader.RunItemSearchQuery)";
                    ReportError(errMsg, false);
                    return remoteFiles;
                }

                if (TraceMode)
                {
                    var previewLength = Math.Min(fileInfoListJSON.Length, 75);
                    OnDebugEvent("Response received, convert to a dictionary: " + fileInfoListJSON.Substring(0, previewLength));
                }

                // Convert the response to a dictionary
                var remoteFileInfoList = JsonTools.JsonToFileList(fileInfoListJSON, metadataUrl, "MyEMSLReader.RunItemSearchQuery", out var jsonError);

                if (remoteFileInfoList is null)
                {
                    ReportError(jsonError);
                    return remoteFiles;
                }

                var duplicateHashCount = 0;

                // Data uploaded to Pacifica (aka MyEMSL) between '2023-10-31 22:13:00' and '2023-12-19 22:00:00' was inadvertently stored as zero-byte files
                // Any files with a Submission time in that range, are assumed to be invalid (Submission time comes from Created time in the metadata)
                var corruptDataStartTime = new DateTime(2023, 10, 31, 22, 13, 00);
                var corruptDataEndTime = new DateTime(2023, 12, 19, 22, 00,00);

                // Note that two files in the same directory could have the same hash value (but different names),
                // so we cannot simply compare file hashes

                foreach (var fileObj in remoteFileInfoList)
                {
                    var fileName = fileObj.Filename;
                    var fileHash = fileObj.HashSum;
                    var subdirectory = fileObj.SubDir;

                    // Windows style path
                    var relativeFilePath = Path.Combine(subdirectory, fileName);

                    if (remoteFiles.TryGetValue(relativeFilePath, out var fileVersions))
                    {
                        // Make sure that fileVersions doesn't already have a version of this file with this specific SHA-1 hash
                        // This can happen if the same subdirectory is pushed into MyEMSL twice, and the file modification times have changed
                        // but the file contents have not changed

                        if (FileHashExists(fileVersions, fileHash))
                        {
                            if (string.Equals(fileHash, "none", StringComparison.OrdinalIgnoreCase))
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
                                OnDebugEvent("Remote file listing reports the same file with the same hash more than once;\n" +
                                             "  ignoring duplicate hash {0} for {1}", fileHash, relativeFilePath);
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

                    if (fileObj.Created >= corruptDataStartTime && fileObj.Created <= corruptDataEndTime)
                    {
                        // Ignore this file
                        continue;
                    }

                    var remoteFileInfo = new ArchivedFileInfo(datasetName, fileObj)
                    {
                        DatasetYearQuarter = string.Empty,
                        Instrument = instrument
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

                    fileVersions.Add(remoteFileInfo);
                }

                if (duplicateHashCount > DUPLICATE_HASH_MESSAGES_TO_LOG)
                {
                    OnDebugEvent("Duplicate hash value found for {0} files in MyEMSL", duplicateHashCount);
                }

                return remoteFiles;
            }
            catch (Exception ex)
            {
                ReportError("Error in MyEMSLReader.RunItemSearchQuery", ex);
                return remoteFiles;
            }
        }

        /// <summary>
        /// Examines the keys in datasetsAndSubDirLists to make sure that they are not a mix of datasets, dataset IDs, and data package IDs
        /// </summary>
        /// <param name="datasetsAndSubDirLists">Dictionary to examine</param>
        private static void ValidateDatasetInfoDictionary(Dictionary<string, SortedSet<string>> datasetsAndSubDirLists)
        {
            var tagCountDatasetID = 0;
            var tagCountDataPkgID = 0;
            var tagCountDatasetName = 0;

            foreach (var item in datasetsAndSubDirLists.Keys)
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
                throw new Exception("Coding error: datasetsAndSubDirLists has both data package ID and dataset ID entries");
            }

            if (tagCountDataPkgID > 0 && tagCountDatasetName > 0)
            {
                throw new Exception("Coding error: datasetsAndSubDirLists has both data package ID and dataset name entries");
            }

            if (tagCountDatasetID > 0 && tagCountDatasetName > 0)
            {
                throw new Exception("Coding error: datasetsAndSubDirLists has both dataset ID and dataset name entries");
            }
        }
    }
}
