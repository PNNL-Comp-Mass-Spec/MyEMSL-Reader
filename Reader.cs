using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Jayrock.Json.Conversion;
using Pacifica.Core;

namespace MyEMSLReader
{
    /// <summary>
    /// This class contacts MyEMSL to find all of the files associated with the given dataset (by name or ID)
    ///   Optionally filter on Instrument name to guarantee you are finding the desired files
    ///   Optionally filter on Subdirectory name below the dataset folder to limit the search space
    /// Also supports searching by Data Package ID
    /// </summary>
    /// <remarks>Written by Matthew Monroe for PNNL in August 2013.  Last updated February 2014</remarks>
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
        public Int64 LastSearchFileCountMatched
        {
            get;
            private set;
        }

        public int LastSearchFileCountReturned
        {
            // ReSharper disable once UnusedAutoPropertyAccessor.Global
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

        public bool TraceMode { get; set; }

        private bool mUseTestInstance;

        /// <summary>
        /// When False use https://my.emsl.pnl.gov/myemsl/elasticsearch/simple_items
        /// When True use  https://test0.my.emsl.pnl.gov/myemsl/search/simple/index.shtml
        /// or if UseItemSearch=True use https://dev1.my.emsl.pnl.gov/myemsl/status/index.php/api/item_search
        /// </summary>
        public bool UseTestInstance
        {
            get
            {
                return mUseTestInstance;
            }
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
        // ReSharper disable once UnusedMember.Global
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

            SortedSet<string> subDirsForDataset;

            if (dctDatasetsAndSubDirLists.TryGetValue(datasetName, out subDirsForDataset))
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
                int datasetID;
                if (int.TryParse(datasetIDText, out datasetID))
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

                var lstFiles = new List<ArchivedFileInfo>();

                // Keys in this file are remote file paths; values are the transaction ID for that file
                var remoteFilePaths = new Dictionary<string, ArchivedFileInfo>();

                LastSearchFileCountMatched = 0;
                LastSearchFileCountReturned = 0;

                var filterOnInstrument = !string.IsNullOrWhiteSpace(instrumentName);

                foreach (var searchTerm in dctSearchTerms)
                {

                    // Run the query against the Item Search service
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

                        // Select the newest version of the item
                        var newestVersion = (from item in remoteFile.Value orderby item.TransactionID descending select item).First();

                        if (remoteFilePaths.TryGetValue(remoteFile.Key, out var existingArchiveFile))
                        {
                            if (newestVersion.TransactionID > existingArchiveFile.TransactionID)
                            {
                                lstFiles.Remove(existingArchiveFile);
                                lstFiles.Add(newestVersion);
                                remoteFilePaths[remoteFile.Key] = newestVersion;
                            }
                        }
                        else
                        {
                            lstFiles.Add(newestVersion);
                            remoteFilePaths.Add(remoteFile.Key, newestVersion);
                            LastSearchFileCountReturned += 1;
                        }

                    }

                }

                // Filter the results
                lstFiles = FilterSearchResults(dctDatasetsAndSubDirListsCleaned, recurse, lstFiles, filterOnSubDir);

                // Return the results, sorted by folder path and file name
                return (from item in lstFiles orderby item.PathWithInstrumentAndDatasetWindows select item).ToList();

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
            List<ArchivedFileInfo> lstFiles,
            bool filterOnSubDir)
        {

            if (dctDatasetsAndSubDirLists.Count > 0)
            {
                if (dctDatasetsAndSubDirLists.First().Key.StartsWith(DATASET_ID_TAG))
                {
                    // Filter the files to remove any that are not an exact match to the dataset ID
                    lstFiles = FilterFilesByDatasetID(lstFiles, dctDatasetsAndSubDirLists.Keys);
                }
                else if (dctDatasetsAndSubDirLists.First().Key.StartsWith(DATA_PKG_ID_TAG))
                {
                    // Files stored in MyEMSL for data packages do not have the data package ID associated with them (each file has DatasetID = 0)
                    // Thus, we cannot filter them and will instead need to trust the results returned by MyEMSL
                }
                else
                {
                    // Filter the files to remove any that are not an exact match to the dataset names in dctSearchTerms
                    lstFiles = FilterFilesByDatasetName(lstFiles, dctDatasetsAndSubDirLists.Keys);
                }
            }

            if (!recurse)
            {
                // Filter the files to remove any not in the "root" folder
                lstFiles = FilterFilesNoRecursion(lstFiles, dctDatasetsAndSubDirLists);
            }

            if (filterOnSubDir)
            {
                // Filter on subDir
                lstFiles = FilterFilesBySubDir(lstFiles, dctDatasetsAndSubDirLists);
            }

            return lstFiles;
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

        internal List<ArchivedFileInfo> ParseElasticSearchResults(string responseData, out string authToken)
        {
            if (string.IsNullOrWhiteSpace(responseData))
            {
                authToken = string.Empty;
                return new List<ArchivedFileInfo>();
            }

            // Convert the results to a Json dictionary object
            var dctResults = Utilities.JsonToObject(responseData);
            return ParseElasticSearchResults(dctResults, out authToken);
        }

        /// <summary>
        /// Parse the search results from Elastic Search to generate a list of files
        /// </summary>
        /// <param name="dctResults"></param>
        /// <param name="authToken">Output parameter: Authorization token (if available)</param>
        /// <returns></returns>
        internal List<ArchivedFileInfo> ParseElasticSearchResults(Dictionary<string, object> dctResults, out string authToken)
        {

            authToken = string.Empty;

            try
            {

                // Check for an error
                string errorMessage;
                if (!ValidSearchResults(dctResults, out errorMessage))
                {
                    ReportError("Error parsing search results (MyEMSLReader.ParseElasticSearchResults): " + errorMessage);
                    return new List<ArchivedFileInfo>();
                }

                // Read the EMSL Authorization Token (will be present if we used ScanMode.ObtainAuthToken
                authToken = ReadDictionaryValue(dctResults, "myemsl_auth_token", string.Empty);

                // Extract out the hits section
                var dctHits = RetrieveDictionaryObjectByKey(dctResults, "hits");

                LastSearchFileCountMatched = ReadDictionaryValue(dctHits, "total", -1);
                if (LastSearchFileCountMatched < 0)
                {
                    ReportError("Hits section did not have the 'total' entry (MyEMSLReader.ParseElasticSearchResults)");
                    return new List<ArchivedFileInfo>();
                }

                if (LastSearchFileCountMatched == 0)
                    return new List<ArchivedFileInfo>();

                // Extract out the list of hits
                var dctFiles = RetrieveDictionaryListByKey(dctHits, "hits");

                // Enumerate the files in dctFiles
                var lstFiles = new List<ArchivedFileInfo>();

                // This dictionary keeps track of the newest version of each unique file
                // Keys in this dictionary are relative file paths while indices are the item index in lstFiles
                var dctMostRecentVersionPointers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                var itemIndex = 0;
                foreach (var dctFile in dctFiles)
                {
                    try
                    {

                        var fileID = ReadDictionaryValue(dctFile, "_id", 0);

                        var dctFileInfo = RetrieveDictionaryObjectByKey(dctFile, "_source");

                        var instrumentName = ReadDictionaryValue(dctFileInfo, "groups.omics.dms.instrument", string.Empty);

                        // The transaction ID is incremented every time a group of files is submitted (aka one bundle)
                        // All files submitted in the same .tar file will have the same transaction ID
                        // If two files have the exact same name and path, the newer one will have a larger transaction ID
                        var transID = ReadDictionaryValue(dctFileInfo, "trans", 0);

                        var submissionTime = ReadDictionaryValue(dctFileInfo, "stime", string.Empty);
                        var publicFile = ReadDictionaryValue(dctFileInfo, "aged", false);

                        var fileName = ReadDictionaryValue(dctFileInfo, "filename", string.Empty);

                        var fileSizeBytes = ReadDictionaryValue(dctFileInfo, "size", 0);
                        var datasetName = ReadDictionaryValue(dctFileInfo, "groups.omics.dms.dataset", string.Empty);

                        var datasetID = (int)ReadDictionaryValue(dctFileInfo, "groups.omics.dms.dataset_id", 0);
                        var dataPackageID = (int)ReadDictionaryValue(dctFileInfo, "groups.omics.dms.datapackage_id", 0);

                        var datasetYearQuarter = ReadDictionaryValue(dctFileInfo, "groups.omics.dms.date_code", string.Empty);
                        var subDir = ReadDictionaryValue(dctFileInfo, "subdir", string.Empty);

                        var dctHashInfo = RetrieveDictionaryObjectByKey(dctFileInfo, "hash");

                        var fileSha1Hash = ReadDictionaryValue(dctHashInfo, "sha1", string.Empty);

                        var archiveFile = new ArchivedFileInfo(datasetName, fileName, subDir, fileID, instrumentName, datasetYearQuarter, dctFile)
                        {
                            Hash = fileSha1Hash,
                            HashType = ArchivedFileInfo.SHA1,
                            FileSizeBytes = fileSizeBytes,
                            TransactionID = transID,
                            SubmissionTime = submissionTime,
                            IsPublicFile = publicFile,
                            DatasetID = datasetID,
                            DataPackageID = dataPackageID
                        };

                        int existingIndex;

                        if (dctMostRecentVersionPointers.TryGetValue(archiveFile.PathWithDataset, out existingIndex))
                        {
                            // Found a duplicate file
                            if (IncludeAllRevisions)
                            {
                                // Including all revisions of a file
                                lstFiles.Add(archiveFile);

                                if (lstFiles[existingIndex].TransactionID < archiveFile.TransactionID)
                                {
                                    // This file is newer; update dctUniqueFiles
                                    dctMostRecentVersionPointers[archiveFile.PathWithDataset] = lstFiles.Count - 1;
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
                            dctMostRecentVersionPointers.Add(archiveFile.PathWithDataset, lstFiles.Count - 1);
                        }


                    }
                    catch (Exception ex)
                    {
                        OnWarningEvent("Error parsing item " + itemIndex + "; will be skipped: " + ex.Message);
                    }

                    itemIndex++;
                }

                return lstFiles;

            }
            catch (Exception ex)
            {
                ReportError("Error in MyEMSLReader.ParseElasticSearchResults: " + ex.Message, ex);
                return new List<ArchivedFileInfo>();
            }

        }

        /// <summary>
        /// Parse the search results from the Item Search service to generate a list of files
        /// </summary>
        /// <param name="dctResults"></param>
        /// <returns></returns>
        internal List<ArchivedFileInfo> ParseItemSearchResults(Dictionary<string, object> dctResults)
        {

            try
            {

                // Check for an error
                string errorMessage;
                if (!ValidSearchResults(dctResults, out errorMessage))
                {
                    ReportError("Error parsing search results (MyEMSLReader.ParseItemSearchResults): " + errorMessage);
                    return new List<ArchivedFileInfo>();
                }

                LastSearchFileCountMatched = 0;

                if (!dctResults.ContainsKey("transactions"))
                {
                    if (dctResults.ContainsKey("results_count"))
                    {
                        LastSearchFileCountMatched = ReadDictionaryValue(dctResults, "results_count", 0);
                        if (LastSearchFileCountMatched > 0)
                        {
                            ReportError("results_count is non-zero but the 'transactions' section was not found; Item Search service error (MyEMSLReader.ParseItemSearchResults)");
                        }
                    }
                    else
                    {
                        ReportError("Response did not contain a 'results_count' or a 'transactions' section; Item Search service error (MyEMSLReader.ParseItemSearchResults)");
                    }

                    return new List<ArchivedFileInfo>();
                }

                // Extract out the transactions section
                var dctTransactions = RetrieveDictionaryObjectByKey(dctResults, "transactions");

                if (dctTransactions.Count == 0)
                {
                    return new List<ArchivedFileInfo>();
                }

                // Enumerate the entries in dctTransactions
                var lstFiles = new List<ArchivedFileInfo>();

                // This dictionary keeps track of the newest version of each unique file
                // Keys in this dictionary are relative file paths while indices are the item index in lstFiles
                var dctMostRecentVersionPointers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                var tranIndex = 0;
                foreach (var transaction in dctTransactions)
                {
                    try
                    {
                        // The transaction ID is incremented every time a group of files is submitted (aka one bundle)
                        // All files submitted in the same .tar file will have the same transaction ID
                        // If two files have the exact same name and path, the newer one will have a larger transaction ID
                        long transID;

                        if (!long.TryParse(transaction.Key, out transID))
                        {
                            Console.WriteLine("Non-numeric transID for transaction " + tranIndex + "; skipping transaction: " + transaction.Key);
                            tranIndex++;
                            continue;
                        }

                        var dctTransactionInfo = (Dictionary<string, object>)transaction.Value;

                        var dctMetadata = RetrieveDictionaryObjectByKey(dctTransactionInfo, "metadata");

                        var dctFiles = RetrieveDictionaryObjectByKey(dctTransactionInfo, "file_info");

                        var itemIndex = 0;
                        foreach (var fileEntry in dctFiles)
                        {
                            try
                            {

                                int fileID;
                                if (!int.TryParse(fileEntry.Key, out fileID))
                                {
                                    Console.WriteLine("Non-numeric fileID for item " + itemIndex + " in transaction " + tranIndex + "; skipping item: " + fileEntry.Key);
                                    itemIndex++;
                                    continue;
                                }

                                var dctFileInfo = (Dictionary<string, object>)fileEntry.Value;

                                var instrumentName = ReadDictionaryValue(dctMetadata, "omics.dms.instrument", string.Empty);
                                if (string.IsNullOrEmpty(instrumentName))
                                {
                                    foreach (var metadataKey in dctMetadata.Keys)
                                    {
                                        if (metadataKey.StartsWith("Instrument"))
                                        {
                                            instrumentName = ReadDictionaryValue(dctMetadata, metadataKey, string.Empty);
                                            break;
                                        }
                                    }
                                }

                                var submissionTime = ReadDictionaryValue(dctFileInfo, "submit_time", string.Empty);
                                var publicFile = ReadDictionaryValue(dctFileInfo, "aged", false);

                                var fileName = ReadDictionaryValue(dctFileInfo, "filename", string.Empty);

                                var fileSizeBytes = ReadDictionaryValue(dctFileInfo, "size_in_bytes", 0);
                                var datasetName = ReadDictionaryValue(dctMetadata, "omics.dms.dataset", string.Empty);

                                var datasetID = (int)ReadDictionaryValue(dctMetadata, "omics.dms.dataset_id", 0);
                                var dataPackageID = (int)ReadDictionaryValue(dctMetadata, "omics.dms.datapackage_id", 0);

                                var datasetYearQuarter = ReadDictionaryValue(dctMetadata, "omics.dms.date_code", string.Empty);
                                var subDir = ReadDictionaryValue(dctFileInfo, "full_path", string.Empty);

                                if (!string.IsNullOrEmpty(subDir))
                                {
                                    var lastSlashIndex = subDir.LastIndexOf('/');
                                    if (lastSlashIndex <= 0)
                                    {
                                        subDir = string.Empty;
                                    }
                                    else
                                    {
                                        subDir = subDir.Substring(0, lastSlashIndex);
                                    }
                                }

                                var fileSha1Hash = ReadDictionaryValue(dctFileInfo, "hashsum", string.Empty);

                                var archiveFile = new ArchivedFileInfo(datasetName, fileName, subDir, fileID, instrumentName, datasetYearQuarter, dctMetadata)
                                {
                                    Hash = fileSha1Hash,
                                    HashType = ArchivedFileInfo.SHA1,
                                    FileSizeBytes = fileSizeBytes,
                                    TransactionID = transID,
                                    SubmissionTime = submissionTime,
                                    IsPublicFile = publicFile,
                                    DatasetID = datasetID,
                                    DataPackageID = dataPackageID
                                };

                                int existingIndex;

                                if (dctMostRecentVersionPointers.TryGetValue(archiveFile.PathWithDataset, out existingIndex))
                                {
                                    // Found a duplicate file
                                    if (IncludeAllRevisions)
                                    {
                                        // Including all revisions of a file
                                        lstFiles.Add(archiveFile);

                                        if (lstFiles[existingIndex].TransactionID < archiveFile.TransactionID)
                                        {
                                            // This file is newer; update dctUniqueFiles
                                            dctMostRecentVersionPointers[archiveFile.PathWithDataset] = lstFiles.Count - 1;
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
                                    dctMostRecentVersionPointers.Add(archiveFile.PathWithDataset, lstFiles.Count - 1);
                                }

                            }
                            catch (Exception ex)
                            {
                                OnWarningEvent("Error parsing item " + itemIndex + " in transaction " + tranIndex + "; skipping item: " + ex.Message);
                            }

                            itemIndex++;
                            LastSearchFileCountMatched++;
                        }

                    }
                    catch (Exception ex)
                    {
                        OnWarningEvent("Error parsing transaction " + tranIndex + "; skipping transaction: " + ex.Message);
                    }

                    tranIndex++;
                }

                return lstFiles;

            }
            catch (Exception ex)
            {
                ReportError("Error in MyEMSLReader.ParseItemSearchResults: " + ex.Message, ex);
                return new List<ArchivedFileInfo>();
            }

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

        private string PossiblyQuoteString(string text)
        {
            if (text.Contains(" "))
                return '"' + text + '"';

            return text;
        }

        /// <summary>
        /// Use Elastic Search to find files in MyEMSL matching the given search times
        /// </summary>
        /// <param name="dctSearchTerms">Query search terms</param>
        /// <param name="logicalOperator">Whether to AND or OR the search terms together</param>
        /// <returns>List of files</returns>
        [Obsolete("Deprecated in June 2017")]
        private List<ArchivedFileInfo> QueryElasticSearch(
            List<KeyValuePair<string, string>> dctSearchTerms,
            SearchOperator logicalOperator)
        {

            try
            {

                if (MaxFileCount < 1)
                    MaxFileCount = 1;

                LastSearchFileCountMatched = 0;
                LastSearchFileCountReturned = 0;

                var dctResults = RunElasticSearchQuery(dctSearchTerms, MaxFileCount, logicalOperator);

                if (dctResults == null || dctResults.Count == 0)
                {
                    if (string.IsNullOrWhiteSpace(ErrorMessage))
                        ReportError("MyEMSLReader.RunElasticSearchQuery returned an empty xml result");
                    LastSearchFileCountReturned = 0;
                    return new List<ArchivedFileInfo>();
                }

                // Parse the results (note that authToken will always be empty because we used ScanMode.SimpleSearch)
                string authToken;
                var lstFiles = ParseElasticSearchResults(dctResults, out authToken);

                LastSearchFileCountReturned = lstFiles.Count;

                return lstFiles;
            }
            catch (Exception ex)
            {
                if (string.IsNullOrWhiteSpace(ErrorMessage))
                    ReportError("Error in MyEMSLReader.QueryElasticSearch: " + ex.Message);
                else if (ThrowErrors)
                    throw;

                return new List<ArchivedFileInfo>();
            }

        }

        protected sealed override void ResetStatus()
        {
            base.ResetStatus();
        }

        internal Dictionary<string, object> RunElasticSearchQuery(List<KeyValuePair<string, string>> dctSearchTerms, int maxFileCount, SearchOperator logicalOperator)
        {
            CookieContainer cookieJar = null;
            return RunElasticSearchQuery(dctSearchTerms, maxFileCount, logicalOperator, ScanMode.SimpleSearch, ref cookieJar);
        }

        /// <summary>
        /// Run an elastic search query against MyEMSL
        /// </summary>
        /// <param name="dctSearchTerms">Dictionary of terms to search for</param>
        /// <param name="maxFileCount">Maximum number of hits to return</param>
        /// <param name="logicalOperator">Whether to AND or OR the search terms together</param>
        /// <param name="scanMode">Scan mode (0=Simple Search, 1=Search, but obtain a myemsl_auth_token, 2 = Create a ScrollID)</param>
        /// <param name="cookieJar"></param>
        /// <returns>Json results dictionary</returns>
        /// <remarks>Be sure to call Logout() when scanMode is not 0 </remarks>
        [Obsolete("Obsolete in June 2017; use RunItemSearchQuery instead")]
        internal Dictionary<string, object> RunElasticSearchQuery(
            List<KeyValuePair<string, string>> dctSearchTerms,
            int maxFileCount,
            SearchOperator logicalOperator,
            ScanMode scanMode,
            ref CookieContainer cookieJar)
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

            var dctResults = new Dictionary<string, object>();
            var currentURL = "Undefined";
            var currentStatus = "authenticating";

            try
            {
                throw new NotImplementedException("Use RunItemSearchQuery instead of RunElasticSearchQuery");

                var searchSpec = new Dictionary<string, string>
                {
                    {"default_operator", "AND"},
                    {"default_field", "_all"}
                };

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
                        throw new ArgumentOutOfRangeException("Unrecognized value for logicalOperator: " +
                                                              logicalOperator);
                }

                var queryTerms = new StringBuilder();
                foreach (var searchTerm in dctSearchTerms)
                {
                    if (queryTerms.Length > 0)
                        queryTerms.Append(operatorString);

                    queryTerms.Append(searchTerm.Key + ":" + PossiblyQuoteString(searchTerm.Value));
                }
                searchSpec.Add("query", queryTerms.ToString());

                var queryStringSpec = new Dictionary<string, object>
                {
                    {"query_string", searchSpec}
                };

                var mustSpec = new Dictionary<string, object>
                {
                    {"must", queryStringSpec}
                };

                var boolSpec = new Dictionary<string, object>
                {
                    {"bool", mustSpec}
                };

                var querySpec = new Dictionary<string, object>
                {
                    {"query", boolSpec},
                    {"from", 0},
                    {"size", maxFileCount}
                };

                // The following Callback allows us to access the MyEMSL server even if the certificate is expired or untrusted
                // This hack was added in March 2014 because Proto-10 reported error
                //   "Could not establish trust relationship for the SSL/TLS secure channel"
                //   when accessing https://my.emsl.pnl.gov/
                // This workaround requires these two using statements:
                //   using System.Net.Security;
                //   using System.Security.Cryptography.X509Certificates;

                // Could use this to ignore all certificates (not wise)
                // System.Net.ServicePointManager.ServerCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;

                // Instead, only allow certain domains, as defined by ValidateRemoteCertificate
                if (ServicePointManager.ServerCertificateValidationCallback == null)
                    ServicePointManager.ServerCertificateValidationCallback += Utilities.ValidateRemoteCertificate;

                if (mPacificaConfig.UseTestInstance != UseTestInstance)
                {
                    mPacificaConfig.UseTestInstance = UseTestInstance;
                }

                // Code deprecated in June 2017

                // Call the testauth service to obtain a cookie for this session
                // var authURL = mPacificaConfig.TestAuthUri;
                var authURL = "";

                //if (mPacificaConfig.UseTestInstance)
                //{
                //    authURL = "https://" + mPacificaConfig.SearchServerHostName + mPacificaConfig.TEST_AUTH_RELATIVE_PATH;
                //}

                currentURL = string.Copy(authURL);

                var auth = new Auth(new Uri(authURL));

                if (cookieJar == null)
                {
                    if (!auth.GetAuthCookies(out cookieJar))
                    {
                        ReportError("MyEMSLReader auto-login to " + authURL + " failed authentication for user " +
                                    Environment.UserDomainName + @"\" + Environment.UserName);
                        return dctResults;
                    }
                }

                // var URL = mPacificaConfig.ElasticSearchUri;
                var URL = "";

                if (mPacificaConfig.UseTestInstance)
                {
                    // Expected url: https://test0.my.emsl.pnl.gov/myemsl/search/simple/index.shtml
                    URL += "index.shtml";
                }
                else
                {
                    // Expected url: https://my.emsl.pnl.gov/myemsl/elasticsearch/simple_items
                    URL += "simple_items";
                }

                if (scanMode == ScanMode.ObtainAuthToken)
                {
                    URL += "?auth";
                }
                else if (scanMode == ScanMode.CreateScrollID)
                {
                    URL += "?search_type=scan";
                }

                var queryEnabled = true;
                var responseData = string.Empty;
                const int maxAttempts = 4;
                var mostRecentException = new Exception("Uninitialized exception");

                while (queryEnabled)
                {
                    querySpec["size"] = maxFileCount;
                    // var postData = Utilities.ObjectToJson(querySpec);
                    var postData = "";
                    const bool allowEmptyResponseData = false;

                    currentURL = string.Copy(URL);
                    currentStatus = "Posting JSON {" + postData + "}";

                    var retrievalSuccess = SendHTTPRequestWithRetry(URL, cookieJar, postData, EasyHttp.HttpMethod.Post,
                                                                    maxAttempts, allowEmptyResponseData,
                                                                    out responseData, out mostRecentException);

                    if (!retrievalSuccess || string.IsNullOrWhiteSpace(responseData))
                    {
                        queryEnabled = false;
                    }
                    else
                    {
                        // Convert the results to a Json dictionary object
                        dctResults = Utilities.JsonToObject(responseData);

                        // Check whether the maximum number of files was returned
                        // If it was, bump up the max file count and try again (within reason)
                        Int64 fileCountMatched = 0;
                        string errorMessage;
                        if (ValidSearchResults(dctResults, out errorMessage))
                        {

                            // Extract out the hits section
                            var dctHits = RetrieveDictionaryObjectByKey(dctResults, "hits");
                            if (dctHits.Count > 0)
                            {
                                fileCountMatched = ReadDictionaryValue(dctHits, "total", -1);
                            }

                        }

                        if (fileCountMatched >= maxFileCount)
                        {
                            maxFileCount *= 3;
                            if (maxFileCount > 500000)
                                queryEnabled = false;
                        }
                        else
                        {
                            queryEnabled = false;
                        }
                    }

                }

                currentStatus = "Examining response";

                if (string.IsNullOrEmpty(responseData))
                {
                    var msg = "No results returned from MyEMSL after " + maxAttempts + " attempts (MyEMSLReader.RunElasticSearchQuery)";
                    if (mostRecentException != null)
                    {
                        if (mostRecentException.Message.StartsWith("Aurora Offline"))
                            msg += ": Aurora Offline";
                        else
                            msg += ": " + mostRecentException.Message;
                    }

                    ReportError(msg, mostRecentException);
                }

                currentStatus = "Logging out";

                if (scanMode == ScanMode.SimpleSearch)
                {
                    // Utilities.Logout(cookieJar);
                }

                return dctResults;

            }
            catch (WebException ex)
            {
                var response = ex.Response as HttpWebResponse;

                if (response?.StatusCode == HttpStatusCode.ServiceUnavailable)
                {
                    // MyEMSL is Offline
                    ReportError("MyEMSL is offline (MyEMSLReader.RunElasticSearchQuery)", false);
                    return dctResults;
                }

                ReportError(
                    "WebException in MyEMSLReader.RunElasticSearchQuery contacting " + currentURL +
                    " [" + currentStatus + "]: " + ex.Message, ex);

                return dctResults;
            }
            catch (Exception ex)
            {
                ReportError(
                    "Error in MyEMSLReader.RunElasticSearchQuery contacting " + currentURL +
                    " [" + currentStatus + "]: " + ex.Message, ex);

                return dctResults;
            }

        }

        /// <summary>
        /// Call the Item Search service to find the matching items
        /// </summary>
        /// <param name="searchKey">Key to search on</param>
        /// <param name="searchValue">Value to match</param>
        /// <returns>Dictionary where keys are relative file paths; values are file info details</returns>
        /// <remarks>A given remote file could have multiple hash values if multiple versions of the file have been uploaded</remarks>
        internal Dictionary<string, List<ArchivedFileInfo>> RunItemSearchQuery(string searchKey, string searchValue)
        {

            if (TraceMode)
                OnDebugEvent("Entering RunItemSearchQuery");

            // Keys in this dictionary are relative file paths; values are file info details
            // A given remote file could have multiple hash values if multiple versions of the file have been uploaded
            var remoteFiles = new Dictionary<string, List<ArchivedFileInfo>>();

            string datasetName;
            int datasetId;
            string instrument;

            if (string.Equals(QUERY_SPEC_DATASET_ID, searchKey))
            {

                if (!int.TryParse(searchValue, out datasetId))
                {
                    ReportError("Search value is not numeric: " + searchValue + "; expecting a dataset ID");
                }

                // Contact DMS to retrieve the dataset name for this dataset ID
                // This is a temporary fix until MyEMSL reports Dataset Name
                datasetName = LookupDatasetNameByID(datasetId, out instrument);

                if (TraceMode)
                    OnDebugEvent("Dataset ID " + datasetId + " is " + datasetName);

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

                datasetId = LookupDatasetIDByName(datasetName, out instrument);

                if (TraceMode)
                    OnDebugEvent("Dataset " + datasetName + " has ID " + datasetId);

                searchKey = QUERY_SPEC_DATASET_ID;
                searchValue = datasetId.ToString();
            }
            else
            {
                throw new NotSupportedException("MyEMSL metadata search currently only supports searching by dataset_id or dataset name");
            }

            try
            {

                // Example URLs:
                // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_id/403490
                // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.dataset_name/CPTAC_CompRef_P32_TMT11_17_18Jun17_Samwise_REP-17-05-01

                // Future:
                // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.data_package_id/2819
                // https://metadata.my.emsl.pnl.gov/fileinfo/files_for_keyvalue/omics.dms.experiment/6Apr15

                var metadataURL = string.Format(mPacificaConfig.MetadataServerUri + "/fileinfo/files_for_keyvalue/{0}/{1}",
                    searchKey, searchValue);

                if (TraceMode)
                    OnDebugEvent("Contacting " + metadataURL);

                // Retrieve a list of files already in MyEMSL for this dataset
                var fileInfoListJSON = EasyHttp.Send(mPacificaConfig, metadataURL, out HttpStatusCode responseStatusCode);

                if (string.IsNullOrEmpty(fileInfoListJSON))
                {
                    var msg = "No results returned from MyEMSL (MyEMSLReader.RunItemSearchQuery)";
                    ReportError(msg);
                    return dctResults;
                }

                // Convert the response to a dictionary
                var jsa = (Jayrock.Json.JsonArray)JsonConvert.Import(fileInfoListJSON);
                var remoteFileInfoList = Utilities.JsonArrayToDictionaryList(jsa);

                // Keys in this dictionary are relative file paths; values are file info details
                // A given remote file could have multiple hash values if multiple versions of the file have been uploaded
                var remoteFiles = new Dictionary<string, List<ArchivedFileInfo>>();

                // Note that two files in the same directory could have the same hash value (but different names),
                // so we cannot simply compare file hashes

                foreach (var fileObj in remoteFileInfoList)
                {
                    var fileName = Utilities.GetDictionaryValue(fileObj, "name");
                    var fileId = Utilities.GetDictionaryValue(fileObj, "_id", 0);
                    var fileHash = Utilities.GetDictionaryValue(fileObj, "hashsum");
                    var subFolder = Utilities.GetDictionaryValue(fileObj, "subdir");

                    var relativeFilePath = Path.Combine(subFolder, fileName);

                    if (remoteFiles.TryGetValue(relativeFilePath, out var fileVersions))
                    {
                        if (FileHashExists(fileVersions, fileHash))
                        {
                            ReportError("Remote file listing reports the same file with the same hash more than once; " +
                                        "ignoring: " + relativeFilePath + " with hash " + fileHash);
                            continue;
                        }

                        // Add the file to fileVersions
                    }
                    else
                    {
                        fileVersions = new List<ArchivedFileInfo>();
                        remoteFiles.Add(relativeFilePath, fileVersions);
                    }

                    var remoteFileInfo = new ArchivedFileInfo(datasetName, fileName, subFolder, fileId)
                    {
                        DatasetID = datasetId,
                        DatasetYearQuarter = "",
                        FileSizeBytes = Utilities.GetDictionaryValue(fileObj, "size", 0),
                        Instrument = instrument,
                        IsPublicFile = false,
                        Hash = fileHash,
                        HashType = Utilities.GetDictionaryValue(fileObj, "hashtype"),
                        SubmissionTime = Utilities.GetDictionaryValue(fileObj, "created"),
                        TransactionID = Utilities.GetDictionaryValue(fileObj, "transaction_id", 0)
                    };

                    // remoteFileInfo.DataPackageID = 0;
                    // remoteFileInfo.Metadata = ...;

                    // var updatedInMyEMSL = Utilities.GetDictionaryValue(fileObj, "updated");
                    // var deletedInMyEMSL = Utilities.GetDictionaryValue(fileObj, "deleted");

                    var creationTime = Utilities.GetDictionaryValue(fileObj, "ctime");
                    var lastWriteTime = Utilities.GetDictionaryValue(fileObj, "mtime");

                    remoteFileInfo.UpdateSourceFileTimes(creationTime, lastWriteTime);

                    fileVersions.Add(remoteFileInfo);

                }

                return dctResults;

            }
            catch (Exception ex)
            {
                ReportError("Error in MyEMSLReader.RunItemSearchQuery: " + ex.Message, ex);
                return dctResults;
            }

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
