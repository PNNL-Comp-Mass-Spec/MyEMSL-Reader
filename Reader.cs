using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
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

        protected const string QUERY_SPEC_INSTRUMENT = "groups.omics.dms.instrument";
        protected const string QUERY_SPEC_DATASET_ID = "groups.omics.dms.dataset_id";
        protected const string QUERY_SPEC_DATASET_NAME = "groups.omics.dms.dataset";

        protected const string QUERY_SPEC_DATA_PACKAGE_ID = "groups.omics.dms.datapackage_id";
        protected const string QUERY_SPEC_DATA_PACKAGE_ID_ALT = "extended_metadata.gov_pnnl_emsl_dms_datapackage.id";

        protected const string QUERY_SPEC_FILENAME = "filename";
        protected const string QUERY_SPEC_EUS_PROPOSAL = "proposals";
        protected const string QUERY_SPEC_ANALYSIS_TOOL = "extended_metadata.gov_pnnl_emsl_dms_analysisjob.tool.name.untouched";
        protected const string QUERY_SPEC_DATASET_NAME_ALT = "extended_metadata.gov_pnnl_emsl_dms_dataset.name.untouched";

        protected const string DATASET_ID_TAG = "#*#*#_DATASET_ID_";
        protected const string DATA_PKG_ID_TAG = "#*#*#_DATA_PKG_ID_";

        #endregion

        #region "Enums"

        internal enum ScanMode
        {
            SimpleSearch = 0,
            ObtainAuthToken = 1,		// Perform a scan, but also obtain an authorization token
            CreateScrollID = 2			// Create a scroll ID
        }

        protected enum SearchEntity
        {
            DatasetName = 0,
            DatasetID = 1,
            DataPackageID = 2
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
        public List<ArchivedFileInfo> FindFilesByDataPackageID(int dataPkgID)
        {
            string subDir = string.Empty;
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
            string instrumentName = string.Empty;
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
            string instrumentName = string.Empty;
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
            string instrumentName = string.Empty;
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

            string instrumentName = string.Empty;
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
            string instrumentName = string.Empty;
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
                dctSearchTerms.Add(new KeyValuePair<string, string>(QUERY_SPEC_DATASET_NAME, dataset.Key));
            }

            string instrumentName = string.Empty;
            return FindFilesByDataset(dctDatasetsAndSubDirLists, recurse, instrumentName, dctSearchTerms);
        }

        #endregion

        #region "Protected Methods"

        /// <summary>
        /// Convert a dictionary of dataset names and SubDir names to a dictionary of dataset names and SubDir lists (with one item per list for each dataset)
        /// </summary>
        /// <param name="dctDatasetsAndSubDirs"></param>
        /// <returns>Dictionary where Keys are dataset names, and Values are a list of subdirectory names to filter on for the given dataset</returns>
        protected Dictionary<string, SortedSet<string>> ConvertDatasetSubDirDictToSubDirListDict(
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
        protected void ConvertDatasetSubDirAddToDictionary(
            Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists,
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

        protected List<ArchivedFileInfo> FilterFilesNoRecursion(
            List<ArchivedFileInfo> lstFiles,
            Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists)
        {
            var lstFilesFiltered = new List<ArchivedFileInfo>();
            var entityType = GetEntityType(dctDatasetsAndSubDirLists);

            string currentDataset = string.Empty;
            SortedSet<string> currentSubDirList = dctDatasetsAndSubDirLists.First().Value;

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

                foreach (string subDir in currentSubDirList)
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
                string datasetIDText = item.Replace(DATASET_ID_TAG, string.Empty);
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

        protected List<ArchivedFileInfo> FilterFilesBySubDir(
            List<ArchivedFileInfo> lstFiles,
            Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists)
        {
            var lstFilesFiltered = new List<ArchivedFileInfo>();
            var entityType = GetEntityType(dctDatasetsAndSubDirLists);

            string currentDataset = string.Empty;
            SortedSet<string> currentSubDirList = dctDatasetsAndSubDirLists.First().Value;

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

                foreach (string subDir in currentSubDirList)
                {

                    var lstRequiredSubDirTree = subDir.Split(new char[] { '/', '\\' }).ToList();

                    var lstFileSubDirTree = file.SubDirPath.Split(new char[] { '/', '\\' }).ToList();

                    if (lstFileSubDirTree.Count >= lstRequiredSubDirTree.Count)
                    {
                        int matchCount = 0;
                        for (int i = 0; i < lstRequiredSubDirTree.Count; i++)
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
        /// Keys are dataset names, 
        /// Values are a list of subdirectory names to filter on for the given dataset 
        /// (exact match; cannot contain wildcards; empty list means do not filter by subdirectory name)
        /// </param>
        /// <param name="recurse">True to recursively find files</param>
        /// <param name="instrumentName">Ignored if dctDatasetsAndSubDirLists has more than one entry</param>
        /// <param name="dctSearchTerms">If dctDatasetsAndSubDirLists only has one entry, then the dataset name will be ignored (it cannot be blank, but it could be "dummy" or "unknown" or "0", etc.)</param>
        /// <returns>Files that were found</returns>
        /// <remarks>
        /// If the keys in dctDatasetsAndSubDirLists start with DATASET_ID_TAG then they are Dataset IDs and not dataset names
        /// If the keys in dctDatasetsAndSubDirLists start with DATA_PKG_ID_TAG then they are Data Package IDs and not dataset names
        /// </remarks>
        protected List<ArchivedFileInfo> FindFilesByDataset(
            Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists,
            bool recurse,
            string instrumentName,
            List<KeyValuePair<string, string>> dctSearchTerms)
        {

            try
            {
                ResetStatus();

                bool filterOnSubDir = false;

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

                SearchOperator logicalOperator;

                if (dctDatasetsAndSubDirListsCleaned.Count == 1)
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

                // Run the query
                List<ArchivedFileInfo> lstFiles = QueryElasticSearch(dctSearchTerms, logicalOperator);

                // Filter the results
                lstFiles = FilterElasticSearchResults(dctDatasetsAndSubDirListsCleaned, recurse, lstFiles, filterOnSubDir);

                // Return the results, sorted by folder path and file name
                return (from item in lstFiles orderby item.PathWithInstrumentAndDatasetWindows select item).ToList();

            }
            catch (Exception ex)
            {
                if (string.IsNullOrWhiteSpace(ErrorMessage))
                    ReportError("Error in MyEMSLReader.Reader.FindFilesByDataset: " + ex.Message);
                else if (ThrowErrors)
                    throw;

                return new List<ArchivedFileInfo>();
            }
        }

        private List<ArchivedFileInfo> FilterElasticSearchResults(
            Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists,
            bool recurse, List<ArchivedFileInfo> lstFiles,
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

        protected SearchEntity GetEntityType(Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists)
        {
            var entityType = SearchEntity.DatasetName;

            if (dctDatasetsAndSubDirLists.First().Key.StartsWith(DATASET_ID_TAG))
                entityType = SearchEntity.DatasetID;
            else if (dctDatasetsAndSubDirLists.First().Key.StartsWith(DATA_PKG_ID_TAG))
                entityType = SearchEntity.DataPackageID;

            return entityType;
        }

        internal List<ArchivedFileInfo> ParseResults(string responseData, out string authToken)
        {
            if (string.IsNullOrWhiteSpace(responseData))
            {
                authToken = string.Empty;
                return new List<ArchivedFileInfo>();
            }

            // Convert the results to a Json dictionary object
            Dictionary<string, object> dctResults = Utilities.JsonToObject(responseData);
            return ParseResults(dctResults, out authToken);
        }

        /// <summary>
        /// Parse the search results from Elastic Search to generate a list of files
        /// </summary>
        /// <param name="dctResults"></param>
        /// <param name="authToken">Output parameter: Authorization token (if available)</param>
        /// <returns></returns>
        internal List<ArchivedFileInfo> ParseResults(Dictionary<string, object> dctResults, out string authToken)
        {

            authToken = string.Empty;

            try
            {

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
                var dctHits = RetrieveDictionaryObjectByKey(dctResults, "hits");

                LastSearchFileCountMatched = ReadDictionaryValue(dctHits, "total", -1);
                if (LastSearchFileCountMatched < 0)
                {
                    ReportError("Hits section did not have the 'total' entry");
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

                        var datasetID = (int)ReadDictionaryValue(dctFileInfo, "groups.omics.dms.dataset_id", 0);
                        var dataPackageID = (int)ReadDictionaryValue(dctFileInfo, "groups.omics.dms.datapackage_id", 0);

                        string datasetYearQuarter = ReadDictionaryValue(dctFileInfo, "groups.omics.dms.date_code", string.Empty);
                        string subDir = ReadDictionaryValue(dctFileInfo, "subdir", string.Empty);

                        var dctHashInfo = RetrieveDictionaryObjectByKey(dctFileInfo, "hash");

                        string fileSha1Hash = ReadDictionaryValue(dctHashInfo, "sha1", string.Empty);

                        var archiveFile = new ArchivedFileInfo(datasetName, fileName, subDir, fileID, instrumentName, datasetYearQuarter, dctFile)
                        {
                            Sha1Hash = fileSha1Hash,
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
            Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists,
            ArchivedFileInfo file,
            SearchEntity entityType,
            out SortedSet<string> subDirs)
        {
            bool success = true;

            if (entityType == SearchEntity.DatasetID)
            {
                if (!dctDatasetsAndSubDirLists.TryGetValue(DATASET_ID_TAG + file.DatasetID, out subDirs))
                {
                    OnErrorMessage(new MessageEventArgs("File " + file.FileID + " has an unrecognized dateset ID: " + file.DatasetID + "; skipping"));
                    success = false;
                }
            }
            else if (entityType == SearchEntity.DataPackageID)
            {
                if (!dctDatasetsAndSubDirLists.TryGetValue(DATA_PKG_ID_TAG + file.DataPackageID, out subDirs))
                {
                    OnErrorMessage(new MessageEventArgs("File " + file.FileID + " has an unrecognized data package ID: " + file.DataPackageID + "; skipping"));
                    success = false;
                }
            }
            else
            {
                if (!dctDatasetsAndSubDirLists.TryGetValue(file.Dataset, out subDirs))
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

                if (MaxFileCount < 1)
                    MaxFileCount = 1;

                LastSearchFileCountMatched = 0;
                LastSearchFileCountReturned = 0;

                Dictionary<string, object> dctResults = RunQuery(dctSearchTerms, MaxFileCount, logicalOperator);

                if (dctResults == null || dctResults.Count == 0)
                {
                    if (string.IsNullOrWhiteSpace(ErrorMessage))
                        ReportError("RunQuery returned an empty xml result");
                    LastSearchFileCountReturned = 0;
                    return new List<ArchivedFileInfo>();
                }

                // Parse the results (note that authToken will always be empty because we used ScanMode.SimpleSearch)
                string authToken;
                List<ArchivedFileInfo> lstFiles = ParseResults(dctResults, out authToken);

                LastSearchFileCountReturned = lstFiles.Count;

                return lstFiles;
            }
            catch (Exception ex)
            {
                if (string.IsNullOrWhiteSpace(ErrorMessage))
                    ReportError("Error in MyEMSLReader.Reader.QueryElasticSearch: " + ex.Message);
                else if (ThrowErrors)
                    throw;

                return new List<ArchivedFileInfo>();
            }

        }

        protected new void ResetStatus()
        {
            base.ResetStatus();
        }

        internal Dictionary<string, object> RunQuery(List<KeyValuePair<string, string>> dctSearchTerms, int maxFileCount)
        {
            CookieContainer cookieJar = null;
            return RunQuery(dctSearchTerms, maxFileCount, SearchOperator.And, ScanMode.SimpleSearch, ref cookieJar);
        }

        internal Dictionary<string, object> RunQuery(List<KeyValuePair<string, string>> dctSearchTerms, int maxFileCount, SearchOperator logicalOperator)
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
        /// <param name="cookieJar"></param>
        /// <returns>Json results dictionary</returns>
        /// <remarks>Be sure to call Logout() when scanMode is not 0 </remarks>
        internal Dictionary<string, object> RunQuery(
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

            try
            {
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
                        throw new ArgumentOutOfRangeException("Unrecognized value for logicalOperator: " + logicalOperator.ToString());
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

                // Call the testauth service to obtain a cookie for this session
                string authURL = Configuration.TestAuthUri;
                var auth = new Auth(new Uri(authURL));

                if (cookieJar == null)
                {
                    if (!auth.GetAuthCookies(out cookieJar))
                    {
                        ReportError("Auto-login to ingest.my.emsl.pnl.gov failed authentication for user " + Environment.UserDomainName + @"\" + Environment.UserName);
                        return dctResults;
                    }
                }

                string URL = Configuration.ElasticSearchUri + "simple_items";

                if (scanMode == ScanMode.ObtainAuthToken)
                {
                    URL += "?auth";
                }
                else if (scanMode == ScanMode.CreateScrollID)
                {
                    URL += "?search_type=scan";
                }

                bool queryEnabled = true;
                string responseData = string.Empty;
                const int maxAttempts = 4;
                var mostRecentException = new Exception("Uninitialized exception");

                while (queryEnabled)
                {
                    querySpec["size"] = maxFileCount;
                    string postData = Utilities.ObjectToJson(querySpec);
                    const bool allowEmptyResponseData = false;

                    bool retrievalSuccess = SendHTTPRequestWithRetry(URL, cookieJar, postData, EasyHttp.HttpMethod.Post, maxAttempts, allowEmptyResponseData, out responseData, out mostRecentException);

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

                if (string.IsNullOrEmpty(responseData))
                {
                    string msg = "No results returned from MyEMSL after " + maxAttempts + " attempts";
                    if (mostRecentException != null)
                    {
                        if (mostRecentException.Message.StartsWith("Aurora Offline"))
                            msg += ": Aurora Offline";
                        else
                            msg += ": " + mostRecentException.Message;
                    }

                    ReportError(msg, mostRecentException);
                }

                if (scanMode == ScanMode.SimpleSearch)
                {
                    Utilities.Logout(cookieJar);
                }

                return dctResults;

            }
            catch (Exception ex)
            {
                ReportError("Error in MyEMSLReader.Reader.RunQuery: " + ex.Message, ex);
                return dctResults;
            }

        }

        /// <summary>
        /// Examines the keys in dctDatasetsAndSubDirLists to make sure that they are not a mix of datasets, dataset IDs, and data package IDs
        /// </summary>
        /// <param name="dctDatasetsAndSubDirLists">Dictionary to examine</param>
        private static void ValidateDatasetInfoDictionary(Dictionary<string, SortedSet<string>> dctDatasetsAndSubDirLists)
        {

            int tagCountDatasetID = 0;
            int tagCountDataPkgID = 0;
            int tagCountDatasetName = 0;

            foreach (string item in dctDatasetsAndSubDirLists.Keys)
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
