using PRISM;

namespace MyEMSLDownloader
{
    internal class CommandLineOptions
    {
        // Ignore Spelling: ser, wildcards

        [Option("Dataset", ArgPosition = 1, HelpShowsDefault = false,
            HelpText = "Dataset Name (can also be a Dataset ID)")]
        public string DatasetName { get; set; }

        [Option("DatasetID", HelpShowsDefault = false,
            HelpText = "Dataset ID", Min = 1)]
        public int DatasetID { get; set; }

        [Option("DataPkg", HelpShowsDefault = false,
            HelpText = "Retrieve files from a specific data package", Min = 1)]
        public int DataPkgID { get; set; }

        [Option("SubDir", ArgPosition = 2, HelpShowsDefault = false,
            HelpText = "Name of a subdirectory in the dataset")]
        public string Subdirectory { get; set; }

        /// <summary>
        /// File spec for finding files
        /// </summary>
        /// <remarks>
        /// Single file name or a file spec like *.txt
        /// Specify a list of names and/or specs by separating with a semicolon and using switch /FileSplit
        /// For example: analysis.baf|ser
        /// </remarks>
        [Option("Files", HelpShowsDefault = false,
            HelpText = "Filter for specific files, for example /Files:*.txt\n" +
                       "Files will be downloaded to the directory with the .exe; override using /O")]
        public string FileMask { get; set; }

        /// <summary>
        /// Set to true to indicate that mFileMask contains a semicolon-separated list of file names and/or file specs
        /// </summary>
        [Option("FileSplit", HelpShowsDefault = false,
            HelpText = "Use to indicate that /Files contains a list of file names and/or file specs, " +
                       "separated by semicolons. For example, use\n" +
                       "/Files:analysis.baf;ser /FileSplit")]
        public bool FileSplit { get; set; }

        [Option("FileList", HelpShowsDefault = false,
            HelpText = "Specify a file with a list of datasets and files to retrieve. " +
                       "The file must be a tab-delimited text file, with columns Dataset (or DatasetID) and File, " +
                       "and optionally with column SubDir. The file names in the File column are allowed " +
                       "to contain wildcards. When /FileList is used, /D is automatically enabled")]
        public string FileListPath { get; set; }

        [Option("FileID", HelpShowsDefault = false,
            HelpText = "Specify the MyEMSL ID of a file to download (as seen with /V). " +
                       "This mode does not use Simple Search to find files and can thus be used " +
                       "to retrieve a file that Simple Search does not find. Provide a comma separated list " +
                       "to retrieve multiple files.")]
        public string FileIDList { get; set; }

        [Option("O", HelpShowsDefault = false,
            HelpText = "Use to specify an output directory that isn't the current directory")]
        public string OutputDirectoryPath { get; set; }

        [Option("D", HelpShowsDefault = false,
            HelpText = "Create a directory with the dataset name, then store the files within that directory")]
        public bool MultiDatasetMode { get; set; }

        [Option("Preview", HelpShowsDefault = false,
            HelpText = "View files that would be downloaded, but not actually download them.")]
        public bool PreviewMode { get; set; }

        [Option("V", "Verbose", HelpShowsDefault = false,
            HelpText = "Enable verbose preview, showing extended details about each file")]
        public bool VerbosePreview { get; set; }


        [Option("Test", HelpShowsDefault = false,
            HelpText = "Perform automatic tests using predefined dataset names")]
        public bool AutoTestMode { get; set; }

        [Option("Trace", HelpShowsDefault = false,
            HelpText = "Display additional debug information")]
        public bool TraceMode { get; set; }

        [Option("UseTest", HelpShowsDefault = false,
            HelpText = "Connect to test0.my.emsl.pnl.gov instead of my.emsl.pnl.gov")]
        public bool UseTestInstance { get; set; }

        public CommandLineOptions()
        {
            DatasetName = string.Empty;
            DatasetID = 0;
            DataPkgID = 0;
            Subdirectory = string.Empty;
            FileMask = string.Empty;
            FileSplit = false;
            OutputDirectoryPath = string.Empty;

            MultiDatasetMode = false;

            PreviewMode = false;
            VerbosePreview = false;
            AutoTestMode = false;
            TraceMode = false;
            UseTestInstance = false;
        }

        public bool Validate()
        {
            if (int.TryParse(DatasetName, out var datasetId))
            {
                if (datasetId <= 0)
                {
                    ShowErrorMessage("Dataset ID should be a positive integer, not " + DatasetName);
                    return false;
                }

                if (DatasetID == 0)
                {
                    DatasetID = datasetId;
                }
            }

            if (!string.IsNullOrWhiteSpace(FileListPath))
            {
                MultiDatasetMode = true;
            }

            return true;
        }
        private static void ShowErrorMessage(string message)
        {
            ConsoleMsgUtils.ShowError(message);
        }
    }
}
