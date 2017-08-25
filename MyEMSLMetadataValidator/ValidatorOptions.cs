using System;
using System.IO;
using System.Reflection;
using PRISM;

namespace MyEMSLMetadataValidator
{
    public class ValidatorOptions
    {
        private const string PROGRAM_DATE = "July 30, 2017";

        public const string DMS_CONNECTION_STRING = "Data Source=gigasax;Initial Catalog=DMS_Capture;Integrated Security=SSPI";

        public ValidatorOptions()
        {
            DatasetIdStart = 0;
            DatasetIdEnd = int.MaxValue;
            DatasetIdFile = string.Empty;

            OutputFolderPath = string.Empty;
            AppendToOutput = true;

            DMSLookupBatchSize = 1000;
            MyEMSLReaderBatchSize = 5;

            MSecBetweenLookup = 500;
            Preview = false;
        }

        [Option("start", ArgPosition = 1, HelpText = "First Dataset ID")]
        public int DatasetIdStart { get; set; }

        [Option("end", ArgPosition = 2, HelpText = "Last Dataset ID to validate", HelpShowsDefault = true)]
        public int DatasetIdEnd { get; set; }

        [Option("IDFile", "IDs", HelpText = "File with dataset IDs to check (one ID per line)", HelpShowsDefault = true)]
        public string DatasetIdFile { get; set; }

        [Option("dmsBatch", HelpText = "Number of Dataset IDs per batch when polling DMS",
            HelpShowsDefault = true, Min = 100, Max = 5000)]
        public int DMSLookupBatchSize { get; set; }

        [Option("readerBatch", HelpText = "Number of Dataset IDs per batch when retrieving metadata using MyEMSLReader",
            HelpShowsDefault = true, Min = 1, Max = 50)]
        public int MyEMSLReaderBatchSize { get; set; }

        [Option("output", "o", ArgPosition = 3,
            HelpText = "Path to the folder where the results file will be created (relative path or absolute path)")]
        public string OutputFolderPath { get; set; }

        [Option("append", HelpText = "Append results to the output file", HelpShowsDefault = true)]
        public bool AppendToOutput { get; set; }

        [Option("msec", HelpText = "Minimum number of milliseconds between each metadata lookup call to MyEMSL",
            HelpShowsDefault = true, Min = 50, Max = 10000)]
        public int MSecBetweenLookup { get; set; }

        [Option("preview", HelpText = "Preview queries to DMS and metadata lookups")]
        public bool Preview { get; set; }

        public static string GetAppVersion()
        {
            var version = Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";

            return version;
        }

        public void OutputSetOptions()
        {
            Console.WriteLine("Using options:");

            if (!string.IsNullOrWhiteSpace(DatasetIdFile))
            {
                Console.WriteLine(" Reading Dataset IDs from: {0}", DatasetIdFile);
            }
            else
            {
                Console.WriteLine(" DatasetID start: {0}", DatasetIdStart);
                if (DatasetIdEnd < int.MaxValue)
                    Console.WriteLine(" DatasetID end: {0}", DatasetIdEnd);
            }

            Console.WriteLine(" Output folder path: {0}", OutputFolderPath);
            Console.WriteLine(" Append to output: {0}", AppendToOutput);

            if (Preview)
                Console.WriteLine("Previewing validation actions");
        }

        public bool ValidateArgs()
        {
            if (string.IsNullOrWhiteSpace(OutputFolderPath))
            {
                var currentFolder = new DirectoryInfo(".");
                OutputFolderPath = currentFolder.FullName;
            }

            if (DatasetIdStart == 0 && string.IsNullOrWhiteSpace(DatasetIdFile))
            {
                Console.WriteLine("You must either provide a starting DatasetID or specify a DatasetIDFile");
                return false;
            }

            return true;
        }

    }
}