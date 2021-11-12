using System;
using System.IO;
using System.Reflection;
using System.Threading;
using PRISM;
using PRISM.Logging;

namespace MyEMSLMetadataValidator
{
    internal static class Program
    {
        private static double mPercentComplete;
        private static DateTime mLastProgressUpdateTime = DateTime.UtcNow;

        private static int Main(string[] args)
        {
            try
            {
                var asmName = typeof(Program).GetTypeInfo().Assembly.GetName();
                var exeName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);
                var version = ValidatorOptions.GetAppVersion();

                var parser = new CommandLineParser<ValidatorOptions>(asmName.Name, version)
                {
                    ProgramInfo = "This program validates metadata in MyEMSL. " + Environment.NewLine +
                                  "Specify a starting DatasetID to validate and " +
                                  "optionally specify an ending DatasetID" + Environment.NewLine + Environment.NewLine +
                                  "DMS will be polled to determine which folders MyEMSL should be tracking for each dataset, " +
                                  "along with the number of files. Next MyEMSL will be contacted for each dataset to confirm " +
                                  "see which files / folders it is tracking.  A report is written listing the expected info " +
                                  "along with the actual tracked metadata",

                    ContactInfo = "Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)" +
                                  Environment.NewLine + Environment.NewLine +
                                  "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                                  "Website: http://panomics.pnnl.gov/ or https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics",

                    UsageExamples = {
                        exeName + "/Start:200000 /End:201000",
                        exeName + "200000 201000",
                        exeName + "200000 201000 OutputFolder",
                        exeName + "/IDFile:DatasetIDFile.txt"
                    }
                };

                var result = parser.ParseArgs(args);
                var options = result.ParsedResults;

                if (!result.Success)
                {
                    if (parser.CreateParamFileProvided)
                    {
                        return 0;
                    }

                    // Delay for 1500 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                    Thread.Sleep(1500);
                    return -1;
                }

                if (!options.ValidateArgs(out var errorMessage))
                {
                    parser.PrintHelp();

                    Console.WriteLine();
                    ConsoleMsgUtils.ShowWarning("Validation error:");
                    ConsoleMsgUtils.ShowWarning(errorMessage);

                    Thread.Sleep(1500);
                    return -1;
                }

                options.OutputSetOptions();

                var validator = new MetadataValidator(options);
                RegisterEvents(validator);

                var success = validator.ValidateMyEMSLMetadata();

                if (success)
                    return 0;

                Thread.Sleep(1000);
                return -1;
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error occurred in Program->Main: " + Environment.NewLine + ex.Message, ex);
                Thread.Sleep(1000);
                return -1;
            }
        }

        private static void ShowErrorMessage(string message, Exception ex = null)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        #region "Event Handlers"

        private static void RegisterEvents(IEventNotifier processingClass)
        {
            processingClass.DebugEvent += OnDebugEvent;
            processingClass.StatusEvent += OnStatusEvent;
            processingClass.ErrorEvent += OnErrorEvent;
            processingClass.WarningEvent += OnWarningEvent;
            processingClass.ProgressUpdate += OnProgressUpdate;
        }

        private static void OnDebugEvent(string message)
        {
            ConsoleMsgUtils.ShowDebug(message);
        }

        private static void OnStatusEvent(string message)
        {
            Console.WriteLine(message);
        }

        private static void OnErrorEvent(string message, Exception ex)
        {
            ShowErrorMessage(message, ex);
        }

        private static void OnWarningEvent(string message)
        {
            ConsoleMsgUtils.ShowWarning(message);
        }

        private static void OnProgressUpdate(string progressMessage, float percentComplete)
        {
            if (!(percentComplete > mPercentComplete) && !(DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 15))
                return;

            if (!(DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 1))
                return;

            if (progressMessage.Trim().Equals("..."))
                Console.WriteLine(progressMessage + percentComplete.ToString("0.0") + "% complete");
            else
                Console.WriteLine(progressMessage + ": " + percentComplete.ToString("0.0") + "% complete");

            mPercentComplete = percentComplete;
            mLastProgressUpdateTime = DateTime.UtcNow;
        }

        #endregion
    }
}
