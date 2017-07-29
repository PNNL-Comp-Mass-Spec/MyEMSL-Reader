using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using MyEMSLReader;
using PRISM;

namespace MyEMSLMetadataValidator
{

    internal static class Program
    {

        static double mPercentComplete;
        static DateTime mLastProgressUpdateTime = DateTime.UtcNow;

        static int Main(string[] args)
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

                    ContactInfo = "Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2017" +
                                  Environment.NewLine + Environment.NewLine +
                                  "E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com" + Environment.NewLine +
                                  "Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov or http://www.sysbio.org/resources/staff/",

                    UsageExamples = {
                        exeName + "/Start:200000 /End:201000",
                        exeName + "200000 201000",
                        exeName + "200000 201000 OutputFolder"
                    }
                };

                var parseResults = parser.ParseArgs(args);
                var options = parseResults.ParsedResults;

                if (!parseResults.Success || !options.ValidateArgs())
                {
                    System.Threading.Thread.Sleep(1500);
                    return -1;
                }

                options.OutputSetOptions();

                var validator = new MetadataValidator(options);
                RegisterEvents(validator);

                var success = validator.ValidateMyEMSLMetadata();

                if (success)
                    return 0;

                System.Threading.Thread.Sleep(1000);
                return -1;
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error occurred in Program->Main: " + Environment.NewLine + ex.Message, ex);
                System.Threading.Thread.Sleep(1000);
                return -1;
            }

        }

        private static void ShowErrorMessage(string strMessage, Exception ex = null)
        {
            const string strSeparator = "------------------------------------------------------------------------------";

            Console.WriteLine();
            Console.WriteLine(strSeparator);
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(strMessage);

            if (ex != null)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine(clsStackTraceFormatter.GetExceptionStackTraceMultiLine(ex));
            }
            Console.ResetColor();
            Console.WriteLine(strSeparator);
            Console.WriteLine();

            WriteToErrorStream(strMessage);
        }

        private static void WriteToErrorStream(string strErrorMessage)
        {
            try
            {
                using (var swErrorStream = new StreamWriter(Console.OpenStandardError()))
                {
                    swErrorStream.WriteLine(strErrorMessage);
                }
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch
            {
                // Ignore errors here
            }
        }

        #region "Event Handlers"

        private static void RegisterEvents(clsEventNotifier oProcessingClass)
        {
            oProcessingClass.DebugEvent += OnDebugEvent;
            oProcessingClass.StatusEvent += OnStatusEvent;
            oProcessingClass.ErrorEvent += OnErrorEvent;
            oProcessingClass.WarningEvent += OnWarningEvent;
            oProcessingClass.ProgressUpdate += OnProgressUpdate;
        }

        private static void OnDebugEvent(string message)
        {
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine(message);
            Console.ResetColor();
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
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(message);
            Console.ResetColor();
        }

        private static void OnProgressUpdate(string progressmessage, float percentcomplete)
        {
            if (percentcomplete > mPercentComplete || DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 15)
            {
                if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 1)
                {
                    if (progressmessage.Trim().Equals("..."))
                        Console.WriteLine(progressmessage + percentcomplete.ToString("0.0") + "% complete");
                    else
                        Console.WriteLine(progressmessage + ": " + percentcomplete.ToString("0.0") + "% complete");

                    mPercentComplete = percentcomplete;
                    mLastProgressUpdateTime = DateTime.UtcNow;
                }
            }
        }


        #endregion
    }
}
