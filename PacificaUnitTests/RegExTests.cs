using System;
using System.Text.RegularExpressions;
using NUnit.Framework;

namespace PacificaUnitTests
{
    [TestFixture]
    public class RegExTests
    {
        // Ignore Spelling: pacifica

        [Test]
        public void TestRegexEx()
        {
            // Example error as of January 2021:
            // Traceback (most recent call last):\n  File "/opt/pacifica/lib64/python3.6/site-packages/pacifica/ingest/tasks.py", line 88, in ingest_files\n    ingest_obj.ingest()\n  File "/opt/pacifica/lib64/python3.6/site-packages/pacifica/ingest/tarutils.py", line 248, in ingest\n    ingest.upload_file_in_file(info, self.tar)\n  File "/opt/pacifica/lib64/python3.6/site-packages/pacifica/ingest/tarutils.py", line 79, in upload_file_in_file\n    size = int(ret_dict['total_bytes'])\nKeyError: 'total_bytes'\n\n'total_bytes'

            const string exception = "Traceback (most recent call last):\n  File \"/opt/pacifica/lib64/python3.6/site-packages/pacifica/ingest/tasks.py\", line 88, in ingest_files\n    ingest_obj.ingest()\n  File \"/opt/pacifica/lib64/python3.6/site-packages/pacifica/ingest/tarutils.py\", line 248, in ingest\n    ingest.upload_file_in_file(info, self.tar)\n  File \"/opt/pacifica/lib64/python3.6/site-packages/pacifica/ingest/tarutils.py\", line 79, in upload_file_in_file\n    size = int(ret_dict['total_bytes'])\nKeyError: 'total_bytes'\n\n'total_bytes'";

            // Original (equivalent)
            // Use a RegEx to remove unnecessary text that makes some downstream evaluation harder (like checks in stored procedures)
            var exceptionUpdater = new Regex(@"Traceback \(most recent call last\):\s+File", RegexOptions.IgnoreCase);
            //var exceptionClean = exceptionUpdater.Replace(exception, "in file");
            //Console.WriteLine("; exception " + exceptionClean.Substring(0, 75) + " ...");
            Console.WriteLine("; exception " + exceptionUpdater.Replace(exception, "in file").Substring(0, 75) + " ...");

            // Update 1 (still truncate at 75)
            var exceptionClean = exceptionUpdater.Replace(exception, "in");
            Console.WriteLine("; exception " + exceptionClean.Substring(0, 75) + " ...");

            var keyErrorRegex = new Regex(@"(KeyError:\s.+[^\n])", RegexOptions.IgnoreCase);
            var keyError = keyErrorRegex.Match(exception).Value; // == string.Empty if no match found.
            Console.WriteLine(keyError);

            // Lazy match of a string with at least 75 characters, until (and excluding) a new line, tab, or two spaces
            var exceptionTruncate = new Regex(@"((?:.|[\r\n]){50,}?)(?=(?:  )|[\r\n\t])");
            Console.WriteLine("'" + exceptionTruncate.Match(exceptionClean).Value + "'");
        }
    }
}
