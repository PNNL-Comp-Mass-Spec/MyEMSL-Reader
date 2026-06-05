using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Security;
#if NETSTANDARD2_0_OR_GREATER || NETCOREAPP2_0_OR_GREATER
using System.Runtime.InteropServices;
#endif
using System.Security.Cryptography.X509Certificates;
using System.Security.Principal;
using System.Text;
using System.Text.RegularExpressions;

namespace Pacifica.Core
{
    /// <summary>
    /// General utility methods
    /// </summary>
    public static class Utilities
    {
        // Ignore Spelling: Pacifica, Sha

        private static Regex sha1HashRegex = new Regex(@"^[a-zA-Z0-9]{40}$", RegexOptions.Compiled);

        /// <summary>
        /// Create an X509 certificate instance. Wrapper to avoid obsolete method warnings in .NET 9.0+
        /// </summary>
        /// <param name="certificateFilePath"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public static X509Certificate2 CreateX509Certificate(string certificateFilePath, string password)
        {
#if !NET9_0_OR_GREATER
            return new X509Certificate2(certificateFilePath, password, X509KeyStorageFlags.PersistKeySet);
#else
            // Creating a certificate via constructor is obsolete in .NET 9.0, using X509CertificateLoader instead
            return X509CertificateLoader.LoadPkcs12FromFile(certificateFilePath, password, X509KeyStorageFlags.PersistKeySet);
#endif
        }

        /// <summary>
        /// Try to delete the file; ignore any errors
        /// </summary>
        /// <param name="fileToDelete"></param>
        public static void DeleteFileIgnoreErrors(FileInfo fileToDelete)
        {
            try
            {
                fileToDelete.Delete();
            }
            catch
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Generate the SHA1 hash for the file at <paramref name="filePath"/>
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        [Obsolete("Use the method that takes a FileInfo object")]
        public static string GenerateSha1Hash(string filePath)
        {
            return GenerateSha1Hash(new FileInfo(filePath));
        }

        /// <summary>
        /// Generate the SHA1 hash for the file specified by <paramref name="file"/>
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        /// <exception cref="FileNotFoundException"></exception>
        public static string GenerateSha1Hash(FileInfo file)
        {
            if (!file.Exists)
            {
                throw new FileNotFoundException("File not found in GenerateSha1Hash: " + file.FullName);
            }

            // It would be preferable to just use the built-in SHA1 calculation, but we're having some issues with it on one server with large files.
            // Time comparison for execution (sha1sum and 7-zip both create another thread and a process), for the 2nd run on the same file to avoid bias from caching:
            // All times in seconds, files read from an SSD
            // File size    DotNet   sha1sum    7-zip
            // 87 KB         0.009     0.256    0.252
            // 102 MB        0.269     0.268    0.252
            // 326 MB        0.855     0.767    0.768
            // 906 MB        2.249     1.803    1.810
            // 1.93 GB       5.030     3.860    3.877

            var success = GenerateSha1HashSha1Sum(file, out var sha1Hash);
            if (!success)
            {
                success = GenerateSha1Hash7Zip(file, out sha1Hash);
            }

            if (success)
            {
                return sha1Hash;
            }

            return GenerateSha1HashDotNet(file);
        }

        /// <summary>
        /// Generate the SHA1 hash for the file specified by <paramref name="file"/> using the .NET SHA1 class
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        /// <exception cref="FileNotFoundException"></exception>
        public static string GenerateSha1HashDotNet(FileInfo file)
        {
            if (!file.Exists)
            {
                throw new FileNotFoundException("File not found in GenerateSha1HashDotNet: " + file.FullName);
            }

            var path = PossiblyConvertToLongPath(file.FullName);
            return HashUtilities.ComputeFileHashSha1(path, true);
        }

        /// <summary>
        /// Generate the SHA1 hash for the file specified by <paramref name="file"/> using the GnuOnWindows sha1sum.exe if present at C:\DMS_Programs\GnuOnWindows
        /// </summary>
        /// <param name="file"></param>
        /// <param name="hash">SHA-1 sum of the file</param>
        /// <returns>True unless sha1sum.exe could not be found</returns>
        /// <exception cref="FileNotFoundException"></exception>
        public static bool GenerateSha1HashSha1Sum(FileInfo file, out string hash)
        {
            if (!file.Exists)
            {
                throw new FileNotFoundException("File not found in GenerateSha1HashSha1Sum: " + file.FullName);
            }

            const string sha1SumPath = @"C:\DMS_Programs\GnuOnWindows\sha1sum.exe";
            if (!File.Exists(sha1SumPath))
            {
                hash = string.Empty;
                return false;
            }

            // Use ProgRunner to run the program
            var path = PossiblyConvertToLongPath(file.FullName);


            // Some issues with network paths
            if (path.StartsWith(@"\\"))
            {
                // Converting to the alternate path separator prevents sha1sum.exe from interpreting the first '\' as escaping the second
                path = path.Replace(@"\", "/");
                // Another option that just feels weird, but also works...
                //path = @"\" + path;
            }

            var p = new ProgRunner()
            {
                Program = sha1SumPath,
                Arguments = $"\"{path}\"",
                CacheStandardOutput = true,
                CreateNoWindow = true,
                Repeat = false,
                MonitoringInterval = 250,
                EchoOutputToConsole = false,
                WorkDir = ".",
            };

            try
            {
                // Start the program executing
                p.StartAndMonitorProgram();

                // Loop until program is complete, or until MaxRuntimeSeconds seconds elapses
                while (p.State != ProgRunner.States.NotMonitoring)
                {
                    AppUtils.SleepMilliseconds(250);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error calculating SHA-1 sum using sha1sum.exe", ex);
            }

            hash = "";
            var output = p.CachedConsoleOutput;
            if (output.Contains(" failed "))
            {
                return false;
            }

            // Output should match "\[hash] *[filename/path]"
            var split = output.Replace("\r", "").Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var line in split)
            {
                // Trim whitespace, and the leading '\' that sha1sum.exe outputs
                var parts = line.Trim().Trim('\\').Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                hash = parts[0];
            }

            if (string.IsNullOrWhiteSpace(hash))
            {
                return false;
            }

            if (!sha1HashRegex.IsMatch(hash))
            {
                Console.WriteLine(output);
                hash = "";
                return false;
            }

            return true;
        }

        /// <summary>
        /// Generate the SHA1 hash for the file specified by <paramref name="file"/> using 7z.exe in a standard 7-zip installation
        /// </summary>
        /// <param name="file"></param>
        /// <param name="hash">SHA-1 sum of the file</param>
        /// <returns>True unless 7z.exe could not be found</returns>
        /// <exception cref="FileNotFoundException"></exception>
        public static bool GenerateSha1Hash7Zip(FileInfo file, out string hash)
        {
            if (!file.Exists)
            {
                throw new FileNotFoundException("File not found in GenerateSha1Hash7Zip: " + file.FullName);
            }

            const string _7zPathX64 = @"C:\Program Files\7-Zip\7z.exe";
            const string _7zPathX86 = @"C:\Program Files (x86)\7-Zip\7z.exe";

            string _7zPath;

            if (File.Exists(_7zPathX64))
            {
                _7zPath = _7zPathX64;
            }
            else if (File.Exists(_7zPathX86))
            {
                _7zPath = _7zPathX86;
            }
            else
            {
                hash = string.Empty;
                return false;
            }

            // Use ProgRunner to run the program
            var path = PossiblyConvertToLongPath(file.FullName);
            var p = new ProgRunner()
            {
                Program = _7zPath,
                Arguments = $"h -scrcSHA1 -bd \"{path}\"",
                CacheStandardOutput = true,
                CreateNoWindow = true,
                Repeat = false,
                MonitoringInterval = 250,
                EchoOutputToConsole = false,
                WorkDir = "."
            };

            try
            {
                // Start the program executing
                p.StartAndMonitorProgram();

                // Loop until program is complete, or until MaxRuntimeSeconds seconds elapses
                while (p.State != ProgRunner.States.NotMonitoring)
                {
                    AppUtils.SleepMilliseconds(250);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error calculating SHA-1 sum using 7z.exe", ex);
            }

            // Output should match (much more verbose than sha1sum):
            //
            // [7-zip version]
            //
            // Scanning
            // 1 file, [file size]
            //
            // SHA1               Size  Name
            // ----------  -----------  ----------
            // [hash]      [file size]  [file name]
            // ----------  -----------
            // [hash]      [file size]
            //
            // Size: [file size]
            //
            // SHA1   for data:             [hash]
            //
            // Everything is Ok

            hash = "";
            var output = p.CachedConsoleOutput;
            var split = output.Replace("\r", "").Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var line in split)
            {
                if (line.StartsWith("SHA1"))
                {
                    var parts = line.Trim().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    hash = parts[parts.Length - 1];
                }
            }

            // Output includes a table of SHA1 hashes where the header starts with 'SHA1' and ends with 'Name'
            if (string.IsNullOrWhiteSpace(hash) || hash.Equals("Name"))
            {
                return false;
            }

            if (!sha1HashRegex.IsMatch(hash))
            {
                Console.WriteLine(output);
                hash = "";
                return false;
            }

            return true;
        }

        /// <summary>
        /// Lookup the value associated with the given key in the dictionary
        /// </summary>
        /// <param name="dictionary">Dictionary</param>
        /// <param name="keyName">Key name</param>
        /// <param name="valueIfMissingOrNull">Value to return if the dictionary value is missing or null</param>
        /// <returns>Value for the given key, or valueIfMissingOrNull if the value is missing or null</returns>
        // ReSharper disable once UnusedMember.Global
        public static bool GetDictionaryValue(IReadOnlyDictionary<string, string> dictionary, string keyName, bool valueIfMissingOrNull)
        {
            var valueText = GetDictionaryValue(dictionary, keyName, valueIfMissingOrNull.ToString());

            if (bool.TryParse(valueText, out var value))
            {
                return value;
            }

            return valueIfMissingOrNull;
        }

        /// <summary>
        /// Lookup the integer value associated with the given key in the dictionary
        /// </summary>
        /// <param name="dictionary">Dictionary</param>
        /// <param name="keyName">Key name</param>
        /// <param name="valueIfMissingOrNull">Integer to return if the dictionary value is missing or null</param>
        /// <returns>Value for the given key, or valueIfMissingOrNull if the value is missing or null</returns>
        // ReSharper disable once UnusedMember.Global
        public static int GetDictionaryValue(IReadOnlyDictionary<string, string> dictionary, string keyName, int valueIfMissingOrNull)
        {
            var valueText = GetDictionaryValue(dictionary, keyName, valueIfMissingOrNull.ToString());

            if (int.TryParse(valueText, out var value))
            {
                return value;
            }

            return valueIfMissingOrNull;
        }

        /// <summary>
        /// Lookup the value associated with the given key in the dictionary
        /// </summary>
        /// <param name="dictionary">Dictionary</param>
        /// <param name="keyName">Key name</param>
        /// <param name="valueIfMissingOrNull">Value to return if the dictionary value is missing or null</param>
        /// <returns>Value for the given key, or valueIfMissingOrNull if the value is missing or null</returns>
        public static string GetDictionaryValue(IReadOnlyDictionary<string, string> dictionary, string keyName, string valueIfMissingOrNull)
        {
            if (dictionary.TryGetValue(keyName, out var value))
            {
                return value ?? valueIfMissingOrNull;
            }

            return valueIfMissingOrNull;
        }

        /// <summary>
        /// Return the path as-is if less than 260 characters in length
        /// Otherwise, convert to a long path
        /// </summary>
        /// <param name="path"></param>
        public static string PossiblyConvertToLongPath(string path)
        {
            return path.Length < NativeIOFileTools.FILE_PATH_LENGTH_THRESHOLD ?
                       path :
                       NativeIOFileTools.GetWin32LongPath(path);
        }

        /// <summary>
        /// Return a consistent temp directory for temporary files
        /// </summary>
        /// <param name="config"></param>
        /// <returns></returns>
        public static DirectoryInfo GetTempDirectory(Configuration config)
        {
            if (!string.IsNullOrEmpty(config.LocalTempDirectory))
            {
                return new DirectoryInfo(config.LocalTempDirectory);
            }

            return new DirectoryInfo(Path.GetTempPath());
        }

        /// <summary>
        /// Return the metadata file name for this upload job
        /// </summary>
        /// <param name="jobNumber"></param>
        /// <returns></returns>
        public static string GetMetadataFilenameForJob(string jobNumber)
        {
            if (string.IsNullOrWhiteSpace(jobNumber))
            {
                return "MyEMSL_metadata_CaptureJob_000000.txt";
            }

            return "MyEMSL_metadata_CaptureJob_" + jobNumber + ".txt";
        }

        /// <summary>
        /// Get the current user's username
        /// </summary>
        /// <param name="cleanDomain"></param>
        /// <returns></returns>
        public static string GetUserName(bool cleanDomain = false)
        {
#if NETSTANDARD2_0_OR_GREATER || NETCOREAPP2_0_OR_GREATER
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return string.Empty;
            }
#endif

            var userIdent = WindowsIdentity.GetCurrent();
            var userName = userIdent.Name;

            if (cleanDomain)
            {
                userName = userName.Substring(userName.IndexOf('\\') + 1);
            }

            return userName;
        }

        /// <summary>
        /// Callback used to validate the certificate in an SSL conversation
        /// </summary>
        /// <param name="cert">Certificate to be validated</param>
        /// <param name="policyErrors">Results from the default certificate validation</param>
        /// <param name="errorMessage"></param>
        /// <returns>True if the server is trusted</returns>
        public static bool ValidateRemoteCertificate(
            X509Certificate cert,
            SslPolicyErrors policyErrors,
            out string errorMessage)
        {
            if (policyErrors == SslPolicyErrors.None)
            {
                // .NET server certificate validation succeeded
                errorMessage = "";
                return true;
            }

            var message = new StringBuilder();
            message.Append(".NET server certificate validation failed; errors: " + policyErrors);
            ConsoleMsgUtils.ShowWarning(message.ToString());
            ConsoleMsgUtils.ShowWarning("Checking certificate name against a known list");

            var trustedDomains = new List<string>
            {
                "my.emsl.pnnl.gov",
                "my.emsl.pnl.gov",
                "*.my.emsl.pnnl.gov",
                "*.my.emsl.pnl.gov",
                "emsl.pnnl.gov",
                "emsl.pnl.gov",
                "*.emsl.pnnl.gov",
                "*.emsl.pnl.gov",
                "pnnl.gov",
                "pnl.gov"
            };

            var cnMatcher = new Regex("CN=([^ ,]+),", RegexOptions.IgnoreCase);
            var cnMatch = cnMatcher.Match(cert.Subject);
            string domainToValidate;

            if (cnMatch.Success)
            {
                domainToValidate = cnMatch.Groups[1].ToString();
            }
            else
            {
                ConsoleMsgUtils.ShowWarning("Subject line of the certificate did not contain 'CN=domain'; this is unexpected");
                ConsoleMsgUtils.ShowWarning("Will use the full subject: " + cert.Subject);
                domainToValidate = cert.Subject;
            }

            // ConsoleMsgUtils.ShowDebug("Checking " + domainToValidate + " against trusted domains");

            foreach (var domainName in trustedDomains)
            {
                if (domainToValidate.IndexOf(domainName, StringComparison.OrdinalIgnoreCase) < 0)
                {
                    continue;
                }

                errorMessage = string.Empty;
                return true;
            }

            var mismatchMessage = "Pacifica.Core.Utilities.ValidateRemoteCertificate: The domain name associated with the certificate is not trusted: " + domainToValidate;
            message.Append("\n" + mismatchMessage);
            ConsoleMsgUtils.ShowWarning(mismatchMessage);

            Console.WriteLine("Trusted domains, as defined in ValidateRemoteCertificate:");

            foreach (var domainName in trustedDomains)
            {
                Console.WriteLine("  " + domainName);
            }

            message.Append("; trusted domains: " + string.Join(", ", trustedDomains));
            errorMessage = message.ToString();

            return false;
        }
    }
}
