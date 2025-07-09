using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Security;
#if NETSTANDARD2_0_OR_GREATER
using System.Runtime.InteropServices;
#endif
using System.Security.Cryptography;
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

        private static SHA1 _hashProvider;

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
            byte[] fileHash;

            if (!file.Exists)
            {
                throw new FileNotFoundException("File not found in GenerateSha1Hash: " + file.FullName);
            }

            _hashProvider ??= SHA1.Create();

            using (var sourceFile = new FileStream(PossiblyConvertToLongPath(file.FullName), FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                fileHash = _hashProvider.ComputeHash(sourceFile);
            }

            return ToHexString(fileHash);
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
        /// Convert a byte buffer to a hex-encoded string
        /// </summary>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public static string ToHexString(byte[] buffer)
        {
            return BitConverter.ToString(buffer).Replace("-", string.Empty).ToLower();
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
#if NETSTANDARD2_0_OR_GREATER
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return string.Empty;
            }
#endif

#pragma warning disable CA1416
            var userIdent = WindowsIdentity.GetCurrent();
            var userName = userIdent.Name;

            if (cleanDomain)
            {
                userName = userName.Substring(userName.IndexOf('\\') + 1);
            }

            return userName;
#pragma warning restore CA1416
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
