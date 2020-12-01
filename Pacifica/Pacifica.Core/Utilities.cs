using Jayrock.Json;
using Jayrock.Json.Conversion;
using PRISM;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Security.Principal;
using System.Text;
using System.Text.RegularExpressions;

namespace Pacifica.Core
{
    public static class Utilities
    {
        // Ignore Spelling: absolutelocalpath, subdir, hashsum, Json

        /// <summary>
        /// Decode a password
        /// </summary>
        /// <param name="encodedPwd">Encoded password</param>
        /// <returns>Clear text password</returns>
        public static string DecodePassword(string encodedPwd)
        {
            return EncryptDecrypt(encodedPwd, false);
        }

        /// <summary>
        /// Encode or decode a password
        /// </summary>
        /// <param name="password">Password</param>
        /// <param name="encrypt">True to encode the password; false to decode the password</param>
        /// <returns>Encoded password</returns>
        private static string EncryptDecrypt(string password, bool encrypt)
        {
            // Convert the password string to a character array
            var pwdChars = password.ToCharArray();
            var pwdCharsAdj = new List<char>();

            var pwdBytes = pwdChars.Select(t => (byte)t).ToList();

            var modTest = encrypt ? 1 : 0;

            // Modify the byte array by shifting alternating bytes up or down and converting back to char
            for (var index = 0; index < pwdBytes.Count; index++)
            {
                if (index % 2 == modTest)
                {
                    pwdBytes[index]++;
                }
                else
                {
                    pwdBytes[index]--;
                }

                pwdCharsAdj.Add((char)pwdBytes[index]);
            }

            return string.Join("", pwdCharsAdj);
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

        private static SHA1Managed _hashProvider;
        public static string GenerateSha1Hash(string filePath)
        {
            byte[] fileHash;

            var fi = new FileInfo(filePath);

            if (!fi.Exists)
            {
                throw new FileNotFoundException("File not found in GenerateSha1Hash: " + filePath);
            }

            if (_hashProvider == null)
            {
                _hashProvider = new SHA1Managed();
            }

            using (var sourceFile = new FileStream(fi.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                fileHash = _hashProvider.ComputeHash(sourceFile);
            }

            var hashString = ToHexString(fileHash);

            return hashString;
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
        /// Lookup the integer value associated with the given key in the dictionary
        /// </summary>
        /// <param name="dictionary">Dictionary</param>
        /// <param name="keyName">Key name</param>
        /// <param name="valueIfMissingOrNull">Int-64 value to return if the dictionary value is missing or null</param>
        /// <returns>Value for the given key, or valueIfMissingOrNull if the value is missing or null</returns>
        // ReSharper disable once UnusedMember.Global
        public static long GetDictionaryValue(IReadOnlyDictionary<string, object> dictionary, string keyName, long valueIfMissingOrNull)
        {
            if (!dictionary.TryGetValue(keyName, out var itemValue))
            {
                return valueIfMissingOrNull;
            }

            if (itemValue != null)
            {
                var itemValueString = itemValue.ToString();
                if (long.TryParse(itemValueString, out var itemValueNumber))
                {
                    return itemValueNumber;
                }
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
        public static string GetDictionaryValue(IReadOnlyDictionary<string, object> dictionary, string keyName, string valueIfMissingOrNull = "")
        {
            if (!dictionary.TryGetValue(keyName, out var itemValue))
            {
                return valueIfMissingOrNull;
            }

            if (itemValue is string itemValueString)
            {
                return itemValueString;
            }

            return valueIfMissingOrNull;
        }

        public static List<FileInfoObject> GetFileListFromMetadataObject(List<Dictionary<string, object>> metadataObject)
        {
            var fileList = new List<FileInfoObject>();
            foreach (var item in metadataObject)
            {
                if (item.TryGetValue("destinationTable", out var destTable))
                {
                    var t = (string)destTable;
                    if (string.Equals(t, "files", StringComparison.OrdinalIgnoreCase))
                    {
                        fileList.Add(new FileInfoObject(
                            (string)item["absolutelocalpath"],
                            (string)item["subdir"],
                            (string)item["hashsum"]
                        ));
                    }
                }
            }

            return fileList;
        }

        public static DirectoryInfo GetTempDirectory(Configuration config)
        {
            if (!string.IsNullOrEmpty(config.LocalTempDirectory))
            {
                return new DirectoryInfo(config.LocalTempDirectory);
            }

            return new DirectoryInfo(Path.GetTempPath());
        }

        public static string ToHexString(byte[] buffer)
        {
            return BitConverter.ToString(buffer).Replace("-", string.Empty).ToLower();
        }

        public static Dictionary<string, object> JsonToObject(string jsonString)
        {
            var jso = (JsonObject)JsonConvert.Import(jsonString);
            return JsonObjectToDictionary(jso);
        }

        public static string ObjectToJson(IList metadataList)
        {
            if (metadataList == null)
            {
                return string.Empty;
            }

            var jso = new JsonArray(metadataList);
            return jso.ToString();
        }

        public static Dictionary<string, object> JsonObjectToDictionary(JsonObject jso)
        {
            var settingsDictionary = new Dictionary<string, object>();

            if (jso == null)
            {
                ConsoleMsgUtils.ShowWarning("Skipping null item in JsonObjectToDictionary");
                return settingsDictionary;
            }

            foreach (string key in jso.Names)
            {
                if (jso[key] == null)
                {
                    jso[key] = string.Empty;
                }

                var value = jso[key];
                if (value.GetType().Name == "JsonObject")
                {
                    var tmpJso = value as JsonObject;
                    settingsDictionary.Add(key, JsonObjectToDictionary(tmpJso));  //Recurse!
                }
                else if (value.GetType().Name == "JsonArray")
                {
                    try
                    {
                        var tmpJsa = value as JsonArray;
                        switch (key)
                        {
                            case "users":
                                // EUS User IDs are always integers
                                settingsDictionary.Add(key, JsonArrayToIntList(tmpJsa));
                                break;

                            case "proposals":
                                // EUS Projects (aka proposals) are usually integers, but not always
                                // Thus, store as strings
                                settingsDictionary.Add(key, JsonArrayToStringList(tmpJsa));
                                break;

                            default:
                                if (tmpJsa == null || tmpJsa.Count == 0)
                                {
                                    settingsDictionary.Add(key, new List<Dictionary<string, object>>());
                                }
                                else
                                {
                                    var nextValue = tmpJsa.GetValue(0);
                                    if (nextValue == null)
                                    {
                                        settingsDictionary.Add(key, new List<Dictionary<string, object>>());
                                    }
                                    else
                                    {
                                        var typeName = nextValue.GetType().Name;

                                        if (typeName == "String" || typeName == "JsonNumber")
                                        {
                                            settingsDictionary.Add(key, JsonArrayToStringList(tmpJsa));
                                        }
                                        else
                                        {
                                            settingsDictionary.Add(key, JsonArrayToDictionaryList(tmpJsa));
                                        }
                                    }
                                }
                                break;
                        }
                    }
                    catch (Exception ex)
                    {
                        ConsoleMsgUtils.ShowWarning("Error in parsing a JsonArray in JsonObjectToDictionary:" + ex.Message);
                    }
                }
                else
                {
                    // Not a JsonObject or a JsonArray
                    // Store it as-is
                    settingsDictionary.Add(key, value);
                }
            }

            return settingsDictionary;
        }

        public static List<string> JsonArrayToStringList(JsonArray jsa)
        {
            var jsonStrings = new List<string>();

            while (jsa.Length > 0)
            {
                var value = jsa.Pop();
                var typeName = value.GetType().Name;
                if (typeName == "JsonNumber" || typeName == "String")
                {
                    jsonStrings.Add(value.ToString());
                }
                else
                {
                    throw new InvalidCastException("JsonArrayToStringList cannot process an item of type " + typeName);
                }
            }

            return jsonStrings;
        }

        public static List<int> JsonArrayToIntList(JsonArray jsa)
        {
            var jsonStrings = JsonArrayToStringList(jsa);
            var parsedIntegers = new List<int>();

            foreach (var jsonItem in jsonStrings)
            {
                if (int.TryParse(jsonItem, out var value))
                {
                    parsedIntegers.Add(value);
                }
                else
                {
                    throw new InvalidCastException("JsonArrayToIntList cannot convert item '" + value + "' to an integer");
                }
            }

            return parsedIntegers;
        }

        public static List<Dictionary<string, object>> JsonArrayToDictionaryList(JsonArray jsa)
        {
            var parsedItems = new List<Dictionary<string, object>>();
            while (jsa.Length > 0)
            {
                var value = jsa.Pop();
                if (value.GetType().Name == "JsonNumber")
                {
                    var parsedValue = new Dictionary<string, object> {
                        { value.ToString(), string.Empty}
                    };
                    parsedItems.Add(parsedValue);
                }
                else if (value.GetType().Name == "String")
                {
                    var parsedValue = new Dictionary<string, object> {
                        { value.ToString(), string.Empty}
                    };
                    parsedItems.Add(parsedValue);
                }
                else if (value.GetType().Name == "JsonObject")
                {
                    var jso = (JsonObject)value;
                    parsedItems.Add(JsonObjectToDictionary(jso));
                }
                else
                {
                    ConsoleMsgUtils.ShowWarning(
                        "Unsupported JsonArrayList type {0}; will not be stored in the dictionary returned by JsonArrayToDictionaryList",
                        value.GetType().Name);
                }
            }
            return parsedItems;
        }

        public static string GetMetadataFilenameForJob(string jobNumber)
        {
            if (string.IsNullOrWhiteSpace(jobNumber))
            {
                return "MyEMSL_metadata_CaptureJob_000000.txt";
            }

            return "MyEMSL_metadata_CaptureJob_" + jobNumber + ".txt";
        }

        public static string GetUserName(bool cleanDomain = false)
        {
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
        /// <param name="sender"></param>
        /// <param name="cert"></param>
        /// <param name="chain"></param>
        /// <param name="policyErrors"></param>
        /// <param name="errorMessage"></param>
        /// <returns>True if the server is trusted</returns>
        [Obsolete("Use the version with two arguments")]
        public static bool ValidateRemoteCertificate(
            object sender,
            X509Certificate cert,
            X509Chain chain,
            SslPolicyErrors policyErrors,
            out string errorMessage)
        {
            return ValidateRemoteCertificate(cert, out errorMessage);
        }

        /// <summary>
        /// Callback used to validate the certificate in an SSL conversation
        /// </summary>
        /// <param name="cert"></param>
        /// <param name="errorMessage"></param>
        /// <returns>True if the server is trusted</returns>
        public static bool ValidateRemoteCertificate(
            X509Certificate cert,
            out string errorMessage)
        {
            var trustedDomains = new List<string>
            {
                "my.emsl.pnnl.gov",
                "my.emsl.pnl.gov",
                "emsl.pnl.gov",
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

            var message = new StringBuilder();
            message.Append("The domain name associated with the certificate is not trusted: " + domainToValidate);
            ConsoleMsgUtils.ShowWarning(message.ToString());

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
