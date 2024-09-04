#if NET48
using System;
using System.Collections;
using System.Collections.Generic;
using Jayrock.Json;
using Jayrock.Json.Conversion;
using Pacifica.Json;
using PRISM;

namespace PacificaUnitTests
{
    /// <summary>
    /// This class is used to compare results between Jayrock.Json (old JSON library) and Newtonsoft.Json (new JSON library)
    /// </summary>
    public static class JayrockJson_Backup
    {
        // Ignore Spelling: ctime, hashsum, hashtype, Jayrock, js, JSON, mtime, Pacifica, subdir

        public static MyEMSLTaskStatus JsonToTaskStatus(string statusResult)
        {
            var statusJSON = JsonToObject(statusResult);

            var taskId = -1;

            if (statusJSON.TryGetValue("job_id", out var value) && int.TryParse(value.ToString(), out var number))
            {
                taskId = number;
            }

            var state = GetDictionaryValue(statusJSON, "state");
            var currentTask = GetDictionaryValue(statusJSON, "task");
            var exception = GetDictionaryValue(statusJSON, "exception");
            var percentCompleteText = GetDictionaryValue(statusJSON, "task_percent");

            return new MyEMSLTaskStatus(taskId, state, currentTask, percentCompleteText, exception);
        }

        public static List<MyEMSLFileInfo> JsonToFileList(string jsonString, out string jsonError)
        {
            var files = new List<MyEMSLFileInfo>();

            // ReSharper disable once StringLiteralTypo

            // Convert the response to a dictionary
            var remoteFileInfoList = ParseJsonToDictionaryList(jsonString, "", "JayrockJson_Backup.JsonToFileInfo", out jsonError);

            if (remoteFileInfoList is null)
            {
                return files;
            }

            foreach (var fileObj in remoteFileInfoList)
            {
                var fileName = GetDictionaryValue(fileObj, "name");
                var fileId = GetDictionaryValue(fileObj, "_id", 0);
                var fileHash = GetDictionaryValue(fileObj, "hashsum");

                var file = new MyEmslFileInfoTest(fileName, fileId, fileHash)
                {
                    SubDir = GetDictionaryValue(fileObj, "subdir"),
                    DatasetYearQuarter = string.Empty,
                    Size = GetDictionaryValue(fileObj, "size", 0),
                    HashType = GetDictionaryValue(fileObj, "hashtype"),
                    TransactionId = GetDictionaryValue(fileObj, "transaction_id", 0)
                };

                var created = GetDictionaryValue(fileObj, "created");
                var updated = GetDictionaryValue(fileObj, "updated");
                var deleted = GetDictionaryValue(fileObj, "deleted");

                file.UpdateRemoteFileTimes(created, updated, deleted);

                var creationTime = GetDictionaryValue(fileObj, "ctime");
                var lastWriteTime = GetDictionaryValue(fileObj, "mtime");

                file.UpdateSourceFileTimes(creationTime, lastWriteTime);

                files.Add(file);
            }

            return files;
        }

        public static List<Dictionary<string, object>> ParseJsonToDictionaryList(string jsonString, string dataUrl, string callingMethodName, out string errorMessage)
        {
            errorMessage = "";
            // Convert the response to a dictionary
            var jsonData = JsonConvert.Import(jsonString);

            if (jsonData is not JsonArray jsArray)
            {
                var errMsg = "Could not convert the JSON string from " + dataUrl + " to a JsonArray (" + callingMethodName + ")";

                if (jsonData is string conversionError && !string.IsNullOrWhiteSpace(conversionError))
                {
                    if (conversionError.Length > 100)
                    {
                        errorMessage = errMsg + ": " + conversionError.Substring(0, 100) + " ...";
                    }
                    else
                    {
                        errorMessage = errMsg + ": " + conversionError;
                    }
                }
                else
                {
                    errorMessage = errMsg;
                }

                return null;
            }

            var data = JsonArrayToDictionaryList(jsArray);
            return data;
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

        public static Dictionary<string, object> JsonObjectToDictionary(JsonObject jsObject)
        {
            var settingsDictionary = new Dictionary<string, object>();

            if (jsObject == null)
            {
                ConsoleMsgUtils.ShowWarning("Skipping null item in JsonObjectToDictionary");
                return settingsDictionary;
            }

            foreach (string key in jsObject.Names)
            {
                jsObject[key] ??= string.Empty;

                var value = jsObject[key];

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

        public static List<string> JsonArrayToStringList(JsonArray jsArray)
        {
            var jsonStrings = new List<string>();

            while (jsArray.Length > 0)
            {
                var value = jsArray.Pop();
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

        public static List<int> JsonArrayToIntList(JsonArray jsArray)
        {
            var jsonStrings = JsonArrayToStringList(jsArray);
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

        public static List<Dictionary<string, object>> JsonArrayToDictionaryList(JsonArray jsArray)
        {
            var parsedItems = new List<Dictionary<string, object>>();

            while (jsArray.Length > 0)
            {
                var value = jsArray.Pop();

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
    }
}
#endif
