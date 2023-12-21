using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Pacifica.Json
{
    /// <summary>
    /// Methods for converting between objects and JSON text
    /// </summary>
    public static class JsonTools
    {
        // Ignore Spelling: JSON, Pacifica

        /// <summary>
        /// Convert JSON text into a list of <see cref="MyEMSLFileInfo"/> objects
        /// </summary>
        /// <param name="jsonString"></param>
        /// <param name="dataUrl"></param>
        /// <param name="callingMethodName"></param>
        /// <param name="errorMessage"></param>
        public static List<MyEMSLFileInfo> JsonToFileList(string jsonString, string dataUrl, string callingMethodName, out string errorMessage)
        {
            try
            {
                errorMessage = "";
                return JsonConvert.DeserializeObject<List<MyEMSLFileInfo>>(jsonString);
            }
            catch (Exception e)
            {
                errorMessage = "Could not convert the JSON string from " + dataUrl + " to a FileList (" + callingMethodName + "): " + e.Message;
                return null;
            }
        }

        /// <summary>
        /// Convert JSON text into a list of <see cref="IUploadMetadata"/>-implementing objects
        /// </summary>
        /// <param name="jsonString"></param>
        /// <param name="dataUrl"></param>
        /// <param name="callingMethodName"></param>
        /// <param name="errorMessage"></param>
        // ReSharper disable once UnusedMember.Global
        public static List<IUploadMetadata> JsonToUploadMetadata(string jsonString, string dataUrl, string callingMethodName, out string errorMessage)
        {
            try
            {
                errorMessage = "";
                return JsonConvert.DeserializeObject<List<IUploadMetadata>>(jsonString, new JsonSerializerSettings
                {
                    ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
                    Converters = new List<JsonConverter> { new UploadMetadataConverter() }
                });
            }
            catch (Exception e)
            {
                errorMessage = "Could not convert the JSON string from " + dataUrl + " to a FileList (" + callingMethodName + "): " + e.Message;
                return null;
            }
        }

        /// <summary>
        /// Convert JSON text into a <see cref="MyEMSLTaskStatus"/> object
        /// </summary>
        /// <param name="jsonString"></param>
        public static MyEMSLTaskStatus JsonToTaskStatus(string jsonString)
        {
            return JsonConvert.DeserializeObject<MyEMSLTaskStatus>(jsonString, new JsonSerializerSettings { ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor });
        }

        /// <summary>
        /// Convert a list of <see cref="IUploadMetadata"/>-implementing objects into JSON text
        /// </summary>
        /// <param name="metadataList"></param>
        /// <param name="fileLimit">If greater than zero, only include this many files in the JSON returned by this method</param>
        public static string UploadMetadataToJson(IReadOnlyList<IUploadMetadata> metadataList, int fileLimit = 0)
        {
            if (metadataList == null)
            {
                return string.Empty;
            }

            if (fileLimit <= 0)
            {
                return JsonConvert.SerializeObject(metadataList);
            }

            var filteredList = new List<IUploadMetadata>();
            var filesAdded = 0;

            foreach (var item in metadataList)
            {
                if (!item.DestinationTable.Equals("Files"))
                {
                    filteredList.Add(item);
                    continue;
                }

                if (filesAdded >= fileLimit)
                    continue;

                filteredList.Add(item);
                filesAdded++;
            }

            return JsonConvert.SerializeObject(filteredList);
        }

        /// <summary>
        /// Converts JSON string into a list of dictionaries
        /// </summary>
        /// <param name="jsonString"></param>
        /// <param name="dataUrl"></param>
        /// <param name="callingMethodName"></param>
        /// <param name="errorMessage"></param>
        // ReSharper disable once UnusedMember.Global
        public static List<Dictionary<string, object>> JsonToDictionaryList(string jsonString, string dataUrl, string callingMethodName, out string errorMessage)
        {
            try
            {
                errorMessage = "";
                return JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(jsonString);
            }
            catch (Exception e)
            {
                errorMessage = "Could not convert the JSON string from " + dataUrl + " to a DictionaryList (" + callingMethodName + "): " + e.Message;
                return null;
            }
        }

        /// <summary>
        /// Converts JSON string to a dictionary
        /// </summary>
        /// <param name="jsonString"></param>
        // ReSharper disable once UnusedMember.Global
        public static Dictionary<string, object> JsonToObject(string jsonString)
        {
            return JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonString);
        }

        /// <summary>
        /// Converts a list of dictionaries to a JSON string
        /// </summary>
        /// <param name="metadataList"></param>
        // ReSharper disable once UnusedMember.Global
        public static string ObjectToJson(IList metadataList)
        {
            if (metadataList == null)
            {
                return string.Empty;
            }

            return JsonConvert.SerializeObject(metadataList);
        }
    }
}
