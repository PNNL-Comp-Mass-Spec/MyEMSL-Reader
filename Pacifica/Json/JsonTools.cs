using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Pacifica.Json
{
    public static class JsonTools
    {
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

        public static MyEMSLTaskStatus JsonToTaskStatus(string jsonString)
        {
            return JsonConvert.DeserializeObject<MyEMSLTaskStatus>(jsonString, new JsonSerializerSettings { ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor });
        }

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
