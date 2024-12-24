using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Pacifica.Json
{
    // Ignore Spelling: Json, Pacifica, serializer

    /// <summary>
    /// Custom JSON converter handler to populate the correct objects when reading from JSON
    /// </summary>
    internal class UploadMetadataConverter : JsonConverter
    {
        /// <inheritdoc cref="JsonConverter.WriteJson"/>>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            //use the default serialization - it works fine
            serializer.Serialize(writer, value);
        }

        /// <inheritdoc cref="JsonConverter.ReadJson"/>>
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            // Load the JSON object from the reader
            var jo = JObject.Load(reader);

            // Read the value of 'destinationTable' (mandatory entry)
            var destTable = jo["destinationTable"]?.Value<string>() ?? "";

            // Check if it's a file entry
            if (destTable.Equals(UploadMetadataFile.DestinationTableName))
            {
                return jo.ToObject<UploadMetadataFile>(serializer);
            }

            // Check if it's a key-value entry
            if (destTable.Equals(UploadMetadataKeyValue.DestinationTableName))
            {
                return jo.ToObject<UploadMetadataKeyValue>(serializer);
            }

            // Minimum: entry with just destinationTable and value
            return jo.ToObject<UploadMetadataValue>(serializer);
        }

        /// <inheritdoc cref="JsonConverter.CanConvert"/>>
        public override bool CanConvert(Type objectType)
        {
            // This is specifically for choosing between implementations of IUploadMetadata
            // Only return true when the target type is IUploadMetadata
            // Return false when the target type is an implementation of IUploadMetadata to prevent endless, useless recursion that leads to StackOverflowException
            return objectType == typeof(IUploadMetadata);
        }
    }
}
