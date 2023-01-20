using Newtonsoft.Json;

namespace Pacifica.Json
{
    /// <summary>
    /// IUploadMetadata implementation for key-value entries
    /// </summary>
    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public class UploadMetadataKeyValue : IUploadMetadata
    {
        /// <summary>
        /// Destination table name used for all 'key-value' entries
        /// </summary>
        public const string DestinationTableName = "TransactionKeyValue";

        /// <summary>
        /// Destination table
        /// </summary>
        [JsonProperty("destinationTable", Required = Required.Always)]
        public string DestinationTable => DestinationTableName;

        /// <summary>
        /// True if the data meets the validity requirements of the metadata type
        /// </summary>
        public bool Valid => !string.IsNullOrWhiteSpace(Key) && !string.IsNullOrWhiteSpace(Value.ToString());

        /// <summary>
        /// Entry key/name
        /// </summary>
        [JsonProperty("key", Required = Required.Always)]
        public string Key { get; private set; }

        /// <summary>
        /// Entry value
        /// </summary>
        [JsonProperty("value", Required = Required.Always)]
        public object Value { get; private set; }

        /// <summary>
        /// Default constructor used by JSON deserialization
        /// </summary>
        [JsonConstructor]
        private UploadMetadataKeyValue() { }

        /// <summary>
        /// Constructor for output preparation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public UploadMetadataKeyValue(string key, object value)
        {
            Key = key;
            Value = value;
        }

        /// <inheritdoc cref="object.ToString"/>
        public override string ToString()
        {
            return $"{DestinationTable}: {Key} : {Value}";
        }
    }
}
