using Newtonsoft.Json;

namespace Pacifica.Json
{
    /// <summary>
    /// IUploadMetadata implementation for value entries (no key)
    /// </summary>
    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public class UploadMetadataValue : IUploadMetadata
    {
        // Ignore Spelling: deserialization

        /// <summary>
        /// Prefix for DestinationTable
        /// </summary>
        public const string DestinationTableBaseName = "Transactions.";

        /// <summary>
        /// Destination table
        /// </summary>
        [JsonProperty("destinationTable", Required = Required.Always)]
        public string DestinationTable { get; private set; }

        /// <summary>
        /// True if the data meets the validity requirements of the metadata type
        /// </summary>
        public bool Valid => DestinationTable.Length > DestinationTableBaseName.Length && !string.IsNullOrWhiteSpace(Value?.ToString());

        /// <summary>
        /// Value
        /// </summary>
        [JsonProperty("value", Required = Required.Always)]
        public object Value { get; private set; }

        /// <summary>
        /// Default constructor used by JSON deserialization
        /// </summary>
        [JsonConstructor]
        private UploadMetadataValue() { }

        /// <summary>
        /// Constructor for output preparation
        /// </summary>
        /// <param name="columnName">Column name, without the prefix supplied by <see cref="DestinationTableBaseName"/></param>
        /// <param name="value">Value for this entry</param>
        public UploadMetadataValue(string columnName, object value)
        {
            DestinationTable = DestinationTableBaseName + columnName;
            Value = value;
        }

        /// <inheritdoc cref="object.ToString"/>
        public override string ToString()
        {
            return $"{DestinationTable}: {Value}";
        }
    }
}
