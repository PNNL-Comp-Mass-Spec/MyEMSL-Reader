namespace Pacifica.Json
{
    /// <summary>
    /// Interface for upload metadata
    /// </summary>
    public interface IUploadMetadata
    {
        // Ignore Spelling: Json, Pacifica

        /// <summary>
        /// All entries must contain a value for destination table
        /// </summary>
        string DestinationTable { get; }

        /// <summary>
        /// True if the data meets the validity requirements of the metadata type
        /// </summary>
        bool Valid { get; }
    }
}
