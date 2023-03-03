using System;
using Newtonsoft.Json;
using Pacifica.Core;

namespace Pacifica.Json
{
    /// <summary>
    /// IUploadMetadata implementation for file entries
    /// </summary>
    [JsonObject(MemberSerialization = MemberSerialization.OptIn)]
    public class UploadMetadataFile : IUploadMetadata
    {
        // Ignore Spelling: absolutelocalpath, ctime, deserialization, hashsum, hashtype, mimetype, mtime, subdir

        /// <summary>
        /// Destination table name used for all 'File' entries
        /// </summary>
        public const string DestinationTableName = "Files";

        /// <summary>
        /// Destination table
        /// </summary>
        [JsonProperty("destinationTable", Required = Required.Always)]
        public string DestinationTable => DestinationTableName;

        /// <summary>
        /// True if the data meets the validity requirements of the metadata type
        /// </summary>
        public bool Valid => !string.IsNullOrWhiteSpace(Name) && !string.IsNullOrWhiteSpace(Size) && !Size.Equals("0") && !string.IsNullOrWhiteSpace(HashSum);

        /// <summary>
        /// File name
        /// </summary>
        [JsonProperty("name", Required = Required.Always)]
        public string Name { get; private set; }

        // ReSharper disable once StringLiteralTypo

        /// <summary>
        /// Absolute path to the local file
        /// </summary>
        [JsonProperty("absolutelocalpath", Required = Required.Always)]
        public string AbsoluteLocalPath { get; private set; }

        /// <summary>
        /// File subdirectory path (from dataset base directory)
        /// </summary>
        [JsonProperty("subdir", Required = Required.Always)]
        public string SubDir { get; private set; }

        /// <summary>
        /// File size
        /// </summary>
        [JsonProperty("size", Required = Required.Always)]
        public string Size { get; private set; }

        /// <summary>
        /// File hash sum, using algorithm specified by <see cref="HashType"/>
        /// </summary>
        [JsonProperty("hashsum", Required = Required.Always)]
        public string HashSum { get; private set; }

        /// <summary>
        /// File MIME type (binary file)
        /// </summary>
        [JsonProperty("mimetype", Required = Required.Always)]
        public string MimeType => "application/octet-stream";

        /// <summary>
        /// Hash algorithm used for <see cref="HashSum"/>
        /// </summary>
        [JsonProperty("hashtype", Required = Required.Always)]
        public string HashType => "sha1";

        /// <summary>
        /// File creation time (UTC)
        /// </summary>
        [JsonProperty("ctime", Required = Required.Always)]
        public DateTime FileCreationTimeUtc { get; private set; }

        /// <summary>
        /// File last modified time (UTC)
        /// </summary>
        [JsonProperty("mtime", Required = Required.Always)]
        public DateTime FileLastModifiedTimeUtc { get; private set; }

        /// <summary>
        /// Default constructor used by JSON deserialization
        /// </summary>
        [JsonConstructor]
        private UploadMetadataFile() { }

        /// <summary>
        /// Constructor for output preparation
        /// </summary>
        /// <param name="file">File object supplying necessary metadata</param>
        /// <param name="subDir">Dataset subdirectory path that contains the file</param>
        public UploadMetadataFile(FileInfoObject file, string subDir)
        {
            Name = file.FileName;
            AbsoluteLocalPath = file.AbsoluteLocalPath;
            SubDir = subDir;
            Size = file.FileSizeInBytes.ToString();
            HashSum = file.Sha1HashHex;
            // Note: older implementation added '.ToString("s")' below, but NewtonSoft JSON automatically formats the dates the same way,
            // while not re-formatting that way when reading JSON back in to a string (because it converts to DateTime first)
            FileCreationTimeUtc = file.CreationTime.ToUniversalTime();
            FileLastModifiedTimeUtc = file.LastWriteTime.ToUniversalTime();
        }

        /// <inheritdoc cref="object.ToString"/>
        public override string ToString()
        {
            return $"{DestinationTable}: {Name}, {SubDir}, {HashSum}";
        }

        /// <summary>
        /// Method for the round-trip/upgrade JSON testing
        /// </summary>
        /// <param name="name"></param>
        /// <param name="absoluteLocalPath"></param>
        /// <param name="subDir"></param>
        /// <param name="size"></param>
        /// <param name="hashSum"></param>
        /// <param name="fileCreationTime"></param>
        /// <param name="fileLastModifiedTime"></param>
        public UploadMetadataFile(string name, string absoluteLocalPath, string subDir, string size, string hashSum, string fileCreationTime, string fileLastModifiedTime)
        {
            Name = name;
            AbsoluteLocalPath = absoluteLocalPath;
            SubDir = subDir;
            Size = size;
            HashSum = hashSum;
            FileCreationTimeUtc = DateTime.Parse(fileCreationTime);
            FileLastModifiedTimeUtc = DateTime.Parse(fileLastModifiedTime);
        }
    }
}
