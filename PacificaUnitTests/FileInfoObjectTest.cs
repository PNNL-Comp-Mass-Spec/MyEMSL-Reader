using System;

namespace PacificaUnitTests
{
    public class FileInfoObjectTest
    {
        // Ignore Spelling: Pacifica, Sha, Utc

        /// <summary>
        /// Full path to the local file
        /// </summary>
        public string AbsoluteLocalPath { get; }

        /// <summary>
        /// Relative destination directory, with Unix-style slashes
        /// </summary>
        public string RelativeDestinationDirectory { get; }

        /// <summary>
        /// Filename
        /// </summary>
        public string FileName { get; }

        /// <summary>
        /// SHA-1 hash of the file
        /// </summary>
        public string Sha1HashHex { get; }

        /// <summary>
        /// File size, in bytes
        /// </summary>
        public long FileSizeInBytes { get; }

        /// <summary>
        /// File creation time
        /// </summary>
        public DateTime CreationTime { get; }

        /// <summary>
        /// File creation time
        /// </summary>
        public string CreationTimeUtc { get; }

        /// <summary>
        /// File last write time
        /// </summary>
        public DateTime LastWriteTime { get; }

        /// <summary>
        /// File last write time
        /// </summary>
        public string LastWriteTimeUtc { get; }

        public FileInfoObjectTest(string fileName, string absoluteLocalPath, string relativeDestinationDirectory, long fileSizeInBytes, string sha1HashHex, DateTime creationTime, DateTime lastWriteTime)
        {
            AbsoluteLocalPath = absoluteLocalPath;
            RelativeDestinationDirectory = relativeDestinationDirectory;
            FileName = fileName;
            Sha1HashHex = sha1HashHex;
            FileSizeInBytes = fileSizeInBytes;
            CreationTime = creationTime;
            LastWriteTime = lastWriteTime;
            CreationTimeUtc = creationTime.ToUniversalTime().ToString("s");
            LastWriteTimeUtc = lastWriteTime.ToUniversalTime().ToString("s");
        }

        public FileInfoObjectTest(string fileName, string absoluteLocalPath, string relativeDestinationDirectory, long fileSizeInBytes, string sha1HashHex, string creationTime, string lastWriteTime)
        {
            AbsoluteLocalPath = absoluteLocalPath;
            RelativeDestinationDirectory = relativeDestinationDirectory;
            FileName = fileName;
            Sha1HashHex = sha1HashHex;
            FileSizeInBytes = fileSizeInBytes;
            CreationTimeUtc = creationTime;
            LastWriteTimeUtc = lastWriteTime;
            CreationTime = DateTime.Parse(creationTime).ToLocalTime();
            LastWriteTime = DateTime.Parse(lastWriteTime).ToLocalTime();
        }
    }
}
