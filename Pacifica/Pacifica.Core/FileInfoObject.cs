using System;
using System.Collections.Generic;
using System.IO;
using PRISM;

namespace Pacifica.Core
{
    public class FileInfoObject
    {
        /// <summary>
        /// Constructor and takes a file path
        /// </summary>
        /// <param name="filePath">Full path to the local file</param>
        /// <param name="baseDSPath">Base dataset folder path</param>
        /// <remarks>
        /// Instantiate a new FileInfoObject, including computing the SHA-1 hash of the file
        /// </remarks>
        // ReSharper disable once UnusedMember.Global
        [Obsolete("Use the constructor that takes a FileInfo object, which has better compatibility for path lengths over 255 characters")]
        public FileInfoObject(string filePath, string baseDSPath)
        {
            File = new FileInfo(filePath);
            AbsoluteLocalPath = File.FullName;
            mRelativeDestinationDirectory = GetRelativeDestinationDirectory(File, baseDSPath);
            Sha1HashHex = Utilities.GenerateSha1Hash(filePath);
        }

        /// <summary>
        /// Constructor that takes a FileInfo object
        /// </summary>
        /// <param name="fileToAdd">File info</param>
        /// <param name="baseDSPath">Base dataset folder path</param>
        /// <param name="fileTools">FileTools object, which is used to copy files with long paths to the local temp directory</param>
        /// <remarks>
        /// Instantiate a new FileInfoObject, including computing the SHA-1 hash of the file
        /// </remarks>
        // ReSharper disable once UnusedMember.Global
        public FileInfoObject(FileInfo fileToAdd, string baseDSPath, FileTools fileTools)
        {
            File = fileToAdd;
            AbsoluteLocalPath = fileToAdd.FullName;
            mRelativeDestinationDirectory = GetRelativeDestinationDirectory(fileToAdd, baseDSPath);
            Sha1HashHex = Utilities.GenerateSha1Hash(fileToAdd, fileTools);
        }

        /// <summary>
        /// Instantiate a new FileInfoObject; auto-computes the SHA-1 hash if sha1Hash is blank or is not exactly 40 characters long
        /// </summary>
        /// <param name="fileToAdd">File info</param>
        /// <param name="relativeDestinationDirectory">Folder in archive in which to store the file; empty string means to store in the dataset folder</param>
        /// <param name="sha1Hash">SHA-1 hash for the file; if blank, the hash will be auto-computed</param>
        /// <param name="fileTools">FileTools object, which is used to copy files with long paths to the local temp directory</param>
        public FileInfoObject(FileInfo fileToAdd, string relativeDestinationDirectory, string sha1Hash, FileTools fileTools)
        {
            File = fileToAdd;
            AbsoluteLocalPath = fileToAdd.FullName;

            if (string.IsNullOrWhiteSpace(relativeDestinationDirectory))
            {
                relativeDestinationDirectory = string.Empty;
            }

            mRelativeDestinationDirectory = relativeDestinationDirectory;

            if (!string.IsNullOrWhiteSpace(mRelativeDestinationDirectory) && Path.IsPathRooted(mRelativeDestinationDirectory))
            {
                throw new ArgumentException("Relative Destination Directory cannot be rooted; it must be relative: " +
                                            mRelativeDestinationDirectory + " for " + fileToAdd.FullName);
            }

            if (!string.IsNullOrWhiteSpace(sha1Hash) && sha1Hash.Length == 40)
            {
                Sha1HashHex = sha1Hash;
            }
            else
            {
                Sha1HashHex = Utilities.GenerateSha1Hash(fileToAdd, fileTools);
            }
        }

        /// <summary>
        /// File info
        /// </summary>
        public FileInfo File { get; }

        /// <summary>
        /// Full path to the local file
        /// </summary>
        public string AbsoluteLocalPath
        {
            get;
        }

        private readonly string mRelativeDestinationDirectory;

        /// <summary>
        /// Relative destination directory, with Unix-style slashes
        /// </summary>
        public string RelativeDestinationDirectory => ConvertWindowsPathToUnix(mRelativeDestinationDirectory);

        /// <summary>
        ///  Relative destination path, with Unix-style slashes
        /// </summary>
        public string RelativeDestinationFullPath
        {
            get
            {
                string fileName;
                if (!string.IsNullOrWhiteSpace(DestinationFileName))
                {
                    fileName = DestinationFileName;
                }
                else
                {
                    fileName = FileName;
                }
                var fullPath = Path.Combine(RelativeDestinationDirectory, fileName);
                return ConvertWindowsPathToUnix(fullPath);
            }
        }

        /// <summary>
        /// Optional alternate name to use for the file
        /// </summary>
        public string DestinationFileName { get; set; }

        /// <summary>
        /// Filename
        /// </summary>
        public string FileName => File.Name;

        /// <summary>
        /// SHA-1 hash of the file
        /// </summary>
        public string Sha1HashHex
        {
            get;
        }

        /// <summary>
        /// File size, in bytes
        /// </summary>
        public long FileSizeInBytes => File.Length;

        /// <summary>
        /// File creation time
        /// </summary>
        public DateTime CreationTime => File.CreationTime;

        /// <summary>
        /// File last write time
        /// </summary>
        public DateTime LastWriteTime => File.LastWriteTime;

        /// <summary>
        /// Converts a windows path of the form \\proto-7\VOrbi05\2013_2\QC_Shew_13_02_500ng_15May13_Lynx_12-12-04\metadata.xml
        /// to the Linux form proto-7/VOrbi05/2013_2/QC_Shew_13_02_500ng_15May13_Lynx_12-12-04/metadata.xml
        /// </summary>
        /// <param name="path">Unix-style path</param>
        /// <remarks>Removes any leading slashes</remarks>
        private string ConvertWindowsPathToUnix(string path)
        {
            return path.Replace(@"\", "/").TrimStart('/');
        }

        private string GetRelativeDestinationDirectory(FileInfo file, string baseDSPath)
        {
            if (file.Directory == null)
            {
                return string.Empty;
            }

            var relativeDestinationDirectory = GenerateRelativePath(file.Directory.FullName, baseDSPath);

            if (!string.IsNullOrWhiteSpace(mRelativeDestinationDirectory) && Path.IsPathRooted(mRelativeDestinationDirectory))
            {
                throw new ArgumentException(
                    "The relative destination directory returned from GenerateRelativePath is rooted; it must be relative: " +
                    mRelativeDestinationDirectory + " for " + file.FullName);
            }

            return relativeDestinationDirectory;
        }

        /// <summary>
        /// Return the SHA-1 hash
        /// </summary>
        public override string ToString()
        {
            if (string.IsNullOrWhiteSpace(Sha1HashHex))
            {
                return AbsoluteLocalPath;
            }

            return Sha1HashHex.Substring(0, 8) + ": " + AbsoluteLocalPath;
        }

        public static string GenerateRelativePath(string absoluteLocalPath, string basePath)
        {
            if (absoluteLocalPath.StartsWith(basePath, StringComparison.OrdinalIgnoreCase))
            {
                return absoluteLocalPath.Substring(basePath.Length).TrimStart('/', '\\');
            }

            throw new InvalidDataException("Cannot generate relative path in GenerateRelativePath since local path (" + absoluteLocalPath + ") does not contain base path (" + basePath + ")");
        }
    }
}