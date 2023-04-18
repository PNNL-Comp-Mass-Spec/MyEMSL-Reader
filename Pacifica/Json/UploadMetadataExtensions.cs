using Pacifica.Core;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Pacifica.Json
{
    /// <summary>
    /// Some convenience methods for working with IUploadMetadata
    /// </summary>
    public static class UploadMetadataExtensions
    {
        /// <summary>
        /// Add a key-value entry to the list
        /// </summary>
        /// <param name="collection">Target list</param>
        /// <param name="key">Key - if null/whitespace, item will not be added</param>
        /// <param name="value">Value - if null/whitespace, item will not be added</param>
        public static void AddKeyValue(this ICollection<IUploadMetadata> collection, string key, object value)
        {
            if (string.IsNullOrWhiteSpace(key) || string.IsNullOrWhiteSpace(value?.ToString())) return;

            collection.Add(new UploadMetadataKeyValue(key, value));
        }

        /// <summary>
        /// Add a column/value entry to the list
        /// </summary>
        /// <param name="collection">Target list</param>
        /// <param name="columnName">Column - if null/whitespace, item will not be added</param>
        /// <param name="value">Value - if null/whitespace, item will not be added</param>
        public static void AddValue(this ICollection<IUploadMetadata> collection, string columnName, object value)
        {
            if (string.IsNullOrWhiteSpace(columnName) || string.IsNullOrWhiteSpace(value?.ToString())) return;

            collection.Add(new UploadMetadataValue(columnName, value));
        }

        /// <summary>
        /// Add a file entry to the list
        /// </summary>
        /// <param name="collection">Target list</param>
        /// <param name="file">File info object</param>
        /// <param name="subDir">Remote subdirectory relative to the parent dataset or data package</param>
        public static void AddFile(this ICollection<IUploadMetadata> collection, FileInfoObject file, string subDir)
        {
            collection.Add(new UploadMetadataFile(file, subDir));
        }

        /// <summary>
        /// Get the files from the list as a list of <see cref="FileInfoObject"/>
        /// </summary>
        /// <param name="metadataObject"></param>
        public static List<FileInfoObject> GetFileListFromMetadataObject(this List<IUploadMetadata> metadataObject)
        {
            var fileList = new List<FileInfoObject>();

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var item in metadataObject.OfType<UploadMetadataFile>())
            {
                var filePathToUse = Utilities.PossiblyConvertToLongPath(item.AbsoluteLocalPath);

                var file = new FileInfo(filePathToUse);

                fileList.Add(new FileInfoObject(file, item.SubDir, item.HashSum));
            }

            return fileList;
        }

        /// <summary>
        /// Remove the <see cref="UploadMetadataFile"/> instance from the list that matches <paramref name="absoluteLocalPath"/>
        /// </summary>
        /// <param name="metadataObject"></param>
        /// <param name="absoluteLocalPath"></param>
        public static void RemoveFileFromMetadataObject(this List<IUploadMetadata> metadataObject, string absoluteLocalPath)
        {
            for (var i = 0; i < metadataObject.Count; i++)
            {
                if (metadataObject[i] is not UploadMetadataFile item)
                {
                    continue;
                }

                if (absoluteLocalPath.Equals(item.AbsoluteLocalPath))
                {
                    // Remove this item
                    metadataObject.RemoveAt(i);
                    return;
                }
            }
        }
    }
}
