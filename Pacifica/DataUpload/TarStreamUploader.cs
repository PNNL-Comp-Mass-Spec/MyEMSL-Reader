﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using ICSharpCode.SharpZipLib.Tar;
using Pacifica.Core;
using PRISM;

namespace Pacifica.DataUpload
{
    /// <summary>
    /// .tar stream uploader
    /// </summary>
    public class TarStreamUploader
    {
        // Ignore Spelling: addon, Ingester, Pacifica, uploader

        private const string DEBUG_WORKING_DIRECTORY = @"C:\CTM_Workdir";

        /// <summary>
        /// Block size for tar files
        /// </summary>
        private const int TAR_BLOCK_SIZE_BYTES = 512;

        /// <summary>
        /// Used to report status
        /// </summary>
        public const string UPLOADING_FILES = "Uploading files";

        /// <summary>
        /// Debug options
        /// </summary>
        public enum UploadDebugMode
        {
            /// <summary>
            /// Debugging is disabled
            /// </summary>
            DebugDisabled = 0,

            /// <summary>
            /// Authenticate with MyEMSL, but create a local .tar file
            /// </summary>
            CreateTarLocal = 1,

            /// <summary>
            /// Do not contact MyEMSL; create a local .tar file
            /// </summary>
            MyEMSLOfflineMode = 2
        }

        /// <summary>
        /// This event is used by SendFileListToIngester to report upload status
        /// </summary>
        public event EventHandler<StatusEventArgs> StatusUpdate;

        /// <summary>
        /// Error event
        /// </summary>
        public event EventNotifier.ErrorEventEventHandler ErrorEvent;

        /// <summary>
        /// Warning event
        /// </summary>
        public event EventNotifier.WarningEventEventHandler WarningEvent;

        private static long AddTarFileContentLength(string pathInArchive, long fileSizeBytes)
        {
            return AddTarFileContentLength(pathInArchive, fileSizeBytes, out _);
        }

        private static long AddTarFileContentLength(string pathInArchive, long fileSizeBytes, out int headerBlocks)
        {
            long contentLength = 0;
            bool longPath;

            if (pathInArchive.EndsWith("/"))
            {
                // Directory entry
                longPath = (pathInArchive.Length > 100);
            }
            else
            {
                // File entry
                longPath = (pathInArchive.Length > 100);
            }

            // Header block for current file
            headerBlocks = 1;

            if (longPath)
            {
                // SharpZipLib will add two extra 512 byte blocks since this file has an extra long file path
                //  (if the path is over 512 chars, SharpZipLib will add 3 blocks, etc.)
                //
                // The first block will have filename "././@LongLink" and placeholder metadata (file date, file size, etc.)
                // The next block will have the actual long filename
                // The third block is the standard file info block
                var extraBlocks = (int)(Math.Ceiling(pathInArchive.Length / 512.0)) + 1;
                headerBlocks += extraBlocks;
            }

            contentLength += TAR_BLOCK_SIZE_BYTES * headerBlocks;

            // File contents
            long fileBlocks = (int)Math.Ceiling(fileSizeBytes / (double)TAR_BLOCK_SIZE_BYTES);
            contentLength += fileBlocks * TAR_BLOCK_SIZE_BYTES;

            return contentLength;
        }

        // ReSharper disable once SuggestBaseTypeForParameter
        private static void AppendDirectoryToTar(TarOutputStream tarOutputStream, DirectoryInfo sourceFolder, string pathInArchive, ref long bytesWritten)
        {
            var tarEntry = TarEntry.CreateEntryFromFile(sourceFolder.FullName);

            // Override the name
            if (!pathInArchive.EndsWith("/"))
            {
                pathInArchive += "/";
            }

            tarEntry.Name = pathInArchive;
            tarOutputStream.PutNextEntry(tarEntry);
            bytesWritten += AddTarFileContentLength(pathInArchive, 0);
        }

        private static void AppendFileToTar(TarOutputStream tarOutputStream, FileInfo sourceFile, string destFilenameInTar, ref long bytesWritten)
        {
            // ReSharper disable once SuggestVarOrType_SimpleTypes
            using (FileStream inputStream = new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                var fileSize = sourceFile.Length;

                // Create a tar entry named as appropriate. You can set the name to anything,
                // but avoid names starting with drive or UNC.

                var entry = TarEntry.CreateTarEntry(destFilenameInTar);

                // Must set size, otherwise TarOutputStream will fail when output exceeds.
                entry.Size = fileSize;

                // Add the entry to the tar stream, before writing the data.
                tarOutputStream.PutNextEntry(entry);

                // this is copied from TarArchive.WriteEntryCore
                var localBuffer = new byte[32 * 1024];

                while (true)
                {
                    var numRead = inputStream.Read(localBuffer, 0, localBuffer.Length);

                    if (numRead <= 0)
                    {
                        break;
                    }
                    tarOutputStream.Write(localBuffer, 0, numRead);
                }

                bytesWritten += AddTarFileContentLength(destFilenameInTar, sourceFile.Length);
            }
            tarOutputStream.CloseEntry();
        }

        private static long ComputeTarFileSize(SortedDictionary<string, FileInfoObject> fileListObject, FileInfo metadataFile, UploadDebugMode debugMode)
        {
            long contentLength = 0;

            var debugging = (debugMode != UploadDebugMode.DebugDisabled);

            StreamWriter tarMetadataFile;

            if (debugging)
            {
                tarMetadataFile = new StreamWriter(new FileStream(Path.Combine(DEBUG_WORKING_DIRECTORY, "TarMetadata.txt"), FileMode.Create, FileAccess.Write, FileShare.Read))
                {
                    AutoFlush = true
                };

                Console.WriteLine();

                // Note that "HB" stands for HeaderBlocks
                DisplayTarFileSizeDetails(tarMetadataFile, string.Format(
                                              "FileSize".PadRight(12) + "addonBytes".PadRight(12) +
                                              "StartOffset".PadRight(12) + "HB".PadRight(3) + "FilePath"));
            }
            else
            {
                tarMetadataFile = null;
            }

            // Add the metadata file
            var addonBytes = AddTarFileContentLength(EasyHttp.MYEMSL_METADATA_FILE_NAME, metadataFile.Length);

            if (debugging)
            {
                DisplayTarFileSizeDetails(tarMetadataFile, string.Format(
                                              metadataFile.Length.ToString().PadRight(12) + addonBytes.ToString().PadRight(12) +
                                              contentLength.ToString().PadRight(12) + "1".PadRight(3) + "metadata.txt"));
            }

            contentLength += addonBytes;

            // Add the data/ directory

            if (debugging)
            {
                DisplayTarFileSizeDetails(tarMetadataFile, string.Format(
                                              "0".PadRight(12) + TAR_BLOCK_SIZE_BYTES.ToString().PadRight(12) +
                                              contentLength.ToString().PadRight(12) + "1".PadRight(3) + "data/"));
            }

            contentLength += TAR_BLOCK_SIZE_BYTES;

            // This list keeps track of directory entries that have been added to the .tar file
            var directoryEntriesStored = new SortedSet<string>();

            // Add the files to be archived
            foreach (var fileToArchive in fileListObject)
            {
                var sourceFile = new FileInfo(Utilities.PossiblyConvertToLongPath(fileToArchive.Key));

                int headerBlocks;

                if (!string.IsNullOrEmpty(fileToArchive.Value.RelativeDestinationDirectory))
                {
                    if (sourceFile.Directory == null)
                    {
                        throw new DirectoryNotFoundException("Cannot access the parent folder for the source file: " + fileToArchive.Value.RelativeDestinationFullPath);
                    }

                    var parentDirectoryPath = NativeIOFileTools.GetCleanPath(sourceFile.Directory.FullName);

                    if (!directoryEntriesStored.Contains(parentDirectoryPath))
                    {
                        var dirPathInArchive = fileToArchive.Value.RelativeDestinationDirectory.TrimEnd('/') + "/";
                        addonBytes = AddTarFileContentLength(dirPathInArchive, 0, out headerBlocks);

                        if (debugging)
                        {
                            DisplayTarFileSizeDetails(tarMetadataFile, string.Format(
                               "0".PadRight(12) +
                                addonBytes.ToString().PadRight(12) +
                                contentLength.ToString().PadRight(12) +
                                headerBlocks.ToString().PadRight(3) +
                                FileTools.CompactPathString(dirPathInArchive, 150)));
                        }

                        contentLength += addonBytes;

                        directoryEntriesStored.Add(parentDirectoryPath);
                    }
                }

                var pathInArchive = string.Empty;

                if (!string.IsNullOrWhiteSpace(fileToArchive.Value.RelativeDestinationDirectory))
                {
                    pathInArchive += fileToArchive.Value.RelativeDestinationDirectory.TrimEnd('/') + '/';
                }

                pathInArchive += fileToArchive.Value.FileName;

                addonBytes = AddTarFileContentLength(pathInArchive, fileToArchive.Value.FileSizeInBytes, out headerBlocks);

                if (debugging)
                {
                    DisplayTarFileSizeDetails(tarMetadataFile, string.Format(
                       fileToArchive.Value.FileSizeInBytes.ToString().PadRight(12) +
                        addonBytes.ToString().PadRight(12) +
                        contentLength.ToString().PadRight(12) +
                        headerBlocks.ToString().PadRight(3) +
                        FileTools.CompactPathString(fileToArchive.Value.RelativeDestinationFullPath, 150)));
                }

                contentLength += addonBytes;
            }

            // Append two empty blocks (appended by SharpZipLib at the end of the .tar file)
            for (var i = 1; i <= 2; i++)
            {
                if (debugging)
                {
                    DisplayTarFileSizeDetails(tarMetadataFile,
                                              string.Format("0".PadRight(12) + TAR_BLOCK_SIZE_BYTES.ToString().PadRight(12) +
                                              contentLength.ToString().PadRight(12) +
                                              "0".PadRight(3) +
                                              "512 block at end of .tar"));
                }

                contentLength += TAR_BLOCK_SIZE_BYTES;
            }

            // Round up contentLength to the nearest 10240 bytes
            // Note that recordCount is a long to prevent overflow errors when computing finalPaddingLength
            var recordCount = (long)Math.Ceiling(contentLength / (double)TarBuffer.DefaultRecordSize);
            var finalPaddingLength = (recordCount * TarBuffer.DefaultRecordSize) - contentLength;

            if (debugging)
            {
                DisplayTarFileSizeDetails(tarMetadataFile, string.Format(
                                              "0".PadRight(12) + finalPaddingLength.ToString().PadRight(12) +
                                              contentLength.ToString().PadRight(12) + "0".PadRight(3) +
                                              "Padding at end (to make multiple of " + TarBuffer.DefaultRecordSize + ")"));
            }

            contentLength = recordCount * TarBuffer.DefaultRecordSize;

            if (debugging)
            {
                DisplayTarFileSizeDetails(tarMetadataFile, string.Format(
                                              "0".PadRight(12) + "0".PadRight(12) +
                                              contentLength.ToString().PadRight(12) + "0".PadRight(3) +
                                              "End of file"));

                tarMetadataFile.Close();
            }

            return contentLength;
        }

        // ReSharper disable once SuggestBaseTypeForParameter
        private static void DisplayTarFileSizeDetails(StreamWriter tarMetadataFile, string metadataLine)
        {
            var columnMatcher = new Regex(@"^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(.+)", RegexOptions.Compiled);

            var metadataColumns = columnMatcher.Match(metadataLine);

            if (metadataColumns.Success)
            {
                tarMetadataFile?.WriteLine("{0}\t{1}\t{2}\t{3}\t{4}",
                                           metadataColumns.Groups[1],
                                           metadataColumns.Groups[2],
                                           metadataColumns.Groups[3],
                                           metadataColumns.Groups[4],
                                           metadataColumns.Groups[5]);
            }
            else
            {
                tarMetadataFile?.WriteLine(metadataLine);
            }

            ConsoleMsgUtils.ShowDebugCustom(metadataLine, emptyLinesBeforeMessage: 0);
        }

        /// <summary>
        /// Report an error
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex">Exception (allowed to be nothing)</param>
        protected void OnErrorEvent(string message, Exception ex = null)
        {
            if (ErrorEvent == null)
            {
                ConsoleMsgUtils.ShowErrorCustom(message, ex, false, false);
            }
            ErrorEvent?.Invoke(message, ex);
        }

        /// <summary>
        /// Report a warning
        /// </summary>
        /// <param name="message"></param>
        protected void OnWarningEvent(string message)
        {
            if (WarningEvent == null)
            {
                ConsoleMsgUtils.ShowWarningCustom(message);
            }
            WarningEvent?.Invoke(message);
        }

        /// <summary>
        /// Report a status update
        /// </summary>
        /// <param name="percentCompleted">Value between 0 and 100</param>
        /// <param name="totalBytesSent">Total bytes to send</param>
        /// <param name="totalBytesToSend">Total bytes sent</param>
        /// <param name="statusMessage">Status message</param>
        protected void RaiseStatusUpdate(
            double percentCompleted, long totalBytesSent,
            long totalBytesToSend, string statusMessage)
        {
            StatusUpdate?.Invoke(null, new StatusEventArgs(percentCompleted, totalBytesSent, totalBytesToSend, statusMessage));
        }

        /// <summary>
        /// Upload a file via POST
        /// </summary>
        /// <param name="config"></param>
        /// <param name="location"></param>
        /// <param name="serverBaseAddress"></param>
        /// <param name="fileListObject"></param>
        /// <param name="metadataFilePath"></param>
        /// <param name="debugMode"></param>
        /// <returns>Web response data</returns>
        public string SendFileListToIngester(
            Configuration config,
            string location, string serverBaseAddress,
            SortedDictionary<string, FileInfoObject> fileListObject,
            string metadataFilePath,
            UploadDebugMode debugMode = UploadDebugMode.DebugDisabled)
        {
            return Task
                .Run(async () => await SendFileListToIngesterAsync(config, location, serverBaseAddress, fileListObject, metadataFilePath, debugMode))
                .GetAwaiter().GetResult();
        }

        /// <summary>
        /// Upload a file via POST
        /// </summary>
        /// <param name="config"></param>
        /// <param name="location"></param>
        /// <param name="serverBaseAddress"></param>
        /// <param name="fileListObject"></param>
        /// <param name="metadataFilePath"></param>
        /// <param name="debugMode"></param>
        /// <returns>Web response data</returns>
        public async Task<string> SendFileListToIngesterAsync(
            Configuration config,
            string location, string serverBaseAddress,
            SortedDictionary<string, FileInfoObject> fileListObject,
            string metadataFilePath,
            UploadDebugMode debugMode = UploadDebugMode.DebugDisabled)
        {
            var certificateFilePath = EasyHttp.ResolveCertFile(config, "SendFileListToIngester", out var errorMessage);

            if (string.IsNullOrWhiteSpace(certificateFilePath))
            {
                throw new Exception(errorMessage);
            }
            var metadataFile = new FileInfo(metadataFilePath);

            if (debugMode != UploadDebugMode.DebugDisabled)
            {
                var workDir = new DirectoryInfo(DEBUG_WORKING_DIRECTORY);

                if (!workDir.Exists)
                {
                    Console.WriteLine("Creating missing directory: " + workDir.FullName);
                    workDir.Create();
                }
            }

            // Compute the total number of bytes that will be written to the tar file
            var contentLength = ComputeTarFileSize(fileListObject, metadataFile, debugMode);

            // Set this to .CreateTarLocal Authenticate with MyEMSL, but create the .tar file locally instead of sending to the server
            // Set this to .MyEMSLOfflineMode to not contact MyEMSL, and create a local .tar file

            // See method PerformTask in clsArchiveUpdate
            var writeToDisk = (debugMode != UploadDebugMode.DebugDisabled); // aka WriteFile or SaveFile

            if (writeToDisk)
            {
                if (Environment.MachineName.IndexOf("proto", StringComparison.CurrentCultureIgnoreCase) >= 0)
                {
                    throw new Exception("Should not have writeToDisk set to True when running on a Proto-x server");
                }

                var tarFile = new FileInfo(Path.Combine(DEBUG_WORKING_DIRECTORY, "TestFile3.tar"));

                Console.WriteLine();
                Console.WriteLine("Tar file path: " + tarFile.FullName);
                Console.WriteLine();

                using var outFile = new FileStream(tarFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read);
                CreateTarArchive(outFile, config, fileListObject, metadataFile, contentLength);
                outFile.Close();

                return string.Empty;
            }

            var baseUri = new Uri(serverBaseAddress);
            var uploadUri = new Uri(baseUri, location);

            var responseData = string.Empty;
            try
            {
                // Make the request
                var handler = new HttpClientHandler();

                var password = AppUtils.DecodeShiftCipher(Configuration.CLIENT_CERT_PASSWORD);
                var certificate = Utilities.CreateX509Certificate(certificateFilePath, password);
                handler.ClientCertificates.Add(certificate);
                handler.PreAuthenticate = false;

                var certValidation = new CertificateValidation();
                certValidation.ErrorEvent += OnErrorEvent;
                certValidation.WarningEvent += OnWarningEvent;

                handler.ServerCertificateCustomValidationCallback ??= certValidation.ValidateRemoteCertificate;

                config.SetProxy(handler);

                using var client = new HttpClient(handler);

                client.BaseAddress = uploadUri;
                client.Timeout = Timeout.InfiniteTimeSpan;

                var request = new HttpRequestMessage(HttpMethod.Post, uploadUri);
                request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("*/*"));
                request.Headers.ConnectionClose = false;

                request.Content = new PushStreamContent((stream, _, _) =>
                {
                    // write to the stream from where ever you are getting the bytes from
                    CreateTarArchive(stream, config, fileListObject, metadataFile, contentLength);
                    stream.Close();
                }, "application/octet-stream");

                request.Content.Headers.ContentLength = contentLength; // If we don't set a content length, buffered data transfer is used instead of streamed transfer

                using var response = await client.SendAsync(request, HttpCompletionOption.ResponseContentRead);

                // The response should be empty if everything worked
                responseData = await response.Content.ReadAsStringAsync();

                var responseCode = response.StatusCode;
                if (!response.IsSuccessStatusCode)
                {
                    throw new HttpRequestException(responseData);
                }
            }
            catch (HttpRequestException ex)
            {
                EasyHttp.HandleRequestException(ex, uploadUri.ToString());
            }
            catch (TaskCanceledException ex)
            {
                EasyHttp.HandleRequestException(new HttpRequestException("HTTP request timed out", ex), uploadUri.ToString());
            }
            catch (OperationCanceledException ex)
            {
                EasyHttp.HandleRequestException(new HttpRequestException($"HTTP request was cancelled?: {ex.CancellationToken.IsCancellationRequested}", ex), uploadUri.ToString());
            }

            return responseData;
        }

        /// <summary>
        /// Upload a file via POST
        /// </summary>
        /// <param name="outputStream"></param>
        /// <param name="config"></param>
        /// <param name="fileListObject"></param>
        /// <param name="metadataFile"></param>
        /// <param name="tarContentLength">total number of bytes that will be written to the tar file</param>
        /// <returns>Web response data</returns>
        private void CreateTarArchive(
            Stream outputStream,
            Configuration config,
            SortedDictionary<string, FileInfoObject> fileListObject,
            FileInfo metadataFile,
            long tarContentLength)
        {
            long bytesWritten = 0;
            var lastStatusUpdateTime = DateTime.UtcNow;

            RaiseStatusUpdate(0, bytesWritten, tarContentLength, string.Empty);

            // Use SharpZipLib to create the tar file on-the-fly and directly push into the request stream
            // This way, the .tar file is never actually created on a local hard drive
            // Code modeled after https://github.com/icsharpcode/SharpZipLib/wiki/GZip-and-Tar-Samples

            var tarOutputStream = new TarOutputStream(outputStream, Encoding.UTF8);

            var directoryEntries = new SortedSet<string>();

            // Add the metadata.txt file
            AppendFileToTar(tarOutputStream, metadataFile, EasyHttp.MYEMSL_METADATA_FILE_NAME, ref bytesWritten);

            // Add the "data" directory, which will hold all the files
            // Need a dummy "data" directory to do this
            var tempFolder = Utilities.GetTempDirectory(config);
            var dummyDataFolder = new DirectoryInfo(Path.Combine(tempFolder.FullName, "data"));

            if (!dummyDataFolder.Exists)
            {
                dummyDataFolder.Create();
            }

            AppendDirectoryToTar(tarOutputStream, dummyDataFolder, "data", ref bytesWritten);

            var startTime = DateTime.UtcNow;

            foreach (var fileToArchive in fileListObject)
            {
                var sourceFile = new FileInfo(Utilities.PossiblyConvertToLongPath(fileToArchive.Key));

                if (!string.IsNullOrEmpty(fileToArchive.Value.RelativeDestinationDirectory))
                {
                    if (sourceFile.Directory == null)
                    {
                        throw new DirectoryNotFoundException("Cannot access the parent folder for the source file: " + fileToArchive.Value.RelativeDestinationFullPath);
                    }

                    if (!directoryEntries.Contains(sourceFile.Directory.FullName))
                    {
                        // Make a directory entry
                        AppendDirectoryToTar(tarOutputStream, sourceFile.Directory, fileToArchive.Value.RelativeDestinationDirectory, ref bytesWritten);

                        directoryEntries.Add(sourceFile.Directory.FullName);
                    }
                }

                AppendFileToTar(tarOutputStream, sourceFile, fileToArchive.Value.RelativeDestinationFullPath, ref bytesWritten);

                var percentComplete = bytesWritten / (double)tarContentLength * 100;

                // Initially limit status updates to every 3 seconds
                // Increase the time between updates as upload time progresses, with a maximum interval of 90 seconds
                var statusIntervalSeconds = Math.Min(90, 3 + DateTime.UtcNow.Subtract(startTime).TotalSeconds / 10);

                if (DateTime.UtcNow.Subtract(lastStatusUpdateTime).TotalSeconds >= statusIntervalSeconds)
                {
                    lastStatusUpdateTime = DateTime.UtcNow;
                    RaiseStatusUpdate(percentComplete, bytesWritten, tarContentLength, UPLOADING_FILES + ": " + sourceFile.Name);
                }
            }

            // Close the tar file memory stream (to flush the buffers)
            tarOutputStream.IsStreamOwner = false;
            tarOutputStream.Close();
            bytesWritten += TAR_BLOCK_SIZE_BYTES + TAR_BLOCK_SIZE_BYTES;

            RaiseStatusUpdate(100, bytesWritten, tarContentLength, string.Empty);

            RaiseStatusUpdate(100, tarContentLength, tarContentLength, string.Empty);
        }
    }
}
