using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;

namespace MyEMSLReader
{
	public class DownloadQueue
	{
		#region "Structures"
		public struct udtFileToDownload
		{
			public ArchivedFileInfo FileInfo;
			public bool UnzipRequired;
			public string DestFilePath;
		}
		#endregion

		#region "Module variables"

		#endregion

		#region "Events"

		public event MessageEventHandler ErrorEvent;
		public event MessageEventHandler MessageEvent;
		public event ProgressEventHandler ProgressEvent;
		public event FileDownloadedEventHandler FileDownloadedEvent;

		#endregion

		#region "Properties"

		/// <summary>
		/// Keys are MyEMSL File IDs, values are struct udtFileToDownload
		/// </summary>
		public Dictionary<Int64, udtFileToDownload> FilesToDownload
		{
			get;
			private set;
		}

		/// <summary>
		/// Keys are the full paths to the downloaded file, values are extended file info
		/// </summary>
		public Dictionary<string, ArchivedFileInfo> DownloadedFiles
		{
			get;
			private set;
		}

		#endregion

		/// <summary>
		/// Constructor
		/// </summary>
		/// <remarks></remarks>
		public DownloadQueue()
		{
			FilesToDownload = new Dictionary<Int64, udtFileToDownload>();
			DownloadedFiles = new Dictionary<string, ArchivedFileInfo>();
		}

		/// <summary>
		/// Queue a file to be downloaded
		/// </summary>
		/// <param name="fileInfo">Archive File Info</param>
		public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo)
		{
			AddFileToDownloadQueue(fileInfo.FileID, fileInfo, unzipRequired: false, destFilePath: string.Empty);
		}

		/// <summary>
		/// Queue a file to be downloaded
		/// </summary>
		/// <param name="fileInfo">Archive File Info</param>
		/// <param name="unzipRequired">True if the file will need to be unzipped after the download (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)</param>
		public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo, bool unzipRequired)
		{
			string destFilePath = string.Empty;
			AddFileToDownloadQueue(fileInfo.FileID, fileInfo, unzipRequired, destFilePath);
		}

		/// <summary>
		/// Queue a file to be downloaded
		/// </summary>
		/// <param name="myEMSLFileID">MyEMSL File ID</param>
		/// <param name="fileInfo">Archive File Info</param>
		/// <param name="unzipRequired">True if the file will need to be unzipped after the download (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)</param>
		/// <remarks>fileInfo can be null if unzipRequired is false</remarks>
		public void AddFileToDownloadQueue(Int64 myEMSLFileID, ArchivedFileInfo fileInfo, bool unzipRequired)
		{
			string destFilePath = string.Empty;
			AddFileToDownloadQueue(myEMSLFileID, fileInfo, unzipRequired, destFilePath);
		}

		/// <summary>
		/// Queue a file to be downloaded
		/// </summary>
		/// <param name="myEMSLFileID">MyEMSL File ID</param>
		/// <param name="fileInfo">Archive File Info</param>
		/// <param name="unzipRequired">True if the file will need to be unzipped after the download (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)</param>
		/// <param name="destFilePath">Explicit destination file path</param>
		/// <remarks>fileInfo can be null if unzipRequired is false</remarks>
		public void AddFileToDownloadQueue(Int64 myEMSLFileID, ArchivedFileInfo fileInfo, bool unzipRequired, string destFilePath)
		{

			if (FilesToDownload.ContainsKey(myEMSLFileID))
				return;

			if (string.IsNullOrWhiteSpace(destFilePath))
				destFilePath = string.Empty;

			var newFile = new udtFileToDownload
			{
				UnzipRequired = unzipRequired, 
				FileInfo = fileInfo, 
				DestFilePath = destFilePath
			};

			if (newFile.UnzipRequired && fileInfo == null)
			{
				string message = "Cannot queue file " + myEMSLFileID + " for download because the UnzipRequired flag was set, but the ArchivedFileInfo parameter is null";
				Console.WriteLine(message);
				throw new InvalidDataException(message);
			}

			FilesToDownload.Add(myEMSLFileID, newFile);
		}

		public void Clear()
		{
			FilesToDownload.Clear();
		}


		public bool ProcessDownloadQueue(string downloadFolderPath, Downloader.DownloadFolderLayout folderLayout)
		{

			if (FilesToDownload.Count == 0)
			{
				OnError("Download queue is empty; nothing to download");
				return false;
			}

			try
			{
				var downloader = new Downloader();

				// Attach events
				downloader.ErrorEvent += OnErrorEvent;
				downloader.MessageEvent += OnMessageEvent;
				downloader.ProgressEvent += OnProgressEvent;

				var dctDestFilePathOverride = new Dictionary<Int64, string>();
				
				foreach (var fileToDownload in FilesToDownload)
				{
					if (!string.IsNullOrEmpty(fileToDownload.Value.DestFilePath))
					{
						dctDestFilePathOverride.Add(fileToDownload.Key, fileToDownload.Value.DestFilePath);
					}					
				}

				bool success = downloader.DownloadFiles(FilesToDownload.Keys.ToList(), dctDestFilePathOverride, downloadFolderPath, folderLayout);

				if (success)
				{
					DownloadedFiles = downloader.DownloadedFiles;

					foreach (var file in FilesToDownload)
					{
						if (FileDownloadedEvent != null)
						{
							FileDownloadedEvent(this, new FileDownloadedEventArgs(downloadFolderPath, file.Value.FileInfo, file.Value.UnzipRequired));
						}
					}
					FilesToDownload.Clear();

				}

				return success;

			}
			catch (Exception ex)
			{
				OnError("Error in MyEMSLReader.DownloadQueue.ProcessDownloadQueue: " + ex.Message);
				return false;
			}
		}

		#region "Event handlers"

		protected void OnError(string errorMessage)
		{
			if (ErrorEvent != null)
			{
				ErrorEvent(this, new MessageEventArgs(errorMessage));
			}
		}

		private void OnErrorEvent(object sender, MessageEventArgs e)
		{
			OnError("MyEMSL downloader error in MyEMSLReader.DownloadQueue: " + e.Message);
		}

		private void OnMessageEvent(object sender, MessageEventArgs e)
		{
			if (MessageEvent != null)
			{
				MessageEvent(this, new MessageEventArgs("MyEMSL downloader: " + e.Message));
			}
		}

		private void OnProgressEvent(object sender, ProgressEventArgs e)
		{
			if (ProgressEvent != null)
			{
				ProgressEvent(sender, e);
			}
		}

		#endregion
	}
}
