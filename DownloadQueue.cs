using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyEMSLReader
{
	public class DownloadQueue
	{
		#region "Structures"
		public struct udtFileToDownload
		{
			public ArchivedFileInfo FileInfo;
			public bool UnzipRequired;
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
		public Dictionary<Int64, DownloadQueue.udtFileToDownload> FilesToDownload
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

		/// Constructor
		/// </summary>
		/// <param name="datasetName"></param>
		/// <remarks></remarks>
		public DownloadQueue()
		{
			this.FilesToDownload = new Dictionary<Int64, DownloadQueue.udtFileToDownload>();
			this.DownloadedFiles = new Dictionary<string, ArchivedFileInfo>();
		}

		public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo)
		{
			AddFileToDownloadQueue(fileInfo.FileID, fileInfo, unzipRequired: false);
		}

		public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo, bool unzipRequired)
		{
			AddFileToDownloadQueue(fileInfo.FileID, fileInfo, unzipRequired);
		}

		/// <summary>
		/// Queue a file to be downloaded
		/// </summary>
		/// <param name="myEMSLFileID"></param>
		/// <param name="fileInfo">Archive File Info</param>
		/// <param name="unzipRequired"></param>
		/// <remarks>fileInfo can be null if unzipRequired is false</remarks>
		public void AddFileToDownloadQueue(Int64 myEMSLFileID, ArchivedFileInfo fileInfo, bool unzipRequired)
		{

			if (this.FilesToDownload.ContainsKey(myEMSLFileID))
				return;

			var newFile = new udtFileToDownload();
			newFile.UnzipRequired = unzipRequired;
			newFile.FileInfo = fileInfo;

			if (newFile.UnzipRequired && fileInfo == null)
			{
				string message = "Cannot queue file " + myEMSLFileID + " for download because the UnzipRequired flag was set, but the ArchivedFileInfo parameter is null";
				Console.WriteLine(message);
				throw new InvalidDataException(message);
			}

			this.FilesToDownload.Add(myEMSLFileID, newFile);
		}

		public void Clear()
		{
			this.FilesToDownload.Clear();
		}

	
		public bool ProcessDownloadQueue(string downloadFolderPath, Downloader.DownloadFolderLayout folderLayout)
		{

			if (this.FilesToDownload.Count == 0)
			{
				OnError("Download queue is empty; nothing to download");
				return false;
			}

			try
			{
				var downloader = new Downloader();

				// Attach events
				downloader.ErrorEvent += new MessageEventHandler(OnErrorEvent);
				downloader.MessageEvent += new MessageEventHandler(OnMessageEvent);
				downloader.ProgressEvent += new ProgressEventHandler(OnProgressEvent);

				bool success = downloader.DownloadFiles(this.FilesToDownload.Keys.ToList(), downloadFolderPath, folderLayout);

				if (success)
				{
					this.DownloadedFiles = downloader.DownloadedFiles;

					foreach (var file in this.FilesToDownload)
					{
						if (FileDownloadedEvent != null)
						{
							FileDownloadedEvent(this, new FileDownloadedEventArgs(downloadFolderPath, file.Value.FileInfo, file.Value.UnzipRequired));
						}
					}
					this.FilesToDownload.Clear();

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
