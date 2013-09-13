using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyEMSLReader
{
	public delegate void MessageEventHandler(object sender, MessageEventArgs e);
	public delegate void ProgressEventHandler(object sender, ProgressEventArgs e);
	public delegate void FileDownloadedEventHandler(object sender, FileDownloadedEventArgs e);

	public class FileDownloadedEventArgs : EventArgs
	{
		public readonly string DownloadFolderPath;
		public readonly ArchivedFileInfo ArchivedFile;
		public readonly bool UnzipRequired;

		public FileDownloadedEventArgs(string downloadFolderPath,  ArchivedFileInfo archivedFile, bool unzipRequired)
		{
			DownloadFolderPath = downloadFolderPath;
			ArchivedFile = archivedFile;
			UnzipRequired = unzipRequired;
		}
	}

	public class MessageEventArgs : EventArgs
	{
		public readonly string Message;

		public MessageEventArgs(string message)
		{
			Message = message;
		}
	}

	public class ProgressEventArgs : EventArgs
	{
		/// <summary>
		/// Value between 0 and 100
		/// </summary>
		public readonly double PercentComplete;

		public ProgressEventArgs(double percentComplete)
		{
			PercentComplete = percentComplete;
		}
	}

}
