using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace MyEMSLReader
{
	public abstract class DatasetInfoBase
	{

		#region "Constants"

		protected const string MYEMSL_FILEID_TAG = "@MyEMSLID_";
		protected const int CACHE_REFRESH_THRESHOLD_MINUTES = 5;

		#endregion

		#region "Module variables"

		protected List<string> mErrorMessages;
		protected List<ArchivedFileInfo> mArchivedFiles;

		protected DateTime mCacheDate;
		protected bool mCacheIsStale;

		protected DateTime mLastProgressWriteTime;

		protected Reader mReader;

		protected DownloadQueue mDownloadQueue;

		/// <summary>
		/// The most recently downloaded files; keys are the full paths to the downloaded file, values are extended file info
		/// </summary>
		protected Dictionary<string, ArchivedFileInfo> mDownloadedFiles;

		#endregion

		#region "Properties"

		/// <summary>
		/// The most recently downloaded files; keys are the full paths to the downloaded file, values are extended file info
		/// </summary>
		/// <value></value>
		/// <returns></returns>
		/// <remarks>Keys are the full paths to the downloaded file, values are extended file info</remarks>
		public Dictionary<string, ArchivedFileInfo> DownloadedFiles
		{
			get
			{
				return mDownloadQueue.DownloadedFiles;
			}
		}

		/// <summary>
		/// List of error messages
		/// </summary>
		/// <value></value>
		/// <returns></returns>
		/// <remarks>The messages are cleared by when these functions are called: ProcessDownloadQueue and RefreshInfo</remarks>
		public List<string> ErrorMessages
		{
			get
			{
				return mErrorMessages;
			}
		}

		/// <summary>
		/// MyEMSL IDs of files queued to be downloaded
		/// </summary>
		/// <value></value>
		/// <returns></returns>
		/// <remarks>Keys are MyEMSL File IDs, values are struct udtFileToDownload</remarks>
		public Dictionary<Int64, DownloadQueue.udtFileToDownload> FilesToDownload
		{
			get
			{
				return mDownloadQueue.FilesToDownload;
			}
		}
		#endregion

		public DatasetInfoBase()
		{
			mErrorMessages = new List<string>();

			mReader = new Reader();

			// Attach events
			mReader.ErrorEvent += new MessageEventHandler(OnErrorEvent);
			mReader.MessageEvent += new MessageEventHandler(OnMessageEvent);
			mReader.ProgressEvent += new ProgressEventHandler(OnProgressEvent);

			mArchivedFiles = new List<ArchivedFileInfo>();

			mDownloadedFiles = new Dictionary<string, ArchivedFileInfo>(StringComparer.CurrentCultureIgnoreCase);

			mDownloadQueue = new DownloadQueue();

			// Attach events
			mDownloadQueue.ErrorEvent += new MessageEventHandler(OnErrorEvent);
			mDownloadQueue.MessageEvent += new MessageEventHandler(OnMessageEvent);
			mDownloadQueue.ProgressEvent += new ProgressEventHandler(OnProgressEvent);
			mDownloadQueue.FileDownloadedEvent += new FileDownloadedEventHandler(OnFileDownloadedEvent);

			mLastProgressWriteTime = DateTime.UtcNow;
		}

		/// <summary>
		/// Queue a file to be downloaded
		/// </summary>
		/// <param name="fileInfo">Archive File Info</param>
		public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo)
		{
			mDownloadQueue.AddFileToDownloadQueue(fileInfo, unzipRequired: false);
		}

		/// <summary>
		/// Queue a file to be downloaded
		/// </summary>
		/// <param name="fileInfo">Archive File Info</param>
		/// <param name="unzipRequired">True if the file will need to be unzipped after the download (this DLL will not unzip the file; it will simply include this in event FileDownloadedEventArgs)</param>
		public void AddFileToDownloadQueue(ArchivedFileInfo fileInfo, bool unzipRequired)
		{
			mDownloadQueue.AddFileToDownloadQueue(fileInfo, unzipRequired);
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
			mDownloadQueue.AddFileToDownloadQueue(myEMSLFileID, fileInfo, unzipRequired);
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
			mDownloadQueue.AddFileToDownloadQueue(myEMSLFileID, fileInfo, unzipRequired, destFilePath);
		}

		/// <summary>
		/// Appends the MyEMSL File ID tag to a given file path
		/// </summary>
		/// <param name="filePath">Path to which the MyEMSL FileID should be appended</param>
		/// <param name="myEmslFileID">MyEMSL File ID</param>
		/// <returns>New path, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw@MyEMSLID_84327</returns>
		/// <remarks></remarks>
		public static string AppendMyEMSLFileID(string filePath, Int64 myEmslFileID)
		{
			return filePath + MYEMSL_FILEID_TAG + myEmslFileID.ToString();
		}

		public void ClearDownloadQueue()
		{
			mDownloadQueue.Clear();
			mDownloadedFiles.Clear();
		}

		/// <summary>
		/// Parses a path that contains the MyEMSL FileID tag
		/// </summary>
		/// <param name="filePath">Path to parse, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw@MyEMSLID_84327</param>
		/// <returns>MyEMSL File ID if successfully parsed, 0 if not present or a problem</returns>
		/// <remarks></remarks>
		public static Int64 ExtractMyEMSLFileID(string filePath)
		{
			string newFilePath = string.Empty;
			return ExtractMyEMSLFileID(filePath, out newFilePath);
		}

		/// <summary>
		/// Parses a path that contains the MyEMSL FileID tag
		/// </summary>
		/// <param name="filePath">Path to parse, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw@MyEMSLID_84327</param>
		/// <param name="newFilePath">Path with the MyEMSL FileID tag removed, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw</param>
		/// <returns>MyEMSL File ID if successfully parsed, 0 if not present or a problem</returns>
		/// <remarks></remarks>
		public static Int64 ExtractMyEMSLFileID(string filePath, out string newFilePath)
		{

			int charIndex = filePath.LastIndexOf(MYEMSL_FILEID_TAG);
			newFilePath = string.Copy(filePath);

			if (charIndex > 0)
			{
				newFilePath = filePath.Substring(0, charIndex);

				Int64 myEmslFileID;
				string myEmslFileIdText = filePath.Substring(charIndex + MYEMSL_FILEID_TAG.Length);

				if (Int64.TryParse(myEmslFileIdText, out myEmslFileID))
				{
					return myEmslFileID;
				}

			}

			return 0;

		}

		/// <summary>
		/// Looks for the given file, returning any matches as a list
		/// </summary>
		/// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
		/// <returns>List of matching files</returns>
		/// <remarks></remarks>
		public List<DatasetFolderOrFileInfo> FindFiles(string fileName)
		{
			string subFolderName = string.Empty;
			string datasetName = string.Empty;
			bool recurse = true;
			return FindFiles(fileName, subFolderName, datasetName, recurse);
		}

		/// <summary>
		/// Looks for the given file, returning any matches as a list
		/// </summary>
		/// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
		/// <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
		/// <returns>List of matching files</returns>
		public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subFolderName)
		{
			string datasetName = string.Empty;
			bool recurse = true;
			return FindFiles(fileName, subFolderName, datasetName, recurse);
		}

		/// <summary>
		/// Looks for the given file, returning any matches as a list
		/// </summary>
		/// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
		/// <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
		/// <param name="recurse">True to search all subfolders; false to only search the root folder (or only subFolderName)</param>
		/// <returns>List of matching files</returns>
		public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subFolderName, bool recurse)
		{
			string datasetName = string.Empty;
			return FindFiles(fileName, subFolderName, datasetName, recurse);
		}

		/// <summary>
		/// Looks for the given file, returning any matches as a list
		/// </summary>
		/// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
		/// <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
		/// <param name="datasetName">Dataset name filter</param>
		/// <returns>List of matching files</returns>
		public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subFolderName, string datasetName)
		{
			bool recurse = true;
			return FindFiles(fileName, subFolderName, datasetName, recurse);
		}

		/// <summary>
		/// Looks for the given file, returning any matches as a list
		/// </summary>
		/// <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
		/// <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
		/// <param name="datasetName">Dataset name filter</param>
		/// <param name="recurse">True to search all subfolders; false to only search the root folder (or only subFolderName)</param>
		/// <returns>List of matching files</returns>
		/// <remarks></remarks>
		public List<DatasetFolderOrFileInfo> FindFiles(string fileName, string subFolderName, string datasetName, bool recurse)
		{

			// Re-query the web service if the information is out-of-date
			RefreshInfoIfStale();

			var lstMatches = new List<DatasetFolderOrFileInfo>();

			if (string.IsNullOrEmpty(fileName))
			{
				return lstMatches;
			}

			if (mArchivedFiles.Count == 0)
			{
				return lstMatches;
			}

			Regex reFile = GetFileSearchRegEx(fileName);
			Regex reFolder;

			if (!string.IsNullOrEmpty(subFolderName))
			{
				reFolder = GetFileSearchRegEx(subFolderName);
			}
			else
			{
				reFolder = GetFileSearchRegEx("*");
			}

			foreach (var archivedFile in mArchivedFiles)
			{
				if (!string.IsNullOrWhiteSpace(datasetName))
				{
					if (!string.Equals(datasetName, archivedFile.Dataset, StringComparison.CurrentCultureIgnoreCase))
						continue;
				}

				if (reFile.IsMatch(archivedFile.Filename))
				{
					bool isMatch = true;

					if (string.IsNullOrEmpty(subFolderName))
					{
						// Validate that the file resides in the appropriate folder
						if (!recurse && archivedFile.RelativePathWindows.Contains("\\"))
						{
							// Invalid match
							isMatch = false;
						}
					}
					else
					{
						// Require a subfolder match
						isMatch = false;
						if (archivedFile.RelativePathWindows.Contains("\\"))
						{
							List<string> pathParts = archivedFile.RelativePathWindows.Split('\\').ToList();
							for (int pathIndex = pathParts.Count - 2; pathIndex >= 0; pathIndex += -1)
							{
								if (reFolder.IsMatch(pathParts[pathIndex]))
								{
									isMatch = true;
									break;
								}
								if (!recurse)
									break;
							}
						}
					}

					if (isMatch)
					{
						var newMatch = new DatasetFolderOrFileInfo(archivedFile.FileID, false, archivedFile);
						lstMatches.Add(newMatch);
					}
				}

			}

			return lstMatches;

		}

		/// <summary>
		/// Looks for the given folder, returning any matches as a list
		/// </summary>
		/// <param name="folderName">Folder name to find; can contain a wildcard, e.g. SIC*</param>
		/// <returns>List of matching folders</returns>
		/// <remarks></remarks>
		public List<DatasetFolderOrFileInfo> FindFolders(string folderName)
		{
			string datasetName = string.Empty;

			return FindFolders(folderName, datasetName);
		}

		/// <summary>
		/// Looks for the given folder, returning any matches as a list
		/// </summary>
		/// <param name="folderName">Folder name to find; can contain a wildcard, e.g. SIC*</param>
		/// <param name="datasetName">Dataset name filter</param>
		/// <returns>List of matching folders</returns>
		/// <remarks></remarks>
		public List<DatasetFolderOrFileInfo> FindFolders(string folderName, string datasetName)
		{

			// Re-query the web service if the information is out-of-date
			RefreshInfoIfStale();

			var lstMatches = new List<DatasetFolderOrFileInfo>();
			var lstMatchPaths = new SortedSet<string>();

			if (string.IsNullOrEmpty(folderName))
			{
				return lstMatches;
			}

			Regex reFolder = GetFileSearchRegEx(folderName);

			foreach (var archivedFile in mArchivedFiles)
			{
				if (!string.IsNullOrWhiteSpace(datasetName))
				{
					if (!string.Equals(datasetName, archivedFile.Dataset, StringComparison.CurrentCultureIgnoreCase))
						continue;
				}

				if (archivedFile.RelativePathWindows.IndexOf("\\") <= 0)
				{
					continue;
				}

				var fiFile = new FileInfo(archivedFile.RelativePathWindows);
				if (!reFolder.IsMatch(fiFile.Directory.Name))
				{
					continue;
				}

				string relativeFolderPath = string.Copy(archivedFile.RelativePathWindows);
				int charIndex = relativeFolderPath.LastIndexOf("\\");

				if (charIndex > 0)
				{
					relativeFolderPath = relativeFolderPath.Substring(0, charIndex);
				}
				else
				{
					// This is a programming bug
					throw new ArgumentOutOfRangeException("Forward slash not found in the relative file path; this code should not be reached");
				}

				if (lstMatchPaths.Contains(relativeFolderPath))
				{
					continue;
				}

				lstMatchPaths.Add(relativeFolderPath);

				List<string> pathParts = relativeFolderPath.Split('\\').ToList();
				string subDirPath = string.Empty;

				if (pathParts.Count > 1)
				{
					subDirPath = pathParts[0];
					for (int pathIndex = 1; pathIndex <= pathParts.Count - 2; pathIndex++)
					{
						subDirPath = subDirPath + "\\" + pathParts[pathIndex];
					}
					relativeFolderPath = pathParts.Last();
					subDirPath = subDirPath.TrimEnd('\\');
				}

				Int64 fileID = 0;
				bool isFolder = true;
				var newMatch = new DatasetFolderOrFileInfo(fileID, isFolder, new ArchivedFileInfo(archivedFile.Dataset, relativeFolderPath, subDirPath));

				lstMatches.Add(newMatch);
			}

			return lstMatches;

		}

		protected Regex GetFileSearchRegEx(string name)
		{
			string strSearchSpec = "^" + name + "$";
			strSearchSpec = strSearchSpec.Replace("*", ".*");

			return new Regex(strSearchSpec, RegexOptions.Compiled | RegexOptions.IgnoreCase);
		}

		public bool ProcessDownloadQueue(string downloadFolderPath, Downloader.DownloadFolderLayout folderLayout)
		{

			mErrorMessages.Clear();
			mDownloadedFiles.Clear();

			bool success = mDownloadQueue.ProcessDownloadQueue(downloadFolderPath, folderLayout);

			if (success)
			{
				foreach (var downloadedFile in mDownloadQueue.DownloadedFiles)
					mDownloadedFiles.Add(downloadedFile.Key, downloadedFile.Value);
			}

			return success;

		}

		public abstract bool RefreshInfo();

		/// <summary>
		/// Refresh the cached file info if over 5 minutes have elapsed
		/// </summary>
		/// <returns>True if success, false if an error</returns>
		/// <remarks></remarks>
		protected bool RefreshInfoIfStale()
		{
			if (mCacheIsStale || DateTime.UtcNow.Subtract(mCacheDate).TotalMinutes >= CACHE_REFRESH_THRESHOLD_MINUTES)
				return RefreshInfo();
			else
				return true;
		}


		#region "Events"

		public event MessageEventHandler ErrorEvent;
		public event MessageEventHandler MessageEvent;
		public event ProgressEventHandler ProgressEvent;
		public event FileDownloadedEventHandler FileDownloadedEvent;

		#endregion

		#region "Event handlers"

		protected void OnErrorEvent(object sender, MessageEventArgs e)
		{
			mErrorMessages.Add(e.Message);
			if (ErrorEvent != null)
			{
				if (sender.GetType().ToString().EndsWith("DownloadQueue"))
					ErrorEvent(sender, e);
				else
					ErrorEvent(this, new MessageEventArgs("MyEMSL reader error in MyEMSLReader.DatasetInfoBase: " + e.Message));
			}
		}

		protected void OnMessageEvent(object sender, MessageEventArgs e)
		{
			if (MessageEvent != null)
			{
				if (sender.GetType().ToString().EndsWith("DownloadQueue"))
					MessageEvent(sender, e);
				else
					MessageEvent(this, new MessageEventArgs("MyEMSL reader: " + e.Message));
			}
		}

		protected void OnProgressEvent(object sender, ProgressEventArgs e)
		{
			if (ProgressEvent != null)
			{
				ProgressEvent(sender, e);
			}
		}

		protected void OnFileDownloadedEvent(object sender, FileDownloadedEventArgs e)
		{
			if (FileDownloadedEvent != null)
			{
				FileDownloadedEvent(sender, e);
			}
		}

		#endregion

	}
}
