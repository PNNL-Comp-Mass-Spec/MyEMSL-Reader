using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Pacifica.Core;
using ICSharpCode.SharpZipLib.Tar;

namespace MyEMSLReader
{
	/// <summary>
	/// This class contacts MyEMSL to download the desired files, which must be specified by MyEMSL File ID values
	/// Use the Reader class to find files and determine File IDs
	/// </summary>
	/// <remarks>Written by Matthew Monroe for PNNL in August 2013</remarks>
	public class Downloader : MyEMSLBase
	{
		#region "Constants"

		#endregion

		#region "Enums"

		public enum DownloadFolderLayout
		{
			FlatNoSubfolders = 0,
			SingleDataset = 1,
			DatasetNameAndSubFolders = 2,
			InstrumentYearQuarterDataset = 3
		}

		public enum Overwrite
		{
			IfChanged = 0,			// This mode will compute an MD5 hash of the target file and only overwrite the target if the hash values differ
			Always = 1,
			Never = 2
		}
		public enum CartState
		{
			NoCart,
			Unsubmitted,
			Building,
			Available,
			Expired,
			Admin,
			Unknown
		}

		#endregion

		#region "Properties"

		public CartState DownloadCartState
		{
			get;
			private set;
		}

		/// <summary>
		/// The most recently downloaded files.  Keys are the full path to the downloaded file; values are extended file info
		/// </summary>
		public Dictionary<string, ArchivedFileInfo> DownloadedFiles
		{
			get;
			private set;
		}

		/// <summary>
		/// When true, then will always download files using the cart mechanism, which is likely slower if the file is not purged to tape
		/// </summary>
		public bool ForceDownloadViaCart
		{
			get;
			set;
		}

		public Overwrite OverwriteMode
		{
			get;
			set;
		}

		/// <summary>
		/// Percent complete (value between 0 and 100)
		/// </summary>
		public double PercentComplete
		{
			get;
			private set;
		}

		#endregion

		#region "Member Variables"

		Reader mReader;

		#endregion

		#region "Public methods"

		// Constructor
		public Downloader()
		{
			this.ThrowErrors = true;
			this.OverwriteMode = Overwrite.IfChanged;
			this.DownloadedFiles = new Dictionary<string, ArchivedFileInfo>(StringComparer.CurrentCultureIgnoreCase);

			mReader = new Reader();
			ResetStatus();
		}

		public bool DownloadFiles(
			List<long> lstFileIDs,
			string downloadFolderPath,
			DownloadFolderLayout folderLayout = DownloadFolderLayout.SingleDataset,
			int maxMinutesToWait = 1440)
		{
			bool success = false;
			CookieContainer cookieJar = null;

			ResetStatus();

			try
			{
				if (lstFileIDs == null || lstFileIDs.Count == 0)
				{
					ReportError("FileID List is empty; nothing to download");
					return false;
				}

				// Scan for Files
				var scanMode = Reader.ScanMode.ObtainAuthToken;

				string xmlString = ScanForFiles(lstFileIDs, scanMode, ref cookieJar);
				if (string.IsNullOrWhiteSpace(xmlString))
				{
					if (string.IsNullOrWhiteSpace(this.ErrorMessage))
						ReportError("ScanForFiles returned an empty xml result when downloading files");
					return false;
				}

				// Parse the results and determine the Locked status for each file
				// Keys in this dictionary are ArchivedFileInfo objects, values are True if the file is "locked" (i.e. available for immediate download)
				string authToken;
				Dictionary<ArchivedFileInfo, bool> dctFiles = CheckLockedStatus(xmlString, cookieJar, out authToken);
				if (dctFiles.Count == 0)
				{
					if (string.IsNullOrWhiteSpace(this.ErrorMessage))
						ReportError("Query did not return any files");
				}

				if (folderLayout == DownloadFolderLayout.SingleDataset)
				{
					// Assure that the requested files all have the same dataset id
					var lstDatasetIDs = GetUniqueDatasetIDList(dctFiles);

					if (lstDatasetIDs.Count > 1)
					{
						// Look for conflicts
						var lstOutputFilePaths = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase);
						foreach (var archivedFile in dctFiles.Keys)
						{
							if (lstOutputFilePaths.Contains(archivedFile.RelativePathWindows))
							{
								// File conflicts
								ReportMessage("Auto-changing folder layout to 'DatasetNameAndSubFolders' since the files to download come from more than one dataset");
								folderLayout = DownloadFolderLayout.DatasetNameAndSubFolders;
								break;
							}
							else
							{
								lstOutputFilePaths.Add(archivedFile.RelativePathWindows);
							}
						}

					}
				}

				long bytesDownloaded;
				Dictionary<long, string> dctFilesDownloaded;

				// Download "Locked" files (those not purged to tape)
				// Keys in this dictionary are FileIDs, values are relative file paths
				dctFilesDownloaded = DownloadLockedFiles(dctFiles, cookieJar, authToken, downloadFolderPath, folderLayout, out bytesDownloaded);

				// Create a list of the files that remain (files that could not be downloaded directly)
				// These files will be downloaded via the cart mechanism
				var lstFilesRemaining = new List<long>();
				foreach (var fileID in lstFileIDs)
				{
					if (!dctFilesDownloaded.ContainsKey(fileID))
					{
						bool reportMessage = false;
						bool downloadFile = IsDownloadRequired(dctFiles, fileID, downloadFolderPath, folderLayout, reportMessage);
						
						if (downloadFile)
							lstFilesRemaining.Add(fileID);
					}

				}

				if (lstFilesRemaining.Count == 0)
				{
					// All of the files have been downloaded (or already exist and having matching a matching Sha1 hash)
					return true;
				}

				// Scan for the remaining files, thereby creating a ScrollID
				// We will also obtain a new authorization token, which will be associated with the ScrollID
				success = CreateScrollID(lstFilesRemaining, ref cookieJar, out authToken);
				if (!success)
				{
					if (string.IsNullOrWhiteSpace(this.ErrorMessage))
						ReportError("Scroll ID is empty; cannot download files");
					return false;
				}

				// Create a cart
				long cartID = CreateCart(lstFilesRemaining, cookieJar, authToken);
				if (cartID <= 0)
				{
					if (string.IsNullOrWhiteSpace(this.ErrorMessage))
						ReportError("Cart ID is 0; cannot download files");
					return false;
				}

				// Initialize .Tar File Creation
				success = InitializeCartCreation(cartID, cookieJar);
				if (!success)
				{
					if (string.IsNullOrWhiteSpace(this.ErrorMessage))
						ReportError("Error initializing cart " + cartID);
					return false;
				}


				// Wait for the .Tar file to be created
				if (maxMinutesToWait < 2)
					maxMinutesToWait = 2;

				string tarFileURL;
				success = WaitForCartSuccess(cartID, cookieJar, maxMinutesToWait, out tarFileURL);
				if (!success)
				{
					if (string.IsNullOrWhiteSpace(this.ErrorMessage))
						ReportError("Error waiting for cart " + cartID + " to become available");
					return false;
				}

				// Extract the files from the .tar file
				success = DownloadTarFileWithRetry(cookieJar, dctFiles.Keys.ToList<ArchivedFileInfo>(), lstFilesRemaining, bytesDownloaded, downloadFolderPath, folderLayout, tarFileURL, ref dctFilesDownloaded);

			}
			catch (Exception ex)
			{
				if (string.IsNullOrWhiteSpace(this.ErrorMessage))
					ReportError("Error in Downloadfiles: " + ex.Message);
				else if (this.ThrowErrors)
					throw ex;
			}
			finally
			{
				if (cookieJar != null)
					Utilities.Logout(cookieJar);
			}

			return success;
		}

		#endregion

		#region "Protected Methods"

		/// <summary>
		/// Determine the "locked" status of each file
		/// If a file is "locked" then that means the file is available on spinning disk
		/// If the file is not locked, then the file only resides on tape and will need to be restored by the tape robot
		/// </summary>
		/// <param name="xmlString"></param>
		/// <param name="cookieJar"></param>
		/// <param name="authToken"></param>
		/// <returns></returns>
		protected Dictionary<ArchivedFileInfo, bool> CheckLockedStatus(string xmlString, CookieContainer cookieJar, out string authToken)
		{
			var dctFiles = new Dictionary<ArchivedFileInfo, bool>();
			var dtLastStatusTime = DateTime.UtcNow;
			authToken = string.Empty;

			try
			{

				List<ArchivedFileInfo> lstFiles = mReader.ParseResults(xmlString, out authToken);
				if (string.IsNullOrWhiteSpace(authToken))
				{
					ReportError("myemsl_auth_token is empty; cannot download data");
					return dctFiles;
				}

				int fileNumber = 0;
				foreach (var archivedFile in lstFiles)
				{

					fileNumber++;
					if (DateTime.UtcNow.Subtract(dtLastStatusTime).TotalSeconds > 2)
					{
						Console.WriteLine("Checking locked status for files: " + fileNumber + " / " + lstFiles.Count);
						dtLastStatusTime = DateTime.UtcNow;
					}

					bool fileLocked = false;

					if (!this.ForceDownloadViaCart)
					{
						// Construct the URL, e.g. https://my.emsl.pnl.gov/myemsl/item/foo/bar/824531/2.txt?token=ODUiaSI6WyI4MjQ1MzEiXSwicyI6IjIwMTMtMDgtMjBUMTY6MTI6MjEtMDc6MDAiLCJ1IjoiaHVZTndwdFlFZUd6REFBbXVjZXB6dyIsImQiOiAzNjAwJ9NESG37bQjVDlWCJWdrTVqA0wifgrbemVW+nMLgyx/2OfHGk2kFUsrJoOOTdBVsiPrHaeX6/MiaS/szVJKS1ve9UM8pufEEoNEyMBlq7ZxolLfK0Y3OicRPkiKzXZaXkQ7fxc/ec/Ba3uz9wHEs5e+1xYuO36KkSyGGW/xQ7OFx4SyZUm3PrLDk87YPapwoU/30gSk2082oSBOqHuTHzfOjjtbxAIuMa27AbwwOIjG8/Xq4h7squzFNfh/knAkNQ3+21wuZukpsNslWpYO796AFgI2rITaw7HPGJMZKwi+QlMmx27OHE2Qh47b5VQUJUp2tEorFwMjgECo+xX75vg&locked
						// Note that "2.txt" in this URL is just a dummy filename
						// Since we're performing a Head request, it doesn't matter what filename we use
						string URL = Configuration.SearchServerUri + "/myemsl/item/foo/bar/" + archivedFile.FileID + "/2.txt?token=" + authToken + "&locked";

						int maxAttempts = 2;
						Exception mostRecentException;
						HttpStatusCode responseStatusCode;

						WebHeaderCollection responseHeaders = SendHeadRequestWithRetry(URL, cookieJar, maxAttempts, out responseStatusCode, out mostRecentException);

						if (responseStatusCode == HttpStatusCode.ServiceUnavailable)
						{
							fileLocked = false;
						}
						else if (responseHeaders == null || responseHeaders.Count == 0)
						{
							ReportMessage("Error determining if file is available on spinning disk; will assume False");
						}
						else
						{
							// Look for "X-MyEMSL-Locked: true" in the response data
							var headerKeys = responseHeaders.AllKeys.ToList();

							var filteredKeys = GetMyEmslLockedField(headerKeys);

							if (filteredKeys.Count > 0)
							{
								string keyValue = responseHeaders[filteredKeys.First()];
								bool isLocked;
								if (bool.TryParse(keyValue, out isLocked))
								{
									fileLocked = isLocked;
								}
							}

						}
					}

					dctFiles.Add(archivedFile, fileLocked);
				}
			}
			catch (Exception ex)
			{
				ReportError("Exception in CheckLockedStatus: " + ex.Message, ex);
			}

			return dctFiles;
		}


		protected long ComputeTotalBytes(Dictionary<ArchivedFileInfo, bool> dctFiles)
		{
			return ComputeTotalBytes(dctFiles.Keys.ToList<ArchivedFileInfo>());
		}

		protected long ComputeTotalBytes(List<ArchivedFileInfo> dctFiles)
		{
			long bytesToDownload = 0;
			foreach (var archivedFile in dctFiles)
			{
				bytesToDownload += archivedFile.FileSizeBytes;
			}

			return bytesToDownload;
		}

		protected string ConstructDownloadfilePath(DownloadFolderLayout folderLayout, ArchivedFileInfo archivedFile)
		{
			string downloadFilePath;
			switch (folderLayout)
			{
				case DownloadFolderLayout.FlatNoSubfolders:
					downloadFilePath = archivedFile.Filename;
					break;
				case DownloadFolderLayout.SingleDataset:
					downloadFilePath = archivedFile.RelativePathWindows;
					break;
				case DownloadFolderLayout.DatasetNameAndSubFolders:
					downloadFilePath = Path.Combine(archivedFile.Dataset, archivedFile.RelativePathWindows);
					break;
				case DownloadFolderLayout.InstrumentYearQuarterDataset:
					downloadFilePath = archivedFile.PathWithInstrumentAndDatasetWindows;
					break;
				default:
					ReportError("Unrecognized DownloadFolderLayout mode: " + folderLayout.ToString());
					downloadFilePath = Path.Combine(archivedFile.Dataset, archivedFile.RelativePathWindows);
					break;
			}

			if (downloadFilePath.IndexOf("/") > 0)
				downloadFilePath = downloadFilePath.Replace('/', Path.DirectorySeparatorChar);

			return downloadFilePath;
		}

		protected long CreateCart(List<long> lstFiles, CookieContainer cookieJar, string authToken)
		{
			long cartID = 0;

			try
			{
				var querySpec = new Dictionary<string, object>();
				querySpec.Add("items", lstFiles);

				querySpec.Add("auth_token", authToken);

				// Base Url will be https://my.emsl.pnl.gov/myemsl/api/2/cart
				string URL = Configuration.ApiUri + "2/cart";
				string postData = Pacifica.Core.Utilities.ObjectToJson(querySpec);

				int maxAttempts = 4;
				string xmlString = string.Empty;
				Exception mostRecentException;
				bool allowEmptyResponseData = false;

				bool success = SendHTTPRequestWithRetry(URL, cookieJar, postData, EasyHttp.HttpMethod.Post, maxAttempts, allowEmptyResponseData, ref xmlString, out mostRecentException);

				if (string.IsNullOrEmpty(xmlString))
				{
					ReportError("Error creating download cart after " + maxAttempts + " attempts", mostRecentException);
				}

				// Extract the CartID from the response
				Dictionary<string, object> dctResults = Utilities.JsonToObject(xmlString);

				string errorMessage;
				if (!ValidSearchResults(dctResults, out errorMessage))
				{
					ReportError("Error creating download cart: " + errorMessage);
					return 0;
				}

				// Extract the cart_id
				cartID = ReadDictionaryValue(dctResults, "cart_id", 0);
				if (cartID <= 0)
				{
					ReportError("Download cart not created: " + xmlString);
					return 0;
				}

				this.DownloadCartState = CartState.Unsubmitted;
			}
			catch (Exception ex)
			{
				ReportError("Exception in CreateCart: " + ex.Message, ex);
				cartID = 0;
			}

			return cartID;
		}


		protected bool CreateScrollID(List<long> lstFileIDs, ref CookieContainer cookieJar, out string authToken)
		{
			authToken = string.Empty;

			try
			{

				// Scan for Files
				var scanMode = Reader.ScanMode.CreateScrollID;

				string xmlString = ScanForFiles(lstFileIDs, scanMode, ref cookieJar);
				if (string.IsNullOrWhiteSpace(xmlString))
				{
					if (string.IsNullOrWhiteSpace(this.ErrorMessage))
						ReportError("ScanForFiles returned an empty xml result when obtaiing a scroll ID");
					return false;
				}

				// Extract the ScrollID from the response
				Dictionary<string, object> dctResults = Utilities.JsonToObject(xmlString);

				string errorMessage;
				if (!ValidSearchResults(dctResults, out errorMessage))
				{
					ReportError("Error obtaining scroll ID: " + errorMessage);
					return false;
				}

				string scrollID = ReadDictionaryValue(dctResults, "_scroll_id", string.Empty);
				if (string.IsNullOrEmpty(scrollID))
				{
					ReportError("Scroll ID was not created: " + xmlString);
					return false;
				}

				// Obtain a new authorization token by posting to https://my.emsl.pnl.gov/myemsl/elasticsearch/simple_items?search_type=scan&scan&auth
				string URL = Configuration.ElasticSearchUri + "simple_items?search_type=scan&scan&auth";
				string postData = scrollID;

				int maxAttempts = 4;
				xmlString = string.Empty;
				Exception mostRecentException;
				bool allowEmptyResponseData = false;

				bool success = SendHTTPRequestWithRetry(URL, cookieJar, postData, EasyHttp.HttpMethod.Post, maxAttempts, allowEmptyResponseData, ref xmlString, out mostRecentException);
				if (!success)
				{
					string msg = "Error obtaining an AuthToken for the scroll ID";
					if (mostRecentException != null)
						msg += ": " + mostRecentException.Message;

					ReportError(msg);
					return false;
				}

				List<ArchivedFileInfo> lstFiles = mReader.ParseResults(xmlString, out authToken);
				if (string.IsNullOrWhiteSpace(authToken))
				{
					ReportError("myemsl_auth_token is empty; cannot download data using scroll ID");
					return false;
				}

				// Verify that the files in lstFiles match those in lstFileIDs
				var lstReturnedIDs = new SortedSet<long>(from item in lstFiles select item.FileID);

				foreach (var lstFileID in lstFileIDs)
				{
					if (!lstReturnedIDs.Contains(lstFileID))
					{
						ReportError("FileID " + lstFileID + " was not included in the results returned for the scroll ID; downloaded files will be incomplete and the download will thus be aborted");
						return false;
					}
				}

				return true;
			}
			catch (Exception ex)
			{
				ReportError("Exception in CreateCart: " + ex.Message, ex);
				return false;
			}

		}

		protected bool DownloadFile(string URL, CookieContainer cookieJar, int maxAttempts, string downloadFilePath, out Exception mostRecentException)
		{

			mostRecentException = null;

			int timeoutSeconds = 100;
			int attempts = 0;
			bool success = false;

			HttpStatusCode responseStatusCode;

			while (!success && attempts <= maxAttempts)
			{
				try
				{
					attempts++;
					success = EasyHttp.GetFile(URL, cookieJar, out responseStatusCode, downloadFilePath, timeoutSeconds);

					if (!success)
						break;
				}
				catch (Exception ex)
				{
					mostRecentException = ex;
					if (attempts >= maxAttempts)
					{
						success = false;
					}
					else
					{
						// Wait 2 seconds, then retry
						Console.WriteLine("Exception in DownloadFile on attempt " + attempts + ": " + ex.Message);
						System.Threading.Thread.Sleep(2000);
						timeoutSeconds = (int)(Math.Ceiling(timeoutSeconds * 1.5));
						continue;
					}
				}
			}

			return success;
		}

		protected Dictionary<long, string> DownloadLockedFiles(
			Dictionary<ArchivedFileInfo, bool> dctFiles,
			CookieContainer cookieJar,
			string authToken,
			string downloadFolderPath,
			DownloadFolderLayout folderLayout,
			out long bytesDownloaded)
		{
			var dctFilesDownloaded = new Dictionary<long, string>();
			bytesDownloaded = 0;

			try
			{
				// Determine total amount of data to be downloaded
				long bytesToDownload = ComputeTotalBytes(dctFiles);

				var lstLockedFiles = GetLockedFileList(dctFiles);

				foreach (var archivedFile in lstLockedFiles)
				{
					// Construct the URL, e.g. https://my.emsl.pnl.gov/myemsl/item/foo/bar/824531/Euplotes_1_HPRP_1_16_22Nov09_Falcon_09-09-14_peaks.dat?token=ODUiaSI6WyI4MjQ1MzEiXSwicyI6IjIwMTMtMDgtMjBUMTY6MTI6MjEtMDc6MDAiLCJ1IjoiaHVZTndwdFlFZUd6REFBbXVjZXB6dyIsImQiOiAzNjAwJ9NESG37bQjVDlWCJWdrTVqA0wifgrbemVW+nMLgyx/2OfHGk2kFUsrJoOOTdBVsiPrHaeX6/MiaS/szVJKS1ve9UM8pufEEoNEyMBlq7ZxolLfK0Y3OicRPkiKzXZaXkQ7fxc/ec/Ba3uz9wHEs5e+1xYuO36KkSyGGW/xQ7OFx4SyZUm3PrLDk87YPapwoU/30gSk2082oSBOqHuTHzfOjjtbxAIuMa27AbwwOIjG8/Xq4h7squzFNfh/knAkNQ3+21wuZukpsNslWpYO796AFgI2rITaw7HPGJMZKwi+QlMmx27OHE2Qh47b5VQUJUp2tEorFwMjgECo+xX75vg&locked
					string URL = Configuration.SearchServerUri + "/myemsl/item/foo/bar/" + archivedFile.FileID + "/" + archivedFile.Filename + "?token=" + authToken + "&locked";

					string downloadFilePath = ConstructDownloadfilePath(folderLayout, archivedFile);

					downloadFilePath = Path.Combine(downloadFolderPath, downloadFilePath);
					var fiTargetFile = new FileInfo(downloadFilePath);
					if (!fiTargetFile.Directory.Exists)
					{
						ReportMessage("Creating target folder: " + fiTargetFile.Directory.FullName);
						fiTargetFile.Directory.Create();
					}

					int maxAttempts = 5;
					Exception mostRecentException;					
					bool reportMessage = true;

					bool downloadFile = IsDownloadRequired(archivedFile, downloadFilePath, reportMessage);

					if (downloadFile)
					{
						bool retrievalSuccess = DownloadFile(URL, cookieJar, maxAttempts, downloadFilePath, out mostRecentException);

						if (retrievalSuccess)
						{
							dctFilesDownloaded.Add(archivedFile.FileID, archivedFile.PathWithInstrumentAndDatasetWindows);

							UpdateFileModificationTime(fiTargetFile, archivedFile.SubmissionTime);
						}
						else
						{
							// Show the error at the console but do not throw an exception
							if (mostRecentException == null)
								ReportMessage("Failure downloading " + Path.GetFileName(downloadFilePath) + ": unknown reason");
							else
								ReportMessage("Failure downloading " + Path.GetFileName(downloadFilePath) + ": " + mostRecentException.Message);
						}
					}
					else
					{
						// Download skipped
						// Need to add to the downloaded files dictionary so that the file doesn't get downloaded via the .tar file mechanism
						dctFilesDownloaded.Add(archivedFile.FileID, archivedFile.PathWithInstrumentAndDatasetWindows);
					}

					if (!this.DownloadedFiles.ContainsKey(downloadFilePath))
						this.DownloadedFiles.Add(downloadFilePath, archivedFile);

					bytesDownloaded += archivedFile.FileSizeBytes;
					UpdateProgress(bytesDownloaded, bytesToDownload);
				}
			}
			catch (Exception ex)
			{
				ReportError("Exception in DownloadLockedFiles: " + ex.Message, ex);
				return new Dictionary<long, string>();
			}

			return dctFilesDownloaded;
		}

		protected bool DownloadTarFileWithRetry(
			CookieContainer cookieJar,
			List<ArchivedFileInfo> lstFilesInArchive,
			List<long> lstFilesRemaining,
			long bytesDownloaded,
			string downloadFolderPath,
			DownloadFolderLayout folderLayout,
			string tarFileURL,
			ref Dictionary<long, string> dctFilesDownloaded)
		{
			bool success = false;

			try
			{
				int maxAttempts = 5;
				Exception mostRecentException = null;

				int timeoutSeconds = 100;
				int attempts = 0;

				while (!success && attempts <= maxAttempts)
				{
					try
					{
						attempts++;
						success = DownloadAndExtractTarFile(cookieJar, lstFilesInArchive, lstFilesRemaining, bytesDownloaded, downloadFolderPath, folderLayout, tarFileURL, ref dctFilesDownloaded, timeoutSeconds);

						if (!success)
							break;
					}
					catch (Exception ex)
					{
						mostRecentException = ex;
						if (attempts >= maxAttempts)
						{
							success = false;
						}
						else
						{
							// Wait 2 seconds, then retry
							Console.WriteLine("Exception in DownloadTarFileWithRetry on attempt " + attempts + ": " + ex.Message);
							System.Threading.Thread.Sleep(2000);
							timeoutSeconds = (int)(Math.Ceiling(timeoutSeconds * 1.5));
							continue;
						}
					}
				}

				if (success)
				{
					ReportMessage("Successfully extracted files from .tar file at " + tarFileURL);
					UpdateProgress(1, 1);
				}

				if (!success)
				{
					if (mostRecentException == null)
						ReportMessage("Failed to extract files from .tar file: unknown reason");
					else
						ReportMessage("Failed to extract files from .tar file: unknown reason: " + mostRecentException.Message);

					return false;
				}

			}
			catch (Exception ex)
			{
				ReportError("Exception in ExtractTarFile: " + ex.Message, ex);
				return false;
			}

			return success;

		}

		protected bool DownloadAndExtractTarFile(
			CookieContainer cookieJar,
			List<ArchivedFileInfo> lstFilesInArchive,
			List<long> lstFilesRemaining,
			long bytesDownloaded,
			string downloadFolderPath,
			DownloadFolderLayout folderLayout,
			string tarFileURL,
			ref Dictionary<long, string> dctFilesDownloaded,
			int timeoutSeconds = 100)
		{
			double maxTimeoutHours = 24;
			NetworkCredential loginCredentials = null;
			HttpWebRequest request = EasyHttp.InitializeRequest(tarFileURL, ref cookieJar, ref timeoutSeconds, loginCredentials, maxTimeoutHours);

			long bytesToDownload = ComputeTotalBytes(lstFilesInArchive);

			// Prepare the request object
			request.Method = "GET";
			request.PreAuthenticate = false;

			// Receive response		
			HttpWebResponse response = null;
			try
			{
				request.Timeout = timeoutSeconds * 1000;
				response = (HttpWebResponse)request.GetResponse();

				if (response.StatusCode == HttpStatusCode.OK)
				{
					// Download the file and extract the files as the file is downloaded
					// This way, the .tar file is never actually created on a local hard drive
					// Code modeled after https://github.com/icsharpcode/SharpZipLib/wiki/GZip-and-Tar-Samples

					Stream ReceiveStream = response.GetResponseStream();

					TarInputStream tarIn = new TarInputStream(ReceiveStream);
					TarEntry tarEntry;
					while ((tarEntry = tarIn.GetNextEntry()) != null)
					{
						if (tarEntry.IsDirectory)
						{
							continue;
						}

						string sourceFile = tarEntry.Name;

						// Long files (over 100 characters) will have part of their name in tarEntry.Name and part of it in tarEntry.Prefix
						// Check for this
						if (!string.IsNullOrEmpty(tarEntry.Prefix))
							sourceFile = tarEntry.Prefix + '/' + sourceFile;

						// Convert the unix forward slashes in the filenames to windows backslashes
						sourceFile = sourceFile.Replace('/', Path.DirectorySeparatorChar);

						// The Filename of the tar entry should start with a folder name that is a MyEMSL FileID
						int charIndex = sourceFile.IndexOf(Path.DirectorySeparatorChar);
						if (charIndex < 1)
						{
							ReportMessage("Warning, skipping invalid entry in .tar file; does not start with a MyEMSL FileID value: " + sourceFile);
							continue;
						}

						string fileIDText = sourceFile.Substring(0, charIndex);
						long fileID;
						if (!long.TryParse(fileIDText, out fileID))
						{
							ReportMessage("Warning, skipping invalid entry in .tar file; does not start with a MyEMSL FileID value: " + sourceFile);
							continue;
						}

						// Lookup fileID in dctFiles
						var archivedFileLookup = GetArchivedFileByID(lstFilesInArchive, fileID);

						if (archivedFileLookup.Count == 0)
						{
							ReportMessage("Warning, skipping .tar file entry since MyEMSL FileID '" + fileID + "' was not recognized: " + sourceFile);
							continue;
						}

						// Confirm that the name of the file in the .Tar file matches the expected file name
						// Names in the tar file will be limited to 255 characters (including any preceding parent folder names) so we should not compare the full name
						// Furthermore, the primary filename is limited to 100 characters, so it too could be truncated

						var archivedFile = archivedFileLookup.First();

						var fiSourceFile = new FileInfo(sourceFile);
						if (!archivedFile.Filename.ToLower().StartsWith(fiSourceFile.Name.ToLower()))
							ReportMessage("Warning, name conflict; filename in .tar file is " + fiSourceFile.Name + " but expected filename is " + archivedFile.Filename);

						// Define the local file path
						string downloadFilePath = ConstructDownloadfilePath(folderLayout, archivedFile);
						downloadFilePath = Path.Combine(downloadFolderPath, downloadFilePath);

						// Create the target folder if necessary
						var targetFile = new FileInfo(downloadFilePath);
						if (!targetFile.Directory.Exists)
							targetFile.Directory.Create();

						// Extract the file from the stream
						using (var outStr = new FileStream(targetFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))
						{
							tarIn.CopyEntryContents(outStr);
						}

						UpdateFileModificationTime(targetFile, archivedFile.SubmissionTime);

						if (!this.DownloadedFiles.ContainsKey(downloadFilePath))
							this.DownloadedFiles.Add(downloadFilePath, archivedFile);

						bytesDownloaded += archivedFile.FileSizeBytes;
						UpdateProgress(bytesDownloaded, bytesToDownload);
					}

				}
				else
				{
					throw new WebException("HTTP response code not OK in DownloadAndExtractTarFile: " + response.StatusCode + ", " + response.StatusDescription);
				}
			}
			catch (WebException ex)
			{
				string responseData = string.Empty;
				if (ex.Response != null)
				{
					using (StreamReader sr = new StreamReader(ex.Response.GetResponseStream()))
					{
						int maxLines = 20;
						int linesRead = 0;
						while (sr.Peek() > -1 && linesRead < maxLines)
						{
							responseData += sr.ReadLine() + Environment.NewLine;
							linesRead++;
						}
					}
				}
				throw new Exception(responseData, ex);
			}
			finally
			{
				if (response != null)
				{
					((IDisposable)response).Dispose();
				}
			}

			return true;
		}

		protected bool FileMatchesHash(string localFilePath, string Sha1HashExpected)
		{
			bool fileMatchesHash = false;

			try
			{
				string actualSha1Hash = Utilities.GenerateSha1Hash(localFilePath);

				if (actualSha1Hash == Sha1HashExpected)
					fileMatchesHash = true;
			}
			catch (Exception ex)
			{
				ReportError("Exception in FileChanged: " + ex.Message, ex);
				return false;
			}

			return fileMatchesHash;
		}

		protected List<ArchivedFileInfo> GetArchivedFileByID(List<ArchivedFileInfo> lstFilesInArchive, long fileID)
		{
			var archivedFileLookup = (from item in lstFilesInArchive
									  where item.FileID == fileID
									  select item).ToList();
			return archivedFileLookup;
		}

		protected List<ArchivedFileInfo> GetLockedFileList(Dictionary<ArchivedFileInfo, bool> dctFiles)
		{
			var lstLockedFiles = (from item in dctFiles
								  where item.Value == true
								  select item.Key).ToList();
			return lstLockedFiles;
		}

		protected List<string> GetMyEmslLockedField(List<string> headerKeys)
		{
			var lstFilteredKeys = (from item in headerKeys
								   where item.Contains("MyEMSL-Locked")
								   select item).ToList();
			return lstFilteredKeys;
		}

		protected List<int> GetUniqueDatasetIDList(Dictionary<ArchivedFileInfo, bool> dctFiles)
		{
			var lstDatasetIDs = (from item in dctFiles
								 group item by item.Key.DatasetID into g
								 select g.Key).ToList<int>();
			return lstDatasetIDs;
		}

		protected bool InitializeCartCreation(long cartID, CookieContainer cookieJar)
		{
			bool success = false;

			try
			{
				// Note that even though postData is empty we need to "Post" to https://my.emsl.pnl.gov/myemsl/api/2/cart/11?submit
				string URL = Configuration.ApiUri + "2/cart/" + cartID + "?submit";
				string postData = string.Empty;

				int maxAttempts = 4;
				string xmlString = string.Empty;
				Exception mostRecentException;
				bool allowEmptyResponseData = true;

				success = SendHTTPRequestWithRetry(URL, cookieJar, postData, EasyHttp.HttpMethod.Post, maxAttempts, allowEmptyResponseData, ref xmlString, out mostRecentException);
				if (!success)
				{
					string msg = "Error initializing creation of cart " + cartID;
					if (mostRecentException != null)
						msg += ": " + mostRecentException.Message;

					ReportError(msg);
				}
				this.DownloadCartState = CartState.Building;

			}
			catch (Exception ex)
			{
				ReportError("Exception in InitializeCartCreation: " + ex.Message, ex);
				return false;
			}

			return success;
		}

		/// <summary>
		/// Determines whether or not a file should be downloaded
		/// </summary>
		/// <param name="dctFiles"></param>
		/// <param name="fileID"></param>
		/// <param name="folderLayout"></param>
		/// <param name="reportMessage"></param>
		/// <returns></returns>
		protected bool IsDownloadRequired(Dictionary<ArchivedFileInfo, bool> dctFiles, long fileID, string downloadFolderPath, DownloadFolderLayout folderLayout, bool reportMessage)
		{
			var lstMatches = (from item in dctFiles where item.Key.FileID == fileID select item.Key).ToList();

			if (lstMatches.Count == 0)
				return true;

			var archivedFile = lstMatches.First();
			string downloadFilePath = ConstructDownloadfilePath(folderLayout, archivedFile);
			downloadFilePath = Path.Combine(downloadFolderPath, downloadFilePath);

			bool downloadFile = IsDownloadRequired(archivedFile, downloadFilePath, reportMessage);

			return downloadFile;
		}

		/// <summary>
		/// Determines whether or not a file should be downloaded
		/// </summary>
		/// <param name="archivedFile"></param>
		/// <param name="downloadFilePath"></param>
		/// <returns></returns>
		protected bool IsDownloadRequired(ArchivedFileInfo archivedFile, string downloadFilePath, bool reportMessage)
		{
			bool downloadFile;

			if (!File.Exists(downloadFilePath))
			{
				if (reportMessage) ReportMessage("Downloading " + downloadFilePath);
				downloadFile = true;
			}
			else
			{
				switch (this.OverwriteMode)
				{
					case Overwrite.Always:
						if (reportMessage) ReportMessage("Overwriting " + downloadFilePath);
						downloadFile = true;
						break;
					case Overwrite.IfChanged:
						if (string.IsNullOrEmpty(archivedFile.Sha1Hash))
						{
							if (reportMessage) ReportMessage("Overwriting (Sha1 hash missing) " + downloadFilePath);
							downloadFile = true;
							break;
						}

						if (FileMatchesHash(downloadFilePath, archivedFile.Sha1Hash))
						{
							if (reportMessage) ReportMessage("Skipping (file unchanged) " + downloadFilePath);
							downloadFile = false;
						}
						else
						{
							if (reportMessage) ReportMessage("Overwriting changed file " + downloadFilePath);
							downloadFile = true;
						}
						break;
					case Overwrite.Never:
						if (reportMessage) ReportMessage("Skipping (Overwrite disabled) " + downloadFilePath);
						downloadFile = false;
						break;
					default:
						throw new ArgumentOutOfRangeException("Unrecognized OverwriteMode: " + this.OverwriteMode.ToString());
				}
			}

			return downloadFile;
		}

		protected new void ResetStatus()
		{
			base.ResetStatus();
			this.DownloadCartState = CartState.NoCart;
			this.PercentComplete = 0;
			this.DownloadedFiles.Clear();
		}

		private string ScanForFiles(List<long> lstFileIDs, Reader.ScanMode scanMode, ref CookieContainer cookieJar)
		{
			var dctSearchTerms = new List<KeyValuePair<string, string>>();

			foreach (var fileID in lstFileIDs)
			{
				dctSearchTerms.Add(new KeyValuePair<string, string>("_id", fileID.ToString()));
			}

			var logicalOperator = SearchOperator.Or;

			string xmlString = mReader.RunQuery(dctSearchTerms, dctSearchTerms.Count + 1, logicalOperator, scanMode, ref cookieJar);

			return xmlString;

		}

		protected WebHeaderCollection SendHeadRequestWithRetry(
			string URL,
			CookieContainer cookieJar,
			int maxAttempts,
			out HttpStatusCode responseStatusCode,
			out Exception mostRecentException)
		{

			mostRecentException = null;

			var responseHeaders = new WebHeaderCollection();

			int timeoutSeconds = 2;
			int attempts = 0;
			bool success = false;
			responseStatusCode = HttpStatusCode.NotFound;

			while (!success && attempts <= maxAttempts)
			{
				try
				{
					attempts++;
					responseHeaders = EasyHttp.GetHeaders(URL, cookieJar, out responseStatusCode, timeoutSeconds);

					if (responseHeaders == null || responseHeaders.Count == 0)
					{
						Console.WriteLine("Empty headers in SendHeadRequestWithRetry on attempt " + attempts);
						timeoutSeconds = (int)(Math.Ceiling(timeoutSeconds * 1.5));
					}
					else
						success = true;
				}
				catch (Exception ex)
				{
					mostRecentException = ex;

					if (responseStatusCode == HttpStatusCode.ServiceUnavailable)
						// File is not locked; no point in retrying the head request.
						break;

					if (attempts <= maxAttempts)
					{
						// Wait 2 seconds, then retry
						Console.WriteLine("Exception in SendHeadRequestWithRetry on attempt " + attempts + ": " + ex.Message);
						System.Threading.Thread.Sleep(2000);
						timeoutSeconds = (int)(Math.Ceiling(timeoutSeconds * 1.5));
						continue;
					}
				}
			}

			return responseHeaders;
		}


		protected string UpdateCartState(Dictionary<string, object> dctCartInfo, string cartState)
		{
			string tarFileURL = string.Empty;

			if (string.IsNullOrWhiteSpace(cartState))
				return string.Empty;

			switch (cartState)
			{
				case "unsubmitted":
					this.DownloadCartState = CartState.Unsubmitted;
					break;
				case "building":
					this.DownloadCartState = CartState.Building;
					break;
				case "available":
					tarFileURL = ReadDictionaryValue(dctCartInfo, "url", string.Empty);
					if (string.IsNullOrWhiteSpace(tarFileURL))
					{
						ReportMessage("Warning, cart status is " + cartState + " but the download URL was not returned by MyEMSL");
					}
					else
					{
						this.DownloadCartState = CartState.Available;
					}
					break;
				case "expired":
					this.DownloadCartState = CartState.Expired;
					break;
				case "admin":
					this.DownloadCartState = CartState.Admin;
					break;
				case "unknown":
					this.DownloadCartState = CartState.Unknown;
					break;
				default:
					// Some other unknown state; ignore it
					break;
			}

			return tarFileURL;
		}

		protected void UpdateFileModificationTime(FileInfo fiTargetFile, string modificationTime)
		{
			// Update the file modification time
			fiTargetFile.Refresh();
			if (fiTargetFile.Exists)
			{
				DateTime dtWriteTime = DateTime.Parse(modificationTime);
				fiTargetFile.LastWriteTime = dtWriteTime;
			}
		}

		protected void UpdateProgress(long bytesDownloaded, long bytesToDownload)
		{
			if (bytesToDownload > 0)
			{
				double percentComplete = bytesDownloaded / (double)bytesToDownload * 100;
				this.PercentComplete = Math.Round(percentComplete);

				OnProgressUpdate(new ProgressEventArgs(percentComplete));
			}
		}

		protected bool WaitForCartSuccess(long cartID, CookieContainer cookieJar, int maxMinutesToWait, out string tarFileURL)
		{
			DateTime dtStartTime = DateTime.UtcNow;
			DateTime dtLastUpdateTime = DateTime.UtcNow.Subtract(new TimeSpan(0, 0, 50));
			int sleepTimeSeconds = 5;
			bool notifyWhenCartAvailable = false;

			tarFileURL = string.Empty;

			try
			{

				// Construct the URL, e.g. http://my.emsl.pnl.gov/myemsl/api/2/cart/15
				string URL = Configuration.ApiUri + "2/cart/" + cartID;
				string postData = string.Empty;

				int maxAttempts = 3;
				string xmlString = string.Empty;
				Exception mostRecentException;
				bool allowEmptyResponseData = false;


				while (this.DownloadCartState != CartState.Available)
				{
					bool success = SendHTTPRequestWithRetry(URL, cookieJar, postData, EasyHttp.HttpMethod.Get, maxAttempts, allowEmptyResponseData, ref xmlString, out mostRecentException);

					if (success)
					{
						// Extract the Cart status from the response
						Dictionary<string, object> dctResults = Utilities.JsonToObject(xmlString);

						string errorMessage;
						if (!ValidSearchResults(dctResults, out errorMessage))
						{
							ReportMessage("Warning, invalid cart status data: " + xmlString);
						}
						else
						{
							var dctCartInfo = RetrieveDictionaryListByKey(dctResults, "carts");

							if (dctCartInfo.Count == 0)
							{
								Console.WriteLine("Warning: Carts listing is empty");
							}
							else
							{
								// Extract the cart state
								string cartState = ReadDictionaryValue(dctCartInfo[0], "state", string.Empty);

								tarFileURL = UpdateCartState(dctCartInfo[0], cartState);

								if (this.DownloadCartState == CartState.Available)
								{
									break;
								}
								else if (this.DownloadCartState == CartState.Expired)
								{
									ReportError("Cart " + cartID + " is expired and cannot be downloaded; aborting");
									break;
								}
							}
						}
					}

					double minutesElapsed = DateTime.UtcNow.Subtract(dtStartTime).TotalMinutes;

					if (minutesElapsed > maxMinutesToWait)
					{
						ReportError("Over " + maxMinutesToWait + " minutes have elapsed and download cart " + cartID + " is still not ready; aborting");
						break;
					}

					if (minutesElapsed > 5 && sleepTimeSeconds < 15)
						sleepTimeSeconds = 15;

					if (minutesElapsed > 15 && sleepTimeSeconds < 30)
						sleepTimeSeconds = 30;

					if (DateTime.UtcNow.Subtract(dtLastUpdateTime).TotalMinutes >= 1)
					{
						ReportMessage("Waiting for cart " + cartID + " to become available: " + minutesElapsed.ToString("0.0") + " minutes elapsed");
						dtLastUpdateTime = DateTime.UtcNow;
						notifyWhenCartAvailable = true;
					}

					// Sleep for 5 to 30 seconds (depending on how long we've been waiting)
					System.Threading.Thread.Sleep(sleepTimeSeconds * 1000);
				}

			}
			catch (Exception ex)
			{
				ReportError("Exception in WaitForCartSuccess: " + ex.Message, ex);
				return false;
			}

			if (this.DownloadCartState == CartState.Available)
			{
				if (notifyWhenCartAvailable)
					ReportMessage("Cart " + cartID + " is now ready for download");

				return true;
			}
			else
				return false;
		}

		#endregion
	}
}
