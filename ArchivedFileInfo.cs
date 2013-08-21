using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyEMSLReader
{
	public class ArchivedFileInfo
	{

		#region "Auto Properties"
		public string Dataset
		{
			get;
			private set;
		}

		public string Filename
		{
			get;
			private set;
		}
		public string SubDirPath
		{
			get;
			private set;
		}

		public long FileID
		{
			get;
			set;
		}

		public long FileSizeBytes
		{
			get;
			set;
		}

		public string Instrument
		{
			get;
			set;
		}

		#endregion

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="dataset">Dataset name</param>
		/// <param name="filename">Filename</param>
		/// <param name="subDirPath">Subdirectory below dataset (empty if at the dataset level)</param>
		public ArchivedFileInfo(string dataset, string filename, string subDirPath) :
			this(dataset, filename, subDirPath, 0, 0) 
		{ }
		
		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="dataset">Dataset name</param>
		/// <param name="filename">Filename</param>
		/// <param name="subDirPath">Subdirectory below dataset (empty if at the dataset level)</param>
		/// <param name="fileSizeBytes">File size, in bytes</param>
		/// <param name="fileID">MyEMSL File ID</param>
		public ArchivedFileInfo(string dataset, string filename, string subDirPath, long fileSizeBytes, long fileID) :
			this(dataset, filename, subDirPath, fileSizeBytes, fileID, "") 
		{ }

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="dataset">Dataset name</param>
		/// <param name="filename">Filename</param>
		/// <param name="subDirPath">Subdirectory below dataset (empty if at the dataset level)</param>
		/// <param name="fileSizeBytes">File size, in bytes</param>
		/// <param name="fileID">MyEMSL File ID</param>
		/// <param name="instrument">Instrument name</param>
		public ArchivedFileInfo(string dataset, string filename, string subDirPath, long fileSizeBytes, long fileID, string instrument)
		{
			this.Dataset = dataset;
			this.Filename = filename;
			this.SubDirPath = subDirPath;
			this.FileSizeBytes = fileSizeBytes;
			this.FileID = fileID;
			this.Instrument = instrument;
		}

	}
}
