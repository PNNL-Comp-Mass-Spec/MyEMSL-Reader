using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyEMSLReader
{
	public class DatasetFolderOrFileInfo
	{
		/// <summary>
		/// The UCT datetime that the info was cached in memory
		/// </summary>
		public DateTime CacheDateUTC
		{
			get;
			private set;
		}

		/// <summary>
		/// MyEMSL File ID
		/// </summary>
		/// <remarks>Will be 0 if this is a folder</remarks>
		public Int64 FileID
		{
			get;
			private set;
		}

		/// <summary>
		/// True if this entity is a folder in MyEMSL
		/// </summary>
		public bool IsFolder
		{
			get;
			private set;
		}

		/// <summary>
		/// File information
		/// </summary>
		public ArchivedFileInfo FileInfo
		{
			get;
			private set;
		}

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="fileID"></param>
		/// <param name="isFolder"></param>
		/// <param name="fileInfo"></param>
		public DatasetFolderOrFileInfo(Int64 fileID, bool isFolder, ArchivedFileInfo fileInfo)
		{
			CacheDateUTC = DateTime.UtcNow; 
			FileID = fileID;
			IsFolder = isFolder;
			FileInfo = fileInfo;			
		}

		public override string ToString()
		{
			return "FileID " + FileID + "; " + FileInfo.ToString();
		}
	}
}
