using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyEMSLReader
{
	public class DatasetFolderOrFileInfo
	{
		// Will be 0 if this is a folder
		public Int64 FileID
		{
			get;
			private set;
		}

		public bool IsFolder
		{
			get;
			private set;
		}

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
