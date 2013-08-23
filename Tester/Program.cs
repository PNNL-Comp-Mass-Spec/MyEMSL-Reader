using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Tester
{
	class Program
	{
		static void Main(string[] args)
		{
			var lstFileIDs = TestReader();
		
			TestDownloader(lstFileIDs);
		}

		static List<long> TestReader()
		{
			var reader = new MyEMSLReader.Reader();
			reader.IncludeAllRevisions = false;

			string datasetName;
			datasetName = "Blank_B-2_20Apr12_Draco_12-02-37";
			datasetName = "QC_Shew_11_06_pt5_d2_11Jun12_Draco_12-04-14";

			var lstFileIDs = new List<long>();

			bool testExistingCart = true;
			if (testExistingCart)
			{
				lstFileIDs.Add(662508);
				lstFileIDs.Add(662509);
				lstFileIDs.Add(662510);
				lstFileIDs.Add(662511);
				lstFileIDs.Add(662512);
				lstFileIDs.Add(662513);
				lstFileIDs.Add(662514);
				lstFileIDs.Add(662515);
				lstFileIDs.Add(662516);
			}
			else
			{
				var results = reader.FindFilesByDatasetName(datasetName, "QC");
			}

			return lstFileIDs;
		}

		static void TestDownloader(List<long> lstFileIDs)
		{
			var downloader = new MyEMSLReader.Downloader();
			downloader.DownloadFiles(lstFileIDs, @"F:\Temp\MyEMSL", MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders);
		}

	}
}
