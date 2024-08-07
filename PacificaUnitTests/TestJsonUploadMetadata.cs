using System;
using System.Collections.Generic;
using NUnit.Framework;
using Pacifica.DataUpload;
using Pacifica.Json;

namespace PacificaUnitTests
{
    [TestFixture]
    public class TestJsonUploadMetadata
    {
        // Ignore Spelling: absolutelocalpath, backend, ctime, hashsum, hashtype, mimetype, mtime, subdir

        private const string TestJsonMetadata =
            "[{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.instrument\",\"value\":\"Lumos01\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.instrument_id\",\"value\":34245}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.date_code\",\"value\":\"2022_4\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.dataset\",\"value\":\"Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.campaign_name\",\"value\":\"Stacy_cortex_2021\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.experiment_name\",\"value\":\"Stacy_cortex_2022\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.dataset_name\",\"value\":\"Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.campaign_id\",\"value\":\"3759\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.experiment_id\",\"value\":\"326655\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.dataset_id\",\"value\":\"1088263\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"organism_name\",\"value\":\"Glycine_max\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.organism_id\",\"value\":\"1130\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"ncbi_taxonomy_id\",\"value\":\"3847\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.separation_type\",\"value\":\"LC-NanoPot_100min\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.dataset_type\",\"value\":\"HMS-HCD-MSn\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"omics.dms.run_acquisition_length_min\",\"value\":90}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"User of Record\",\"value\":\"43635\"}," +
            "{\"destinationTable\":\"TransactionKeyValue\",\"key\":\"user_of_record\",\"value\":\"43635\"}," +
            "{\"destinationTable\":\"Transactions.instrument\",\"value\":34245}," +
            "{\"destinationTable\":\"Transactions.project\",\"value\":\"51846\"}," +
            "{\"destinationTable\":\"Transactions.submitter\",\"value\":52405}," +
            "{\"destinationTable\":\"Files\",\"name\":\"metadata.xml\",\"absolutelocalpath\":\"F:\\\\Lumos01\\\\2022_4\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\\\metadata.xml\",\"subdir\":\"data/\",\"size\":\"1495\",\"hashsum\":\"63372bf473a495fa2105fdf739a9adf26aff5a4a\",\"mimetype\":\"application/octet-stream\",\"hashtype\":\"sha1\",\"ctime\":\"2022-10-24T17:04:34\",\"mtime\":\"2022-10-24T17:04:34\"}," +
            "{\"destinationTable\":\"Files\",\"name\":\"Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9.raw\",\"absolutelocalpath\":\"F:\\\\Lumos01\\\\2022_4\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9.raw\",\"subdir\":\"data/\",\"size\":\"697568674\",\"hashsum\":\"7696671feeb902576e90e5594964d5b8b87cf470\",\"mimetype\":\"application/octet-stream\",\"hashtype\":\"sha1\",\"ctime\":\"2022-10-24T16:56:49\",\"mtime\":\"2022-10-24T02:14:51\"}," +
            "{\"destinationTable\":\"Files\",\"name\":\"index.html\",\"absolutelocalpath\":\"F:\\\\Lumos01\\\\2022_4\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\\\QC\\\\index.html\",\"subdir\":\"data/QC\",\"size\":\"4557\",\"hashsum\":\"24a8c51befcc6c4d7a83b4272f34d926008c7d43\",\"mimetype\":\"application/octet-stream\",\"hashtype\":\"sha1\",\"ctime\":\"2022-10-24T17:03:46\",\"mtime\":\"2022-10-24T17:03:46\"}," +
            "{\"destinationTable\":\"Files\",\"name\":\"Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_BPI_MS.png\",\"absolutelocalpath\":\"F:\\\\Lumos01\\\\2022_4\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\\\QC\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_BPI_MS.png\",\"subdir\":\"data/QC\",\"size\":\"57067\",\"hashsum\":\"e82f69dd7aa49ab85f8c323e84b9b5ee7bedb9eb\",\"mimetype\":\"application/octet-stream\",\"hashtype\":\"sha1\",\"ctime\":\"2022-10-24T17:03:14\",\"mtime\":\"2022-10-24T17:03:14\"}," +
            "{\"destinationTable\":\"Files\",\"name\":\"Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_BPI_MSn.png\",\"absolutelocalpath\":\"F:\\\\Lumos01\\\\2022_4\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\\\QC\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_BPI_MSn.png\",\"subdir\":\"data/QC\",\"size\":\"51087\",\"hashsum\":\"73556b1203a29ad00c9cd780e99cd911bcd1b1ee\",\"mimetype\":\"application/octet-stream\",\"hashtype\":\"sha1\",\"ctime\":\"2022-10-24T17:03:17\",\"mtime\":\"2022-10-24T17:03:17\"}," +
            "{\"destinationTable\":\"Files\",\"name\":\"Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_DatasetInfo.xml\",\"absolutelocalpath\":\"F:\\\\Lumos01\\\\2022_4\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\\\QC\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_DatasetInfo.xml\",\"subdir\":\"data/QC\",\"size\":\"2260\",\"hashsum\":\"510da615de5388547ecbf920399e346149a94d14\",\"mimetype\":\"application/octet-stream\",\"hashtype\":\"sha1\",\"ctime\":\"2022-10-24T17:03:46\",\"mtime\":\"2022-10-24T17:03:46\"}," +
            "{\"destinationTable\":\"Files\",\"name\":\"Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_HighAbu_LCMS.png\",\"absolutelocalpath\":\"F:\\\\Lumos01\\\\2022_4\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\\\QC\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_HighAbu_LCMS.png\",\"subdir\":\"data/QC\",\"size\":\"280957\",\"hashsum\":\"383e9e4f85137e1a2520bad0aea3388c1e1f2ce2\",\"mimetype\":\"application/octet-stream\",\"hashtype\":\"sha1\",\"ctime\":\"2022-10-24T17:03:42\",\"mtime\":\"2022-10-24T17:03:42\"}," +
            "{\"destinationTable\":\"Files\",\"name\":\"Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_HighAbu_LCMS_MSn.png\",\"absolutelocalpath\":\"F:\\\\Lumos01\\\\2022_4\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\\\QC\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_HighAbu_LCMS_MSn.png\",\"subdir\":\"data/QC\",\"size\":\"486773\",\"hashsum\":\"84a4d6b2a10bdedbe76e5a6b302d4135442861e7\",\"mimetype\":\"application/octet-stream\",\"hashtype\":\"sha1\",\"ctime\":\"2022-10-24T17:03:46\",\"mtime\":\"2022-10-24T17:03:46\"}," +
            "{\"destinationTable\":\"Files\",\"name\":\"Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_LCMS.png\",\"absolutelocalpath\":\"F:\\\\Lumos01\\\\2022_4\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\\\QC\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_LCMS.png\",\"subdir\":\"data/QC\",\"size\":\"392136\",\"hashsum\":\"ae74b2c041e78435a907171874fc4fe0c88ff621\",\"mimetype\":\"application/octet-stream\",\"hashtype\":\"sha1\",\"ctime\":\"2022-10-24T17:03:28\",\"mtime\":\"2022-10-24T17:03:28\"}," +
            "{\"destinationTable\":\"Files\",\"name\":\"Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_LCMS_MSn.png\",\"absolutelocalpath\":\"F:\\\\Lumos01\\\\2022_4\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\\\QC\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_LCMS_MSn.png\",\"subdir\":\"data/QC\",\"size\":\"500630\",\"hashsum\":\"627dd719929715fda2ca92b04b179348891b50da\",\"mimetype\":\"application/octet-stream\",\"hashtype\":\"sha1\",\"ctime\":\"2022-10-24T17:03:37\",\"mtime\":\"2022-10-24T17:03:38\"}," +
            "{\"destinationTable\":\"Files\",\"name\":\"Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_TIC.png\",\"absolutelocalpath\":\"F:\\\\Lumos01\\\\2022_4\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\\\QC\\\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_TIC.png\",\"subdir\":\"data/QC\",\"size\":\"50850\",\"hashsum\":\"4f9fe9faaf8e1c1e50075a29593dd0ea4510d1f3\",\"mimetype\":\"application/octet-stream\",\"hashtype\":\"sha1\",\"ctime\":\"2022-10-24T17:03:19\",\"mtime\":\"2022-10-24T17:03:19\"}]";

        private List<IUploadMetadata> GetUploadMetadataObjects()
        {
            var data = new List<IUploadMetadata>
            {
                new UploadMetadataKeyValue("omics.dms.instrument", "Lumos01"),
                new UploadMetadataKeyValue("omics.dms.instrument_id", 34245),
                new UploadMetadataKeyValue("omics.dms.date_code", "2022_4"),
                new UploadMetadataKeyValue("omics.dms.dataset", "Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9"),
                new UploadMetadataKeyValue("omics.dms.campaign_name", "Stacy_cortex_2021"),
                new UploadMetadataKeyValue("omics.dms.experiment_name", "Stacy_cortex_2022"),
                new UploadMetadataKeyValue("omics.dms.dataset_name", "Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9"),
                new UploadMetadataKeyValue("omics.dms.campaign_id", "3759"),
                new UploadMetadataKeyValue("omics.dms.experiment_id", "326655"),
                new UploadMetadataKeyValue("omics.dms.dataset_id", "1088263"),
                new UploadMetadataKeyValue("organism_name", "Glycine_max"),
                new UploadMetadataKeyValue("omics.dms.organism_id", "1130"),
                new UploadMetadataKeyValue("ncbi_taxonomy_id", "3847"),
                new UploadMetadataKeyValue("omics.dms.separation_type", "LC-NanoPot_100min"),
                new UploadMetadataKeyValue("omics.dms.dataset_type", "HMS-HCD-MSn"),
                new UploadMetadataKeyValue("omics.dms.run_acquisition_length_min", 90),
                new UploadMetadataKeyValue("User of Record", "43635"),
                new UploadMetadataKeyValue("user_of_record", "43635"),
                new UploadMetadataValue("instrument", 34245),
                new UploadMetadataValue("project", "51846"),
                new UploadMetadataValue("submitter", 52405),
                new UploadMetadataFile("metadata.xml", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\metadata.xml", "data/", "1495", "63372bf473a495fa2105fdf739a9adf26aff5a4a", "2022-10-24T17:04:34", "2022-10-24T17:04:34"),
                new UploadMetadataFile("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9.raw", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9.raw", "data/", "697568674", "7696671feeb902576e90e5594964d5b8b87cf470", "2022-10-24T16:56:49", "2022-10-24T02:14:51"),
                new UploadMetadataFile("index.html", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\index.html", "data/QC", "4557", "24a8c51befcc6c4d7a83b4272f34d926008c7d43", "2022-10-24T17:03:46", "2022-10-24T17:03:46"),
                new UploadMetadataFile("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_BPI_MS.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_BPI_MS.png", "data/QC", "57067", "e82f69dd7aa49ab85f8c323e84b9b5ee7bedb9eb", "2022-10-24T17:03:14", "2022-10-24T17:03:14"),
                new UploadMetadataFile("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_BPI_MSn.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_BPI_MSn.png", "data/QC", "51087", "73556b1203a29ad00c9cd780e99cd911bcd1b1ee", "2022-10-24T17:03:17", "2022-10-24T17:03:17"),
                new UploadMetadataFile("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_DatasetInfo.xml", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_DatasetInfo.xml", "data/QC", "2260", "510da615de5388547ecbf920399e346149a94d14", "2022-10-24T17:03:46", "2022-10-24T17:03:46"),
                new UploadMetadataFile("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_HighAbu_LCMS.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_HighAbu_LCMS.png", "data/QC", "280957", "383e9e4f85137e1a2520bad0aea3388c1e1f2ce2", "2022-10-24T17:03:42", "2022-10-24T17:03:42"),
                new UploadMetadataFile("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_HighAbu_LCMS_MSn.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_HighAbu_LCMS_MSn.png", "data/QC", "486773", "84a4d6b2a10bdedbe76e5a6b302d4135442861e7", "2022-10-24T17:03:46", "2022-10-24T17:03:46"),
                new UploadMetadataFile("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_LCMS.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_LCMS.png", "data/QC", "392136", "ae74b2c041e78435a907171874fc4fe0c88ff621", "2022-10-24T17:03:28", "2022-10-24T17:03:28"),
                new UploadMetadataFile("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_LCMS_MSn.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_LCMS_MSn.png", "data/QC", "500630", "627dd719929715fda2ca92b04b179348891b50da", "2022-10-24T17:03:37", "2022-10-24T17:03:38"),
                new UploadMetadataFile("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_TIC.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_TIC.png", "data/QC", "50850", "4f9fe9faaf8e1c1e50075a29593dd0ea4510d1f3", "2022-10-24T17:03:19", "2022-10-24T17:03:19")
            };

            return data;
        }

        [Test]
        [TestCase(TestJsonMetadata)]
        public void TestJsonListMetadata(string jsonInput)
        {
            var nj = JsonTools.JsonToUploadMetadata(jsonInput, "test", "test", out var njError);

            Assert.That(njError, Is.EqualTo(""));
            var expected = GetUploadMetadataObjects();
            Assert.That(nj, Has.Count.EqualTo(expected.Count));

            Compare(expected, nj);
        }

        [Test]
        public void TestListMetadataJson()
        {
            var data = GetUploadMetadataObjects();
            var nj = JsonTools.UploadMetadataToJson(data);

            Assert.That(nj, Is.EqualTo(TestJsonMetadata));
        }

        [Test]
        public void TestListMetadataCompareJson()
        {
            var uploadMetadata = new Upload.UploadMetadata
            {
                DMSInstrumentName = "Lumos01",
                EUSInstrumentID = 34245,
                DateCodeString = "2022_4",
                DatasetName = "Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9",
                CampaignName = "Stacy_cortex_2021",
                ExperimentName = "Stacy_cortex_2022",
                CampaignID = 3759,
                ExperimentID = 326655,
                DatasetID = 1088263,
                OrganismName = "Glycine_max",
                OrganismID = 1130,
                NCBITaxonomyID = 3847,
                SeparationType = "LC-NanoPot_100min",
                DatasetType = "HMS-HCD-MSn",
                AcquisitionLengthMin = 90,
                UserOfRecordList = new List<int>() { 43635 },
                EUSProjectID = "51846",
                EUSOperatorID = 52405
            };

            var filesToUpload = new List<FileInfoObjectTest>
            {
                new("metadata.xml", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\metadata.xml",
                    "", 1495, "63372bf473a495fa2105fdf739a9adf26aff5a4a", "2022-10-24T17:04:34", "2022-10-24T17:04:34"),
                new("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9.raw", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9.raw",
                    "", 697568674, "7696671feeb902576e90e5594964d5b8b87cf470", "2022-10-24T16:56:49", "2022-10-24T02:14:51"),
                new("index.html", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\index.html",
                    "QC", 4557, "24a8c51befcc6c4d7a83b4272f34d926008c7d43", "2022-10-24T17:03:46", "2022-10-24T17:03:46"),
                new("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_BPI_MS.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_BPI_MS.png",
                    "QC", 57067, "e82f69dd7aa49ab85f8c323e84b9b5ee7bedb9eb", "2022-10-24T17:03:14", "2022-10-24T17:03:14"),
                new("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_BPI_MSn.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_BPI_MSn.png",
                    "QC", 51087, "73556b1203a29ad00c9cd780e99cd911bcd1b1ee", "2022-10-24T17:03:17", "2022-10-24T17:03:17"),
                new("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_DatasetInfo.xml", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_DatasetInfo.xml",
                    "QC", 2260, "510da615de5388547ecbf920399e346149a94d14", "2022-10-24T17:03:46", "2022-10-24T17:03:46"),
                new("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_HighAbu_LCMS.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_HighAbu_LCMS.png",
                    "QC", 280957, "383e9e4f85137e1a2520bad0aea3388c1e1f2ce2", "2022-10-24T17:03:42", "2022-10-24T17:03:42"),
                new("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_HighAbu_LCMS_MSn.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_HighAbu_LCMS_MSn.png",
                    "QC", 486773, "84a4d6b2a10bdedbe76e5a6b302d4135442861e7", "2022-10-24T17:03:46", "2022-10-24T17:03:46"),
                new("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_LCMS.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_LCMS.png",
                    "QC", 392136, "ae74b2c041e78435a907171874fc4fe0c88ff621", "2022-10-24T17:03:28", "2022-10-24T17:03:28"),
                new("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_LCMS_MSn.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_LCMS_MSn.png",
                    "QC", 500630, "627dd719929715fda2ca92b04b179348891b50da", "2022-10-24T17:03:37", "2022-10-24T17:03:38"),
                new("Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_TIC.png", "F:\\Lumos01\\2022_4\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9\\QC\\Stacy_cortex_2022_NanoPOTS_400pp_E40node2_Chip4_3CV_C9_TIC.png",
                    "QC", 50850, "4f9fe9faaf8e1c1e50075a29593dd0ea4510d1f3", "2022-10-24T17:03:19", "2022-10-24T17:03:19")
            };

            var listOld = CreatePacificaMetadataObject(uploadMetadata, filesToUpload, out _);
            var listNew = CreatePacificaMetadataObject2(uploadMetadata, filesToUpload, out _);

            var jsonMetadata1 = JayrockJson_Backup.ObjectToJson(listOld);
            var jsonMetadata2 = JsonTools.ObjectToJson(listOld);
            var jsonMetadata3 = JsonTools.UploadMetadataToJson(listNew);

            Assert.Multiple(() =>
            {
                Assert.That(jsonMetadata2, Is.EqualTo(jsonMetadata1));
                Assert.That(jsonMetadata3, Is.EqualTo(jsonMetadata1));
            });
        }

        private void Compare(IReadOnlyList<IUploadMetadata> expected, IReadOnlyList<IUploadMetadata> actual)
        {
            for (var i = 0; i < expected.Count; i++)
            {
                if (expected[i] is UploadMetadataKeyValue expectedKv && actual[i] is UploadMetadataKeyValue actualKv)
                {
                    CompareKv(expectedKv, actualKv);
                }
                else if (expected[i] is UploadMetadataValue expectedV && actual[i] is UploadMetadataValue actualV)
                {
                    CompareV(expectedV, actualV);
                }
                else if (expected[i] is UploadMetadataFile expectedFile && actual[i] is UploadMetadataFile actualFile)
                {
                    CompareFile(expectedFile, actualFile);
                }
                else
                {
                    Console.WriteLine("Type Mismatch!: expected '{0}', actual '{1}'", expected[i].GetType().Name, actual[i].GetType().Name);
                    Assert.That(actual.GetType(), Is.EqualTo(expected.GetType()));
                }
            }
        }

        private void CompareKv(UploadMetadataKeyValue expected, UploadMetadataKeyValue actual)
        {
            Assert.Multiple(() =>
            {
                //Assert.That(actual.DestinationTable, Is.EqualTo(expected.DestinationTable));
                Assert.That(actual.Key, Is.EqualTo(expected.Key));
                Assert.That(actual.Value, Is.EqualTo(expected.Value));
            });
        }

        private void CompareV(UploadMetadataValue expected, UploadMetadataValue actual)
        {
            Assert.Multiple(() =>
            {
                Assert.That(actual.DestinationTable, Is.EqualTo(expected.DestinationTable));
                Assert.That(actual.Value, Is.EqualTo(expected.Value));
            });
        }

        private void CompareFile(UploadMetadataFile expected, UploadMetadataFile actual)
        {
            Assert.Multiple(() =>
            {
                Assert.That(actual.Name, Is.EqualTo(expected.Name));
                Assert.That(actual.AbsoluteLocalPath, Is.EqualTo(expected.AbsoluteLocalPath));
                Assert.That(actual.SubDir, Is.EqualTo(expected.SubDir));
                Assert.That(actual.Size, Is.EqualTo(expected.Size));
                Assert.That(actual.HashSum, Is.EqualTo(expected.HashSum));
                Assert.That(actual.FileCreationTimeUtc, Is.EqualTo(expected.FileCreationTimeUtc));
                Assert.That(actual.FileLastModifiedTimeUtc, Is.EqualTo(expected.FileLastModifiedTimeUtc));
            });
        }

        [Test]
        public void TestConvertToJson()
        {
            var data = CreateTestPacificaMetadataObject();

            var jayrockJson = JayrockJson_Backup.ObjectToJson(data);
            var newtonsoftJson = JsonTools.ObjectToJson(data);

            Assert.That(newtonsoftJson, Is.EqualTo(jayrockJson));
        }

        private static void AppendKVMetadata(ICollection<Dictionary<string, object>> metadataObject, string keyName, object value)
        {
            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "TransactionKeyValue" },
                { "key", keyName },
                { "value", value }
            });
        }

        private static void AppendTransactionMetadata(ICollection<Dictionary<string, object>> metadataObject, string columnName, object value)
        {
            // Example destination table name:
            //  Transactions.instrument
            //  Transactions.project
            //  Transactions.submitter
            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "Transactions." + columnName },
                { "value", value }
            });
        }

        /// <summary>
        /// Create the metadata object with the upload details, including the files to upload
        /// </summary>
        /// <returns>
        /// Dictionary of the information to translate to JSON;
        /// Keys are key names; values are either strings or dictionary objects or even a list of dictionary objects
        /// </returns>
        // ReSharper disable once UnusedMember.Global
        public static List<Dictionary<string, object>> CreateTestPacificaMetadataObject()
        {
            // new metadata object is just a list of dictionary entries
            var metadataObject = new List<Dictionary<string, object>>();

            // Fill out Transaction Key/Value pairs
            AppendKVMetadata(metadataObject, "omics.dms.instrument", "uploadMetadata.DMSInstrumentName");
            AppendKVMetadata(metadataObject, "omics.dms.instrument_id", "eusInfo.EUSInstrumentID");
            AppendKVMetadata(metadataObject, "omics.dms.date_code", "uploadMetadata.DateCodeString");
            AppendKVMetadata(metadataObject, "omics.dms.dataset", "uploadMetadata.DatasetName");
            AppendKVMetadata(metadataObject, "omics.dms.campaign_name", "uploadMetadata.CampaignName");
            AppendKVMetadata(metadataObject, "omics.dms.experiment_name", "uploadMetadata.ExperimentName");
            AppendKVMetadata(metadataObject, "omics.dms.dataset_name", "uploadMetadata.DatasetName");
            AppendKVMetadata(metadataObject, "omics.dms.campaign_id", "uploadMetadata.CampaignID");
            AppendKVMetadata(metadataObject, "omics.dms.experiment_id", "uploadMetadata.ExperimentID");
            AppendKVMetadata(metadataObject, "omics.dms.dataset_id", "uploadMetadata.DatasetID");
            AppendKVMetadata(metadataObject, "omics.dms.run_acquisition_length_min", "uploadMetadata.AcquisitionLengthMin");

            AppendKVMetadata(metadataObject, "User of Record", "user1");
            AppendKVMetadata(metadataObject, "user_of_record", "user1");
            AppendKVMetadata(metadataObject, "User of Record", "user2");
            AppendKVMetadata(metadataObject, "user_of_record", "user2");

            // Append the required metadata
            AppendTransactionMetadata(metadataObject, "instrument", "eusInfo.EUSInstrumentID");
            AppendTransactionMetadata(metadataObject, "project", "eusInfo.EUSProjectID");
            AppendTransactionMetadata(metadataObject, "submitter", "eusInfo.EUSUploaderID");

            // Append the files
            // The subdirectory path must be "data/" or of the form "data/SubDirectory"
            // "data/" is required for files at the root dataset level because the root of the tar file
            // has a metadata.txt file and we would have a conflict if the dataset folder root
            // also had a file named metadata.txt

            // The ingest system will trim out the leading "data/" when storing the SubDir in the system

            // Note the inconsistent requirements; files in the root dataset level must have "data/"
            // while files in subdirectories should have a SubDir that does _not_ end in a forward slash
            // It is likely that this discrepancy has been fixed in the backend python code on the ingest server

            metadataObject.Add(new Dictionary<string, object> {
                { "destinationTable", "Files" },
                { "name", "file.FileName" },
                // ReSharper disable once StringLiteralTypo
                { "absolutelocalpath", "file.AbsoluteLocalPath"},
                { "subdir", "subDirString" },
                { "size", "file.FileSizeInBytes" },
                { "hashsum", "file.Sha1HashHex" },
                { "mimetype", "application/octet-stream" },
                { "hashtype", "sha1" },
                { "ctime", DateTime.Now.ToUniversalTime().ToString("s") },
                { "mtime", new DateTime(2021, 10, 15, 15, 10, 33).ToUniversalTime().ToString("s") }
            });

            return metadataObject;
        }

        /// <summary>
        /// Return the EUS instrument ID, falling back to instrumentIdIfUnknown if eusInstrumentId is empty
        /// </summary>
        /// <param name="eusInstrumentId"></param>
        /// <param name="instrumentIdIfUnknown"></param>
        private static int GetEUSInstrumentID(int eusInstrumentId, int instrumentIdIfUnknown)
        {
            return eusInstrumentId <= 0 ? instrumentIdIfUnknown : eusInstrumentId;
        }

        /// <summary>
        /// Validate the EUS project ID, or use the default
        /// </summary>
        /// <remarks>This is a string because the project ID may contain suffix letters</remarks>
        /// <param name="eusProjectId"></param>
        /// <param name="eusProjectIdIfUnknown"></param>
        private static string GetEUSProjectID(string eusProjectId, string eusProjectIdIfUnknown)
        {
            return string.IsNullOrWhiteSpace(eusProjectId) ? eusProjectIdIfUnknown : eusProjectId;
        }

        private static int GetEUSSubmitterID(int eusOperatorId, int eusOperatorIdIfUnknown)
        {
            // For datasets, eusOperatorID is the instrument operator EUS ID
            // For data packages, it is the EUS ID of the data package owner
            return eusOperatorId == 0 ? eusOperatorIdIfUnknown : eusOperatorId;
        }

        /// <summary>
        /// Create the metadata object with the upload details, including the files to upload
        /// </summary>
        /// <param name="uploadMetadata">Upload metadata</param>
        /// <param name="filesToUpload">Files to upload</param>
        /// <param name="eusInfo">Output parameter: EUS instrument ID, project ID, and uploader ID</param>
        /// <returns>
        /// Dictionary of the information to translate to JSON;
        /// Keys are key names; values are either strings or dictionary objects or even a list of dictionary objects
        /// </returns>
        // ReSharper disable once UnusedMember.Global
        public static List<Dictionary<string, object>> CreatePacificaMetadataObject(
            Upload.UploadMetadata uploadMetadata,
            List<FileInfoObjectTest> filesToUpload,
            out Upload.EUSInfo eusInfo)
        {
            eusInfo = new Upload.EUSInfo();
            eusInfo.Clear();

            // new metadata object is just a list of dictionary entries
            var metadataObject = new List<Dictionary<string, object>>();

            if (uploadMetadata.EUSInstrumentID <= 0)
            {
                // Possibly override EUSInstrument ID
                if (uploadMetadata.DMSInstrumentName.IndexOf("LCQ", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    uploadMetadata.EUSInstrumentID = 1163;
                }

                if (string.Equals(uploadMetadata.DMSInstrumentName, "Exact02", StringComparison.OrdinalIgnoreCase))
                {
                    uploadMetadata.EUSInstrumentID = 34111;
                }

                if (string.Equals(uploadMetadata.DMSInstrumentName, "IMS07_AgTOF04", StringComparison.OrdinalIgnoreCase))
                {
                    uploadMetadata.EUSInstrumentID = 34155;
                }
            }

            // Now that EUS instrument ID is defined, store it and lookup other EUS info
            eusInfo.EUSInstrumentID = GetEUSInstrumentID(uploadMetadata.EUSInstrumentID, Upload.UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID);
            eusInfo.EUSProjectID = GetEUSProjectID(uploadMetadata.EUSProjectID, Upload.DEFAULT_EUS_PROJECT_ID);
            eusInfo.EUSUploaderID = GetEUSSubmitterID(uploadMetadata.EUSOperatorID, Upload.DEFAULT_EUS_OPERATOR_ID);

            // Possibly override EUSProject ID
            if (eusInfo.EUSProjectID.StartsWith("EPR"))
            {
                PRISM.Logging.LogTools.LogWarning("Overriding project {0} with {1} for dataset {2}", eusInfo.EUSProjectID, Upload.DEFAULT_EUS_PROJECT_ID, uploadMetadata.DatasetName);
                eusInfo.EUSProjectID = Upload.DEFAULT_EUS_PROJECT_ID;
            }

            // Fill out Transaction Key/Value pairs
            if (uploadMetadata.DatasetID > 0)
            {
                AppendKVMetadata(metadataObject, "omics.dms.instrument", uploadMetadata.DMSInstrumentName);
                AppendKVMetadata(metadataObject, "omics.dms.instrument_id", eusInfo.EUSInstrumentID);
                AppendKVMetadata(metadataObject, "omics.dms.date_code", uploadMetadata.DateCodeString);
                AppendKVMetadata(metadataObject, "omics.dms.dataset", uploadMetadata.DatasetName);
                AppendKVMetadata(metadataObject, "omics.dms.campaign_name", uploadMetadata.CampaignName);
                AppendKVMetadata(metadataObject, "omics.dms.experiment_name", uploadMetadata.ExperimentName);
                AppendKVMetadata(metadataObject, "omics.dms.dataset_name", uploadMetadata.DatasetName);
                AppendKVMetadata(metadataObject, "omics.dms.campaign_id", uploadMetadata.CampaignID.ToString());
                AppendKVMetadata(metadataObject, "omics.dms.experiment_id", uploadMetadata.ExperimentID.ToString());
                AppendKVMetadata(metadataObject, "omics.dms.dataset_id", uploadMetadata.DatasetID.ToString());

                if (!string.IsNullOrEmpty(uploadMetadata.OrganismName))
                {
                    AppendKVMetadata(metadataObject, "organism_name", uploadMetadata.OrganismName);
                }

                if (uploadMetadata.OrganismID != 0)
                {
                    AppendKVMetadata(metadataObject, "omics.dms.organism_id", uploadMetadata.OrganismID.ToString());
                }

                if (uploadMetadata.NCBITaxonomyID != 0)
                {
                    AppendKVMetadata(metadataObject, "ncbi_taxonomy_id", uploadMetadata.NCBITaxonomyID.ToString());
                }

                if (!string.IsNullOrEmpty(uploadMetadata.SeparationType))
                {
                    AppendKVMetadata(metadataObject, "omics.dms.separation_type", uploadMetadata.SeparationType);
                }

                if (!string.IsNullOrEmpty(uploadMetadata.DatasetType))
                {
                    AppendKVMetadata(metadataObject, "omics.dms.dataset_type", uploadMetadata.DatasetType);
                }

                AppendKVMetadata(metadataObject, "omics.dms.run_acquisition_length_min", uploadMetadata.AcquisitionLengthMin);

                if (uploadMetadata.UserOfRecordList.Count > 0)
                {
                    foreach (var userId in uploadMetadata.UserOfRecordList)
                    {
                        AppendKVMetadata(metadataObject, "User of Record", userId.ToString());
                        AppendKVMetadata(metadataObject, "user_of_record", userId.ToString());
                    }
                }
            }
            else if (uploadMetadata.DataPackageID > 0)
            {
                AppendKVMetadata(metadataObject, "omics.dms.instrument", uploadMetadata.DMSInstrumentName);
                AppendKVMetadata(metadataObject, "omics.dms.instrument_id", eusInfo.EUSInstrumentID);
                AppendKVMetadata(metadataObject, "omics.dms.datapackage_id", uploadMetadata.DataPackageID.ToString());
            }
            else
            {
                throw new Exception("Must define a non-zero DatasetID or a DataPackageID; cannot create the metadata object");
            }

            // Append the required metadata
            AppendTransactionMetadata(metadataObject, "instrument", eusInfo.EUSInstrumentID);
            AppendTransactionMetadata(metadataObject, "project", eusInfo.EUSProjectID);
            AppendTransactionMetadata(metadataObject, "submitter", eusInfo.EUSUploaderID);

            // Append the files
            foreach (var file in filesToUpload)
            {
                // The subdirectory path must be "data/" or of the form "data/SubDirectory"
                // "data/" is required for files at the root dataset level because the root of the tar file
                // has a metadata.txt file and we would have a conflict if the dataset folder root
                // also had a file named metadata.txt

                // The ingest system will trim out the leading "data/" when storing the SubDir in the system

                // Note the inconsistent requirements; files in the root dataset level must have "data/"
                // while files in subdirectories should have a SubDir that does _not_ end in a forward slash
                // It is likely that this discrepancy has been fixed in the backend python code on the ingest server

                string subDirString;

                if (string.IsNullOrWhiteSpace(file.RelativeDestinationDirectory))
                {
                    subDirString = "data/";
                }
                else
                {
                    subDirString = "data/" + file.RelativeDestinationDirectory.Trim('/');
                }

                if (subDirString.Contains("//"))
                {
                    throw new Exception("File path should not have two forward slashes: " + subDirString);
                }

                metadataObject.Add(new Dictionary<string, object> {
                    { "destinationTable", "Files" },
                    { "name", file.FileName },
                    // ReSharper disable once StringLiteralTypo
                    { "absolutelocalpath", file.AbsoluteLocalPath},
                    { "subdir", subDirString },
                    { "size", file.FileSizeInBytes.ToString() },
                    { "hashsum", file.Sha1HashHex },
                    { "mimetype", "application/octet-stream" },
                    { "hashtype", "sha1" },
                    { "ctime", file.CreationTime.ToUniversalTime().ToString("s") },
                    { "mtime", file.LastWriteTime.ToUniversalTime().ToString("s") }
                });
            }

            return metadataObject;
        }

        /// <summary>
        /// Create the metadata object with the upload details, including the files to upload
        /// </summary>
        /// <param name="uploadMetadata">Upload metadata</param>
        /// <param name="filesToUpload">Files to upload</param>
        /// <param name="eusInfo">Output parameter: EUS instrument ID, project ID, and uploader ID</param>
        /// <returns>
        /// Dictionary of the information to translate to JSON;
        /// Keys are key names; values are either strings or dictionary objects or even a list of dictionary objects
        /// </returns>
        // ReSharper disable once UnusedMember.Global
        public static List<IUploadMetadata> CreatePacificaMetadataObject2(
            Upload.UploadMetadata uploadMetadata,
            List<FileInfoObjectTest> filesToUpload,
            out Upload.EUSInfo eusInfo)
        {
            eusInfo = new Upload.EUSInfo();
            eusInfo.Clear();

            // new metadata object is just a list of dictionary entries
            var metadataObject = new List<IUploadMetadata>();

            if (uploadMetadata.EUSInstrumentID <= 0)
            {
                // Possibly override EUSInstrument ID
                if (uploadMetadata.DMSInstrumentName.IndexOf("LCQ", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    uploadMetadata.EUSInstrumentID = 1163;
                }

                if (string.Equals(uploadMetadata.DMSInstrumentName, "Exact02", StringComparison.OrdinalIgnoreCase))
                {
                    uploadMetadata.EUSInstrumentID = 34111;
                }

                if (string.Equals(uploadMetadata.DMSInstrumentName, "IMS07_AgTOF04", StringComparison.OrdinalIgnoreCase))
                {
                    uploadMetadata.EUSInstrumentID = 34155;
                }
            }

            // Now that EUS instrument ID is defined, store it and lookup other EUS info
            eusInfo.EUSInstrumentID = GetEUSInstrumentID(uploadMetadata.EUSInstrumentID, Upload.UNKNOWN_INSTRUMENT_EUS_INSTRUMENT_ID);
            eusInfo.EUSProjectID = GetEUSProjectID(uploadMetadata.EUSProjectID, Upload.DEFAULT_EUS_PROJECT_ID);
            eusInfo.EUSUploaderID = GetEUSSubmitterID(uploadMetadata.EUSOperatorID, Upload.DEFAULT_EUS_OPERATOR_ID);

            // Possibly override EUSProject ID
            if (eusInfo.EUSProjectID.StartsWith("EPR"))
            {
                PRISM.Logging.LogTools.LogWarning("Overriding project {0} with {1} for dataset {2}", eusInfo.EUSProjectID, Upload.DEFAULT_EUS_PROJECT_ID, uploadMetadata.DatasetName);
                eusInfo.EUSProjectID = Upload.DEFAULT_EUS_PROJECT_ID;
            }

            // Fill out Transaction Key/Value pairs
            if (uploadMetadata.DatasetID > 0)
            {
                metadataObject.AddKeyValue("omics.dms.instrument", uploadMetadata.DMSInstrumentName);
                metadataObject.AddKeyValue("omics.dms.instrument_id", eusInfo.EUSInstrumentID);
                metadataObject.AddKeyValue("omics.dms.date_code", uploadMetadata.DateCodeString);
                metadataObject.AddKeyValue("omics.dms.dataset", uploadMetadata.DatasetName);
                metadataObject.AddKeyValue("omics.dms.campaign_name", uploadMetadata.CampaignName);
                metadataObject.AddKeyValue("omics.dms.experiment_name", uploadMetadata.ExperimentName);
                metadataObject.AddKeyValue("omics.dms.dataset_name", uploadMetadata.DatasetName);
                metadataObject.AddKeyValue("omics.dms.campaign_id", uploadMetadata.CampaignID.ToString());
                metadataObject.AddKeyValue("omics.dms.experiment_id", uploadMetadata.ExperimentID.ToString());
                metadataObject.AddKeyValue("omics.dms.dataset_id", uploadMetadata.DatasetID.ToString());

                if (!string.IsNullOrEmpty(uploadMetadata.OrganismName))
                {
                    metadataObject.AddKeyValue("organism_name", uploadMetadata.OrganismName);
                }

                if (uploadMetadata.OrganismID != 0)
                {
                    metadataObject.AddKeyValue("omics.dms.organism_id", uploadMetadata.OrganismID.ToString());
                }

                if (uploadMetadata.NCBITaxonomyID != 0)
                {
                    metadataObject.AddKeyValue("ncbi_taxonomy_id", uploadMetadata.NCBITaxonomyID.ToString());
                }

                if (!string.IsNullOrEmpty(uploadMetadata.SeparationType))
                {
                    metadataObject.AddKeyValue("omics.dms.separation_type", uploadMetadata.SeparationType);
                }

                if (!string.IsNullOrEmpty(uploadMetadata.DatasetType))
                {
                    metadataObject.AddKeyValue("omics.dms.dataset_type", uploadMetadata.DatasetType);
                }

                metadataObject.AddKeyValue("omics.dms.run_acquisition_length_min", uploadMetadata.AcquisitionLengthMin);

                if (uploadMetadata.UserOfRecordList.Count > 0)
                {
                    foreach (var userId in uploadMetadata.UserOfRecordList)
                    {
                        metadataObject.AddKeyValue("User of Record", userId.ToString());
                        metadataObject.AddKeyValue("user_of_record", userId.ToString());
                    }
                }
            }
            else if (uploadMetadata.DataPackageID > 0)
            {
                metadataObject.AddKeyValue("omics.dms.instrument", uploadMetadata.DMSInstrumentName);
                metadataObject.AddKeyValue("omics.dms.instrument_id", eusInfo.EUSInstrumentID);
                metadataObject.AddKeyValue("omics.dms.datapackage_id", uploadMetadata.DataPackageID.ToString());
            }
            else
            {
                throw new Exception("Must define a non-zero DatasetID or a DataPackageID; cannot create the metadata object");
            }

            // Append the required metadata
            //  Transactions.instrument
            //  Transactions.project
            //  Transactions.submitter
            metadataObject.AddValue("instrument", eusInfo.EUSInstrumentID);
            metadataObject.AddValue("project", eusInfo.EUSProjectID);
            metadataObject.AddValue("submitter", eusInfo.EUSUploaderID);

            // Append the files
            foreach (var file in filesToUpload)
            {
                // The subdirectory path must be "data/" or of the form "data/SubDirectory"
                // "data/" is required for files at the root dataset level because the root of the tar file
                // has a metadata.txt file and we would have a conflict if the dataset folder root
                // also had a file named metadata.txt

                // The ingest system will trim out the leading "data/" when storing the SubDir in the system

                // Note the inconsistent requirements; files in the root dataset level must have "data/"
                // while files in subdirectories should have a SubDir that does _not_ end in a forward slash
                // It is likely that this discrepancy has been fixed in the backend python code on the ingest server

                string subDirString;

                if (string.IsNullOrWhiteSpace(file.RelativeDestinationDirectory))
                {
                    subDirString = "data/";
                }
                else
                {
                    subDirString = "data/" + file.RelativeDestinationDirectory.Trim('/');
                }

                if (subDirString.Contains("//"))
                {
                    throw new Exception("File path should not have two forward slashes: " + subDirString);
                }

                metadataObject.Add(new UploadMetadataFile(file.FileName, file.AbsoluteLocalPath, subDirString, file.FileSizeInBytes.ToString(), file.Sha1HashHex, file.CreationTimeUtc, file.LastWriteTimeUtc));
            }

            return metadataObject;
        }
    }
}
