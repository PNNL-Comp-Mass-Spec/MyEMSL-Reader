using System;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using Pacifica.Core;
using PRISM;
using RestSharp;

namespace PacificaUnitTests
{
    [TestFixture]
    internal class PolicyServerTests
    {
        private const string DirectServerUri = "http://172.22.110.29:8182";
        private const string JsonOkay = @"[{""destinationTable"":""TransactionKeyValue"",""Valid"":true,""key"":""omics.dms.dataset"",""value"":""60952_AvMalh_QC_Pool_02_RP_Neg_29Aug24_Olympic_HGold-0609""},{""destinationTable"":""Transactions.instrument"",""Valid"":true,""value"":34231},{""destinationTable"":""Transactions.project"",""Valid"":true,""value"":""60952""},{""destinationTable"":""Transactions.submitter"",""Valid"":true,""value"":62067}]";
        private const string JsonBad = @"[{""destinationTable"":""TransactionKeyValue"",""Valid"":true,""key"":""omics.dms.dataset"",""value"":""60952_AvMalh_QC_Pool_02_RP_Neg_29Aug24_Olympic_HGold-0609""},{""destinationTable"":""Transactions.instrument"",""Valid"":true,""value"":3423123},{""destinationTable"":""Transactions.project"",""Valid"":true,""value"":""60952a""},{""destinationTable"":""Transactions.submitter"",""Valid"":true,""value"":6206723}]";

        private string GetTestUri(Configuration config, bool skipProxy = false)
        {
            var baseUri = config.PolicyServerUri;
            if (skipProxy)
            {
                baseUri = DirectServerUri;
            }

            return baseUri + "/ingest";
        }

        private string GetJsonPostBody(bool useBad = false)
        {
            if (useBad)
            {
                return JsonBad;
            }

            return JsonOkay;
        }

        [Test]
        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        public void TestBasic(bool skipProxy, bool testBadJson)
        {
            var json = GetJsonPostBody(testBadJson);

            var config = new Configuration();
            var url = GetTestUri(config, skipProxy);
            var response = EasyHttp.SendViaThreadStart(config, url, null, out var responseStatusCode, json, EasyHttp.HttpMethod.Post, 20, "application/json");

            Console.WriteLine($"{responseStatusCode} ({(int)responseStatusCode})");
            Console.WriteLine(response);

            Assert.That(response, Does.Not.Contain("underlying connection was closed"));
        }

        [Test]
        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        public void TestRestSharp(bool skipProxy, bool testBadJson)
        {
            var json = GetJsonPostBody(testBadJson);

            var config = new Configuration();
            var url = GetTestUri(config, skipProxy);

            var password = AppUtils.DecodeShiftCipher(Configuration.CLIENT_CERT_PASSWORD);
            var cert = Utilities.CreateX509Certificate(config.ClientCertFilePath, password);
            //cert.Import(config.ClientCertFilePath, password, X509KeyStorageFlags.PersistKeySet);

            var certCollection = new X509Certificate2Collection(cert);
            var opt = new RestClientOptions(url)
            {
                ClientCertificates = certCollection,
                Timeout = TimeSpan.FromSeconds(20)
            };
            var client = new RestClient(opt);

            //var resp = client.Post
            //var response = EasyHttp.SendViaThreadStart(config, url, null, out var responseStatusCode, json, EasyHttp.HttpMethod.Post, 20, "application/json");
            var req = new RestRequest("", Method.Post).AddBody(json, ContentType.Json);
            req.AddHeader("Accepts", "application/json");
            //req.AddBody(json, ContentType.Json);

            var response = client.ExecutePost(req);

            //Console.WriteLine(responseStatusCode);
            Console.WriteLine(response);
            Console.WriteLine($"{response.ResponseStatus} ({(int)response.ResponseStatus})");
            Console.WriteLine($"{response.StatusDescription} ({response.StatusCode}, {(int)response.StatusCode})");
            Console.WriteLine(response.Content);

            Assert.That(response.ResponseStatus, Is.Not.EqualTo(ResponseStatus.TimedOut));
        }

        [Test]
        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        public void TestRestSharpAsync(bool skipProxy, bool testBadJson)
        {
            var config = new Configuration();
            var url = GetTestUri(config, skipProxy);
            var json = GetJsonPostBody(testBadJson);

            var response = Task.Run(async () => await DoRestRequest(json, url, config)).Result;

            //Console.WriteLine(responseStatusCode);
            Console.WriteLine(response);
            Console.WriteLine($"{response.ResponseStatus} ({(int)response.ResponseStatus})");
            Console.WriteLine($"{response.StatusDescription} ({response.StatusCode}, {(int)response.StatusCode})");
            Console.WriteLine(response.Content);

            Assert.That(response.ResponseStatus, Is.Not.EqualTo(ResponseStatus.TimedOut));
        }

        private async Task<RestResponse> DoRestRequest(string json, string url, Configuration config)
        {
            var password = AppUtils.DecodeShiftCipher(Configuration.CLIENT_CERT_PASSWORD);
            var cert = Utilities.CreateX509Certificate(config.ClientCertFilePath, password);
            //cert.Import(config.ClientCertFilePath, password, X509KeyStorageFlags.PersistKeySet);

            var certCollection = new X509Certificate2Collection(cert);
            var opt = new RestClientOptions(url)
            {
                ClientCertificates = certCollection,
                Timeout = TimeSpan.FromSeconds(20),
                Encoding = Encoding.UTF8
            };
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12 | SecurityProtocolType.Tls13;
            var client = new RestClient(opt);
            //var response = EasyHttp.SendViaThreadStart(config, url, null, out var responseStatusCode, json, EasyHttp.HttpMethod.Post, 20, "application/json");
            var req = new RestRequest("", Method.Post).AddBody(json, ContentType.Json);
            req.AddHeader("Accepts", "application/json");
            //req.AddBody(json, ContentType.Json);
            return await client.ExecutePostAsync(req);
        }
    }
}
