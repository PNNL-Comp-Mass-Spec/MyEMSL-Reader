using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Pacifica.Core
{
    public class Configuration
    {
        // Ignore Spelling: http, https

        /// <summary>
        /// Policy Server host name on the production server
        /// </summary>
        public const string DEFAULT_POLICY_SERVER_HOST_NAME = "policydms.my.emsl.pnl.gov";

        /// <summary>
        /// Policy Server host name for testing
        /// </summary>
        public const string TEST_POLICY_SERVER_HOST_NAME = "policydmsdev.my.emsl.pnl.gov";

        /// <summary>
        /// Cart Server host name on the production server
        /// </summary>
        [Obsolete("Old cart mechanism")]
        public const string DEFAULT_CART_SERVER_HOST_NAME = "cart.my.emsl.pnl.gov";

        /// <summary>
        /// File Server host name on the production server
        /// </summary>
        public const string DEFAULT_FILE_SERVER_HOST_NAME = "files.my.emsl.pnl.gov";

        /// <summary>
        /// Metadata Server host name on the production server
        /// </summary>
        public const string DEFAULT_METADATA_SERVER_HOST_NAME = "metadata.my.emsl.pnl.gov";

        /// <summary>
        /// Metadata Server host name for testing
        /// </summary>
        public const string TEST_METADATA_SERVER_HOST_NAME = "metadatadev.my.emsl.pnl.gov";

        /// <summary>
        /// Ingest host name on the production server
        /// </summary>
        public const string DEFAULT_INGEST_HOST_NAME = "ingestdms.my.emsl.pnl.gov";

        /// <summary>
        /// Ingest host name on the test server
        /// </summary>
        public const string TEST_INGEST_HOST_NAME = "ingestdmsdev.my.emsl.pnl.gov";

        /// <summary>
        /// Windows directory with the MyEMSL client certificate file
        /// </summary>
        public const string CLIENT_CERT_DIRECTORY = @"C:\client_certs\";

        [Obsolete("Use property ClientCertFilePath which finds the newest certificate file")]
        internal const string CLIENT_CERT_FILENAME = "svc-dms-cert_2018.pfx";

        [Obsolete("Use property ClientCertFilePath which finds the newest certificate file")]
        public const string CLIENT_CERT_FILEPATH = @"C:\client_certs\" + CLIENT_CERT_FILENAME;

        public const string CLIENT_CERT_PASSWORD = "";

        /// <summary>
        /// Local temp directory
        /// </summary>
        public string LocalTempDirectory { get; set; }

        /// <summary>
        /// If true, use https; otherwise use http
        /// </summary>
        public bool UseSecureDataTransfer { get; set; }

        /// <summary>
        /// Returns either https:// or http://
        /// </summary>
        public string Scheme
        {
            get
            {
                var scheme = UseSecureDataTransfer ? SecuredScheme : UnsecuredScheme;
                return scheme + "://";
            }
        }

        private const string UNSECURED_SCHEME = "http";
        public string UnsecuredScheme => UNSECURED_SCHEME;

        private const string SECURED_SCHEME = "https";
        public string SecuredScheme => SECURED_SCHEME;

        /// <summary>
        /// Server for downloading files via a cart
        /// </summary>
        [Obsolete("Old cart mechanism")]
        public string CartServerHostName { get; set; }

        /// <summary>
        /// Cart download server, default https://cart.my.emsl.pnl.gov
        /// </summary>
        [Obsolete("Old cart mechanism")]
        public string CartServerUri => Scheme + CartServerHostName;

        /// <summary>
        /// Path to the MyEMSL certificate file, e.g. svc-dms-cert_2019.pfx
        /// </summary>
        public string ClientCertFilePath { get; }

        /// <summary>
        /// Server for retrieving files one file at a time
        /// </summary>
        public string FileServerHostName { get; set; }

        /// <summary>
        /// File download server, default https://files.my.emsl.pnl.gov
        /// </summary>
        public string FileServerUri => Scheme + FileServerHostName;

        /// <summary>
        /// Ingest server name
        /// </summary>
        public string IngestServerHostName { get; set; }

        /// <summary>
        /// Ingest server, default https://ingest.my.emsl.pnl.gov
        /// </summary>
        public string IngestServerUri => Scheme + IngestServerHostName;

        /// <summary>
        /// Policy server name
        /// </summary>
        public string PolicyServerHostName { get; set; }

        /// <summary>
        /// Policy server, default https://policy.my.emsl.pnl.gov
        /// </summary>
        public string PolicyServerUri => Scheme + PolicyServerHostName;

        /// <summary>
        /// Metadata server name
        /// </summary>
        public string MetadataServerHostName { get; set; }

        /// <summary>
        /// Metadata server, default https://metadata.my.emsl.pnl.gov
        /// </summary>
        public string MetadataServerUri => Scheme + MetadataServerHostName;

        /// <summary>
        /// Proxy server, default empty string
        /// </summary>
        /// <remarks>Ignored if an empty string</remarks>
        public string HttpProxyUrl { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public Configuration()
        {
            LocalTempDirectory = Path.GetTempPath();

            UseSecureDataTransfer = true;

#pragma warning disable 618
            CartServerHostName = DEFAULT_CART_SERVER_HOST_NAME;
#pragma warning restore 618

            FileServerHostName = DEFAULT_FILE_SERVER_HOST_NAME;
            IngestServerHostName = DEFAULT_INGEST_HOST_NAME;
            PolicyServerHostName = DEFAULT_POLICY_SERVER_HOST_NAME;
            MetadataServerHostName = DEFAULT_METADATA_SERVER_HOST_NAME;

            HttpProxyUrl = string.Empty;

            ClientCertFilePath = FindNewestClientCertFile();
        }

        /// <summary>
        /// Look for the client certificate file (svc-dms.pfx)
        /// </summary>
        /// <returns>Path to the file if found, otherwise an empty string</returns>
        /// <remarks>First checks the directory with the executing assembly, then checks C:\client_certs\</remarks>
        [Obsolete("Use property ClientCertFilePath")]
        public string ResolveClientCertFile()
        {
            return ClientCertFilePath;
        }

        /// <summary>
        /// Look for the newest client certificate file (e.g., svc-dms.pfx)
        /// </summary>
        /// <returns>Path to the file if found, otherwise an empty string</returns>
        /// <remarks>First checks the directory with the executing assembly, then checks C:\client_certs\</remarks>
        private string FindNewestClientCertFile()
        {
            const string CERTIFICATE_FILE_MASK = "*.pfx";

            var certificateFileMatcher = new Regex(@"svc-dms-cert_(?<Year>\d+).pfx", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            try
            {
                var directoriesToCheck = new List<DirectoryInfo>();

                // Look for svc-dms.pfx in the folder with Pacifica.core.dll

                // Full path to Pacifica.core.dll
                var assemblyPath = Assembly.GetExecutingAssembly().Location;

                var assemblyFile = new FileInfo(assemblyPath);
                if (assemblyFile.DirectoryName != null)
                {
                    var workingDirectory = new DirectoryInfo(assemblyFile.DirectoryName);
                    directoriesToCheck.Add(workingDirectory);
                }

                var sharedClientCertDirectory = new DirectoryInfo(CLIENT_CERT_DIRECTORY);
                if (sharedClientCertDirectory.Exists)
                {
                    directoriesToCheck.Add(sharedClientCertDirectory);
                }

                var certificateFilesByYear = new Dictionary<DateTime, FileInfo>();

                // On the first iteration, look for certificate files that match the standard pattern, e.g. svc-dms-cert_2019.pfx
                // If no matches are found, look for any matching .pfx file, using the modification date of the file as the file date
                for (var iteration = 0; iteration < 2; iteration++)
                {
                    foreach (var directoryToCheck in directoriesToCheck)
                    {
                        var certificateFiles = directoryToCheck.GetFiles(CERTIFICATE_FILE_MASK);
                        foreach (var certificateFile in certificateFiles)
                        {
                            var match = certificateFileMatcher.Match(certificateFile.Name);
                            DateTime dayOfYear;

                            if (match.Success)
                            {
                                // Track this file using the last date of the year, e.g. 2019-12-31
                                var certificateYear = int.Parse(match.Groups["Year"].Value);
                                dayOfYear = new DateTime(certificateYear, 12, 31);
                            }
                            else if (iteration > 0)
                            {
                                // Track this file using its last write time
                                dayOfYear = new DateTime(certificateFile.LastWriteTime.Year, certificateFile.LastWriteTime.Month, certificateFile.LastWriteTime.Day);
                            }
                            else
                            {
                                dayOfYear = DateTime.MinValue;
                            }

                            if (dayOfYear > DateTime.MinValue && !certificateFilesByYear.ContainsKey(dayOfYear))
                            {
                                certificateFilesByYear.Add(dayOfYear, certificateFile);
                            }
                        }
                    }

                    if (certificateFilesByYear.Count > 0)
                    {
                        break;
                    }
                }

                var query = (from item in certificateFilesByYear orderby item.Key descending select item.Key).ToList();
                if (query.Count > 0)
                {
                    return certificateFilesByYear[query.First()].FullName;
                }

                return string.Empty;
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowWarning("Exception looking for the newest MyEMSL .pfx certificate file: " + ex.Message);
                return string.Empty;
            }
        }

        /// <summary>
        /// Associate the proxy server (if defined) with the WebRequest
        /// </summary>
        /// <param name="oWebRequest"></param>
        public void SetProxy(HttpWebRequest oWebRequest)
        {
            if (!string.IsNullOrWhiteSpace(HttpProxyUrl))
            {
                oWebRequest.Proxy = new WebProxy(new Uri(HttpProxyUrl));
            }
        }

        private bool mUseTestInstance;

        /// <summary>
        /// When true, upload to ingestdmsdev.my.emsl.pnl.gov instead of ingestdms.my.emsl.pnl.gov
        /// </summary>
        public bool UseTestInstance
        {
            get => mUseTestInstance;

            set
            {
                mUseTestInstance = value;
                UpdateHostNames();
            }
        }

        private void UpdateHostNames()
        {
            if (mUseTestInstance)
            {
                IngestServerHostName = TEST_INGEST_HOST_NAME;
                PolicyServerHostName = TEST_POLICY_SERVER_HOST_NAME;
                MetadataServerHostName = TEST_METADATA_SERVER_HOST_NAME;
            }
            else
            {
                IngestServerHostName = DEFAULT_INGEST_HOST_NAME;
                PolicyServerHostName = DEFAULT_POLICY_SERVER_HOST_NAME;
                MetadataServerHostName = DEFAULT_METADATA_SERVER_HOST_NAME;
            }
        }
    }
}
