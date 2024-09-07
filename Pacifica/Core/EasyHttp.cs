using PRISM;
using System;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Pacifica.Core
{
    public class EasyHttp
    {
        // Ignore Spelling: json, MyEMSL, Pacifica, svc-dms, urlencoded, www

        /// <summary>
        /// Response to return when the thread used to send a request aborts
        /// </summary>
        private const string REQUEST_ABORTED_RESPONSE = "(no response, request aborted)";

        /// <summary>
        /// Response to return when the thread used to send a request catches an exception
        /// </summary>
        private const string REQUEST_EXCEPTION_RESPONSE = "(no response, exception caught)";

        /// <summary>
        /// Response to return the Policy server reports Precondition Failed
        /// </summary>
        public const string REQUEST_PRECONDITION_FAILED_RESPONSE = "(no response, precondition failed)";

        /// <summary>
        /// Response to return when a request times out
        /// </summary>
        public const string REQUEST_TIMEOUT_RESPONSE = "(no response, request timed out)";

        /// <summary>
        /// Metadata file name
        /// </summary>
        // ReSharper disable once IdentifierTypo
        public const string MYEMSL_METADATA_FILE_NAME = "metadata.txt";

        private static X509Certificate2 mLoginCertificate;

        private static Thread mThreadedSend;
        private static CancellationTokenSource mThreadedSendCancel;

        private static UrlContactInfo mUrlContactInfo;

        /// <summary>
        /// An enumeration of standard HTTP methods.
        /// </summary>
        /// <remarks>
        /// Use ExtensionMethods.GetDescription
        /// to pull the description value out of this type.
        /// </remarks>
        public enum HttpMethod
        {
            [Description("GET")]
            Get = 0,
            [Description("POST")]
            Post = 1,
            // ReSharper disable once UnusedMember.Global
            [Description("PUT")]
            Put = 2
        }

        /// <summary>
        /// This event is raised if we are unable to connect to MyEMSL, leading to events
        /// System.Net.HttpRequestException: Unable to connect to the remote server
        /// System.Net.Sockets.SocketException: A connection attempt failed because the connected party did not properly respond after a period of time
        /// </summary>
        public static event EventHandler<MessageEventArgs> MyEMSLOffline;

        /// <summary>
        /// This event is used by SendFileListToIngester to report upload status
        /// </summary>
        public static event EventHandler<StatusEventArgs> StatusUpdate;

        /// <summary>
        /// Error event
        /// </summary>
        public static event EventNotifier.ErrorEventEventHandler ErrorEvent;

        /// <summary>
        /// Abort thread mThreadedSend
        /// </summary>
        private static void AbortThreadedSendNow()
        {
            mThreadedSendCancel.Cancel();

#if (NET48)
            try
            {
                mThreadedSend?.Abort();
            }
            catch
            {
                // Ignore errors
            }
#endif
        }

        /// <summary>
        /// Retrieve a file
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="downloadFilePath">Local file path to save the file as</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>True if success, false if an error</returns>
        // ReSharper disable once UnusedMember.Global
        public static bool GetFile(
            Configuration config,
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            string downloadFilePath,
            int timeoutSeconds = 100,
            NetworkCredential loginCredentials = null)
        {
            var result = Task
                .Run(async () => await GetFileAsync(config, url, cookies, downloadFilePath, timeoutSeconds, loginCredentials))
                .GetAwaiter().GetResult();

            responseStatusCode = result.responseStatusCode;
            return result.result;
        }

        /// <summary>
        /// Retrieve a file
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="downloadFilePath">Local file path to save the file as</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>True if success, false if an error, and response status code</returns>
        // ReSharper disable once UnusedMember.Global
        public static async Task<(bool result, HttpStatusCode responseStatusCode)> GetFileAsync(
            Configuration config,
            string url,
            CookieContainer cookies,
            string downloadFilePath,
            int timeoutSeconds = 100,
            NetworkCredential loginCredentials = null)
        {
            var responseStatusCode = HttpStatusCode.NotFound;

            try
            {
                // Prepare the client object
                using var client = InitializeClient(config, url, ref cookies, ref timeoutSeconds, loginCredentials);
                client.Timeout = TimeSpan.FromSeconds(timeoutSeconds);

                // Receive response
                using var response = await client.GetAsync("", HttpCompletionOption.ResponseHeadersRead);

                responseStatusCode = response.StatusCode;

                if (responseStatusCode == HttpStatusCode.OK)
                {
                    // Download the file
                    using var responseStream = await response.Content.ReadAsStreamAsync();

                    if (responseStream == null)
                    {
                        throw new HttpRequestException("Response stream is null in GetFile");
                    }

                    var buffer = new byte[32767];

                    using var outFile = new FileStream(downloadFilePath, FileMode.Create, FileAccess.Write, FileShare.Read);

                    int bytesRead;

                    // TODO: Potentially just use await responseStream.CopyToAsync(outFile);?
                    // https://www.tugberkugurlu.com/archive/efficiently-streaming-large-http-responses-with-httpclient
                    while ((bytesRead = await responseStream.ReadAsync(buffer, 0, buffer.Length)) != 0)
                    {
                        outFile.Write(buffer, 0, bytesRead);
                    }
                }
                else
                {
                    throw new HttpRequestException(string.Format(
                        "HTTP response code not OK in GetFile: {0}, {1}",
                        response.StatusCode, response.ReasonPhrase));
                }
            }
            catch (HttpRequestException ex)
            {
                HandleRequestException(ex, url, out responseStatusCode);
            }
            catch (TaskCanceledException ex)
            {
                HandleRequestException(new HttpRequestException("HTTP request timed out", ex), url, out responseStatusCode);
            }
            catch (OperationCanceledException ex)
            {
                HandleRequestException(new HttpRequestException($"HTTP request was cancelled?: {ex.CancellationToken.IsCancellationRequested}", ex), url, out responseStatusCode);
            }

            return (true, responseStatusCode);
        }

        /// <summary>
        /// Get the headers for a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <returns>Headers</returns>
        // ReSharper disable once UnusedMember.Global
        public static HttpResponseHeaders GetHeaders(
            Configuration config,
            string url,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100)
        {
            return GetHeaders(config, url, new CookieContainer(), out responseStatusCode, timeoutSeconds);
        }

        /// <summary>
        /// Get the headers for a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="responseStatusCode"></param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>Headers</returns>
        public static HttpResponseHeaders GetHeaders(
            Configuration config,
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100,
            NetworkCredential loginCredentials = null)
        {
            var result = Task
                .Run(async () => await GetHeadersAsync(config, url, cookies, timeoutSeconds, loginCredentials))
                .GetAwaiter().GetResult();

            responseStatusCode = result.responseStatusCode;

            return result.headers;
        }

        /// <summary>
        /// Get the headers for a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>Headers, response status code</returns>
        public static async Task<(HttpResponseHeaders headers, HttpStatusCode responseStatusCode)> GetHeadersAsync(
            Configuration config,
            string url,
            CookieContainer cookies,
            int timeoutSeconds = 100,
            NetworkCredential loginCredentials = null)
        {
            const double maxTimeoutHours = 0.1;
            var responseStatusCode = HttpStatusCode.NotFound;

            try
            {
                // Prepare the request object
                using var client = InitializeClient(config, url, ref cookies, ref timeoutSeconds, loginCredentials, maxTimeoutHours);
                client.Timeout = TimeSpan.FromSeconds(timeoutSeconds);
                var request = new HttpRequestMessage(System.Net.Http.HttpMethod.Head, client.BaseAddress);

                // Receive response
                using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
                responseStatusCode = response.StatusCode;

                return (response.Headers, responseStatusCode);
            }
            catch (HttpRequestException ex)
            {
                HandleRequestException(ex, url, out responseStatusCode);
            }
            catch (TaskCanceledException ex)
            {
                HandleRequestException(new HttpRequestException("HTTP request timed out", ex), url, out responseStatusCode);
            }
            catch (OperationCanceledException ex)
            {
                HandleRequestException(new HttpRequestException($"HTTP request was cancelled?: {ex.CancellationToken.IsCancellationRequested}", ex), url, out responseStatusCode);
            }

            return (null, responseStatusCode);
        }

        private static string GetTrimmedResponseData(Stream responseStream, int maxLines = 20)
        {
            if (responseStream == null)
            {
                return string.Empty;
            }

            var responseData = new StringBuilder();

            if (maxLines < 1)
            {
                maxLines = 1;
            }

            using var reader = new StreamReader(responseStream);

            for (var linesRead = 0; linesRead < maxLines; linesRead++)
            {
                if (reader.EndOfStream)
                {
                    break;
                }

                responseData.AppendLine(reader.ReadLine());
            }

            return responseData.ToString();
        }

        public static void HandleRequestException(HttpRequestException ex, string url)
        {
            var responseData = new WebResponseData();
            HandleRequestException(ex, url, responseData);
        }

        protected static void HandleRequestException(HttpRequestException ex, string url, out HttpStatusCode responseStatusCode)
        {
            responseStatusCode = HttpStatusCode.RequestTimeout;

            var responseData = new WebResponseData
            {
                ResponseStatusCode = responseStatusCode
            };

            try
            {
                HandleRequestException(ex, url, responseData);
            }
            catch
            {
                responseStatusCode = responseData.ResponseStatusCode;
                throw;
            }
        }

        protected static void HandleRequestException(HttpRequestException ex, string url, WebResponseData responseData)
        {
            responseData.RegisterException(ex);

            if (ex.InnerException is WebException wex && wex.Response != null)
            {
                var responseStream = wex.Response.GetResponseStream();
                responseData.ResponseText = GetTrimmedResponseData(responseStream);

                responseData.ResponseStatusCode = ((HttpWebResponse)wex.Response).StatusCode;
            }
            else
            {
#if NET5_0_OR_GREATER
                if (ex.StatusCode.HasValue)
                {
                    responseData.ResponseStatusCode = ex.StatusCode.Value;
                }
#endif

                if (ex.Message.IndexOf("timed out", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    responseData.ResponseText = REQUEST_TIMEOUT_RESPONSE;
                    responseData.ResponseStatusCode = HttpStatusCode.RequestTimeout;
                }
                else if (ex.Message.IndexOf("Precondition Failed", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    responseData.ResponseText = REQUEST_PRECONDITION_FAILED_RESPONSE;
                    responseData.ResponseStatusCode = HttpStatusCode.PreconditionFailed;
                }
                else
                {
                    responseData.ResponseText = string.Empty;
                }
            }

            if (ex.Message.IndexOf("Unable to connect", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                MyEMSLOffline?.Invoke(null, new MessageEventArgs("HandleRequestException", ex.Message));
            }

            if (string.IsNullOrWhiteSpace(responseData.ResponseText))
            {
                throw new Exception("Empty response for " + url + ": " + ex.Message, ex);
            }

            throw new Exception("Response from " + url + ": " + responseData.ResponseText, ex);
        }

        /// <summary>
        /// Initialize a request
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <param name="maxTimeoutHours"></param>
        /// <returns>Web request</returns>
        public static HttpClient InitializeClient(
            Configuration config,
            string url,
            ref CookieContainer cookies,
            ref int timeoutSeconds,
            NetworkCredential loginCredentials,
            double maxTimeoutHours = 24)
        {
            cookies ??= new CookieContainer();

            if (timeoutSeconds < 3)
            {
                timeoutSeconds = 3;
            }

            var maxTimeoutHoursInt = (int)(maxTimeoutHours * 60 * 60);

            if (timeoutSeconds > maxTimeoutHoursInt)
            {
                timeoutSeconds = maxTimeoutHoursInt;
            }

            var urlContactInfo = new UrlContactInfo(config, url, cookies, timeoutSeconds: timeoutSeconds, loginCredentials: loginCredentials);

            return InitializeClient(urlContactInfo);
        }

        /// <summary>
        /// Initialize a HttpClient
        /// </summary>
        private static HttpClient InitializeClient(UrlContactInfo urlContactInfo)
        {
            urlContactInfo.ResponseData.ResetExceptionInfo();

            var uri = new Uri(urlContactInfo.Url);
            var cleanUserName = Utilities.GetUserName(true);

            var handler = new HttpClientHandler();

            handler.PreAuthenticate = false;
            urlContactInfo.Config.SetProxy(handler);

            if (urlContactInfo.LoginCredentials == null)
            {
                if (mLoginCertificate == null)
                {
                    var certificateFilePath = ResolveCertFile(urlContactInfo.Config, "InitializeClient", out var errorMessage);

                    if (string.IsNullOrWhiteSpace(certificateFilePath))
                    {
                        throw new Exception(errorMessage);
                    }

                    var password = AppUtils.DecodeShiftCipher(Configuration.CLIENT_CERT_PASSWORD);
                    mLoginCertificate = new X509Certificate2(certificateFilePath, password, X509KeyStorageFlags.PersistKeySet);
                }
                handler.ClientCertificates.Add(mLoginCertificate);
            }
            else
            {
                handler.Credentials = new CredentialCache
                {
                    { new Uri(urlContactInfo.Url), "Basic", new NetworkCredential(urlContactInfo.LoginCredentials.UserName, urlContactInfo.LoginCredentials.SecurePassword) }
                };
            }

            if (!string.IsNullOrWhiteSpace(cleanUserName))
            {
                var cookie = new Cookie("user_name", cleanUserName)
                {
                    Domain = "pnl.gov"
                };

                urlContactInfo.Cookies.Add(cookie);
            }

            handler.CookieContainer = urlContactInfo.Cookies;
            var client = new HttpClient(handler)
            {
                BaseAddress = uri,
                Timeout = TimeSpan.FromSeconds(urlContactInfo.TimeoutSeconds)
            };

            return client;
        }

        /// <summary>
        /// Return True if responseText is one of the custom responses that this class uses to report errors (timeout, abort, or exception)
        /// </summary>
        /// <param name="responseText"></param>
        public static bool IsResponseError(string responseText)
        {
            return responseText switch
            {
                REQUEST_ABORTED_RESPONSE => true,
                REQUEST_EXCEPTION_RESPONSE => true,
                REQUEST_TIMEOUT_RESPONSE => true,
                _ => false
            };
        }

        /// <summary>
        /// Report an error
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex">Exception (allowed to be nothing)</param>
        protected static void OnErrorEvent(string message, Exception ex)
        {
            if (ErrorEvent == null)
            {
                ConsoleMsgUtils.ShowErrorCustom(message, ex, false, false);
            }
            ErrorEvent?.Invoke(message, ex);
        }

        /// <summary>
        /// Report a status update
        /// </summary>
        /// <param name="percentCompleted">Value between 0 and 100</param>
        /// <param name="totalBytesSent">Total bytes to send</param>
        /// <param name="totalBytesToSend">Total bytes sent</param>
        /// <param name="statusMessage">Status message</param>
        // ReSharper disable once UnusedMember.Global
        protected static void RaiseStatusUpdate(
            double percentCompleted, long totalBytesSent,
            long totalBytesToSend, string statusMessage)
        {
            StatusUpdate?.Invoke(null, new StatusEventArgs(percentCompleted, totalBytesSent, totalBytesToSend, statusMessage));
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <returns>Response data</returns>
        public static string Send(
            Configuration config,
            string url,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100)
        {
            const string postData = "";
            return Send(config, url, out responseStatusCode, postData, HttpMethod.Get, timeoutSeconds);
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <returns>Response data</returns>
        // ReSharper disable once UnusedMember.Global
        public static string Send(
            Configuration config,
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100)
        {
            const string postData = "";
            return Send(config, url, cookies, out responseStatusCode, postData, HttpMethod.Get, timeoutSeconds);
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="postData">Data to post (when method is HttpMethod.Post)</param>
        /// <param name="method">Get, Post, or Put</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <returns>Response data</returns>
        public static string Send(
            Configuration config,
            string url,
            out HttpStatusCode responseStatusCode,
            string postData,
            HttpMethod method = HttpMethod.Get,
            int timeoutSeconds = 100)
        {
            return Send(config, url, new CookieContainer(), out responseStatusCode, postData, method, timeoutSeconds);
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="postData">Data to post (when method is HttpMethod.Post)</param>
        /// <param name="method">Get, Post, or Put</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="contentType">Form/post content-type</param>
        /// <param name="sendStringInHeader">If True, and the method is Get, include postData in the header</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>Response data</returns>
        // ReSharper disable once UnusedMember.Global
        public static string Send(
            Configuration config,
            string url,
            out HttpStatusCode responseStatusCode,
            string postData,
            HttpMethod method,
            int timeoutSeconds,
            string contentType,
            bool sendStringInHeader,
            NetworkCredential loginCredentials)
        {
            return Send(config, url, new CookieContainer(), out responseStatusCode, postData, method, timeoutSeconds, contentType, sendStringInHeader, loginCredentials);
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="postData">Data to post (when method is HttpMethod.Post)</param>
        /// <param name="method">Get, Post, or Put</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>Response data</returns>
        // ReSharper disable once UnusedMember.Global
        public static string Send(
            Configuration config,
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            string postData,
            HttpMethod method,
            int timeoutSeconds,
            NetworkCredential loginCredentials)
        {
            const string contentType = "";
            const bool sendStringInHeader = false;
            return Send(config, url, cookies, out responseStatusCode, postData, method, timeoutSeconds, contentType, sendStringInHeader, loginCredentials);
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <remarks>Uses the Task Parallel Library (TPL)</remarks>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="postData">Data to post (when method is HttpMethod.Post)</param>
        /// <param name="method">Get, Post, or Put</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="contentType">Form/post content-type</param>
        /// <param name="sendStringInHeader">If True, and the method is Get, include postData in the header</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>Response data</returns>
        public static string Send(
            Configuration config,
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            string postData = "",
            HttpMethod method = HttpMethod.Get,
            int timeoutSeconds = 100,
            string contentType = "",
            bool sendStringInHeader = false,
            NetworkCredential loginCredentials = null)
        {
            var urlContactInfo = new UrlContactInfo(
                config, url, cookies, postData, method,
                timeoutSeconds, contentType, sendStringInHeader, loginCredentials);

            // Contact the URL in a separate thread so that we can abort the call if it takes too long
            var cancelToken = new CancellationTokenSource();
            var task = Task.Factory.StartNew(() => Send(urlContactInfo, cancelToken.Token), cancelToken.Token);

            var success = task.Wait((timeoutSeconds + 5) * 1000);

            if (success)
            {
                responseStatusCode = urlContactInfo.ResponseData.ResponseStatusCode;
            }
            else
            {
                urlContactInfo.ResponseData.ResponseText = REQUEST_TIMEOUT_RESPONSE;
                urlContactInfo.ResponseData.ResponseStatusCode = HttpStatusCode.RequestTimeout;
                responseStatusCode = urlContactInfo.ResponseData.ResponseStatusCode;
            }

            return urlContactInfo.ResponseData.ResponseText;
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <remarks>Uses ThreadStart instead of TPL</remarks>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <returns>Response data</returns>
        public static string SendViaThreadStart(
            Configuration config,
            string url,
            out HttpStatusCode responseStatusCode,
            int timeoutSeconds = 100)
        {
            const string postData = "";
            return SendViaThreadStart(config, url, new CookieContainer(), out responseStatusCode, postData, HttpMethod.Get, timeoutSeconds);
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        /// <remarks>Uses ThreadStart instead of TPL</remarks>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="responseStatusCode">Response status code</param>
        /// <param name="postData">Data to post (when method is HttpMethod.Post)</param>
        /// <param name="method">Get, Post, or Put</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="contentType">Form/post content-type</param>
        /// <param name="sendStringInHeader">If True, and the method is Get, include postData in the header</param>
        /// <param name="loginCredentials">Login credentials</param>
        /// <returns>Response data</returns>
        public static string SendViaThreadStart(
            Configuration config,
            string url,
            CookieContainer cookies,
            out HttpStatusCode responseStatusCode,
            string postData = "",
            HttpMethod method = HttpMethod.Get,
            int timeoutSeconds = 100,
            string contentType = "",
            bool sendStringInHeader = false,
            NetworkCredential loginCredentials = null)
        {
            try
            {
                if (timeoutSeconds < 5)
                {
                    timeoutSeconds = 5;
                }

                mUrlContactInfo = new UrlContactInfo(
                    config, url, cookies, postData, method,
                    timeoutSeconds, contentType, sendStringInHeader, loginCredentials);

                var startTime = DateTime.UtcNow;
                var runtimeExceeded = false;
                var threadAborted = false;

                mThreadedSendCancel = new CancellationTokenSource();
                mThreadedSend = new Thread(StartThreadedSend);
                mThreadedSend.Start();

                // Loop until URL call finishes, or until timeoutSeconds elapses
                while (mThreadedSend.ThreadState != ThreadState.Stopped)
                {
                    AppUtils.SleepMilliseconds(25);

                    if (mThreadedSend.ThreadState == ThreadState.Aborted)
                    {
                        threadAborted = true;
                        break;
                    }

                    if (DateTime.UtcNow.Subtract(startTime).TotalSeconds < timeoutSeconds + 5)
                    {
                        continue;
                    }

                    var abortThread = new Thread(AbortThreadedSendNow);
                    abortThread.Start();
                    AppUtils.SleepMilliseconds(25);

                    runtimeExceeded = true;
                    threadAborted = true;
                    break;
                }

                if (!threadAborted && !mUrlContactInfo.ResponseData.ExceptionCaught)
                {
                    responseStatusCode = mUrlContactInfo.ResponseData.ResponseStatusCode;
                    return mUrlContactInfo.ResponseData.ResponseText;
                }

                string responseTextToReturn;

                if (runtimeExceeded)
                {
                    mUrlContactInfo.ResponseData.ResponseText = REQUEST_TIMEOUT_RESPONSE;
                    mUrlContactInfo.ResponseData.ResponseStatusCode = HttpStatusCode.RequestTimeout;
                    responseTextToReturn = mUrlContactInfo.ResponseData.ResponseText;
                }
                else if (mUrlContactInfo.ResponseData.ExceptionCaught)
                {
                    mUrlContactInfo.ResponseData.ResponseText = REQUEST_EXCEPTION_RESPONSE;

                    if (mUrlContactInfo.ResponseData.ResponseStatusCode != HttpStatusCode.PreconditionFailed)
                    {
                        mUrlContactInfo.ResponseData.ResponseStatusCode = HttpStatusCode.BadRequest;
                    }

                    if (string.IsNullOrWhiteSpace(mUrlContactInfo.ResponseData.ExceptionMessage))
                    {
                        responseTextToReturn = mUrlContactInfo.ResponseData.ResponseText;
                    }
                    else
                    {
                        responseTextToReturn = mUrlContactInfo.ResponseData.ResponseText + "; " + mUrlContactInfo.ResponseData.ExceptionMessage;
                    }
                }
                else
                {
                    mUrlContactInfo.ResponseData.ResponseText = REQUEST_ABORTED_RESPONSE;
                    mUrlContactInfo.ResponseData.ResponseStatusCode = HttpStatusCode.BadRequest;
                    responseTextToReturn = mUrlContactInfo.ResponseData.ResponseText;
                }

                responseStatusCode = mUrlContactInfo.ResponseData.ResponseStatusCode;

                return responseTextToReturn;
            }
            catch (Exception ex)
            {
                mUrlContactInfo.ResponseData.ResponseStatusCode = HttpStatusCode.BadRequest;
                throw new Exception("Caught exception while trying to start a thread to contact " + url, ex);
            }
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        private static void Send(UrlContactInfo urlContactInfo, CancellationToken cancelToken)
        {
            Task.Run(async () => await SendAsync(urlContactInfo), cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get or post data to a URL
        /// </summary>
        private static async Task SendAsync(UrlContactInfo urlContactInfo)
        {
            var client = InitializeClient(urlContactInfo);
            urlContactInfo.ResponseData.ResponseStatusCode = HttpStatusCode.NotFound;

            System.Net.Http.HttpMethod method = urlContactInfo.Method switch
            {
                HttpMethod.Post => System.Net.Http.HttpMethod.Post,
                HttpMethod.Put => System.Net.Http.HttpMethod.Put,
                HttpMethod.Get => System.Net.Http.HttpMethod.Get,
                _ => System.Net.Http.HttpMethod.Get
            };

            // Prepare the request object
            using var request = new HttpRequestMessage(method, urlContactInfo.Url);

            if (urlContactInfo.SendStringInHeader && urlContactInfo.Method == HttpMethod.Get)
            {
                request.Headers.Add("X-Json-Data", urlContactInfo.PostData);
            }

            // Set form/post content-type if necessary
            if (urlContactInfo.Method == HttpMethod.Post && !string.IsNullOrEmpty(urlContactInfo.PostData) && string.IsNullOrEmpty(urlContactInfo.ContentType))
            {
                urlContactInfo.ContentType = "application/x-www-form-urlencoded";
            }

            // Write POST data, if POST
            if (urlContactInfo.Method == HttpMethod.Post && urlContactInfo.PostData != null)
            {
                if (!string.IsNullOrWhiteSpace(urlContactInfo.ContentType))
                {
                    // Set Content-Type
                    request.Content = new StringContent(urlContactInfo.PostData, Encoding.UTF8, urlContactInfo.ContentType);
                }
                else
                {
                    request.Content = new StringContent(urlContactInfo.PostData, Encoding.UTF8);
                }
            }

            // Receive response
            urlContactInfo.ResponseData.ResponseText = string.Empty;
            try
            {
                client.Timeout = TimeSpan.FromSeconds(urlContactInfo.TimeoutSeconds);
                using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
                urlContactInfo.ResponseData.ResponseStatusCode = response.StatusCode;
                urlContactInfo.ResponseData.ResponseText = await response.Content.ReadAsStringAsync();
            }
            catch (HttpRequestException ex)
            {
                HandleRequestException(ex, urlContactInfo.Url, urlContactInfo.ResponseData);
            }
            catch (TaskCanceledException ex)
            {
                HandleRequestException(new HttpRequestException("HTTP request timed out", ex), urlContactInfo.Url, urlContactInfo.ResponseData);
            }
            catch (OperationCanceledException ex)
            {
                HandleRequestException(new HttpRequestException($"HTTP request was cancelled?: {ex.CancellationToken.IsCancellationRequested}", ex), urlContactInfo.Url, urlContactInfo.ResponseData);
            }
        }

        /// <summary>
        /// Start a thread to contact the URL
        /// </summary>
        private static void StartThreadedSend()
        {
            try
            {
                Send(mUrlContactInfo, mThreadedSendCancel.Token);
            }
            catch (Exception ex)
            {
                mUrlContactInfo.ResponseData.RegisterException(ex);
                OnErrorEvent(ex.Message, ex);
            }
        }

        /// <summary>
        /// Determine the path to the MyEMSL Certificate file
        /// </summary>
        /// <param name="config">Pacifica Config</param>
        /// <param name="callingMethod">Calling method</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>Path to the file if found, otherwise an empty string</returns>
        public static string ResolveCertFile(Configuration config, string callingMethod, out string errorMessage)
        {
            return ResolveCertFile(config, callingMethod, out _, out errorMessage);
        }

        /// <summary>
        /// Determine the path to the MyEMSL Certificate file
        /// </summary>
        /// <param name="config">Pacifica Config</param>
        /// <param name="callingMethod">Calling method</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <param name="errorMessageDetailed">Output: error message, including calling method</param>
        /// <returns>Path to the file if found, otherwise an empty string</returns>
        public static string ResolveCertFile(Configuration config, string callingMethod, out string errorMessage, out string errorMessageDetailed)
        {
            var certificateFilePath = config.ClientCertFilePath;

            if (!string.IsNullOrWhiteSpace(certificateFilePath))
            {
                errorMessage = string.Empty;
                errorMessageDetailed = string.Empty;
                return certificateFilePath;
            }

            // Example message:
            // MyEMSL certificate file not found in the current directory or at C:\client_certs\svc-dms.pfx
            if (Directory.Exists(@"C:\DMS_Programs"))
            {
                errorMessage = "MyEMSL certificate file not found in the current directory, in " + Configuration.DMS_CLIENT_CERT_DIRECTORY + ", or in " + Configuration.CLIENT_CERT_DIRECTORY;
            }
            else
            {
                errorMessage = "MyEMSL certificate file not found in the current directory or in " + Configuration.CLIENT_CERT_DIRECTORY;
            }

            // Example message:
            // Authentication failure in InitializeRequest; MyEMSL certificate file not found in the current directory or at C:\client_certs\svc-dms.pfx
            errorMessageDetailed = "Authentication failure in " + callingMethod + "; " + errorMessage;

            return string.Empty;
        }
    }
}
