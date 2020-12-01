using System.Net;

namespace Pacifica.Core
{
    internal class UrlContactInfo
    {
        /// <summary>
        /// Configuration options
        /// </summary>
        public Configuration Config { get; }

        /// <summary>
        /// URL
        /// </summary>
        public string Url { get; }

        /// <summary>
        /// Cookies
        /// </summary>
        public CookieContainer Cookies { get; }

        /// <summary>
        /// Tracks response text and response code
        /// </summary>
        public WebResponseData ResponseData { get; }

        /// <summary>
        /// Data to post (when method is HttpMethod.Post)
        /// </summary>
        public string PostData { get; }

        /// <summary>
        /// Get, Post, or Put
        /// </summary>
        public EasyHttp.HttpMethod Method { get; }

        /// <summary>
        /// Timeout, in seconds
        /// </summary>
        public int TimeoutSeconds { get; }

        /// <summary>
        /// Form/post content-type
        /// </summary>
        public string ContentType { get; set; }

        /// <summary>
        /// If True, and the method is Get, include postData in the header
        /// </summary>
        public bool SendStringInHeader { get; }

        /// <summary>
        /// Login credentials
        /// </summary>
        public NetworkCredential LoginCredentials { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="config">Configuration options</param>
        /// <param name="url">URL</param>
        /// <param name="cookies">Cookies</param>
        /// <param name="postData">Data to post (when method is HttpMethod.Post)</param>
        /// <param name="method">Get, Post, or Put</param>
        /// <param name="timeoutSeconds">Timeout, in seconds</param>
        /// <param name="contentType">Form/post content-type</param>
        /// <param name="sendStringInHeader">If True, and the method is Get, include postData in the header</param>
        /// <param name="loginCredentials">Login credentials</param>
        public UrlContactInfo(
            Configuration config,
            string url,
            CookieContainer cookies,
            string postData = "",
            EasyHttp.HttpMethod method = EasyHttp.HttpMethod.Get,
            int timeoutSeconds = 100,
            string contentType = "",
            bool sendStringInHeader = false,
            NetworkCredential loginCredentials = null)
        {
            Config = config;
            Url = url;

            if (cookies == null)
            {
                cookies = new CookieContainer();
            }
            Cookies = cookies;

            ResponseData = new WebResponseData();
            PostData = postData;
            Method = method;
            TimeoutSeconds = timeoutSeconds;
            ContentType = contentType;
            SendStringInHeader = sendStringInHeader;
            LoginCredentials = loginCredentials;
        }
    }
}
