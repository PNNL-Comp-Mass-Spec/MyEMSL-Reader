using System.Net;

namespace MyEMSLReader
{
    /// <summary>
    /// Tracks the response code and response text returned by a call to EasyHttp.Send
    /// </summary>
    class WebResponseData
    {
        /// <summary>
        /// Response code
        /// </summary>
        public HttpStatusCode ResponseStatusCode { get; set; }

        /// <summary>
        /// Response text
        /// </summary>
        public string ResponseText { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public WebResponseData()
        {
            ResponseText = string.Empty;
        }
    }
}
