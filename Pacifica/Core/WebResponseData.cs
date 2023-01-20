using System;
using System.Net;

namespace Pacifica.Core
{
    /// <summary>
    /// Tracks the response code and response text returned by a call to EasyHttp.Send
    /// </summary>
    public class WebResponseData
    {
        /// <summary>
        /// Set to true when an exception was caught
        /// </summary>
        /// <remarks>For details, see exceptionMessage and ExceptionStackTrace</remarks>
        public bool ExceptionCaught { get; private set; }

        /// <summary>
        /// Exception message
        /// </summary>
        public string ExceptionMessage { get; private set; }

        /// <summary>
        /// Stack trace for the exception
        /// </summary>
        public string ExceptionStackTrace { get; private set; }

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
            ResetExceptionInfo();
        }

        /// <summary>
        /// Clear cached exception info
        /// </summary>
        public void ResetExceptionInfo()
        {
            ExceptionCaught = false;
            ExceptionMessage = string.Empty;
            ExceptionStackTrace = string.Empty;
        }

        /// <summary>
        /// Store information on a new exception
        /// </summary>
        /// <param name="ex"></param>
        public void RegisterException(Exception ex)
        {
            if (ExceptionCaught && !string.IsNullOrWhiteSpace(ExceptionMessage))
            {
                // An exception has already been cached
                return;
            }

            ExceptionCaught = true;
            ExceptionMessage = ex.Message;
            ExceptionStackTrace = PRISM.StackTraceFormatter.GetExceptionStackTraceMultiLine(ex);
        }
    }
}
