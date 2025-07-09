using System;

namespace Pacifica.Core
{
    /// <summary>
    /// Message event information
    /// </summary>
    public class MessageEventArgs : EventArgs
    {
        // Ignore Spelling: Pacifica

        /// <summary>
        /// Calling function
        /// </summary>
        public string CallingFunction { get; }

        /// <summary>
        /// Message
        /// </summary>
        public string Message { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="callingFunction"></param>
        /// <param name="message"></param>
        public MessageEventArgs(string callingFunction, string message)
        {
            CallingFunction = callingFunction;
            Message = message;
        }
    }

    /// <summary>
    /// Status event information
    /// </summary>
    public class StatusEventArgs : EventArgs
    {
        /// <summary>
        /// Value between 0 and 100
        /// </summary>
        public double PercentCompleted { get; }

        /// <summary>
        /// Total bytes that have been sent
        /// </summary>
        public long TotalBytesSent { get; }

        /// <summary>
        /// Total bytes to send
        /// </summary>
        public long TotalBytesToSend { get; }

        /// <summary>
        /// Status message
        /// </summary>
        public string StatusMessage { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="percentCompleted">Percent complete, value between 0 and 100</param>
        /// <param name="totalBytesSent">Total bytes sent</param>
        /// <param name="totalBytesToSend">Total bytes to send</param>
        /// <param name="statusMessage">Status message</param>
        public StatusEventArgs(double percentCompleted, long totalBytesSent, long totalBytesToSend, string statusMessage)
        {
            PercentCompleted = percentCompleted;
            TotalBytesSent = totalBytesSent;
            TotalBytesToSend = totalBytesToSend;
            StatusMessage = statusMessage;
        }
    }

    /// <summary>
    /// Upload Completion event information
    /// </summary>
    public class UploadCompletedEventArgs : EventArgs
    {
        /// <summary>
        /// Server response
        /// </summary>
        public string ServerResponse { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="serverResponse"></param>
        public UploadCompletedEventArgs(string serverResponse)
        {
            ServerResponse = serverResponse;
        }
    }
}