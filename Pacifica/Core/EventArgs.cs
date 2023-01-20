using System;

namespace Pacifica.Core
{
    public class MessageEventArgs : EventArgs
    {
        /// <summary>
        /// Calling function
        /// </summary>
        public string CallingFunction { get; }

        /// <summary>
        /// Message
        /// </summary>
        public string Message { get; }

        public MessageEventArgs(string callingFunction, string message)
        {
            CallingFunction = callingFunction;
            Message = message;
        }
    }

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

    public class UploadCompletedEventArgs : EventArgs
    {
        /// <summary>
        /// Server response
        /// </summary>
        public string ServerResponse { get; }

        public UploadCompletedEventArgs(string serverResponse)
        {
            ServerResponse = serverResponse;
        }
    }
}