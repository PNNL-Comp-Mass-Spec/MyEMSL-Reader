using System;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using PRISM;

namespace Pacifica.Core
{
    /// <summary>
    /// Class for the certificate validation callback
    /// </summary>
    internal class CertificateValidation
    {
        /// <summary>
        /// Error event
        /// </summary>
        public event EventNotifier.ErrorEventEventHandler ErrorEvent;

        /// <summary>
        /// Warning event
        /// </summary>
        public event EventNotifier.WarningEventEventHandler WarningEvent;

        /// <summary>
        /// Report an error
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex">Exception (allowed to be nothing)</param>
        protected void OnErrorEvent(string message, Exception ex = null)
        {
            if (ErrorEvent == null)
            {
                ConsoleMsgUtils.ShowErrorCustom(message, ex, false, false);
            }
            ErrorEvent?.Invoke(message, ex);
        }

        /// <summary>
        /// Report a warning
        /// </summary>
        /// <param name="message"></param>
        protected void OnWarningEvent(string message)
        {
            if (WarningEvent == null)
            {
                ConsoleMsgUtils.ShowWarningCustom(message);
            }
            WarningEvent?.Invoke(message);
        }

        /// <summary>
        /// Event handler for System.Net.ServicePointManager.ServerCertificateValidationCallback or HttpClientHandler.ServerCertificateCustomValidationCallback
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="cert"></param>
        /// <param name="chain"></param>
        /// <param name="policyErrors"></param>
        /// <returns></returns>
        public bool ValidateRemoteCertificate(object sender, X509Certificate cert, X509Chain chain, SslPolicyErrors policyErrors)
        {
            var success = Utilities.ValidateRemoteCertificate(cert, policyErrors, out var errorMessage);

            if (success)
            {
                if (!string.IsNullOrWhiteSpace(errorMessage))
                {
                    OnWarningEvent(errorMessage);
                }
                return true;
            }

            OnErrorEvent(errorMessage, null);

            return false;
        }
    }
}
