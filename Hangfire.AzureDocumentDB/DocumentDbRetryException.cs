using System;

namespace Hangfire.Azure
{
    /// <summary>
    /// Represents errors that occur while retry execution limit exceeds
    /// </summary>
    [Serializable]
    public class DocumentDbRetryException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the DocumentDbRetryException class with serialized data.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public DocumentDbRetryException(string message) : base(message)
        {
        }
    }
}
