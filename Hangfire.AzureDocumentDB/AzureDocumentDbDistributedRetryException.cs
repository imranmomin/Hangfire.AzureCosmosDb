using System;

namespace Hangfire.AzureDocumentDB
{
    /// <summary>
    /// Represents errors that occur while acquiring a distributed lock.
    /// </summary>
    [Serializable]
    public class AzureDocumentDbDistributedRetryException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the AzureDocumentDbDistributedRetryException class with serialized data.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public AzureDocumentDbDistributedRetryException(string message) : base(message)
        {
        }
    }
}
