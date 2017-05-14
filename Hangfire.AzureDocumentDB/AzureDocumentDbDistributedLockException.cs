using System;

namespace Hangfire.AzureDocumentDB
{
    /// <summary>
    /// Represents errors that occur while acquiring a distributed lock.
    /// </summary>
    [Serializable]
    public class AzureDocumentDbDistributedLockException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the AzureDocumentDbDistributedLockException class with serialized data.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public AzureDocumentDbDistributedLockException(string message) : base(message)
        {
        }
    }
}
