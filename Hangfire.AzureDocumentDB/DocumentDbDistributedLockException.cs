using System;

namespace Hangfire.Azure
{
    /// <summary>
    /// Represents errors that occur while acquiring a distributed lock.
    /// </summary>
    [Serializable]
    public class DocumentDbDistributedLockException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the DocumentDbDistributedLockException class with serialized data.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public DocumentDbDistributedLockException(string message) : base(message)
        {
        }
    }
}
