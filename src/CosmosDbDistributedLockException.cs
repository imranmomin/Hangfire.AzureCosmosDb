using System;

namespace Hangfire.Azure
{
    /// <summary>
    /// Represents errors that occur while acquiring a distributed lock.
    /// </summary>
    [Serializable]
    public class CosmosDBDistributedLockException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the CosmosDBDistributedLockException class with serialized data.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public CosmosDBDistributedLockException(string message) : base(message) { }
    }
}
