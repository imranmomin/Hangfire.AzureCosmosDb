using System;

namespace Hangfire.Azure;

/// <summary>
///     Represents errors that occur while acquiring a distributed lock.
/// </summary>
[Serializable]
internal class CosmosDbDistributedLockException : Exception
{
	/// <summary>
	///     Initializes a new instance of the CosmosDbDistributedLockException class with serialized data.
	/// </summary>
	/// <param name="message">The message that describes the error.</param>
	/// <param name="key">The key of the resource</param>
	public CosmosDbDistributedLockException(string message, string key) : base(message)
	{
		Key = key;
	}

	public string Key { get; }
}