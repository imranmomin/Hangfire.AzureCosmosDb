using System;

// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
namespace Hangfire.Azure;

/// <summary>
///     Options for CosmosDbStorage
/// </summary>
public class CosmosDbStorageOptions
{
	/// <summary>
	///		Gets or sets a value indicating whether initialization will check for the Container existence and create it if it doesn't exist.
	/// </summary>
	/// <value>Default value is true</value>
	public bool CreateIfNotExists { get; set; } = true;

	/// <summary>
	///     Get or set the interval timespan to process expired entries. Default value 30 minutes.
	///     Expired items under "locks", "jobs", "lists", "sets", "hashs", "counters", "state" will be checked
	/// </summary>
	public TimeSpan ExpirationCheckInterval { get; set; } = TimeSpan.FromMinutes(30);

	/// <summary>
	///     Get or sets the interval timespan to aggregated the counters. Default value 2 minute
	/// </summary>
	public TimeSpan CountersAggregateInterval { get; set; } = TimeSpan.FromMinutes(2);

	/// <summary>
	///     Gets or sets the interval timespan to poll the queue for processing any new jobs. Default value 15 minutes
	/// </summary>
	public TimeSpan QueuePollInterval { get; set; } = TimeSpan.FromSeconds(15);

	/// <summary>
	///		Gets or sets the max item count to aggregate the counters. Default value 100
	/// </summary>
	public int CountersAggregateMaxItemCount { get; set; } = 100;

	internal TimeSpan TransactionalLockTimeout { get; set; } = TimeSpan.FromSeconds(30);

	/// <summary>
	///		Gets or sets the interval timespan for job keep alive interval. Default value 30 seconds
	/// </summary>
	public TimeSpan JobKeepAliveInterval { get; set; } = TimeSpan.FromSeconds(15);
}