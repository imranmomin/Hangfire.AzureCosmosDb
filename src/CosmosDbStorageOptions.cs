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
	///     Create an instance of Azure CosmosDbStorage option with default values
	/// </summary>
	public CosmosDbStorageOptions()
	{
		ExpirationCheckInterval = TimeSpan.FromMinutes(2);
		CountersAggregateInterval = TimeSpan.FromMinutes(2);
		QueuePollInterval = TimeSpan.FromSeconds(15);
	}

	/// <summary>
	///     Get or set the interval timespan to process expired entries. Default value 2 minutes
	///     Expired items under "locks", "jobs", "lists", "sets", "hashs", "counters", "state" will be checked
	/// </summary>
	public TimeSpan ExpirationCheckInterval { get; set; }

	/// <summary>
	///     Get or sets the interval timespan to aggregated the counters. Default value 2 minute
	/// </summary>
	public TimeSpan CountersAggregateInterval { get; set; }

	/// <summary>
	///     Gets or sets the interval timespan to poll the queue for processing any new jobs. Default value 15 minutes
	/// </summary>
	public TimeSpan QueuePollInterval { get; set; }

	/// <summary>
	///		Gets or sets the max item count to aggregate the counters. Default value 100
	/// </summary>
	public int CountersAggregateMaxItemCount { get; set; } = 100;
}