using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure;
using Microsoft.Azure.Cosmos;

// ReSharper disable UnusedMember.Global
// ReSharper disable once CheckNamespace
namespace Hangfire;

/// <summary>
///     Extension methods to use CosmosDBStorage.
/// </summary>
// ReSharper disable once UnusedType.Global
public static class CosmosDbStorageExtensions
{
	/// <summary>
	///     Enables to attach Azure Cosmos DB to Hangfire
	/// </summary>
	/// <param name="configuration">The IGlobalConfiguration object</param>
	/// <param name="url">The url string to Cosmos Database</param>
	/// <param name="authSecret">The secret key for the Cosmos Database</param>
	/// <param name="database">The name of the database to connect with</param>
	/// <param name="collection">The name of the collection on the database</param>
	/// <param name="option"></param>
	/// <param name="storageOptions">The CosmosDbStorage object to override any of the options</param>
	/// <returns></returns>
	public static IGlobalConfiguration<CosmosDbStorage> UseAzureCosmosDbStorage(this IGlobalConfiguration configuration, string url, string authSecret, string database, string collection,
		CosmosClientOptions? option = null,
		CosmosDbStorageOptions? storageOptions = null)
	{
		if (configuration == null) throw new ArgumentNullException(nameof(configuration));
		if (string.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
		if (string.IsNullOrEmpty(authSecret)) throw new ArgumentNullException(nameof(authSecret));

		CosmosDbStorage storage = CosmosDbStorage.Create(url, authSecret, database, collection, option, storageOptions);
		return configuration.UseStorage(storage);
	}

	/// <summary>
	///     Enables to attach Azure Cosmos DB to Hangfire
	/// </summary>
	/// <param name="configuration">The IGlobalConfiguration object</param>
	/// <param name="url">The url string to Cosmos Database</param>
	/// <param name="authSecret">The secret key for the Cosmos Database</param>
	/// <param name="database">The name of the database to connect with</param>
	/// <param name="collection">The name of the collection on the database</param>
	/// <param name="option"></param>
	/// <param name="storageOptions">The CosmosDbStorage object to override any of the options</param>
	/// <param name="cancellationToken">A cancellation token</param>
	/// <returns></returns>
	public static async Task<IGlobalConfiguration<CosmosDbStorage>> UseAzureCosmosDbStorageAsync(this IGlobalConfiguration configuration, string url, string authSecret, string database, string collection,
		CosmosClientOptions? option = null,
		CosmosDbStorageOptions? storageOptions = null,
		CancellationToken cancellationToken = default)
	{
		if (configuration == null) throw new ArgumentNullException(nameof(configuration));
		if (string.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
		if (string.IsNullOrEmpty(authSecret)) throw new ArgumentNullException(nameof(authSecret));

		CosmosDbStorage storage = await CosmosDbStorage.CreateAsync(url, authSecret, database, collection, option, storageOptions, cancellationToken);
		return configuration.UseStorage(storage);
	}
}