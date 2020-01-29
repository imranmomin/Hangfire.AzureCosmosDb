using System;
using Hangfire.Azure;
using Microsoft.Azure.Cosmos;

// ReSharper disable UnusedMember.Global
// ReSharper disable once CheckNamespace
namespace Hangfire
{
    /// <summary>
    /// Extension methods to user CosmosDB Storage.
    /// </summary>
    // ReSharper disable once UnusedType.Global
    public static class CosmosDbStorageExtensions
    {
        /// <summary>
        /// Enables to attach Azure CosmosDB to Hangfire
        /// </summary>
        /// <param name="configuration">The IGlobalConfiguration object</param>
        /// <param name="url">The url string to CosmosDB Database</param>
        /// <param name="authSecret">The secret key for the CosmosDB Database</param>
        /// <param name="database">The name of the database to connect with</param>
        /// <param name="collection">The name of the collection on the database</param>
        /// <param name="option"></param>
        /// <param name="storageOptions">The CosmosDbStorage object to override any of the options</param>
        /// <returns></returns>
        public static IGlobalConfiguration<CosmosDbStorage> UseAzureCosmosDbStorage(this IGlobalConfiguration configuration, string url, string authSecret, string database, string collection, CosmosClientOptions option = null, CosmosDbStorageOptions storageOptions = null)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (string.IsNullOrEmpty(url)) throw new ArgumentNullException(nameof(url));
            if (string.IsNullOrEmpty(authSecret)) throw new ArgumentNullException(nameof(authSecret));

            CosmosDbStorage storage = new CosmosDbStorage(url, authSecret, database, collection, option, storageOptions);
            return configuration.UseStorage(storage);
        }
    }
}
