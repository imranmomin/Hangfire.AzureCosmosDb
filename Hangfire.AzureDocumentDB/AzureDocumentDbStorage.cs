using System;
using System.Collections.Generic;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Hangfire.Server;
using Hangfire.Storage;
using Hangfire.Logging;
using Hangfire.AzureDocumentDB.Json;
using Hangfire.AzureDocumentDB.Queue;

namespace Hangfire.AzureDocumentDB
{
    /// <summary>
    /// AzureDocumentDbStorage extend the storage option for Hangfire.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    public sealed class AzureDocumentDbStorage : JobStorage
    {
        internal AzureDocumentDbStorageOptions Options { get; }

        internal PersistentJobQueueProviderCollection QueueProviders { get; }

        internal DocumentClient Client { get; }

        internal Uri CollectionUri { get; private set; }

        /// <summary>
        /// Initializes the AzureDocumentDbStorage form the url auth secret provide.
        /// </summary>
        /// <param name="url">The url string to DocumentDb Database</param>
        /// <param name="authSecret">The secret key for the DocumentDb Database</param>
        /// <param name="database">The name of the database to connect with</param>
        /// <param name="collection">The name of the collection on the database</param>
        /// <exception cref="ArgumentNullException"><paramref name="url"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="authSecret"/> argument is null.</exception>
        public AzureDocumentDbStorage(string url, string authSecret, string database, string collection) : this(new AzureDocumentDbStorageOptions { Endpoint = new Uri(url), AuthSecret = authSecret, DatabaseName = database, CollectionName = collection }) { }

        /// <summary>
        /// Initializes the AzureDocumentDbStorage form the url auth secret provide.
        /// </summary>
        /// <param name="url">The url string to DocumentDb Database</param>
        /// <param name="authSecret">The secret key for the DocumentDb Database</param>
        /// <param name="database">The name of the database to connect with</param>
        /// <param name="options">The AzureDocumentDbStorageOptions object to override any of the options</param>
        /// <param name="collection">The name of the collection on the database</param>
        /// <exception cref="ArgumentNullException"><paramref name="url"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="authSecret"/> argument is null.</exception>
        public AzureDocumentDbStorage(string url, string authSecret, string database, string collection, AzureDocumentDbStorageOptions options) : this(Transform(url, authSecret, database, collection, options)) { }

        /// <summary>
        /// Initializes the AzureDocumentDbStorage form the url auth secret provide.
        /// </summary>
        /// <param name="options">The AzureDocumentDbStorageOptions object to override any of the options</param>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
        private AzureDocumentDbStorage(AzureDocumentDbStorageOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            Options = options;

            ConnectionPolicy connectionPolicy = ConnectionPolicy.Default;
            connectionPolicy.RequestTimeout = options.RequestTimeout;
            Client = new DocumentClient(options.Endpoint, options.AuthSecret, connectionPolicy);
            Client.OpenAsync().GetAwaiter().GetResult();

            Initialize();

            Newtonsoft.Json.JsonConvert.DefaultSettings = () => new Newtonsoft.Json.JsonSerializerSettings
            {
                NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore,
                DateTimeZoneHandling = Newtonsoft.Json.DateTimeZoneHandling.Utc,
                ContractResolver = new DocumentContractResolver()
            };

            JobQueueProvider provider = new JobQueueProvider(this);
            QueueProviders = new PersistentJobQueueProviderCollection(provider);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IStorageConnection GetConnection() => new AzureDocumentDbConnection(this);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IMonitoringApi GetMonitoringApi() => new AzureDocumentDbMonitoringApi(this);

#pragma warning disable 618
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore 618
        {
            yield return new ExpirationManager(this);
            yield return new CountersAggregator(this);
        }

        /// <summary>
        /// Prints out the storage options
        /// </summary>
        /// <param name="logger"></param>
        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Azure DocumentDB job storage:");
            logger.Info($"     DocumentDB Url: {Options.Endpoint.AbsoluteUri}");
            logger.Info($"     Request Timeout: {Options.RequestTimeout}");
            logger.Info($"     Counter Agggerate Interval: {Options.CountersAggregateInterval.TotalSeconds} seconds");
            logger.Info($"     Queue Poll Interval: {Options.QueuePollInterval.TotalSeconds} seconds");
            logger.Info($"     Expiration Check Interval: {Options.ExpirationCheckInterval.TotalSeconds} seconds");
            logger.Info($"     Queue: {string.Join(",", Options.Queues)}");
        }

        /// <summary>
        /// Return the name of the database
        /// </summary>
        /// <returns></returns>
        public override string ToString() => $"DoucmentDb Database : {Options.DatabaseName}";

        private void Initialize()
        {
            ILog logger = LogProvider.For<AzureDocumentDbStorage>();
            Uri databaseUri = UriFactory.CreateDatabaseUri(Options.DatabaseName);

            // create database
            logger.Info($"Creating database : {Options.DatabaseName}");
            Client.CreateDatabaseIfNotExistsAsync(new Database { Id = Options.DatabaseName }).GetAwaiter().GetResult();

            logger.Info($"Creating document collection : {Options.CollectionName}");
            Client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, new DocumentCollection { Id = Options.CollectionName }).GetAwaiter().GetResult();
            CollectionUri = UriFactory.CreateDocumentCollectionUri(Options.DatabaseName, Options.CollectionName);
        }

        private static AzureDocumentDbStorageOptions Transform(string url, string authSecret, string database, string collection, AzureDocumentDbStorageOptions options)
        {
            if (options == null) options = new AzureDocumentDbStorageOptions();

            options.Endpoint = new Uri(url);
            options.AuthSecret = authSecret;
            options.DatabaseName = database;
            options.CollectionName = collection;

            return options;
        }

    }
}
