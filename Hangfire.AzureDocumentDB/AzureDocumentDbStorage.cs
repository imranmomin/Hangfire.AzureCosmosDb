using System;
using System.Collections.Generic;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Hangfire.Server;
using Hangfire.Storage;
using Hangfire.Logging;
using Hangfire.AzureDocumentDB.Queue;

namespace Hangfire.AzureDocumentDB
{
    /// <summary>
    /// FirebaseStorage extend the storage option for Hangfire.
    /// </summary>
    public sealed class AzureDocumentDbStorage : JobStorage
    {
        internal AzureDocumentDbStorageOptions Options { get; }

        internal PersistentJobQueueProviderCollection QueueProviders { get; }

        internal DocumentClient Client { get; }

        /// <summary>
        /// Initializes the FirebaseStorage form the url auth secret provide.
        /// </summary>
        /// <param name="url">The url string to Firebase Database</param>
        /// <param name="authSecret">The secret key for the Firebase Database</param>
        /// <param name="database">The name of the database to connect with</param>
        /// <exception cref="ArgumentNullException"><paramref name="url"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="authSecret"/> argument is null.</exception>
        public AzureDocumentDbStorage(string url, string authSecret, string database) : this(new AzureDocumentDbStorageOptions { Endpoint = new Uri(url), AuthSecret = authSecret, DatabaseName = database }) { }

        /// <summary>
        /// Initializes the FirebaseStorage form the url auth secret provide.
        /// </summary>
        /// <param name="url">The url string to Firebase Database</param>
        /// <param name="authSecret">The secret key for the Firebase Database</param>
        /// <param name="database">The name of the database to connect with</param>
        /// <param name="options">The FirebaseStorage object to override any of the options</param>
        /// <exception cref="ArgumentNullException"><paramref name="url"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="authSecret"/> argument is null.</exception>
        public AzureDocumentDbStorage(string url, string authSecret, string database, AzureDocumentDbStorageOptions options) : this(Transform(url, authSecret, database, options)) { }

        /// <summary>
        /// Initializes the FirebaseStorage form the url auth secret provide.
        /// </summary>
        /// <param name="options">The FirebaseStorage object to override any of the options</param>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
        private AzureDocumentDbStorage(AzureDocumentDbStorageOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            Options = options;

            ConnectionPolicy connectionPolicy = ConnectionPolicy.Default;
            connectionPolicy.RequestTimeout = options.RequestTimeout;
            Client = new DocumentClient(options.Endpoint, options.AuthSecret, connectionPolicy);
            Client.OpenAsync().GetAwaiter().GetResult();

            // create the database all the collections
            Initialize();

            Newtonsoft.Json.JsonConvert.DefaultSettings = () => new Newtonsoft.Json.JsonSerializerSettings
            {
                NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore,
                DefaultValueHandling = Newtonsoft.Json.DefaultValueHandling.Ignore,
                DateTimeZoneHandling = Newtonsoft.Json.DateTimeZoneHandling.Utc,
                TypeNameHandling = Newtonsoft.Json.TypeNameHandling.All
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
            logger.Info("Using the following options for Firebase job storage:");
            logger.Info($"     Firebase Url: {Options.Endpoint.AbsoluteUri}");
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
            logger.Info("Creating document collection : servers");
            Client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, new DocumentCollection { Id = "servers" }).GetAwaiter().GetResult();
            logger.Info("Creating document collection : queues");
            Client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, new DocumentCollection { Id = "queues" }).GetAwaiter().GetResult();
            logger.Info("Creating document collection : hashes");
            Client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, new DocumentCollection { Id = "hashes" }).GetAwaiter().GetResult();
            logger.Info("Creating document collection : lists");
            Client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, new DocumentCollection { Id = "lists" }).GetAwaiter().GetResult();
            logger.Info("Creating document collection : counters");
            Client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, new DocumentCollection { Id = "counters" }).GetAwaiter().GetResult();
            logger.Info("Creating document collection : jobs");
            Client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, new DocumentCollection { Id = "jobs" }).GetAwaiter().GetResult();
            logger.Info("Creating document collection : states");
            Client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, new DocumentCollection { Id = "states" }).GetAwaiter().GetResult();
            logger.Info("Creating document collection : sets");
            Client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, new DocumentCollection { Id = "sets" }).GetAwaiter().GetResult();
            logger.Info("Creating document collection : locks");
            Client.CreateDocumentCollectionIfNotExistsAsync(databaseUri, new DocumentCollection { Id = "locks" }).GetAwaiter().GetResult();
        }

        private static AzureDocumentDbStorageOptions Transform(string url, string authSecret, string database, AzureDocumentDbStorageOptions options)
        {
            if (options == null) options = new AzureDocumentDbStorageOptions();

            options.Endpoint = new Uri(url);
            options.AuthSecret = authSecret;
            options.DatabaseName = database;

            return options;
        }

    }
}
