using System;
using System.Collections.Generic;

using Hangfire.Server;
using Hangfire.Storage;
using Hangfire.Logging;
using Hangfire.AzureDocumentDB.Queue;

using Microsoft.Azure.Documents.Client;

namespace Hangfire.AzureDocumentDB
{
    /// <summary>
    /// FirebaseStorage extend the storage option for Hangfire.
    /// </summary>
    public sealed class AzureDocumentDbStorage : JobStorage
    {
        internal AzureDocumentDbStorageOptions Options { get; }

        internal PersistentJobQueueProviderCollection QueueProviders { get; }

        /// <summary>
        /// Initializes the FirebaseStorage form the url & auth secret provide.
        /// </summary>
        /// <param name="url">The url string to Firebase Database</param>
        /// <param name="authSecret">The secret key for the Firebase Database</param>
        /// <exception cref="ArgumentNullException"><paramref name="url"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="authSecret"/> argument is null.</exception>
        public AzureDocumentDbStorage(string url, string authSecret) : this(new AzureDocumentDbStorageOptions { Endpoint = new Uri(url), AuthSecret = authSecret }) { }

        /// <summary>
        /// Initializes the FirebaseStorage form the url & auth secret provide.
        /// </summary>
        /// <param name="options">The FirebaseStorage object to override any of the options</param>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
        public AzureDocumentDbStorage(AzureDocumentDbStorageOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            Options = options;

            using (DocumentClient client = new DocumentClient(options.Endpoint, options.AuthSecret))
            {
                client.OpenAsync();
            }

            JobQueueProvider provider = new JobQueueProvider(this);
            QueueProviders = new PersistentJobQueueProviderCollection(provider);
        }

        public override IStorageConnection GetConnection() => new AzureDocumentDbConnection(this);

        public override IMonitoringApi GetMonitoringApi() => new AzureDocumentDbMonitoringApi(this);

#pragma warning disable 618
        public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore 618
        {
            yield return new ExpirationManager(this);
            yield return new CountersAggregator(this);
        }

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

        public override string ToString() => $"Firbase Database : {Options.Endpoint.AbsoluteUri}";

    }
}
