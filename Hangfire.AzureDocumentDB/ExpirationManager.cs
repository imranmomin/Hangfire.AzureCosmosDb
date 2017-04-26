using System;
using System.Linq;
using System.Threading;
using System.Collections.Generic;

using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Documents.Client;

using Hangfire.Server;
using Hangfire.Logging;
using Hangfire.AzureDocumentDB.Entities;

namespace Hangfire.AzureDocumentDB
{
#pragma warning disable 618
    internal class ExpirationManager : IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.For<ExpirationManager>();
        private const string distributedLockKey = "expirationmanager";
        private static readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(5);
        private static readonly string[] documents = { "locks", "jobs", "lists", "sets", "hashes", "counters" };
        private readonly TimeSpan checkInterval;

        private readonly AzureDocumentDbStorage storage;

        public ExpirationManager(AzureDocumentDbStorage storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            this.storage = storage;
            checkInterval = storage.Options.ExpirationCheckInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (string document in documents)
            {
                Logger.Debug($"Removing outdated records from the '{document}' document.");

                using (new AzureDocumentDbDistributedLock(distributedLockKey, defaultLockTimeout, storage))
                {
                    string responseContinuation = null;
                    Uri collectionUri = UriFactory.CreateDocumentCollectionUri(storage.Options.DatabaseName, document);

                    do
                    {
                        FeedOptions QueryOptions = new FeedOptions { MaxItemCount = 50, RequestContinuation = responseContinuation };
                        IDocumentQuery<DocumentEntity> query = storage.Client.CreateDocumentQuery<DocumentEntity>(collectionUri, QueryOptions)
                            .AsDocumentQuery();

                        if (query.HasMoreResults)
                        {
                            FeedResponse<DocumentEntity> response = query.ExecuteNextAsync<DocumentEntity>(cancellationToken).GetAwaiter().GetResult();
                            responseContinuation = response.ResponseContinuation;

                            List<DocumentEntity> entities = response
                                .Where(c => c.ExpireOn < DateTime.UtcNow)
                                .Where(entity => document != "counters" || !(entity is Counter) || ((Counter)entity).Type != CounterTypes.Raw).ToList();

                            foreach (DocumentEntity entity in entities)
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                                storage.Client.DeleteDocumentAsync(entity.SelfLink).GetAwaiter().GetResult();
                            }
                        }

                    } while (!string.IsNullOrEmpty(responseContinuation));

                }

                Logger.Trace($"Outdated records removed from the '{document}' document.");
                cancellationToken.WaitHandle.WaitOne(checkInterval);
            }
        }
    }
}