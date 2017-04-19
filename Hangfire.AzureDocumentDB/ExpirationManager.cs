using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Documents.Client;

using Hangfire.Logging;
using Hangfire.Server;
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
                        FeedOptions QueryOptions = new FeedOptions { MaxItemCount = 100, RequestContinuation = responseContinuation };
                        IDocumentQuery<FireEntity> query = storage.Client.CreateDocumentQuery<FireEntity>(collectionUri, QueryOptions)
                            .Where(c => c.ExpireOn.HasValue && c.ExpireOn < DateTime.UtcNow)
                            .AsDocumentQuery();

                        if (query.HasMoreResults)
                        {
                            Task<FeedResponse<FireEntity>> task = query.ExecuteNextAsync<FireEntity>(cancellationToken);
                            responseContinuation = task.Result.ResponseContinuation;

                            List<FireEntity> entities = task.Result.Where(entity => document != "counters" || !(entity is Counter) || (entity as Counter).Type != CounterTypes.Raw).ToList();

                            foreach (FireEntity entity in entities)
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                                storage.Client.DeleteDocumentAsync(entity.SelfLink);
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