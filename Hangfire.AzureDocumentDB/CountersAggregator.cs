using System;
using System.Net;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using Hangfire.Server;
using Hangfire.Logging;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Hangfire.Azure.Helper;
using Hangfire.Azure.Documents;

namespace Hangfire.Azure
{
#pragma warning disable 618
    internal class CountersAggregator : IServerComponent
#pragma warning restore 618
    {
        private readonly ILog logger = LogProvider.For<CountersAggregator>();
        private const string DISTRIBUTED_LOCK_KEY = "locks:countersaggragator";
        private readonly TimeSpan defaultLockTimeout;
        private readonly DocumentDbStorage storage;
        private readonly FeedOptions queryOptions = new FeedOptions { MaxItemCount = 1000 };

        public CountersAggregator(DocumentDbStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            defaultLockTimeout = TimeSpan.FromSeconds(30) + storage.Options.CountersAggregateInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            using (new DocumentDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
            {
                logger.Trace("Aggregating records in 'Counter' table.");

                List<Counter> rawCounters = storage.Client.CreateDocumentQuery<Counter>(storage.CollectionUri, queryOptions)
                    .Where(c => c.Type == CounterTypes.Raw && c.DocumentType == DocumentTypes.Counter)
                    .AsEnumerable()
                    .ToList();

                Dictionary<string, (int Sum, DateTime? ExpireOn)> counters = rawCounters.GroupBy(c => c.Key)
                    .ToDictionary(k => k.Key, v => (Sum: v.Sum(c => c.Value), ExpireOn: v.Max(c => c.ExpireOn)));

                foreach (string key in counters.Keys)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (counters.TryGetValue(key, out var data))
                    {
                        string id = key.GenerateHash();
                        Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, id);
                        Task<DocumentResponse<Counter>> readTask = storage.Client.ReadDocumentAsync<Counter>(uri, cancellationToken: cancellationToken);
                        readTask.Wait(cancellationToken);

                        Counter aggregated;
                        if (readTask.Result.StatusCode != HttpStatusCode.OK)
                        {
                            aggregated = new Counter
                            {
                                Id = id,
                                Key = key,
                                Type = CounterTypes.Aggregrate,
                                Value = data.Sum,
                                ExpireOn = data.ExpireOn
                            };
                        }
                        else if (readTask.Result.StatusCode == HttpStatusCode.OK && readTask.Result.Document.Type == CounterTypes.Aggregrate)
                        {
                            aggregated = readTask.Result;
                            aggregated.Value += data.Sum;
                            aggregated.ExpireOn = data.ExpireOn;
                        }
                        else
                        {
                            logger.Warn($"Document with ID: {id} is a {readTask.Result.Document.Type.ToString()} type which could not be aggregated");
                            continue;
                        }

                        Task<ResourceResponse<Document>> task = storage.Client.UpsertDocumentAsync(storage.CollectionUri, aggregated, cancellationToken: cancellationToken);

                        Task continueTask = task.ContinueWith(t =>
                        {
                            if (t.Result.StatusCode == HttpStatusCode.Created || t.Result.StatusCode == HttpStatusCode.OK)
                            {
                                string[] deleteCounterIds = rawCounters.Where(c => c.Key == key).Select(c => c.Id).ToArray();
                                Array.ForEach(deleteCounterIds, documentId =>
                                {
                                    uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, documentId);
                                    storage.Client.DeleteDocumentAsync(uri, cancellationToken: cancellationToken).Wait(cancellationToken);
                                });

                                logger.Trace($"Total {deleteCounterIds.Length} records from the 'Counter:{aggregated.Key}' were aggregated.");
                            }
                        }, cancellationToken, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Current);

                        continueTask.Wait(cancellationToken);
                    }
                }
            }

            logger.Trace("Records from the 'Counter' table aggregated.");
            cancellationToken.WaitHandle.WaitOne(storage.Options.CountersAggregateInterval);
        }

        public override string ToString() => GetType().ToString();

    }
}
