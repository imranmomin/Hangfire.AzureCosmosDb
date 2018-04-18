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

using Hangfire.Azure.Documents;

namespace Hangfire.Azure
{
#pragma warning disable 618
    internal class CountersAggregator : IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog logger = LogProvider.For<CountersAggregator>();
        private const string DISTRIBUTED_LOCK_KEY = "countersaggragator";
        private static readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(2);
        private readonly DocumentDbStorage storage;
        private readonly FeedOptions queryOptions = new FeedOptions { MaxItemCount = 1000 };

        public CountersAggregator(DocumentDbStorage storage) => this.storage = storage ?? throw new ArgumentNullException(nameof(storage));

        public void Execute(CancellationToken cancellationToken)
        {
            logger.Debug("Aggregating records in 'Counter' table.");

            using (new DocumentDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
            {
                List<Counter> rawCounters = storage.Client.CreateDocumentQuery<Counter>(storage.CollectionUri, queryOptions)
                    .Where(c => c.Type == CounterTypes.Raw && c.DocumentType == DocumentTypes.Counter)
                    .AsEnumerable()
                    .ToList();

                Dictionary<string, (int Sum, DateTime? ExpireOn)> counters = rawCounters.GroupBy(c => c.Key)
                    .ToDictionary(k => k.Key, v => (Sum: v.Sum(c => c.Value), ExpireOn: v.Max(c => c.ExpireOn)));

                Array.ForEach(counters.Keys.ToArray(), key =>
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (counters.TryGetValue(key, out var data))
                    {
                        Counter aggregated = storage.Client.CreateDocumentQuery<Counter>(storage.CollectionUri, queryOptions)
                             .Where(c => c.Type == CounterTypes.Aggregrate && c.DocumentType == DocumentTypes.Counter && c.Key == key)
                             .AsEnumerable()
                             .FirstOrDefault();

                        if (aggregated == null)
                        {
                            aggregated = new Counter
                            {
                                Key = key,
                                Type = CounterTypes.Aggregrate,
                                Value = data.Sum,
                                ExpireOn = data.ExpireOn
                            };
                        }
                        else
                        {
                            aggregated.Value += data.Sum;
                            aggregated.ExpireOn = data.ExpireOn;
                        }

                        Task<ResourceResponse<Document>> task = storage.Client.UpsertDocumentAsync(storage.CollectionUri, aggregated);

                        Task continueTask = task.ContinueWith(t =>
                        {
                            if (t.Result.StatusCode == HttpStatusCode.Created || t.Result.StatusCode == HttpStatusCode.OK)
                            {
                                string[] deleteCounterIds = rawCounters.Where(c => c.Key == key).Select(c => c.Id).ToArray();
                                Array.ForEach(deleteCounterIds, id =>
                                {
                                    Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, id);
                                    storage.Client.DeleteDocumentAsync(uri).Wait(cancellationToken);
                                });
                                logger.Trace($"Total {deleteCounterIds.Length} records from the 'Counter:{aggregated.Key}' were aggregated.");
                            }
                        }, cancellationToken, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Current);

                        continueTask.Wait(cancellationToken);
                    }
                });
            }

            logger.Trace("Records from the 'Counter' table aggregated.");
            cancellationToken.WaitHandle.WaitOne(storage.Options.CountersAggregateInterval);
        }

        public override string ToString() => GetType().ToString();

    }
}
