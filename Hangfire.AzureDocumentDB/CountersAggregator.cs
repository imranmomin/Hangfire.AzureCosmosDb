using System;
using System.Net;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using Hangfire.Server;
using Hangfire.Logging;
using Hangfire.Azure.Documents;
using Microsoft.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Microsoft.Azure.Documents.Client;

namespace Hangfire.Azure
{
#pragma warning disable 618
    internal class CountersAggregator : IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog logger = LogProvider.For<CountersAggregator>();
        private const string DISTRIBUTED_LOCK_KEY = "countersaggragator";
        private static readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(5);
        private readonly TimeSpan checkInterval;
        private readonly DocumentDbStorage storage;
        private readonly FeedOptions queryOptions = new FeedOptions { MaxItemCount = 1000 };
        private readonly Uri spDeleteDocumentIfExistsUri;

        public CountersAggregator(DocumentDbStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            checkInterval = storage.Options.CountersAggregateInterval;
            spDeleteDocumentIfExistsUri = UriFactory.CreateStoredProcedureUri(storage.Options.DatabaseName, storage.Options.CollectionName, "deleteDocumentIfExists");
        }

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
                    .ToDictionary(k => k.Key, v=> (Sum: v.Sum(c => c.Value), ExpireOn: v.Max(c => c.ExpireOn)));

                Array.ForEach(counters.Keys.ToArray(), key =>
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (counters.TryGetValue(key, out var data))
                    {
                        Counter aggregated = storage.Client.CreateDocumentQuery<Counter>(storage.CollectionUri, queryOptions)
                             .Where(c => c.Key == key && c.Type == CounterTypes.Aggregrate && c.DocumentType == DocumentTypes.Counter)
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

                        Task<ResourceResponse<Document>> task = storage.Client.UpsertDocumentWithRetriesAsync(storage.CollectionUri, aggregated);

                        Task continueTask = task.ContinueWith(t =>
                        {
                            if (t.Result.StatusCode == HttpStatusCode.Created || t.Result.StatusCode == HttpStatusCode.OK)
                            {
                                List<string> deleteCountersr = rawCounters.Where(c => c.Key == key).Select(c => c.Id).ToList();
                                Task<StoredProcedureResponse<bool>> procedureTask = storage.Client.ExecuteStoredProcedureAsync<bool>(spDeleteDocumentIfExistsUri, deleteCountersr);
                                procedureTask.Wait(cancellationToken);
                            }
                        }, cancellationToken, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Current);

                        continueTask.Wait(cancellationToken);
                    }
                });
            }

            logger.Trace("Records from the 'Counter' table aggregated.");
            cancellationToken.WaitHandle.WaitOne(checkInterval);
        }
        
        public override string ToString() => GetType().ToString();

    }
}
