﻿using System;
using System.Net;
using System.Linq;
using System.Threading;
using System.Collections.Generic;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Hangfire.Server;
using Hangfire.Logging;
using Hangfire.AzureDocumentDB.Entities;

namespace Hangfire.AzureDocumentDB
{
#pragma warning disable 618
    internal class CountersAggregator : IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.For<CountersAggregator>();
        private const string distributedLockKey = "countersaggragator";
        private static readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(5);
        private readonly TimeSpan checkInterval;

        private readonly AzureDocumentDbStorage storage;

        private readonly FeedOptions QueryOptions = new FeedOptions { MaxItemCount = 1000 };
        private readonly Uri CounterDocumentCollectionUri;

        public CountersAggregator(AzureDocumentDbStorage storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            this.storage = storage;
            checkInterval = storage.Options.CountersAggregateInterval;
            CounterDocumentCollectionUri = UriFactory.CreateDocumentCollectionUri(storage.Options.DatabaseName, "counters");
        }

        public void Execute(CancellationToken cancellationToken)
        {
            Logger.Debug("Aggregating records in 'Counter' table.");

            using (new AzureDocumentDbDistributedLock(distributedLockKey, defaultLockTimeout, storage))
            {
                List<Counter> rawCounters = storage.Client.CreateDocumentQuery<Counter>(CounterDocumentCollectionUri, QueryOptions)
                    .Where(c => c.Type == CounterTypes.Raw)
                    .AsEnumerable()
                    .ToList();

                Dictionary<string, Tuple<int, DateTime?>> counters = rawCounters.GroupBy(c => c.Key)
                    .ToDictionary(k => k.Key, v => new Tuple<int, DateTime?>(v.Sum(c => c.Value), v.Max(c => c.ExpireOn)));

                Array.ForEach(counters.Keys.ToArray(), key =>
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    Tuple<int, DateTime?> data;
                    if (counters.TryGetValue(key, out data))
                    {
                        Counter aggregated = storage.Client.CreateDocumentQuery<Counter>(CounterDocumentCollectionUri, QueryOptions)
                             .Where(c => c.Key == key && c.Type == CounterTypes.Aggregrate)
                             .AsEnumerable()
                             .FirstOrDefault();

                        if (aggregated == null)
                        {
                            aggregated = new Counter
                            {
                                Key = key,
                                Type = CounterTypes.Aggregrate,
                                Value = data.Item1,
                                ExpireOn = data.Item2
                            };
                        }
                        else
                        {
                            aggregated.Value += data.Item1;
                            aggregated.ExpireOn = data.Item2;
                        }

                        ResourceResponse<Document> response = storage.Client.UpsertDocumentAsync(CounterDocumentCollectionUri, aggregated).GetAwaiter().GetResult();
                        if (response.StatusCode == HttpStatusCode.Accepted)
                        {
                            List<Counter> deleteCountersr = rawCounters.Where(c => c.Key == key).ToList();
                            deleteCountersr.ForEach(counter => storage.Client.DeleteDocumentAsync(counter.SelfLink).GetAwaiter().GetResult());
                        }
                    }
                });
            }

            Logger.Trace("Records from the 'Counter' table aggregated.");
            cancellationToken.WaitHandle.WaitOne(checkInterval);
        }

        public override string ToString() => GetType().ToString();

    }
}
