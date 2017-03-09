using System;
using System.Net;
using System.Linq;
using System.Threading;
using System.Collections.Generic;

using Hangfire.Logging;
using Hangfire.Server;
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
        private readonly AzureDocumentDbConnection connection;
        private readonly TimeSpan checkInterval;

        public CountersAggregator(AzureDocumentDbStorage storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            connection = (AzureDocumentDbConnection)storage.GetConnection();
            checkInterval = storage.Options.CountersAggregateInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            Logger.Debug("Aggregating records in 'Counter' table.");

            using (new AzureDocumentDbDistributedLock(distributedLockKey, defaultLockTimeout, connection.Client))
            {
                FirebaseResponse response = connection.Client.Get("counters/raw");
                if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
                {
                    Dictionary<string, Dictionary<string, Counter>> counters = response.ResultAs<Dictionary<string, Dictionary<string, Counter>>>();
                    Array.ForEach(counters.Keys.ToArray(), key =>
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        Dictionary<string, Counter> data;
                        if (counters.TryGetValue(key, out data))
                        {
                            data = data.ToDictionary(k => k.Key, v => v.Value);

                            int value = data.Sum(c => c.Value.Value);
                            DateTime? expireOn = data.Max(c => c.Value.ExpireOn);
                            Counter aggregated = new Counter
                            {
                                Value = value,
                                ExpireOn = expireOn
                            };

                            FirebaseResponse counterResponse = connection.Client.Get($"counters/aggregrated/{key}");
                            if (counterResponse.StatusCode == HttpStatusCode.OK && !counterResponse.IsNull())
                            {
                                Counter counter = counterResponse.ResultAs<Counter>();
                                if (counter != null)
                                {
                                    aggregated.Value += counter.Value;
                                }
                            }

                            // update the aggregrated counter
                            FirebaseResponse aggResponse = connection.Client.Set($"counters/aggregrated/{key}", aggregated);
                            if (aggResponse.StatusCode == HttpStatusCode.OK)
                            {
                                // delete all the counter references for the key
                                Array.ForEach(data.Keys.ToArray(), reference => connection.Client.Delete($"counters/raw/{key}/{reference}"));
                            }
                        }
                    });
                }
            }

            Logger.Trace("Records from the 'Counter' table aggregated.");
            cancellationToken.WaitHandle.WaitOne(checkInterval);
        }

        public override string ToString() => GetType().ToString();

    }
}
