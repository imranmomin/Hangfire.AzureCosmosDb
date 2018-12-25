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
        private const string DISTRIBUTED_LOCK_KEY = "locks:counters:aggragator";
        private readonly TimeSpan defaultLockTimeout;
        private readonly DocumentDbStorage storage;
        private readonly Uri spDeleteDocumentsUri;

        public CountersAggregator(DocumentDbStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            defaultLockTimeout = TimeSpan.FromSeconds(30) + storage.Options.CountersAggregateInterval;
            spDeleteDocumentsUri = UriFactory.CreateStoredProcedureUri(storage.Options.DatabaseName, storage.Options.CollectionName, "deleteDocuments");
        }

        public void Execute(CancellationToken cancellationToken)
        {
            using (new DocumentDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
            {
                logger.Trace("Aggregating records in 'Counter' table.");

                List<Counter> rawCounters = storage.Client.CreateDocumentQuery<Counter>(storage.CollectionUri)
                    .Where(c => c.DocumentType == DocumentTypes.Counter && c.Type == CounterTypes.Raw)
                    .ToQueryResult();

                Dictionary<string, (int Value, DateTime? ExpireOn, List<Counter> Counters)> counters = rawCounters.GroupBy(c => c.Key)
                    .ToDictionary(k => k.Key, v => (Value: v.Sum(c => c.Value), ExpireOn: v.Max(c => c.ExpireOn), Counters: v.ToList()));

                foreach (string key in counters.Keys)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (counters.TryGetValue(key, out var data))
                    {
                        Counter aggregated;
                        string id = $"{key}:{CounterTypes.Aggregate}".GenerateHash();
                        Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, id);

                        try
                        {
                            Task<DocumentResponse<Counter>> readTask = storage.Client.ReadDocumentWithRetriesAsync<Counter>(uri, cancellationToken: cancellationToken);
                            readTask.Wait(cancellationToken);

                            if (readTask.Result.StatusCode == HttpStatusCode.OK)
                            {
                                aggregated = readTask.Result;
                                aggregated.Value += data.Value;
                                aggregated.ExpireOn = new[] { aggregated.ExpireOn, data.ExpireOn }.Max();
                            }
                            else
                            {
                                logger.Warn($"Document with ID: {id} is a {readTask.Result.Document.Type.ToString()} type which could not be aggregated");
                                continue;
                            }
                        }
                        catch (AggregateException ex) when (ex.InnerException is DocumentClientException clientException)
                        {
                            if (clientException.StatusCode == HttpStatusCode.NotFound)
                            {
                                aggregated = new Counter
                                {
                                    Id = id,
                                    Key = key,
                                    Type = CounterTypes.Aggregate,
                                    Value = data.Value,
                                    ExpireOn = data.ExpireOn
                                };
                            }
                            else
                            {
                                logger.ErrorException("Error while reading document", ex.InnerException);
                                continue;
                            }
                        }

                        Task<ResourceResponse<Document>> task = storage.Client.UpsertDocumentWithRetriesAsync(storage.CollectionUri, aggregated, cancellationToken: cancellationToken);
                        Task continueTask = task.ContinueWith(t =>
                        {
                            if (t.Result.StatusCode == HttpStatusCode.Created || t.Result.StatusCode == HttpStatusCode.OK)
                            {
                                int deleted = 0;
                                ProcedureResponse response;
                                string ids = string.Join(",", data.Counters.Select(c => $"'{c.Id}'").ToArray());
                                string sql = $"SELECT doc._self FROM doc WHERE doc.type = {(int)DocumentTypes.Counter} AND doc.counter_type = {(int)CounterTypes.Raw} AND doc.id IN ({ids})";

                                do
                                {
                                    Task<StoredProcedureResponse<ProcedureResponse>> procedureTask = storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spDeleteDocumentsUri, sql);
                                    procedureTask.Wait(cancellationToken);

                                    response = procedureTask.Result;
                                    deleted += response.Affected;

                                } while (response.Continuation); // if the continuation is true; run the procedure again


                                logger.Trace($"Total {deleted} records from the 'Counter:{aggregated.Key}' were aggregated.");
                            }
                        }, cancellationToken, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Current);

                        continueTask.Wait(cancellationToken);
                    }
                }

                logger.Trace("Records from the 'Counter' table aggregated.");
            }

            cancellationToken.WaitHandle.WaitOne(storage.Options.CountersAggregateInterval);
        }

        public override string ToString() => GetType().ToString();

    }
}
