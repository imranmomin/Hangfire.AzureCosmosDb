using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Hangfire.Storage;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure.Queue;

public class JobQueue : IPersistentJobQueue
{
    private readonly ILog logger = LogProvider.For<JobQueue>();
    private readonly CosmosDbStorage storage;
    private const string DISTRIBUTED_LOCK_KEY = "locks:job:dequeue";
    private readonly TimeSpan defaultLockTimeout;
    private readonly TimeSpan invisibilityTimeout = TimeSpan.FromMinutes(15);
    private readonly object syncLock = new();
    private readonly PartitionKey partitionKey = new((int)DocumentTypes.Queue);

    public JobQueue(CosmosDbStorage storage)
    {
        this.storage = storage;
        defaultLockTimeout = TimeSpan.FromSeconds(30).Add(storage.StorageOptions.QueuePollInterval);
    }

    public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
    {
        if (queues == null) throw new ArgumentNullException(nameof(queues));
        if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

        lock (syncLock)
        {
            IEnumerable<string> queueParams = Enumerable.Range(0, queues.Length).Select((_, i) => $"@queue_{i}");
            string query = $"SELECT TOP 1 * FROM doc WHERE doc.type = @type AND doc.name IN ({string.Join(", ", queueParams)}) " +
                           "AND (NOT IS_DEFINED(doc.fetched_at) OR doc.fetched_at < @timeout) ORDER BY doc.name ASC, doc.created_on ASC";

            QueryDefinition sql = new QueryDefinition(query)
                .WithParameter("@type", (int)DocumentTypes.Queue);

            for (int index = 0; index < queues.Length; index++)
            {
                string queue = queues[index];
                sql.WithParameter($"@queue_{index}", queue);
            }

            while (true)
            {
                CosmosDbDistributedLock? distributedLock = null;
                cancellationToken.ThrowIfCancellationRequested();
                logger.Trace("Looking for any jobs from the queue");

                try
                {
                    distributedLock = new CosmosDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage);

                    int invisibilityTimeoutEpoch = DateTime.UtcNow.Add(invisibilityTimeout.Negate()).ToEpoch();
                    sql.WithParameter("@timeout", invisibilityTimeoutEpoch);

                    QueryRequestOptions queryRequestOptions = new QueryRequestOptions { PartitionKey = partitionKey, MaxItemCount = 1 };
                    Documents.Queue? data = storage.Container.GetItemQueryIterator<Documents.Queue>(sql, requestOptions: queryRequestOptions)
                        .ToQueryResult()
                        .FirstOrDefault();

                    if (data != null)
                    {
                        // mark the document that it was fetched
                        PatchOperation[] patchOperations = { PatchOperation.Set("/fetched_at", DateTime.UtcNow.ToEpoch()) };
                        PatchItemRequestOptions patchItemRequestOptions = new PatchItemRequestOptions { IfMatchEtag = data.ETag };

                        Task<ItemResponse<Documents.Queue>> task = storage.Container.PatchItemWithRetriesAsync<Documents.Queue>(data.Id, partitionKey, patchOperations, patchItemRequestOptions, cancellationToken);
                        task.Wait(cancellationToken);

                        data = task.Result;

                        logger.Trace($"Found job [{data.JobId}] from the queue : [{data.Name}]");
                        return new FetchedJob(storage, data);
                    }
                }
                catch (CosmosDbDistributedLockException exception) when (exception.Key == DISTRIBUTED_LOCK_KEY)
                {
                    logger.Debug($@"An exception was thrown during acquiring distributed lock on the [{DISTRIBUTED_LOCK_KEY}] resource within [{defaultLockTimeout.TotalSeconds}] seconds." +
                                 $@"Outdated records were not removed. It will be retried in [{storage.StorageOptions.QueuePollInterval.TotalSeconds}] seconds.");
                }
                finally
                {
                    distributedLock?.Dispose();
                }

                logger.Trace($"Unable to find any jobs in the queue. Will check the queue for jobs in [{storage.StorageOptions.QueuePollInterval.TotalSeconds}] seconds");
                cancellationToken.WaitHandle.WaitOne(storage.StorageOptions.QueuePollInterval);
            }
        }
    }

    public void Enqueue(string queue, string jobId) => Enqueue(queue, jobId, DateTime.UtcNow);

    public void Enqueue(string queue, string jobId, DateTime createdOn)
    {
        Documents.Queue data = new Documents.Queue
        {
            Name = queue,
            JobId = jobId,
            CreatedOn = createdOn
        };

        Task<ItemResponse<Documents.Queue>> task = storage.Container.CreateItemWithRetriesAsync(data, partitionKey);
        task.Wait();
    }
}