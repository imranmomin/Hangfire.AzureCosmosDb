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

namespace Hangfire.Azure.Queue
{
    internal class JobQueue : IPersistentJobQueue
    {
        private readonly ILog logger = LogProvider.For<JobQueue>();
        private readonly CosmosDbStorage storage;
        private const string DISTRIBUTED_LOCK_KEY = "locks:job:dequeue";
        private readonly TimeSpan defaultLockTimeout;
        private readonly TimeSpan invisibilityTimeout = TimeSpan.FromMinutes(15);
        private readonly object syncLock = new object();
        private readonly PartitionKey partitionKey = new PartitionKey((int)DocumentTypes.Queue);

        public JobQueue(CosmosDbStorage storage)
        {
            this.storage = storage;
            defaultLockTimeout = TimeSpan.FromSeconds(30) + storage.StorageOptions.QueuePollInterval;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            lock (syncLock)
            {
                IEnumerable<string> queueParams = Enumerable.Range(0, queues.Length).Select((q, i) => $"@queue_{i}");
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
                    cancellationToken.ThrowIfCancellationRequested();
                    logger.Trace("Looking for any jobs from the queue");

                    using (new CosmosDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
                    {
                        int invisibilityTimeoutEpoch = DateTime.UtcNow.Add(invisibilityTimeout.Negate()).ToEpoch();
                        sql.WithParameter("@timeout", invisibilityTimeoutEpoch);

                        Documents.Queue data = storage.Container.GetItemQueryIterator<Documents.Queue>(sql, requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
                            .ToQueryResult()
                            .FirstOrDefault();

                        if (data != null)
                        {
                            // mark the document
                            data.FetchedAt = DateTime.UtcNow;
                            Task<ItemResponse<Documents.Queue>> task = storage.Container.ReplaceItemWithRetriesAsync(data, data.Id, partitionKey, cancellationToken: cancellationToken);
                            task.Wait(cancellationToken);

                            logger.Trace($"Found job {data.JobId} from the queue : {data.Name}");
                            return new FetchedJob(storage, data);
                        }
                    }

                    logger.Trace($"Unable to find any jobs in the queue. Will check the queue for jobs in {storage.StorageOptions.QueuePollInterval.TotalSeconds} seconds");
                    cancellationToken.WaitHandle.WaitOne(storage.StorageOptions.QueuePollInterval);
                }
            }
        }

        public void Enqueue(string queue, string jobId)
        {
            Documents.Queue data = new Documents.Queue
            {
                Name = queue,
                JobId = jobId,
                CreatedOn = DateTime.UtcNow
            };

            Task<ItemResponse<Documents.Queue>> task = storage.Container.CreateItemWithRetriesAsync(data, partitionKey);
            task.Wait();
        }
    }
}