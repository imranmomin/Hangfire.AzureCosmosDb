using System;
using System.Net;
using System.Threading.Tasks;

using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Logging;

using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure
{
    internal class CosmosDbDistributedLock : IDisposable
    {
        private readonly ILog logger = LogProvider.For<CosmosDbDistributedLock>();
        private readonly string resource;
        private readonly CosmosDbStorage storage;
        private readonly PartitionKey partitionKey = new PartitionKey((int)DocumentTypes.Lock);
        private string resourceId;

        public CosmosDbDistributedLock(string resource, TimeSpan timeout, CosmosDbStorage storage)
        {
            this.resource = resource;
            this.storage = storage;
            Acquire(timeout);
        }

        public void Dispose()
        {
            if (!string.IsNullOrEmpty(resourceId))
            {
                Task task = storage.Container.DeleteItemWithRetriesAsync<Lock>(resourceId, partitionKey).ContinueWith(t =>
                {
                    resourceId = string.Empty;
                    logger.Trace($"Lock released for {resource}");
                });
                task.Wait();
            }
        }

        private void Acquire(TimeSpan timeout)
        {
            logger.Trace($"Trying to acquire lock for {resource} within {timeout.TotalSeconds} seconds");

            System.Diagnostics.Stopwatch acquireStart = new System.Diagnostics.Stopwatch();
            acquireStart.Start();

            string id = $"{resource}:{DocumentTypes.Lock}".GenerateHash();

            while (string.IsNullOrEmpty(resourceId))
            {
                // default ttl for lock document
                TimeSpan ttl = DateTime.UtcNow.Add(timeout).AddMinutes(1).TimeOfDay;

                try
                {
                    Task<ItemResponse<Lock>> readTask = storage.Container.ReadItemWithRetriesAsync<Lock>(id, partitionKey);
                    readTask.Wait();

                    if (readTask.Result.Resource != null)
                    {
                        Lock @lock = readTask.Result.Resource;
                        @lock.ExpireOn = DateTime.UtcNow.Add(timeout);
                        @lock.TimeToLive = (int)ttl.TotalSeconds;

                        Task<ItemResponse<Lock>> updateTask = storage.Container.UpsertItemWithRetriesAsync(@lock, partitionKey);
                        updateTask.Wait();

                        if (updateTask.Result.StatusCode == HttpStatusCode.OK)
                        {
                            resourceId = id;
                            break;
                        }
                    }
                }
                catch (AggregateException ex) when (ex.InnerException is CosmosException exception && exception.StatusCode == HttpStatusCode.NotFound)
                {
                    Lock @lock = new Lock
                    {
                        Id = id,
                        Name = resource,
                        ExpireOn = DateTime.UtcNow.Add(timeout),
                        TimeToLive = (int)ttl.TotalSeconds
                    };

                    Task<ItemResponse<Lock>> createTask = storage.Container.UpsertItemWithRetriesAsync(@lock, partitionKey);
                    createTask.Wait();

                    if (createTask.Result.StatusCode == HttpStatusCode.OK || createTask.Result.StatusCode == HttpStatusCode.Created)
                    {
                        resourceId = id;
                        break;
                    }
                }

                // check the timeout
                if (acquireStart.ElapsedMilliseconds > timeout.TotalMilliseconds)
                {
                    throw new CosmosDbDistributedLockException($"Could not place a lock on the resource '{resource}': Lock timeout.");
                }

                // sleep for 2000 millisecond
                logger.Trace($"Unable to acquire lock for {resource}. Will check try after 2 seconds");
                System.Threading.Thread.Sleep(2000);
            }

            logger.Trace($"Acquired lock for {resource} in {acquireStart.Elapsed.TotalMilliseconds:#.##} ms");
        }

    }
}