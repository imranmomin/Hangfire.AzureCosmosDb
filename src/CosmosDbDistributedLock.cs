using System;
using System.Net;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure
{
    public class CosmosDbDistributedLock : IDisposable
    {
        private readonly ILog logger = LogProvider.For<CosmosDbDistributedLock>();
        private readonly string resource;
        private readonly CosmosDbStorage storage;
        private readonly PartitionKey partitionKey = new((int)DocumentTypes.Lock);
        private string? resourceId;

        public CosmosDbDistributedLock(string resource, TimeSpan timeout, CosmosDbStorage storage)
        {
            this.resource = resource;
            this.storage = storage;
            Acquire(timeout);
        }

        public void Dispose()
        {
            if (resourceId != null)
            {
                try
                {
                    Task task = storage.Container.DeleteItemWithRetriesAsync<Lock>(resourceId, partitionKey).ContinueWith(_ =>
                    {
                        resourceId = null;
                        logger.Trace($"Lock released for {resource}");
                    });
                    task.Wait();
                }
                catch (Exception exception)
                {
                    logger.ErrorException($"Unable to release the lock for {resource}", exception);
                }
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
              
                try
                {
                    Task<ItemResponse<Lock>> readTask = storage.Container.ReadItemWithRetriesAsync<Lock>(id, partitionKey);
                    readTask.Wait();

                    if (readTask.Result.Resource != null)
                    {
                        // TODO: check if the item has already expired.
                        // If it safe to remove it here or have a some type of counter
                        logger.Trace($"Unable to acquire lock for {resource}. It already has an active lock on for it.");
                    }
                }
                catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
                {
                    // default ttl for lock document
                    // this is if the expiration manager was not able to remove the orphan lock in time.
                    TimeSpan ttl = timeout.Add(TimeSpan.FromSeconds(15));

                    Lock @lock = new Lock
                    {
                        Id = id,
                        Name = resource,
                        ExpireOn = DateTime.UtcNow.Add(timeout),
                        TimeToLive = (int)ttl.TotalSeconds
                    };

                    Task<ItemResponse<Lock>> createTask = storage.Container.UpsertItemWithRetriesAsync(@lock, partitionKey);
                    createTask.Wait();

                    if (createTask.Result.StatusCode is HttpStatusCode.OK or HttpStatusCode.Created)
                    {
                        resourceId = id;
                        break;
                    }
                }

                // check the timeout
                if (acquireStart.ElapsedMilliseconds > timeout.TotalMilliseconds)
                {
                    throw new CosmosDbDistributedLockException($"Could not place a lock on the resource [{resource}]: Lock timeout reached [{timeout.TotalSeconds}] seconds.", resource);
                }

                // sleep for 2000 millisecond
                logger.Trace($"Unable to acquire lock for [{resource}]. Will check try after [2] seconds");
                System.Threading.Thread.Sleep(2000);
            }

            logger.Trace($"Acquired lock for [{resource}] in [{acquireStart.Elapsed.TotalMilliseconds:#.##}] ms");
        }
    }
}