using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Hangfire.Logging;
using Hangfire.Storage;

using Hangfire.Azure.Helper;
using Microsoft.Azure.Cosmos;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Queue
{
    internal class FetchedJob : IFetchedJob
    {
        private readonly ILog logger = LogProvider.GetLogger(typeof(FetchedJob));
        private readonly object syncRoot = new object();
        private readonly Timer timer;
        private readonly CosmosDbStorage storage;
        private readonly Documents.Queue data;
        private bool disposed;
        private bool removedFromQueue;
        private bool reQueued;

        public FetchedJob(CosmosDbStorage storage, Documents.Queue data)
        {
            this.storage = storage;
            this.data = data;

            TimeSpan keepAliveInterval = TimeSpan.FromMinutes(5);
            timer = new Timer(KeepAliveJobCallback, data, keepAliveInterval, keepAliveInterval);
        }

        public string JobId => data.JobId;

        public void Dispose()
        {
            if (disposed) return;
            disposed = true;

            timer?.Dispose();

            lock (syncRoot)
            {
                if (!removedFromQueue && !reQueued)
                {
                    Requeue();
                }
            }
        }

        public void RemoveFromQueue()
        {
            lock (syncRoot)
            {
                try
                {
                    Task<ItemResponse<Documents.Queue>> task = storage.Container.DeleteItemWithRetriesAsync<Documents.Queue>(data.Id, PartitionKey.None);
                    task.Wait();
                }
                finally
                {
                    removedFromQueue = true;
                }
            }

        }

        public void Requeue()
        {
            lock (syncRoot)
            {
                data.CreatedOn = DateTime.UtcNow;
                data.FetchedAt = null;

                try
                {
                    Task<ItemResponse<Documents.Queue>> task = storage.Container.ReplaceItemWithRetriesAsync(data, data.Id);
                    task.Wait();
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    data.Id = Guid.NewGuid().ToString();
                    data.SelfLink = null;

                    Task<ItemResponse<Documents.Queue>> task = storage.Container.CreateItemWithRetriesAsync(data, PartitionKey.None);
                    task.Wait();
                }
                finally
                {
                    reQueued = true;
                }
            }
        }

        private void KeepAliveJobCallback(object obj)
        {
            lock (syncRoot)
            {
                if (reQueued || removedFromQueue) return;

                try
                {
                    Documents.Queue queue = (Documents.Queue)obj;
                    queue.FetchedAt = DateTime.UtcNow;

                    Task<ItemResponse<Documents.Queue>> task = storage.Container.ReplaceItemWithRetriesAsync(queue, queue.Id);
                    task.Wait();

                    logger.Trace($"Keep-alive query for job: {queue.Id} sent");
                }
                catch (Exception ex)
                {
                    logger.DebugException($"Unable to execute keep-alive query for job: {data.Id}", ex);
                }
            }
        }

    }
}
