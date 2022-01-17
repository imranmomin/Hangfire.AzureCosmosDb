using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Hangfire.Storage;
using Microsoft.Azure.Cosmos;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Queue
{
    public class FetchedJob : IFetchedJob
    {
        private readonly ILog logger = LogProvider.GetLogger(typeof(FetchedJob));
        private readonly object syncRoot = new();
        private readonly Timer timer;
        private readonly CosmosDbStorage storage;
        private Documents.Queue data;
        private bool disposed;
        private bool removedFromQueue;
        private bool reQueued;
        private readonly PartitionKey partitionKey = new((int)DocumentTypes.Queue);

        public FetchedJob(CosmosDbStorage storage, Documents.Queue data)
        {
            this.storage = storage;
            this.data = data;

            TimeSpan keepAliveInterval = TimeSpan.FromSeconds(15);
            timer = new Timer(KeepAliveJobCallback, data, keepAliveInterval, keepAliveInterval);
        }

        public string JobId => data.JobId;

        public string Queue => data.Name;

        public DateTime? FetchedAt => data.FetchedAt;

        public void Dispose()
        {
            if (disposed) return;
            disposed = true;

            timer.Dispose();

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
                    ItemRequestOptions requestOptions = new ItemRequestOptions { IfMatchEtag = data.ETag };
                    Task<ItemResponse<Documents.Queue>> task = storage.Container.DeleteItemWithRetriesAsync<Documents.Queue>(data.Id, partitionKey, requestOptions);
                    task.Wait();
                }
                catch (Exception exception)
                {
                    logger.ErrorException($"Unable to remove the job {JobId} from the queue {data.Name}", exception);
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
                try
                {
                    PatchOperation[] patchOperations =
                    {
                        PatchOperation.Remove("/fetched_at"),
                        PatchOperation.Set("/created_on", DateTime.UtcNow.ToEpoch())
                    };
                    PatchItemRequestOptions patchItemRequestOptions = new PatchItemRequestOptions { IfMatchEtag = data.ETag };

                    Task<ItemResponse<Documents.Queue>> task = storage.Container.PatchItemWithRetriesAsync<Documents.Queue>(data.Id, partitionKey, patchOperations, patchItemRequestOptions);
                    task.Wait();

                    data = task.Result;
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    /* ignore */
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

                    PatchOperation[] patchOperations = { PatchOperation.Set("/fetched_at", DateTime.UtcNow.ToEpoch()) };
                    PatchItemRequestOptions patchItemRequestOptions = new PatchItemRequestOptions { IfMatchEtag = data.ETag };

                    Task<ItemResponse<Documents.Queue>> task = storage.Container.PatchItemWithRetriesAsync<Documents.Queue>(queue.Id, partitionKey, patchOperations, patchItemRequestOptions);
                    task.Wait();

                    data = task.Result;

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