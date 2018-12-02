using System;
using System.Threading;
using System.Threading.Tasks;

using Hangfire.Logging;
using Hangfire.Storage;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Queue
{
    internal class FetchedJob : IFetchedJob
    {
        private readonly ILog logger = LogProvider.GetLogger(typeof(FetchedJob));
        private readonly object syncRoot = new object();
        private readonly Timer timer;
        private readonly DocumentDbStorage storage;
        private readonly Documents.Queue data;
        private bool disposed;
        private bool removedFromQueue;
        private bool reQueued;

        public FetchedJob(DocumentDbStorage storage, Documents.Queue data)
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
                Task<ResourceResponse<Document>> task = storage.Client.DeleteDocumentAsync(data.SelfLink);
                task.Wait();
                removedFromQueue = true;
            }
        }

        public void Requeue()
        {
            lock (syncRoot)
            {
                data.CreatedOn = DateTime.UtcNow;
                data.FetchedAt = null;

                Task<ResourceResponse<Document>> task = storage.Client.ReplaceDocumentAsync(data.SelfLink, data);
                task.Wait();
                reQueued = true;
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
                    Task<ResourceResponse<Document>> task = storage.Client.ReplaceDocumentAsync(queue.SelfLink, queue);
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
