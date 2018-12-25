using System;
using System.Net;
using System.Threading.Tasks;

using Hangfire.Logging;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Hangfire.Azure.Helper;
using Hangfire.Azure.Documents;

namespace Hangfire.Azure
{
    internal class DocumentDbDistributedLock : IDisposable
    {
        private readonly ILog logger = LogProvider.For<DocumentDbDistributedLock>();
        private readonly string resource;
        private readonly DocumentDbStorage storage;
        private string resourceId;

        public DocumentDbDistributedLock(string resource, TimeSpan timeout, DocumentDbStorage storage)
        {
            this.resource = resource;
            this.storage = storage;
            Acquire(timeout);
        }

        public void Dispose()
        {
            if (!string.IsNullOrEmpty(resourceId))
            {
                Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, resourceId);
                Task task = storage.Client.DeleteDocumentWithRetriesAsync(uri).ContinueWith(t =>
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
            Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, id);

            while (string.IsNullOrEmpty(resourceId))
            {
                // default ttl for lock document
                TimeSpan ttl = DateTime.UtcNow.Add(timeout).AddMinutes(1).TimeOfDay;

                try
                {
                    Task<DocumentResponse<Lock>> readTask = storage.Client.ReadDocumentWithRetriesAsync<Lock>(uri);
                    readTask.Wait();

                    if (readTask.Result.Document != null)
                    {
                        Lock @lock = readTask.Result.Document;
                        @lock.ExpireOn = DateTime.UtcNow.Add(timeout);
                        @lock.TimeToLive = (int)ttl.TotalSeconds;

                        Task<ResourceResponse<Document>> updateTask = storage.Client.UpsertDocumentWithRetriesAsync(storage.CollectionUri, @lock);
                        updateTask.Wait();

                        if (updateTask.Result.StatusCode == HttpStatusCode.OK)
                        {
                            resourceId = id;
                            break;
                        }
                    }
                }
                catch (AggregateException ex) when (ex.InnerException is DocumentClientException clientException && clientException.StatusCode == HttpStatusCode.NotFound)
                {
                    Lock @lock = new Lock
                    {
                        Id = id,
                        Name = resource,
                        ExpireOn = DateTime.UtcNow.Add(timeout),
                        TimeToLive = (int)ttl.TotalSeconds
                    };

                    Task<ResourceResponse<Document>> createTask = storage.Client.UpsertDocumentWithRetriesAsync(storage.CollectionUri, @lock);
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
                    throw new DocumentDbDistributedLockException($"Could not place a lock on the resource '{resource}': Lock timeout.");
                }

                // sleep for 2000 millisecond
                logger.Trace($"Unable to acquire lock for {resource}. Will check try after 2 seconds");
                System.Threading.Thread.Sleep(2000);
            }

            logger.Trace($"Acquired lock for {resource} in {acquireStart.Elapsed.TotalSeconds} seconds");
        }

    }
}