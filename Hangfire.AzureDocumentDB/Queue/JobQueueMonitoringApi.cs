using System;
using System.Net;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Azure.Documents.Client;

namespace Hangfire.AzureDocumentDB.Queue
{
    internal class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly AzureDocumentDbStorage storage;
        private readonly IEnumerable<string> queues;
        private readonly Uri QueueDocumentCollectionUri;
        private readonly FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };

        public JobQueueMonitoringApi(AzureDocumentDbStorage storage)
        {
            this.storage = storage;
            queues = storage.Options.Queues;
            QueueDocumentCollectionUri = UriFactory.CreateDocumentCollectionUri(storage.Options.DatabaseName, "queues");
        }

        public IEnumerable<string> GetQueues() => queues;

        public int GetEnqueuedCount(string queue)
        {
            return storage.Client.CreateDocumentQuery<Entities.Queue>(QueueDocumentCollectionUri, QueryOptions)
                .Where(q => q.Name == queue)
                .AsEnumerable()
                .Count();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
           return storage.Client.CreateDocumentQuery<Entities.Queue>(QueueDocumentCollectionUri, QueryOptions)
                .Where(q => q.Name == queue)
                .Skip(from).Take(perPage)
                .Select(c => c.JobId)
                .ToList();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage) => GetEnqueuedJobIds(queue, from, perPage);

    }
}