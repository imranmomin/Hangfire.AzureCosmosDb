using System.Linq;
using System.Collections.Generic;

using Microsoft.Azure.Documents.Client;

namespace Hangfire.AzureDocumentDB.Queue
{
    internal class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly AzureDocumentDbStorage storage;
        private readonly IEnumerable<string> queues;
        private readonly FeedOptions QueryOptions = new FeedOptions { MaxItemCount = 100 };

        public JobQueueMonitoringApi(AzureDocumentDbStorage storage)
        {
            this.storage = storage;
            queues = storage.Options.Queues;
        }

        public IEnumerable<string> GetQueues() => queues;

        public int GetEnqueuedCount(string queue)
        {
            return storage.Client.CreateDocumentQuery<Entities.Queue>(storage.Collections.QueueDocumentCollectionUri, QueryOptions)
                .Where(q => q.Name == queue && q.DocumentType == Entities.DocumentTypes.Queue)
                .AsEnumerable()
                .Select(q => 1)
                .Count();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return storage.Client.CreateDocumentQuery<Entities.Queue>(storage.Collections.QueueDocumentCollectionUri, QueryOptions)
                 .Where(q => q.Name == queue && q.DocumentType == Entities.DocumentTypes.Queue)
                 .AsEnumerable()
                 .Skip(from).Take(perPage)
                 .Select(c => c.JobId)
                 .ToList();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage) => GetEnqueuedJobIds(queue, from, perPage);

    }
}