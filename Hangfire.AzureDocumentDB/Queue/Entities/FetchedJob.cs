using System;
using System.Linq;

using Hangfire.Storage;

using Microsoft.Azure.Documents.Client;

namespace Hangfire.AzureDocumentDB.Queue
{
    internal class FetchedJob : IFetchedJob
    {
        private readonly AzureDocumentDbStorage storage;
        private readonly Uri QueueDocumentCollectionUri;
        private readonly FeedOptions QueryOptions = new FeedOptions { MaxItemCount = 1 };

        public FetchedJob(AzureDocumentDbStorage storage, Entities.Queue data)
        {
            this.storage = storage;
            Id = data.Id;
            JobId = data.JobId;
            Queue = data.Name;
            SelfLink = data.SelfLink;
            QueueDocumentCollectionUri = UriFactory.CreateDocumentCollectionUri(storage.Options.DatabaseName, "queues");
        }

        private string Id { get; }

        private string SelfLink { get; }

        public string JobId { get; }

        private string Queue { get; }

        public void Dispose()
        {
        }

        public void RemoveFromQueue()
        {
           bool exists = storage.Client.CreateDocumentQuery(QueueDocumentCollectionUri, QueryOptions)
                .Where(d => d.Id == Id)
                .AsEnumerable()
                .Any();

            if (exists) storage.Client.DeleteDocumentAsync(SelfLink).GetAwaiter().GetResult();
        }

        public void Requeue()
        {
            Entities.Queue data = new Entities.Queue
            {
                Id = Id,
                Name = Queue,
                JobId = JobId
            };
            storage.Client.UpsertDocumentAsync(QueueDocumentCollectionUri, data).GetAwaiter().GetResult();
        }
    }
}
