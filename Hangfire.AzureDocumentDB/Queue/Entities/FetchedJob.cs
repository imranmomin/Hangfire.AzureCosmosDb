using System.Linq;

using Hangfire.Storage;

using Microsoft.Azure.Documents.Client;

namespace Hangfire.AzureDocumentDB.Queue
{
    internal class FetchedJob : IFetchedJob
    {
        private readonly AzureDocumentDbStorage storage;
        private readonly FeedOptions QueryOptions = new FeedOptions { MaxItemCount = 1 };

        public FetchedJob(AzureDocumentDbStorage storage, Entities.Queue data)
        {
            this.storage = storage;
            Id = data.Id;
            JobId = data.JobId;
            Queue = data.Name;
            SelfLink = data.SelfLink;
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
            // TODO: move to stored procedure
            bool exists = storage.Client.CreateDocumentQuery(storage.CollectionUri, QueryOptions)
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
            storage.Client.UpsertDocumentAsync(storage.CollectionUri, data).GetAwaiter().GetResult();
        }
    }
}
