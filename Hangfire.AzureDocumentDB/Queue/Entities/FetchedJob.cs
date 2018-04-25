using System;
using System.Threading.Tasks;

using Hangfire.Storage;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Queue
{
    internal class FetchedJob : IFetchedJob
    {
        private readonly DocumentDbStorage storage;

        public FetchedJob(DocumentDbStorage storage, Documents.Queue data)
        {
            this.storage = storage;
            Id = data.Id;
            JobId = data.JobId;
            Queue = data.Name;
        }

        private string Id { get; }

        public string JobId { get; }

        private string Queue { get; }

        public void Dispose() { }

        public void RemoveFromQueue()
        {
            Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, Id);
            Task<ResourceResponse<Document>> task = storage.Client.DeleteDocumentAsync(uri);
            task.Wait();
        }

        public void Requeue()
        {
            Documents.Queue data = new Documents.Queue
            {
                Id = Id,
                Name = Queue,
                JobId = JobId,
                CreatedOn = DateTime.UtcNow,
                FetchedAt = null
            };

            Task<ResourceResponse<Document>> task = storage.Client.UpsertDocumentAsync(storage.CollectionUri, data);
            task.Wait();
        }
    }
}
