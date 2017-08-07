using System.Threading.Tasks;

using Hangfire.Storage;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

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

        public void Dispose()
        {
        }

        public void RemoveFromQueue()
        {
            var spDeleteDocumentIfExistsUri = UriFactory.CreateStoredProcedureUri(storage.Options.DatabaseName, storage.Options.CollectionName, "deleteDocumentIfExists");
            Task<StoredProcedureResponse<bool>> task = storage.Client.ExecuteStoredProcedureAsync<bool>(spDeleteDocumentIfExistsUri, Id);
            task.Wait();
        }

        public void Requeue()
        {
            Documents.Queue data = new Documents.Queue
            {
                Id = Id,
                Name = Queue,
                JobId = JobId
            };
            Task<ResourceResponse<Document>> task = storage.Client.UpsertDocumentAsync(storage.CollectionUri, data);
            task.Wait();
        }
    }
}
