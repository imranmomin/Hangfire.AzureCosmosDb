using System.Net;

using Hangfire.Storage;

namespace Hangfire.AzureDocumentDB.Queue
{
    internal class FetchedJob : IFetchedJob
    {
        private readonly AzureDocumentDbConnection connection;

        public FetchedJob(AzureDocumentDbStorage storage, string queue, string jobId, string reference)
        {
            connection = (AzureDocumentDbConnection)storage.GetConnection();
            JobId = jobId;
            Queue = queue;
            Reference = reference;
        }

        private string Reference { get; }

        public string JobId { get; }

        private string Queue { get; }

        public void Dispose()
        {
        }

        public void RemoveFromQueue() => connection.Client.Delete($"queue/{Queue}/{Reference}");

        public void Requeue()
        {
            FirebaseResponse response = connection.Client.Get($"queue/{Queue}/{Reference}");
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                connection.Client.Push($"queue/{Queue}", JobId);
            }
        }
    }
}
