using System.Net;
using System.Linq;
using System.Collections.Generic;

namespace Hangfire.AzureDocumentDB.Queue
{
    internal class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly AzureDocumentDbConnection connection;
        private readonly IEnumerable<string> queues;

        public JobQueueMonitoringApi(AzureDocumentDbStorage storage)
        {
            connection = (AzureDocumentDbConnection)storage.GetConnection();
            queues = storage.Options.Queues;
        }

        public IEnumerable<string> GetQueues() => queues;

        public int GetEnqueuedCount(string queue)
        {
            FirebaseResponse response = connection.Client.Get($"queue/{queue}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, string> collection = response.ResultAs<Dictionary<string, string>>();
                return collection.Count;
            }
            return default(int);
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            FirebaseResponse response = connection.Client.Get($"queue/{queue}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, string> collection = response.ResultAs<Dictionary<string, string>>();
                return collection.Skip(from).Take(perPage).Select(c => c.Value);
            }
            return null;
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage) => GetEnqueuedJobIds(queue, from, perPage);

    }
}