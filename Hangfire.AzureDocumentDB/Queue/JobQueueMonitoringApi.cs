using System.Linq;
using System.Collections.Generic;

using Hangfire.Azure.Documents;
using Microsoft.Azure.Documents;

namespace Hangfire.Azure.Queue
{
    internal class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly DocumentDbStorage storage;
        private readonly IEnumerable<string> queues;

        public JobQueueMonitoringApi(DocumentDbStorage storage)
        {
            this.storage = storage;
            queues = storage.Options.Queues;
        }

        public IEnumerable<string> GetQueues() => queues;

        public int GetEnqueuedCount(string queue)
        {
            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT VALUE COUNT(1) FROM c WHERE c.name = @name AND c.type = @type",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@name", queue),
                    new SqlParameter("@type", DocumentTypes.Queue)
                }
            };

            return storage.Client.CreateDocumentQuery<int>(storage.CollectionUri, sql)
                .AsEnumerable()
                .FirstOrDefault();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri)
                .Where(q => q.Name == queue && q.DocumentType == DocumentTypes.Queue)
                .Select(c => c.JobId)
                .AsEnumerable()
                .Skip(from).Take(perPage)
                .ToList();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage) => GetEnqueuedJobIds(queue, from, perPage);

    }
}