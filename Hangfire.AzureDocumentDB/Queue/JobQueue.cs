using System;
using System.Net;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Storage;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Hangfire.Azure.Queue
{
    internal class JobQueue : IPersistentJobQueue
    {
        private readonly DocumentDbStorage storage;
        private const string DISTRIBUTED_LOCK_KEY = "locks:job:dequeue";
        private readonly TimeSpan defaultLockTimeout = TimeSpan.FromSeconds(10);
        private readonly TimeSpan invisibilityTimeout = TimeSpan.FromMinutes(30);
        private readonly object syncLock = new object();

        public JobQueue(DocumentDbStorage storage) => this.storage = storage;

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            int index = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                lock (syncLock)
                {
                    using (new DocumentDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
                    {
                        string queue = queues.ElementAt(index);
                        int invisibilityTimeoutEpoch = DateTime.UtcNow.Add(invisibilityTimeout.Negate()).ToEpoch();

                        SqlQuerySpec sql = new SqlQuerySpec
                        {
                            QueryText = "SELECT TOP 1 * FROM doc WHERE doc.type = @type AND doc.name = @name AND " +
                                        "((NOT is_defined(doc.fetched_at)) OR (is_defined(doc.fetched_at) AND doc.fetched_at < @timeout))",
                            Parameters = new SqlParameterCollection
                            {
                                new SqlParameter("@type", Documents.DocumentTypes.Queue),
                                new SqlParameter("@name", queue),
                                new SqlParameter("@timeout", invisibilityTimeoutEpoch)
                            }
                        };

                        Documents.Queue data = storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri, sql, new FeedOptions { MaxItemCount = 1 })
                            .AsEnumerable()
                            .FirstOrDefault();

                        if (data != null && !string.IsNullOrEmpty(data.JobId))
                        {
                            // mark the document
                            data.FetchedAt = DateTime.UtcNow;

                            Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, data.Id);
                            Task<ResourceResponse<Document>> task = storage.Client.ReplaceDocumentAsync(uri, data);
                            task.Wait(cancellationToken);

                            return new FetchedJob(storage, data);
                        }
                    }
                }

                Thread.Sleep(storage.Options.QueuePollInterval);
                index = (index + 1) % queues.Length;
            }
        }

        public void Enqueue(string queue, string jobId)
        {
            Documents.Queue data = new Documents.Queue
            {
                Name = queue,
                JobId = jobId,
                CreatedOn = DateTime.UtcNow
            };

            Task<ResourceResponse<Document>> task = storage.Client.CreateDocumentAsync(storage.CollectionUri, data);
            task.Wait();
        }
    }
}