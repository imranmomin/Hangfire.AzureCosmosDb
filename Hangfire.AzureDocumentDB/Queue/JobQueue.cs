using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;

using Hangfire.Storage;
using Microsoft.Azure.Documents;
using Hangfire.Azure.Documents.Helper;

namespace Hangfire.Azure.Queue
{
    internal class JobQueue : IPersistentJobQueue
    {
        private readonly DocumentDbStorage storage;
        private const string DISTRIBUTED_LOCK_KEY = "locks:job:dequeue";
        private readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(1);
        private readonly TimeSpan checkInterval;
        private readonly object syncLock = new object();
        private readonly FeedOptions queryOptions = new FeedOptions { MaxItemCount = 1 };
        private readonly Uri spDeleteDocumentIfExistsUri;

        public JobQueue(DocumentDbStorage storage)
        {
            this.storage = storage;
            checkInterval = storage.Options.QueuePollInterval;
            spDeleteDocumentIfExistsUri = UriFactory.CreateStoredProcedureUri(storage.Options.DatabaseName, storage.Options.CollectionName, "deleteDocumentIfExists");
        }

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

                        Documents.Queue data = storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri, queryOptions)
                            .Where(q => q.Name == queue && q.DocumentType == Documents.DocumentTypes.Queue)
                            .AsEnumerable()
                            .FirstOrDefault();

                        if (data != null)
                        {
                            Task<StoredProcedureResponse<bool>> task = storage.Client.ExecuteStoredProcedureAsync<bool>(spDeleteDocumentIfExistsUri, data.Id);
                            task.Wait(cancellationToken);

                            if (task.Result.Response)
                            {
                                return new FetchedJob(storage, data);
                            }
                        }
                    }
                }

                Thread.Sleep(checkInterval);
                index = (index + 1) % queues.Length;
            }
        }

        public void Enqueue(string queue, string jobId)
        {
            Documents.Queue data = new Documents.Queue
            {
                Name = queue,
                JobId = jobId
            };

            Task<ResourceResponse<Document>> task = storage.Client.CreateDocumentWithRetriesAsync(storage.CollectionUri, data);
            task.Wait();
        }
    }
}