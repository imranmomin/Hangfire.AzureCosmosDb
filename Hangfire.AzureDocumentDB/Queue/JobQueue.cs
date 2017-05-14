using System;
using System.Linq;
using System.Threading;

using Microsoft.Azure.Documents.Client;

using Hangfire.Storage;
using Hangfire.AzureDocumentDB.Helper;

namespace Hangfire.AzureDocumentDB.Queue
{
    internal class JobQueue : IPersistentJobQueue
    {
        private readonly AzureDocumentDbStorage storage;
        private readonly string dequeueLockKey = "locks:job:dequeue";
        private readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(1);
        private readonly TimeSpan checkInterval;
        private readonly object syncLock = new object();

        private readonly FeedOptions QueryOptions = new FeedOptions { MaxItemCount = 1 };

        public JobQueue(AzureDocumentDbStorage storage)
        {
            this.storage = storage;
            checkInterval = storage.Options.QueuePollInterval;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            int index = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                lock (syncLock)
                {
                    using (new AzureDocumentDbDistributedLock(dequeueLockKey, defaultLockTimeout, storage))
                    {
                        string queue = queues.ElementAt(index);

                        Entities.Queue data = storage.Client.CreateDocumentQuery<Entities.Queue>(storage.Collections.QueueDocumentCollectionUri, QueryOptions)
                            .Where(q => q.Name == queue && q.DocumentType == Entities.DocumentTypes.Queue)
                            .AsEnumerable()
                            .FirstOrDefault();

                        if (data != null)
                        {
                            storage.Client.DeleteDocumentWithRetriesAsync(data.SelfLink).GetAwaiter().GetResult();
                            return new FetchedJob(storage, data);
                        }
                    }
                }

                Thread.Sleep(checkInterval);
                index = (index + 1) % queues.Length;
            }
        }

        public void Enqueue(string queue, string jobId)
        {
            Entities.Queue data = new Entities.Queue
            {
                Name = queue,
                JobId = jobId
            };
            storage.Client.CreateDocumentWithRetriesAsync(storage.Collections.QueueDocumentCollectionUri, data).GetAwaiter().GetResult();
        }
    }
}