using System;
using System.Linq;
using System.Threading;
using System.Collections.Generic;

using Hangfire.Storage;
 
namespace Hangfire.AzureDocumentDB.Queue
{
    internal class JobQueue : IPersistentJobQueue
    {
        private readonly AzureDocumentDbStorage storage;
        private readonly AzureDocumentDbConnection connection;
        private readonly string dequeueLockKey = "locks:job:dequeue";
        private readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(1);
        private readonly TimeSpan checkInterval;
        private readonly object syncLock = new object();

        public JobQueue(AzureDocumentDbStorage storage)
        {
            this.storage = storage;
            connection = (AzureDocumentDbConnection)storage.GetConnection();
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
                    using (new AzureDocumentDbDistributedLock(dequeueLockKey, defaultLockTimeout, connection.Client))
                    {
                        string queue = queues.ElementAt(index);

                        QueryBuilder buidler = QueryBuilder.New();
                        buidler.OrderBy("$key");
                        buidler.LimitToFirst(1);
                        FirebaseResponse response = connection.Client.Get($"queue/{queue}", buidler);
                        if (response.StatusCode == System.Net.HttpStatusCode.OK)
                        {
                            Dictionary<string, string> collection = response.ResultAs<Dictionary<string, string>>();
                            var data = collection?.Select(q => new { Queue = queue, Reference = q.Key, JobId = q.Value })
                                .FirstOrDefault();

                            if (!string.IsNullOrEmpty(data?.JobId) && !string.IsNullOrEmpty(data.Reference))
                            {
                                connection.Client.Delete($"queue/{data.Queue}/{data.Reference}");
                                return new FetchedJob(storage, data.Queue, data.JobId, data.Reference);
                            }
                        }
                    }
                }

                Thread.Sleep(checkInterval);
                index = (index + 1) % queues.Length;
            }
        }

        public void Enqueue(string queue, string jobId) => connection.Client.Push($"queue/{queue}", jobId);
    }
}