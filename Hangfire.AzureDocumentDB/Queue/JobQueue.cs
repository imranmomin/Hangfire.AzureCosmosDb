using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using Hangfire.Logging;
using Hangfire.Storage;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Hangfire.Azure.Helper;
using Hangfire.Azure.Documents.Helper;

namespace Hangfire.Azure.Queue
{
    internal class JobQueue : IPersistentJobQueue
    {
        private readonly ILog logger = LogProvider.For<JobQueue>();
        private readonly DocumentDbStorage storage;
        private const string DISTRIBUTED_LOCK_KEY = "locks:job:dequeue";
        private readonly TimeSpan defaultLockTimeout;
        private readonly TimeSpan invisibilityTimeout = TimeSpan.FromMinutes(15);
        private readonly object syncLock = new object();

        public JobQueue(DocumentDbStorage storage)
        {
            this.storage = storage;
            defaultLockTimeout = TimeSpan.FromSeconds(30) + storage.Options.QueuePollInterval;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            lock (syncLock)
            {
                IEnumerable<string> queueParams = Enumerable.Range(0, queues.Length).Select((q, i) => $"@queue_{i}");
                string query = $"SELECT TOP 1 * FROM doc WHERE doc.type = @type AND doc.name IN ({string.Join(", ", queueParams)}) " +
                               "AND (NOT IS_DEFINED(doc.fetched_at) OR doc.fetched_at < @timeout) ORDER BY doc.created_on";

                List<SqlParameter> parameters = new List<SqlParameter> { new SqlParameter("@type", Documents.DocumentTypes.Queue) };
                for (int index = 0; index < queues.Length; index++)
                {
                    string queue = queues[index];
                    parameters.Add(new SqlParameter($"@queue_{index}", queue));
                }

                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    logger.Trace("Looking for any jobs from the queue");

                    using (new DocumentDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
                    {
                        int invisibilityTimeoutEpoch = DateTime.UtcNow.Add(invisibilityTimeout.Negate()).ToEpoch();

                        SqlQuerySpec sql = new SqlQuerySpec
                        {
                            QueryText = query,
                            Parameters = new SqlParameterCollection(parameters)
                        };
                        sql.Parameters.Add(new SqlParameter("@timeout", invisibilityTimeoutEpoch));

                        Documents.Queue data = storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri, sql)
                            .ToQueryResult()
                            .FirstOrDefault();

                        if (data != null)
                        {
                            // mark the document
                            data.FetchedAt = DateTime.UtcNow;

                            Uri replaceUri = new Uri(data.SelfLink, UriKind.Relative);
                            Task<ResourceResponse<Document>> task = storage.Client.ReplaceDocumentWithRetriesAsync(replaceUri, data, cancellationToken: cancellationToken);
                            task.Wait(cancellationToken);

                            logger.Trace($"Found job {data.JobId} from the queue : {data.Name}");
                            return new FetchedJob(storage, data);
                        }
                    }

                    logger.Trace($"Unable to find any jobs in the queue. Will check the queue for jobs in {storage.Options.QueuePollInterval.TotalSeconds} seconds");
                    cancellationToken.WaitHandle.WaitOne(storage.Options.QueuePollInterval);
                }
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

            Task<ResourceResponse<Document>> task = storage.Client.CreateDocumentWithRetriesAsync(storage.CollectionUri, data);
            task.Wait();
        }
    }
}