using System;
using System.Linq;
using System.Collections.Generic;

using Hangfire.Azure.Documents;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Hangfire.Azure.Helper;

namespace Hangfire.Azure.Queue
{
    internal class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly DocumentDbStorage storage;
        private readonly List<string> queuesCache = new List<string>();
        private DateTime cacheUpdated;
        private readonly object cacheLock = new object();
        private static readonly TimeSpan queuesCacheTimeout = TimeSpan.FromSeconds(5);

        public JobQueueMonitoringApi(DocumentDbStorage storage) => this.storage = storage;

        public IEnumerable<string> GetQueues()
        {
            lock (cacheLock)
            {
                if (queuesCache.Count == 0 || cacheUpdated.Add(queuesCacheTimeout) < DateTime.UtcNow)
                {
                    SqlQuerySpec sql = new SqlQuerySpec
                    {
                        QueryText = "SELECT DISTINCT VALUE doc.name FROM doc WHERE doc.type = @type",
                        Parameters = new SqlParameterCollection
                        {
                            new SqlParameter("@type", DocumentTypes.Queue)
                        }
                    };

                    FeedOptions feedOptions = new FeedOptions
                    {
                        MaxItemCount = 10,
                        MaxBufferedItemCount = 10,
                        MaxDegreeOfParallelism = -1
                    };

                    IEnumerable<string> result = storage.Client.CreateDocumentQuery<string>(storage.CollectionUri, sql, feedOptions)
                        .ToQueryResult();

                    queuesCache.Clear();
                    queuesCache.AddRange(result);
                    cacheUpdated = DateTime.UtcNow;
                }

                return queuesCache.ToList();
            }
        }

        public int GetEnqueuedCount(string queue)
        {
            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.name = @name",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@name", queue),
                    new SqlParameter("@type", DocumentTypes.Queue)
                }
            };

            FeedOptions feedOptions = new FeedOptions
            {
                MaxItemCount = 1,
                MaxBufferedItemCount = 1,
                MaxDegreeOfParallelism = -1
            };

            return storage.Client.CreateDocumentQuery<int>(storage.CollectionUri, sql, feedOptions)
                .ToQueryResult()
                .FirstOrDefault();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT VALUE doc.job_id FROM doc WHERE doc.type = @type AND doc.name = @name AND NOT IS_DEFINED(doc.fetched_at) ORDER BY doc.created_on",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@type", DocumentTypes.Queue),
                    new SqlParameter("@name", queue)
                }
            };

            FeedOptions feedOptions = new FeedOptions
            {
                MaxItemCount = 10,
                MaxBufferedItemCount = 100,
                MaxDegreeOfParallelism = -1
            };

            return storage.Client.CreateDocumentQuery<string>(storage.CollectionUri, sql, feedOptions)
                .ToQueryResult()
                .Skip(from).Take(perPage);
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT VALUE doc.job_id FROM doc WHERE doc.type = @type AND doc.name = @name AND IS_DEFINED(doc.fetched_at) ORDER BY doc.created_on",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@type", DocumentTypes.Queue),
                    new SqlParameter("@name", queue)
                }
            };

            FeedOptions feedOptions = new FeedOptions
            {
                MaxItemCount = 10,
                MaxBufferedItemCount = 100,
                MaxDegreeOfParallelism = -1
            };

            return storage.Client.CreateDocumentQuery<string>(storage.CollectionUri, sql, feedOptions)
                .ToQueryResult()
                .Skip(from).Take(perPage);
        }

        public (int? EnqueuedCount, int? FetchedCount) GetEnqueuedAndFetchedCount(string queue)
        {
            FeedOptions feedOptions = new FeedOptions
            {
                MaxItemCount = 10,
                MaxBufferedItemCount = 100,
                MaxDegreeOfParallelism = -1
            };

            (int EnqueuedCount, int FetchedCount) result = storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri, feedOptions)
                .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue)
                .ToQueryResult()
                .GroupBy(q => q.Name)
                .Select(v => (EnqueuedCount: v.Sum(q => q.FetchedAt.HasValue ? 0 : 1), FetchedCount: v.Sum(q => q.FetchedAt.HasValue ? 1 : 0)))
                .FirstOrDefault();

            return result;
        }

    }
}