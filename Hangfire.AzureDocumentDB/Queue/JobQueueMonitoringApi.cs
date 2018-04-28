using System;
using System.Linq;
using System.Collections.Generic;

using Hangfire.Azure.Documents;
using Microsoft.Azure.Documents;

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
                    IEnumerable<string> result = storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri)
                        .Where(q => q.DocumentType == DocumentTypes.Queue)
                        .Select(q => q.Name)
                        .AsEnumerable()
                        .Distinct();

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
                QueryText = "SELECT VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.name = @name",
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
            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT VALUE doc.job_id FROM doc WHERE doc.type = @type AND doc.name = @name AND NOT is_defined(doc.fetched_at) ORDER BY doc.created_on",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@type", DocumentTypes.Queue),
                    new SqlParameter("@name", queue)
                }
            };

            return storage.Client.CreateDocumentQuery<string>(storage.CollectionUri, sql)
                .AsEnumerable()
                .Skip(from + 1).Take(perPage + 1);
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT VALUE doc.job_id FROM doc WHERE doc.type = @type AND doc.name = @name AND is_defined(doc.fetched_at) ORDER BY doc.created_on",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@type", DocumentTypes.Queue),
                    new SqlParameter("@name", queue)
                }
            };

            return storage.Client.CreateDocumentQuery<string>(storage.CollectionUri, sql)
                .AsEnumerable()
                .Skip(from + 1).Take(perPage + 1);
        }

        public (int? EnqueuedCount, int? FetchedCount) GetEnqueuedAndFetchedCount(string queue)
        {
            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT * FROM doc WHERE doc.type = @type AND doc.name = @name",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@type", DocumentTypes.Queue),
                    new SqlParameter("@name", queue)
                }
            };

            (int EnqueuedCount, int FetchedCount) result = storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri, sql)
                  .AsEnumerable()
                  .GroupBy(q => q.Name)
                  .Select(v => (EnqueuedCount: v.Sum(q => q.FetchedAt.HasValue ? 0 : 1), FetchedCount: v.Sum(q => q.FetchedAt.HasValue ? 1 : 0)))
                  .FirstOrDefault();

            return result;
        }

    }
}