using System;
using System.Linq;
using System.Collections.Generic;

using Hangfire.Azure.Helper;
using Hangfire.Azure.Documents;

using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

namespace Hangfire.Azure.Queue
{
    internal class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly CosmosDbStorage storage;
        private readonly List<string> queuesCache = new List<string>();
        private DateTime cacheUpdated;
        private readonly object cacheLock = new object();
        private static readonly TimeSpan queuesCacheTimeout = TimeSpan.FromSeconds(5);

        public JobQueueMonitoringApi(CosmosDbStorage storage) => this.storage = storage;

        public IEnumerable<string> GetQueues()
        {
            lock (cacheLock)
            {
                if (queuesCache.Count == 0 || cacheUpdated.Add(queuesCacheTimeout) < DateTime.UtcNow)
                {
                    QueryDefinition sql = new QueryDefinition("SELECT DISTINCT VALUE doc['name'] FROM doc WHERE doc.type = @type")
                            .WithParameter("@type", (int)DocumentTypes.Queue);


                    IEnumerable<string> result = storage.Container.GetItemQueryIterator<string>(sql).ToQueryResult();
                    queuesCache.Clear();
                    queuesCache.AddRange(result);
                    cacheUpdated = DateTime.UtcNow;
                }

                return queuesCache.ToList();
            }
        }

        public int GetEnqueuedCount(string queue)
        {
            QueryDefinition sql =
                new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.name = @name")
                    .WithParameter("@name", queue)
                    .WithParameter("@type", (int)DocumentTypes.Queue);

            return storage.Container.GetItemQueryIterator<int>(sql)
                .ToQueryResult()
                .FirstOrDefault();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return storage.Container.GetItemLinqQueryable<Documents.Queue>()
                .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue && q.FetchedAt.IsDefined() == false)
                .OrderBy(q => q.CreatedOn)
                .Skip(from).Take(perPage)
                .Select(q => q.JobId)
                .ToQueryResult();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            return storage.Container.GetItemLinqQueryable<Documents.Queue>()
                .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue && q.FetchedAt.IsDefined())
                .OrderBy(q => q.CreatedOn)
                .Skip(from).Take(perPage)
                .Select(q => q.JobId)
                .ToQueryResult();
        }

        public (int? EnqueuedCount, int? FetchedCount) GetEnqueuedAndFetchedCount(string queue)
        {
            (int EnqueuedCount, int FetchedCount) result = storage.Container.GetItemLinqQueryable<Documents.Queue>()
                .Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue)
                .Select(q => new { q.Name, EnqueuedCount = q.FetchedAt.IsDefined() ? 0 : 1, FetchedCount = q.FetchedAt.IsDefined() ? 1 : 0 })
                .ToQueryResult()
                .GroupBy(q => q.Name)
                .Select(v => (EnqueuedCount: v.Sum(q => q.EnqueuedCount), FetchedCount: v.Sum(q => q.FetchedCount)))
                .FirstOrDefault();

            return result;
        }

    }
}