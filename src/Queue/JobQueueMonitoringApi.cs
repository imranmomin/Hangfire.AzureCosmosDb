using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

namespace Hangfire.Azure.Queue;

public class JobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
{
	private static readonly TimeSpan queuesCacheTimeout = TimeSpan.FromSeconds(5);
	private readonly object cacheLock = new();
	private readonly PartitionKey partitionKey = new((int)DocumentTypes.Queue);
	private readonly List<string> queuesCache = new();
	private readonly CosmosDbStorage storage;
	private DateTime cacheUpdated;

	public JobQueueMonitoringApi(CosmosDbStorage storage)
	{
		this.storage = storage;
	}

	public IEnumerable<string> GetQueues()
	{
		lock (cacheLock)
		{
			// if cached and not expired return the cached item
			if (queuesCache.Count != 0 && cacheUpdated.Add(queuesCacheTimeout) >= DateTime.UtcNow) return queuesCache.ToList();

			QueryDefinition sql = new QueryDefinition("SELECT DISTINCT VALUE doc['name'] FROM doc WHERE doc.type = @type")
				.WithParameter("@type", (int)DocumentTypes.Queue);

			List<string> result = storage.Container.GetItemQueryIterator<string>(sql, requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
				.ToQueryResult()
				.ToList();

			queuesCache.Clear();
			queuesCache.AddRange(result);
			cacheUpdated = DateTime.UtcNow;

			return queuesCache;
		}
	}

	public int GetEnqueuedCount(string queue)
	{
		QueryDefinition sql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.name = @name AND NOT IS_DEFINED(doc.fetched_at) ")
			.WithParameter("@name", queue)
			.WithParameter("@type", (int)DocumentTypes.Queue);

		return storage.Container.GetItemQueryIterator<int>(sql, requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
			.ToQueryResult()
			.FirstOrDefault();
	}

	public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage) => storage.Container.GetItemLinqQueryable<Documents.Queue>(requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
		.Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue && q.FetchedAt.IsDefined() == false)
		.OrderBy(q => q.CreatedOn)
		.Skip(from).Take(perPage)
		.Select(q => q.JobId)
		.ToQueryResult()
		.ToList();

	public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage) => storage.Container.GetItemLinqQueryable<Documents.Queue>(requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
		.Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue && q.FetchedAt.IsDefined())
		.OrderBy(q => q.CreatedOn)
		.Skip(from).Take(perPage)
		.Select(q => q.JobId)
		.ToQueryResult()
		.ToList();

	public (int? EnqueuedCount, int? FetchedCount) GetEnqueuedAndFetchedCount(string queue)
	{
		(int EnqueuedCount, int FetchedCount) result = storage.Container.GetItemLinqQueryable<Documents.Queue>(requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
			.Where(q => q.DocumentType == DocumentTypes.Queue && q.Name == queue)
			.Select(q => new { q.Name, EnqueuedCount = q.FetchedAt.IsDefined() ? 0 : 1, FetchedCount = q.FetchedAt.IsDefined() ? 1 : 0 })
			.ToQueryResult()
			.GroupBy(q => q.Name)
			.Select(v => (EnqueuedCount: v.Sum(q => q.EnqueuedCount), FetchedCount: v.Sum(q => q.FetchedCount)))
			.FirstOrDefault();

		return result;
	}
}