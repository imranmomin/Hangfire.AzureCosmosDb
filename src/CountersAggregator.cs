using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Hangfire.Server;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure;
#pragma warning disable 618
public class CountersAggregator : IServerComponent
#pragma warning restore 618
{
	private const string DISTRIBUTED_LOCK_KEY = "locks:counters:aggregator";
	private readonly ILog logger = LogProvider.For<CountersAggregator>();
	private readonly PartitionKey partitionKey = new((int)DocumentTypes.Counter);
	private readonly CosmosDbStorage storage;

	public CountersAggregator(CosmosDbStorage storage)
	{
		this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
	}

	public void Execute(CancellationToken cancellationToken)
	{
		CosmosDbDistributedLock? distributedLock = null;

		try
		{
			// check if the token was cancelled
			cancellationToken.ThrowIfCancellationRequested();

			// get the distributed lock
			distributedLock = new CosmosDbDistributedLock(DISTRIBUTED_LOCK_KEY, storage.StorageOptions.CountersAggregateInterval, storage);

			logger.Trace("Aggregating records in [Counter] table.");

			// get the total records to be aggregated 
			QueryDefinition sql = new("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.counterType = @counterType");
			sql.WithParameter("@counterType", (int)CounterTypes.Raw);

			int total = storage.Container.GetItemQueryIterator<int>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Counter })
				.ToQueryResult()
				.FirstOrDefault();

			logger.Trace($"Total of [{total}] records to be aggregated from the [Counter] table.");

			if (total > 0)
			{
				sql = new QueryDefinition($"SELECT TOP {storage.StorageOptions.CountersAggregateMaxItemCount} * FROM doc WHERE doc.counterType = @counterType ORDER BY doc.expire_on");
				sql.WithParameter("@counterType", (int)CounterTypes.Raw);

				int completed = 0;
				while (completed < total)
				{
					// check if the token was cancelled
					cancellationToken.ThrowIfCancellationRequested();

					// get the counters
					List<Counter> rawCounters = storage.Container.GetItemQueryIterator<Counter>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Counter })
						.ToQueryResult()
						.ToList();

					// break the loop when no records found
					if (rawCounters.Count == 0) break;

					Dictionary<string, (int Value, DateTime? ExpireOn, List<Counter> Counters)> counters = rawCounters.GroupBy(c => c.Key)
						.ToDictionary(k => k.Key, v => (Value: v.Sum(c => c.Value), ExpireOn: v.Max(c => c.ExpireOn), Counters: v.ToList()));

					foreach (string key in counters.Keys)
					{
						// check if the token was cancelled
						cancellationToken.ThrowIfCancellationRequested();

						if (!counters.TryGetValue(key, out (int Value, DateTime? ExpireOn, List<Counter> Counters) data)) continue;

						string id = $"{key}:{CounterTypes.Aggregate}".GenerateHash();
						Task<ItemResponse<Counter>> task;

						try
						{
							// ReSharper disable once MethodSupportsCancellation
							Task<ItemResponse<Counter>> readTask = storage.Container.ReadItemWithRetriesAsync<Counter>(id, partitionKey);
							// ReSharper disable once MethodSupportsCancellation
							readTask.Wait();

							Counter aggregated = readTask.Result.Resource;

							DateTime? expireOn = new[] { aggregated.ExpireOn, data.ExpireOn }.Max();
							int? expireOnEpoch = expireOn?.ToEpoch();

							PatchOperation[] patchOperations =
							{
								PatchOperation.Increment("/value", data.Value),
								PatchOperation.Set("/expire_on", expireOnEpoch)
							};

							PatchItemRequestOptions patchItemRequestOptions = new()
							{
								IfMatchEtag = aggregated.ETag
							};

							// ReSharper disable once MethodSupportsCancellation
							task = storage.Container.PatchItemWithRetriesAsync<Counter>(aggregated.Id, partitionKey, patchOperations, patchItemRequestOptions);
						}
						catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
						{
							Counter aggregated = new()
							{
								Id = id,
								Key = key,
								Type = CounterTypes.Aggregate,
								Value = data.Value,
								ExpireOn = data.ExpireOn
							};

							// ReSharper disable once MethodSupportsCancellation
							task = storage.Container.CreateItemWithRetriesAsync(aggregated, partitionKey);
						}

						// ReSharper disable once MethodSupportsCancellation
						task.Wait();

						// now - remove the raw counters
						string ids = string.Join(",", data.Counters.Select(c => $"'{c.Id}'").ToArray());
						string query = $"SELECT * FROM doc WHERE doc.counterType = {(int)CounterTypes.Raw} AND doc.id IN ({ids})";
						int deleted = storage.Container.ExecuteDeleteDocuments(query, partitionKey);
						completed += deleted;
					}
				}

				logger.Trace($"Total [{completed}] records from the [Counter] table were aggregated.");
			}
		}
		catch (CosmosDbDistributedLockException exception) when (exception.Key == DISTRIBUTED_LOCK_KEY)
		{
			logger.Debug($@"An exception was thrown during acquiring distributed lock on the [{DISTRIBUTED_LOCK_KEY}] resource within [{storage.StorageOptions.CountersAggregateInterval.TotalSeconds}] seconds." +
			             $@"Counter records were not aggregated. It will be retried in [{storage.StorageOptions.CountersAggregateInterval.TotalSeconds}] seconds.");
		}
		finally
		{
			distributedLock?.Dispose();
		}

		// wait for the interval specified
		cancellationToken.WaitHandle.WaitOne(storage.StorageOptions.CountersAggregateInterval);
	}

	public override string ToString() => GetType().ToString();
}