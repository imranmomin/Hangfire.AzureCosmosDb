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

			QueryRequestOptions queryRequestOptions = new()
			{
				PartitionKey = partitionKey,
				MaxItemCount = 100
			};

			List<Counter> rawCounters = storage.Container.GetItemLinqQueryable<Counter>(requestOptions: queryRequestOptions)
				.Where(x => x.Type == CounterTypes.Raw)
				.OrderBy(x => x.ExpireOn)
				.ToQueryResult()
				.ToList();

			Dictionary<string, (int Value, DateTime? ExpireOn, List<Counter> Counters)> counters = rawCounters.GroupBy(c => c.Key)
				.ToDictionary(k => k.Key, v => (Value: v.Sum(c => c.Value), ExpireOn: v.Max(c => c.ExpireOn), Counters: v.ToList()));

			foreach (string key in counters.Keys)
			{
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
				catch (AggregateException ex) when (ex.InnerException is CosmosException)
				{
					logger.ErrorException("Error while reading document", ex);
					continue;
				}

				// ReSharper disable once MethodSupportsCancellation
				task.Wait();

				// now - remove the raw counters
				string ids = string.Join(",", data.Counters.Select(c => $"'{c.Id}'").ToArray());
				string query = $"SELECT * FROM doc WHERE doc.counterType = {(int)CounterTypes.Raw} AND doc.id IN ({ids})";
				int deleted = storage.Container.ExecuteDeleteDocuments(query, partitionKey);
				logger.Trace($"Total [{deleted}] records from the ['Counter:{key}'] were aggregated.");
			}

			logger.Trace($"Records [{rawCounters.Count}] from the [Counter] table aggregated.");
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