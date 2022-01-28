using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Hangfire.Server;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure;
#pragma warning disable 618
public class CountersAggregator : IServerComponent
#pragma warning restore 618
{
	private const string DISTRIBUTED_LOCK_KEY = "locks:counters:aggragator";
	private readonly TimeSpan defaultLockTimeout;
	private readonly ILog logger = LogProvider.For<CountersAggregator>();
	private readonly PartitionKey partitionKey = new((int)DocumentTypes.Counter);
	private readonly CosmosDbStorage storage;

	public CountersAggregator(CosmosDbStorage storage)
	{
		this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
		defaultLockTimeout = TimeSpan.FromSeconds(30).Add(storage.StorageOptions.CountersAggregateInterval);
	}

	public void Execute(CancellationToken cancellationToken)
	{
		bool isCompleted = false;

		while (cancellationToken.IsCancellationRequested == false && isCompleted == false)
		{
			CosmosDbDistributedLock? distributedLock = null;

			try
			{
				// check if the token was cancelled
				cancellationToken.ThrowIfCancellationRequested();

				// get the distributed lock
				distributedLock = new CosmosDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage);

				logger.Trace("Aggregating records in [Counter] table.");

				QueryDefinition sql = new QueryDefinition("SELECT * FROM doc WHERE AND doc.counterType = @counterType")
					.WithParameter("@counterType", (int)CounterTypes.Raw);

				QueryRequestOptions queryRequestOptions = new()
				{
					PartitionKey = partitionKey,
					MaxItemCount = 100
				};

				List<Counter> rawCounters = storage.Container.GetItemQueryIterator<Counter>(sql, requestOptions: queryRequestOptions)
					.ToQueryResult()
					.ToList();

				if (rawCounters.Count == 0) break;

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
						Task<ItemResponse<Counter>> readTask = storage.Container.ReadItemWithRetriesAsync<Counter>(id, partitionKey, cancellationToken: cancellationToken);
						// ReSharper disable once MethodSupportsCancellation
						readTask.Wait();

						Counter aggregated = readTask.Result.Resource;

						PatchOperation[] patchOperations =
						{
							PatchOperation.Increment("/value", data.Value),
							PatchOperation.Set("/expire_on", new[] { aggregated.ExpireOn, data.ExpireOn }.Max())
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
				isCompleted = true;
			}
			catch (CosmosDbDistributedLockException exception) when (exception.Key == DISTRIBUTED_LOCK_KEY)
			{
				logger.Debug($@"An exception was thrown during acquiring distributed lock on the [{DISTRIBUTED_LOCK_KEY}] resource within [{defaultLockTimeout.TotalSeconds}] seconds." +
				             @"Counter records were not aggregated. It will be retried in [5] seconds.");

				// will wait for 5 seconds
				Thread.Sleep(5000);
			}
			finally
			{
				distributedLock?.Dispose();
			}
		}

		// wait for the interval specified
		cancellationToken.WaitHandle.WaitOne(storage.StorageOptions.CountersAggregateInterval);
	}

	public override string ToString() => GetType().ToString();
}