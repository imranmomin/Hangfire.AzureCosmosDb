using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Queue;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Microsoft.Azure.Cosmos;
using Job = Hangfire.Common.Job;

namespace Hangfire.Azure;

internal sealed class CosmosDbConnection : JobStorageConnection
{
	public CosmosDbConnection(CosmosDbStorage storage)
	{
		Storage = storage ?? throw new ArgumentNullException(nameof(storage));
		QueueProviders = storage.QueueProviders;
	}

	public CosmosDbStorage Storage { get; }
	public PersistentJobQueueProviderCollection QueueProviders { get; }

	public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
	{
		if (string.IsNullOrWhiteSpace(resource)) throw new ArgumentNullException(nameof(resource));
		return new CosmosDbDistributedLock(resource, timeout, Storage);
	}

	public override IWriteOnlyTransaction CreateWriteTransaction() => new CosmosDbWriteOnlyTransaction(this);

	#region Job

	public override string CreateExpiredJob(Job job, IDictionary<string, string?> parameters, DateTime createdAt, TimeSpan expireIn)
	{
		if (job == null) throw new ArgumentNullException(nameof(job));
		if (parameters == null) throw new ArgumentNullException(nameof(parameters));

		InvocationData invocationData = InvocationData.SerializeJob(job);
		Documents.Job entityJob = new()
		{
			InvocationData = invocationData,
			Arguments = invocationData.Arguments,
			CreatedOn = createdAt,
			ExpireOn = createdAt.Add(expireIn),
			Parameters = parameters.Select(p => new Parameter
			{
				Name = p.Key,
				Value = p.Value
			}).ToArray()
		};

		Task<ItemResponse<Documents.Job>> task = Storage.Container.CreateItemWithRetriesAsync(entityJob, PartitionKeys.Job);
		task.Wait();

		return entityJob.Id;
	}

	public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
	{
		if (queues == null) throw new ArgumentNullException(nameof(queues));
		if (queues.Length == 0) throw new ArgumentNullException(nameof(queues));

		IPersistentJobQueueProvider[] providers = queues.Select(q => QueueProviders.GetProvider(q))
			.Distinct()
			.ToArray();

		if (providers.Length != 1) throw new InvalidOperationException($"Multiple provider instances registered for queues: [{string.Join(", ", queues)}]. You should choose only one type of persistent queues per server instance.");

		IPersistentJobQueue persistentQueue = providers.Single().GetJobQueue();
		IFetchedJob queue = persistentQueue.Dequeue(queues, cancellationToken);
		return queue;
	}

	public override JobData? GetJobData(string? jobId)
	{
		if (jobId == null) throw new ArgumentNullException(nameof(jobId));
		if (Guid.TryParse(jobId, out Guid _) == false) return null;

		try
		{
			Task<ItemResponse<Documents.Job>> task = Storage.Container.ReadItemWithRetriesAsync<Documents.Job>(jobId, PartitionKeys.Job);
			task.Wait();

			Documents.Job data = task.Result.Resource;
			InvocationData invocationData = data.InvocationData;
			invocationData.Arguments = data.Arguments;

			Job? job = null;
			JobLoadException? loadException = null;

			try
			{
				job = invocationData.DeserializeJob();
			}
			catch (JobLoadException ex)
			{
				loadException = ex;
			}

			return new JobData
			{
				Job = job,
				State = data.StateName,
				CreatedAt = data.CreatedOn.ToLocalTime(),
				LoadException = loadException
			};
		}
		catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
		{
			/* ignored */
		}

		return null;
	}

	public override StateData? GetStateData(string? jobId)
	{
		if (jobId == null) throw new ArgumentNullException(nameof(jobId));
		if (Guid.TryParse(jobId, out Guid _) == false) return null;

		try
		{
			Task<ItemResponse<Documents.Job>> task = Storage.Container.ReadItemWithRetriesAsync<Documents.Job>(jobId, PartitionKeys.Job);
			task.Wait();

			Documents.Job job = task.Result.Resource;

			// get the state document
			Task<ItemResponse<State>> stateTask = Storage.Container.ReadItemWithRetriesAsync<State>(job.StateId, PartitionKeys.State);
			stateTask.Wait();

			State state = stateTask.Result.Resource;
			return new StateData
			{
				Name = state.Name,
				Reason = state.Reason,
				Data = state.Data
			};
		}
		catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
		{
			/* ignored */
		}

		return null;
	}

	#endregion

	#region Parameter

	public override string? GetJobParameter(string id, string name)
	{
		if (string.IsNullOrWhiteSpace(id)) throw new ArgumentNullException(nameof(id));
		if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException(nameof(name));
		if (Guid.TryParse(id, out Guid _) == false) return null;

		try
		{
			Task<ItemResponse<Documents.Job>> task = Storage.Container.ReadItemWithRetriesAsync<Documents.Job>(id, PartitionKeys.Job);
			task.Wait();

			Documents.Job data = task.Result.Resource;
			return data.Parameters.Where(p => p.Name == name).Select(p => p.Value).FirstOrDefault();
		}
		catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
		{
			/* ignored */
		}

		return null;
	}

	public override void SetJobParameter(string id, string name, string value)
	{
		if (string.IsNullOrWhiteSpace(id)) throw new ArgumentNullException(nameof(id));
		if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException(nameof(name));

		int retry = 0;
		bool complete;
		const string resource = "locks:job:update";
		CosmosDbDistributedLock? distributedLock = null;

		do
		{
			complete = true;

			try
			{
				distributedLock = new CosmosDbDistributedLock(resource, Storage.StorageOptions.TransactionalLockTimeout, Storage);

				Task<ItemResponse<Documents.Job>> readTask = Storage.Container.ReadItemWithRetriesAsync<Documents.Job>(id, PartitionKeys.Job);
				readTask.Wait();

				Documents.Job data = readTask.Result.Resource;
				int index = Array.FindIndex(data.Parameters, x => x.Name == name);
				index = index == -1 ? data.Parameters.Length : index;

				PatchItemRequestOptions patchItemRequestOptions = new() { IfMatchEtag = data.ETag };
				PatchOperation[] patchOperations =
				{
					PatchOperation.Set($"/parameters/{index}", new Parameter { Name = name, Value = value })
				};

				Task<ItemResponse<Documents.Job>> task = Storage.Container.PatchItemWithRetriesAsync<Documents.Job>(id, PartitionKeys.Job, patchOperations, patchItemRequestOptions);
				task.Wait();
			}
			catch (CosmosDbDistributedLockException ex) when (ex.Key == resource)
			{
				/* ignore */
				retry += 1;
				complete = false;
			}
			finally
			{
				distributedLock?.Dispose();
			}

		} while (retry <= 3 && complete == false);
	}

	#endregion

	#region Set

	public override TimeSpan GetSetTtl(string key)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

		QueryDefinition sql = new QueryDefinition("SELECT TOP 1 VALUE MIN(doc['expire_on']) FROM doc WHERE doc.key = @key")
			.WithParameter("@key", key);

		int? expireOn = Storage.Container.GetItemQueryIterator<int?>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Set })
			.ToQueryResult()
			.FirstOrDefault();

		return expireOn.HasValue ? expireOn.Value.ToDateTime() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
	}

	public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

		List<Set> result = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Set })
			.Where(s => s.Key == key)
			.ToQueryResult()
			.ToList();

		return result
			.OrderBy(x => x.Score)
			.Select((x, i) => new { x.Value, row = i + 1 })
			.Where(x => x.row >= startingFrom + 1 && x.row <= endingAt + 1)
			.Select(s => s.Value)
			.ToList();
	}

	public override long GetCounter(string key)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

		QueryDefinition sql = new QueryDefinition("SELECT TOP 1 VALUE SUM(doc['value']) FROM doc WHERE doc.key = @key")
			.WithParameter("@key", key);

		return Storage.Container.GetItemQueryIterator<long>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Counter })
			.ToQueryResult()
			.FirstOrDefault();
	}

	public override long GetSetCount(string key)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

		QueryDefinition sql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.key = @key")
			.WithParameter("@key", key);

		return Storage.Container.GetItemQueryIterator<long>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Set })
			.ToQueryResult()
			.FirstOrDefault();
	}

	public override HashSet<string> GetAllItemsFromSet(string key)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

		IEnumerable<string> sets = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Set })
			.Where(s => s.Key == key)
			.Select(s => s.Value)
			.ToQueryResult();

		return new HashSet<string>(sets);
	}

	public override string? GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore) => GetFirstByLowestScoreFromSet(key, fromScore, toScore, 1).FirstOrDefault();

	public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));
		if (count <= 0) throw new ArgumentException("The value must be a positive number", nameof(count));
		if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.", nameof(toScore));

		QueryDefinition sql = new QueryDefinition($"SELECT TOP {count} VALUE doc['value'] FROM doc WHERE doc.key = @key AND (doc.score BETWEEN @from AND @to) ORDER BY doc.score")
			.WithParameter("@key", key)
			.WithParameter("@from", fromScore)
			.WithParameter("@to", toScore);

		return Storage.Container.GetItemQueryIterator<string>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Set })
			.ToQueryResult()
			.ToList();
	}

	#endregion

	#region Server

	public override void AnnounceServer(string serverId, ServerContext context)
	{
		if (string.IsNullOrWhiteSpace(serverId)) throw new ArgumentNullException(nameof(serverId));
		if (context == null) throw new ArgumentNullException(nameof(context));

		Documents.Server server = new()
		{
			Id = serverId,
			Workers = context.WorkerCount,
			Queues = context.Queues,
			CreatedOn = DateTime.UtcNow,
			LastHeartbeat = DateTime.UtcNow
		};

		Task<ItemResponse<Documents.Server>> task = Storage.Container.UpsertItemWithRetriesAsync(server, PartitionKeys.Server);
		task.Wait();
	}

	public override void Heartbeat(string serverId)
	{
		if (string.IsNullOrWhiteSpace(serverId)) throw new ArgumentNullException(nameof(serverId));

		try
		{
			PatchOperation[] patchOperations =
			{
				PatchOperation.Set("/last_heartbeat", DateTime.UtcNow.ToEpoch())
			};
			Task<ItemResponse<Documents.Server>> task = Storage.Container.PatchItemWithRetriesAsync<Documents.Server>(serverId, PartitionKeys.Server, patchOperations);
			task.Wait();
		}
		catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
		{
			/* ignored */
		}
	}

	public override void RemoveServer(string serverId)
	{
		if (string.IsNullOrWhiteSpace(serverId)) throw new ArgumentNullException(nameof(serverId));

		try
		{
			Task<ItemResponse<Documents.Server>> task = Storage.Container.DeleteItemWithRetriesAsync<Documents.Server>(serverId, PartitionKeys.Server);
			task.Wait();
		}
		catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
		{
			/* ignored */
		}
	}

	public override int RemoveTimedOutServers(TimeSpan timeOut)
	{
		if (timeOut.Duration() != timeOut) throw new ArgumentException(@"invalid timeout", nameof(timeOut));

		int lastHeartbeat = DateTime.UtcNow.Add(timeOut.Negate()).ToEpoch();
		string query = $"SELECT * FROM doc WHERE IS_DEFINED(doc.last_heartbeat) AND doc.last_heartbeat <= {lastHeartbeat}";

		return Storage.Container.ExecuteDeleteDocuments(query, PartitionKeys.Server);
	}

	#endregion

	#region Hash

	public override Dictionary<string, string?>? GetAllEntriesFromHash(string key)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

		Dictionary<string, string?> result = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Hash })
			.Where(h => h.Key == key)
			.Select(h => new { h.Field, h.Value })
			.ToQueryResult()
			.ToDictionary(h => h.Field, h => h.Value);

		return result.Count > 0 ? result : null;
	}

	public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));
		if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

		int retry = 0;
		bool complete;
		const string resource = "locks:set:hash";
		CosmosDbDistributedLock? distributedLock = null;

		do
		{
			// ReSharper disable once RedundantAssignment
			complete = true;

			try
			{
				distributedLock = new CosmosDbDistributedLock(resource, Storage.StorageOptions.TransactionalLockTimeout, Storage);

				Data<Hash> data = new();
				List<Hash> hashes = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Hash })
					.Where(h => h.Key == key)
					.ToQueryResult()
					.ToList();

				// ReSharper disable once PossibleMultipleEnumeration
				Hash[] sources = keyValuePairs.Select(k => new Hash
				{
					Key = key,
					Field = k.Key,
					Value = k.Value.TryParseToEpoch()
				}).ToArray();

				foreach (Hash source in sources)
				{
					int count = hashes.Count(x => x.Field == source.Field);

					switch (count)
					{
						// if for some reason we find more than 1 document for the same field
						// lets remove all the documents except one
						case > 1:
						{
							Hash hash = hashes.First(x => x.Field == source.Field);
							hash.Value = source.Value;
							data.Items.Add(hash);

							string query = $"SELECT * FROM doc WHERE doc.key = '{hash.Key}' AND doc.field = '{hash.Field}' AND doc.id != '{hash.Id}'";
							Storage.Container.ExecuteDeleteDocuments(query, PartitionKeys.Hash);
							break;
						}
						case 1:
						{
							Hash hash = hashes.Single(x => x.Field == source.Field);
							if (string.Equals(hash.Value, source.Value, StringComparison.InvariantCultureIgnoreCase) == false)
							{
								hash.Value = source.Value;
								data.Items.Add(hash);
							}
							break;
						}
						case 0:
							data.Items.Add(source);
							break;
					}
				}

				Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);
				break;
			}
			catch (CosmosDbDistributedLockException exception) when (exception.Key == resource)
			{
				/* ignore */
				retry += 1;
				complete = false;
			}
			finally
			{
				distributedLock?.Dispose();
			}

		} while (retry <= 3 && complete == false);
	}

	public override long GetHashCount(string key)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

		QueryDefinition sql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.key = @key")
			.WithParameter("@key", key);

		return Storage.Container.GetItemQueryIterator<long>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Hash })
			.ToQueryResult()
			.FirstOrDefault();
	}

	public override string? GetValueFromHash(string key, string name)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));
		if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException(nameof(name));

		QueryDefinition sql = new QueryDefinition("SELECT TOP 1 VALUE doc['value'] FROM doc WHERE doc.key = @key AND doc.field = @field")
			.WithParameter("@key", key)
			.WithParameter("@field", name);

		return Storage.Container.GetItemQueryIterator<string>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Hash })
			.ToQueryResult()
			.FirstOrDefault();
	}

	public override TimeSpan GetHashTtl(string key)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

		QueryDefinition sql = new QueryDefinition("SELECT TOP 1 VALUE MIN(doc['expire_on']) FROM doc WHERE doc.key = @key")
			.WithParameter("@key", key);

		int? expireOn = Storage.Container.GetItemQueryIterator<int?>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Hash })
			.ToQueryResult()
			.FirstOrDefault();

		return expireOn.HasValue ? expireOn.Value.ToDateTime() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
	}

	#endregion

	#region List

	public override List<string> GetAllItemsFromList(string key)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

		return Storage.Container.GetItemLinqQueryable<List>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.List })
			.Where(l => l.Key == key)
			.OrderByDescending(l => l.CreatedOn)
			.Select(l => l.Value)
			.ToQueryResult()
			.ToList();
	}

	public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

		List<List> result = Storage.Container.GetItemLinqQueryable<List>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.List })
			.Where(l => l.Key == key)
			.ToQueryResult()
			.ToList();

		return result
			.OrderByDescending(x => x.CreatedOn)
			.Select((x, i) => new { x.Value, row = i + 1 })
			.Where(x => x.row >= startingFrom + 1 && x.row <= endingAt + 1)
			.Select(s => s.Value)
			.ToList();
	}

	public override TimeSpan GetListTtl(string key)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

		QueryDefinition sql = new QueryDefinition("SELECT TOP 1 VALUE MIN(doc['expire_on']) FROM doc WHERE doc.key = @key")
			.WithParameter("@key", key);

		int? expireOn = Storage.Container.GetItemQueryIterator<int?>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.List })
			.ToQueryResult()
			.FirstOrDefault();

		return expireOn.HasValue ? expireOn.Value.ToDateTime() - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
	}

	public override long GetListCount(string key)
	{
		if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

		QueryDefinition sql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.key = @key")
			.WithParameter("@key", key);

		return Storage.Container.GetItemQueryIterator<long>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.List })
			.ToQueryResult()
			.FirstOrDefault();
	}

	#endregion
}