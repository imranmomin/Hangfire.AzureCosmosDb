using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Queue;
using Hangfire.States;
using Hangfire.Storage;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure;

internal class CosmosDbWriteOnlyTransaction : JobStorageTransaction
{
	private readonly List<Action> commands = new();
	private readonly CosmosDbConnection connection;

	public CosmosDbWriteOnlyTransaction(CosmosDbConnection connection)
	{
		this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
	}

	private void QueueCommand(Action command) => commands.Add(command);

	public override void Commit()
	{
		int retry = 0;
		bool complete;
		const string resource = "locks:batch:commit";
		CosmosDbDistributedLock? distributedLock = null;
		int commandIndex = 0;

		do
		{
			complete = true;

			try
			{
				distributedLock = new CosmosDbDistributedLock(resource, connection.Storage.StorageOptions.TransactionalLockTimeout, connection.Storage);

				do
				{
					Action command = commands.ElementAt(commandIndex);
					command.Invoke();
					commandIndex += 1;
				} while (commandIndex <= commands.Count - 1);

				// clear the commands array
				commands.Clear();
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

	public override void Dispose() { }

	#region Queue

	public override void AddToQueue(string queue, string jobId)
	{
		if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
		if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

		QueueCommand(() =>
		{
			IPersistentJobQueueProvider provider = connection.QueueProviders.GetProvider(queue);
			IPersistentJobQueue persistentQueue = provider.GetJobQueue();
			persistentQueue.Enqueue(queue, jobId);
		});
	}

	#endregion

	#region Counter

	public override void DecrementCounter(string key)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

		QueueCommand(() =>
		{
			Counter data = new()
			{
				Key = key,
				Type = CounterTypes.Raw,
				Value = -1
			};

			connection.Storage.Container.CreateItemWithRetries(data, PartitionKeys.Counter);
		});
	}

	public override void DecrementCounter(string key, TimeSpan expireIn)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
		if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));

		QueueCommand(() =>
		{
			Counter data = new()
			{
				Key = key,
				Type = CounterTypes.Raw,
				Value = -1,
				ExpireOn = DateTime.UtcNow.Add(expireIn)
			};

			connection.Storage.Container.CreateItemWithRetries(data, PartitionKeys.Counter);
		});
	}

	public override void IncrementCounter(string key)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

		QueueCommand(() =>
		{
			Counter data = new()
			{
				Key = key,
				Type = CounterTypes.Raw,
				Value = 1
			};

			connection.Storage.Container.CreateItemWithRetries(data, PartitionKeys.Counter);
		});
	}

	public override void IncrementCounter(string key, TimeSpan expireIn)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
		if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));

		QueueCommand(() =>
		{
			Counter data = new()
			{
				Key = key,
				Type = CounterTypes.Raw,
				Value = 1,
				ExpireOn = DateTime.UtcNow.Add(expireIn)
			};

			connection.Storage.Container.CreateItemWithRetries(data, PartitionKeys.Counter);
		});
	}

	#endregion

	#region Job

	public override void ExpireJob(string jobId, TimeSpan expireIn)
	{
		if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
		if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));
		int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();

		QueueCommand(() =>
		{
			int retry = 0;
			bool complete;
			const string resource = "locks:job:update";
			CosmosDbDistributedLock? distributedLock = null;

			do
			{
				complete = true;

				try
				{
					distributedLock = new CosmosDbDistributedLock(resource, connection.Storage.StorageOptions.TransactionalLockTimeout, connection.Storage);
					PatchOperation[] patchOperations = { PatchOperation.Set("/expire_on", epoch) };
					connection.Storage.Container.PatchItemWithRetries<Job>(jobId, PartitionKeys.Job, patchOperations);
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
		});

		// we need to also remove the state documents
		QueueCommand(() =>
		{
			QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.State };
			QueryDefinition sql = new($"SELECT VALUE doc.id FROM doc WHERE doc.job_id = '{jobId}'");

			string[] records = connection.Storage.Container.GetItemQueryIterator<string>(sql, requestOptions: requestOptions)
				.ToQueryResult()
				.ToArray();

			ExpireDocuments<State>(epoch, records, PartitionKeys.State);
		});
	}

	public override void PersistJob(string jobId)
	{
		if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

		QueueCommand(() =>
		{
			int retry = 0;
			bool complete;
			const string resource = "locks:job:update";
			CosmosDbDistributedLock? distributedLock = null;

			do
			{
				complete = true;

				try
				{
					distributedLock = new CosmosDbDistributedLock(resource, connection.Storage.StorageOptions.TransactionalLockTimeout, connection.Storage);
					PatchOperation[] patchOperations = { PatchOperation.Remove("/expire_on") };
					connection.Storage.Container.PatchItemWithRetries<Job>(jobId, PartitionKeys.Job, patchOperations);
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

		});
	}

	#endregion

	#region State

	public override void SetJobState(string jobId, IState state)
	{
		if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
		if (state == null) throw new ArgumentNullException(nameof(state));

		QueueCommand(() =>
		{
			int retry = 0;
			bool complete;
			const string resource = "locks:job:update";
			CosmosDbDistributedLock? distributedLock = null;

			do
			{
				complete = true;

				try
				{
					distributedLock = new CosmosDbDistributedLock(resource, connection.Storage.StorageOptions.TransactionalLockTimeout, connection.Storage);

					State data = new()
					{
						JobId = jobId,
						Name = state.Name,
						Reason = state.Reason,
						CreatedOn = DateTime.UtcNow,
						Data = state.SerializeData()
					};

					connection.Storage.Container.CreateItemWithRetries(data, PartitionKeys.State);

					PatchOperation[] patchOperations =
					{
						PatchOperation.Set("/state_id", data.Id),
						PatchOperation.Set("/state_name", data.Name)
					};

					connection.Storage.Container.PatchItemWithRetries<Job>(jobId, PartitionKeys.Job, patchOperations);
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
		});
	}

	public override void AddJobState(string jobId, IState state)
	{
		if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
		if (state == null) throw new ArgumentNullException(nameof(state));

		QueueCommand(() =>
		{
			State data = new()
			{
				JobId = jobId,
				Name = state.Name,
				Reason = state.Reason,
				CreatedOn = DateTime.UtcNow,
				Data = state.SerializeData()
			};

			connection.Storage.Container.CreateItemWithRetries(data, PartitionKeys.State);
		});
	}

	#endregion

	#region Set

	public override void RemoveFromSet(string key, string value)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
		if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

		QueueCommand(() =>
		{
			string[] sets = connection.Storage.Container.GetItemLinqQueryable<List>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Set })
				.Where(s => s.Key == key)
				.Select(s => new { s.Id, s.Value })
				.ToQueryResult()
				.Where(s => s.Value == value) // value may contain json string.. which interfere with query 
				.Select(s => s.Id)
				.ToArray();

			if (sets.Length == 0) return;

			string ids = string.Join(",", sets.Select(s => $"'{s}'"));
			string query = $"SELECT doc._self FROM doc WHERE doc.id IN ({ids})";
			connection.Storage.Container.ExecuteDeleteDocuments(query, PartitionKeys.Set);
		});
	}

	public override void AddToSet(string key, string value) => AddToSet(key, value, 0.0);

	public override void AddToSet(string key, string value, double score)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
		if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

		QueueCommand(() =>
		{
			List<Set> sets = connection.Storage.Container.GetItemLinqQueryable<Set>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Set })
				.Where(s => s.Key == key)
				.ToQueryResult()
				.Where(s => s.Value == value) // value may contain json string.. which interfere with query 
				.ToList();

			if (sets.Count == 0)
			{
				sets.Add(new Set
				{
					Key = key,
					Value = value,
					Score = score,
					CreatedOn = DateTime.UtcNow
				});
			}
			else
				// set the sets score
				sets.ForEach(s => s.Score = score);

			Data<Set> data = new(sets);
			connection.Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Set);
		});
	}

	public override void PersistSet(string key)
	{
		if (key == null) throw new ArgumentNullException(nameof(key));
		QueueCommand(() => PersistDocuments<Set>(key, PartitionKeys.Set));
	}

	public override void ExpireSet(string key, TimeSpan expireIn)
	{
		if (key == null) throw new ArgumentNullException(nameof(key));

		QueueCommand(() =>
		{
			int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();
			QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
			QueryDefinition sql = new($"SELECT VALUE doc.id FROM doc WHERE doc.key = '{key}'");

			string[] records = connection.Storage.Container.GetItemQueryIterator<string>(sql, requestOptions: requestOptions)
				.ToQueryResult()
				.ToArray();

			ExpireDocuments<Set>(epoch, records, PartitionKeys.Set);
		});
	}

	public override void AddRangeToSet(string key, IList<string> items)
	{
		if (key == null) throw new ArgumentNullException(nameof(key));
		if (items == null) throw new ArgumentNullException(nameof(items));

		QueueCommand(() =>
		{
			List<Set> sets = items.Select(value => new Set
			{
				Key = key,
				Value = value,
				Score = 0.00,
				CreatedOn = DateTime.UtcNow
			}).ToList();

			Data<Set> data = new(sets);
			connection.Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Set);
		});
	}

	public override void RemoveSet(string key)
	{
		if (key == null) throw new ArgumentNullException(nameof(key));

		QueueCommand(() =>
		{
			string query = $"SELECT doc._self FROM doc WHERE doc.key = '{key}'";
			connection.Storage.Container.ExecuteDeleteDocuments(query, PartitionKeys.Set);
		});
	}

	#endregion

	#region Hash

	public override void RemoveHash(string key)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

		QueueCommand(() =>
		{
			string query = $"SELECT doc._self FROM doc WHERE doc.key = '{key}'";
			connection.Storage.Container.ExecuteDeleteDocuments(query, PartitionKeys.Hash);
		});
	}

	public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string?>> keyValuePairs)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
		if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

		QueueCommand(() =>
		{
			Data<Hash> data = new();

			List<Hash> hashes = connection.Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Hash })
				.Where(h => h.Key == key)
				.ToQueryResult()
				.ToList();

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
						connection.Storage.Container.ExecuteDeleteDocuments(query, PartitionKeys.Hash);
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

			connection.Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);
		});
	}

	public override void ExpireHash(string key, TimeSpan expireIn)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

		QueueCommand(() =>
		{
			int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();
			QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Hash };
			QueryDefinition sql = new($"SELECT VALUE doc.id FROM doc WHERE doc.key = '{key}'");

			string[] records = connection.Storage.Container.GetItemQueryIterator<string>(sql, requestOptions: requestOptions)
				.ToQueryResult()
				.ToArray();

			ExpireDocuments<Hash>(epoch, records, PartitionKeys.Hash);
		});
	}

	public override void PersistHash(string key)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
		QueueCommand(() => PersistDocuments<Hash>(key, PartitionKeys.Hash));
	}

	#endregion

	#region List

	public override void InsertToList(string key, string value)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
		if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

		QueueCommand(() =>
		{
			List data = new()
			{
				Key = key,
				Value = value,
				CreatedOn = DateTime.UtcNow
			};

			connection.Storage.Container.CreateItemWithRetries(data, PartitionKeys.List);
		});
	}

	public override void RemoveFromList(string key, string value)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
		if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

		QueueCommand(() =>
		{
			string[] lists = connection.Storage.Container.GetItemLinqQueryable<List>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.List })
				.Where(l => l.Key == key)
				.Select(l => new { l.Id, l.Value })
				.ToQueryResult()
				.Where(l => l.Value == value)
				.Select(l => l.Id)
				.ToArray();

			if (lists.Length == 0) return;

			string ids = string.Join(",", lists.Select(l => $"'{l}'"));
			string query = $"SELECT doc._self FROM doc WHERE doc.id IN ({ids})";
			connection.Storage.Container.ExecuteDeleteDocuments(query, PartitionKeys.List);
		});
	}

	public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
	{
		if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

		QueueCommand(() =>
		{
			string[] lists = connection.Storage.Container.GetItemLinqQueryable<List>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.List })
				.Where(l => l.Key == key)
				.OrderByDescending(l => l.CreatedOn)
				.Select(l => l.Id)
				.ToQueryResult()
				.Select((l, index) => new { Id = l, index })
				.Where(l => l.index < keepStartingFrom || l.index > keepEndingAt)
				.Select(l => l.Id)
				.ToArray();

			if (lists.Length == 0) return;

			string ids = string.Join(",", lists.Select(l => $"'{l}'"));
			string query = $"SELECT doc._self FROM doc WHERE doc.id IN ({ids})";
			connection.Storage.Container.ExecuteDeleteDocuments(query, PartitionKeys.List);
		});
	}

	public override void ExpireList(string key, TimeSpan expireIn)
	{
		if (key == null) throw new ArgumentNullException(nameof(key));

		QueueCommand(() =>
		{
			int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();
			QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
			QueryDefinition sql = new($"SELECT VALUE doc.id FROM doc WHERE doc.key = '{key}'");

			string[] records = connection.Storage.Container.GetItemQueryIterator<string>(sql, requestOptions: requestOptions)
				.ToQueryResult()
				.ToArray();

			ExpireDocuments<List>(epoch, records, PartitionKeys.List);
		});
	}

	public override void PersistList(string key)
	{
		if (key == null) throw new ArgumentNullException(nameof(key));
		QueueCommand(() => PersistDocuments<List>(key, PartitionKeys.List));
	}

	#endregion

	#region PRIVATE

	private void PersistDocuments<T>(string key, PartitionKey partitionKey)
	{
		QueryRequestOptions requestOptions = new() { PartitionKey = partitionKey };
		QueryDefinition sql = new($"SELECT VALUE doc.id FROM doc WHERE doc.key = '{key}' AND IS_DEFINED(doc.expire_on)");

		string[] records = connection.Storage.Container.GetItemQueryIterator<string>(sql, requestOptions: requestOptions)
			.ToQueryResult()
			.ToArray();

		if (records.Length == 0) return;

		PatchOperation[] patchOperations = { PatchOperation.Remove("/expire_on") };

		if (records.Length == 1)
		{
			string id = records.First();
			PatchItemRequestOptions patchItemRequestOptions = new() { FilterPredicate = $"FROM doc WHERE doc.key = '{key}' AND IS_DEFINED(doc.expire_on)" };
			connection.Storage.Container.PatchItemWithRetries<T>(id, partitionKey, patchOperations, patchItemRequestOptions);
		}
		else
		{
			TransactionalBatchPatchItemRequestOptions batchPatchItemRequestOptions = new() { FilterPredicate = $"FROM doc WHERE doc.key = '{key}' AND IS_DEFINED(doc.expire_on)" };
			TransactionalBatch transactionalBatch = connection.Storage.Container.CreateTransactionalBatch(partitionKey);
			foreach (string id in records)
			{
				transactionalBatch.PatchItem(id, patchOperations, batchPatchItemRequestOptions);
			}
			transactionalBatch.ExecuteAsync().ExecuteWithRetriesAsync().ExecuteSynchronously();
		}
	}

	private void ExpireDocuments<T>(int expireOn, string[] records, PartitionKey partitionKey)
	{
		if (records.Length == 0) return;

		PatchOperation[] patchOperations = { PatchOperation.Set("/expire_on", expireOn) };

		if (records.Length == 1)
		{
			string id = records.First();
			PatchItemRequestOptions patchItemRequestOptions = new() { FilterPredicate = $"FROM doc WHERE NOT IS_DEFINED(doc.expire_on) OR doc.expire_on != {expireOn}" };
			connection.Storage.Container.PatchItemWithRetries<T>(id, partitionKey, patchOperations, patchItemRequestOptions);
		}
		else
		{
			TransactionalBatchPatchItemRequestOptions batchPatchItemRequestOptions = new() { FilterPredicate = $"FROM doc WHERE NOT IS_DEFINED(doc.expire_on) OR doc.expire_on != {expireOn}" };
			TransactionalBatch transactionalBatch = connection.Storage.Container.CreateTransactionalBatch(partitionKey);
			foreach (string id in records)
			{
				transactionalBatch.PatchItem(id, patchOperations, batchPatchItemRequestOptions);
			}
			transactionalBatch.ExecuteAsync().ExecuteWithRetriesAsync().ExecuteSynchronously();
		}
	}

	#endregion
}