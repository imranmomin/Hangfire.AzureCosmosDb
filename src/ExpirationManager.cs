using System;
using System.Threading;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Hangfire.Server;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure;

#pragma warning disable 618
internal class ExpirationManager : IServerComponent
#pragma warning restore 618
{
	private const string DISTRIBUTED_LOCK_KEY = "locks:expiration:manager";
	private readonly DocumentTypes[] documents = { DocumentTypes.Lock, DocumentTypes.Job, DocumentTypes.List, DocumentTypes.Set, DocumentTypes.Hash, DocumentTypes.Counter, DocumentTypes.State };
	private readonly ILog logger = LogProvider.For<ExpirationManager>();
	private readonly CosmosDbStorage storage;

	public ExpirationManager(CosmosDbStorage storage)
	{
		this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
	}

	public void Execute(CancellationToken cancellationToken)
	{
		CosmosDbDistributedLock? distributedLock = null;
		int expireOn = DateTime.UtcNow.ToEpoch();

		try
		{
			// check if the token was cancelled
			cancellationToken.ThrowIfCancellationRequested();

			// get the distributed lock
			distributedLock = new CosmosDbDistributedLock(DISTRIBUTED_LOCK_KEY, storage.StorageOptions.ExpirationCheckInterval, storage);

			foreach (DocumentTypes type in documents)
			{
				cancellationToken.ThrowIfCancellationRequested();

				logger.Trace($"Removing outdated records from the [{type}] table.");

				string query = $"SELECT * FROM doc WHERE IS_DEFINED(doc.expire_on) AND doc.expire_on < {expireOn}";

				// remove only the aggregate counters when the type is Counter
				if (type == DocumentTypes.Counter) query += $" AND doc.counterType = {(int)CounterTypes.Aggregate}";

				int deleted = storage.Container.ExecuteDeleteDocuments(query, new PartitionKey((int)type));

				logger.Trace($"Outdated [{deleted}] records removed from the [{type}] table.");
			}
		}
		catch (CosmosDbDistributedLockException exception) when (exception.Key == DISTRIBUTED_LOCK_KEY)
		{
			logger.Debug($@"An exception was thrown during acquiring distributed lock on the [{DISTRIBUTED_LOCK_KEY}] resource within [{storage.StorageOptions.ExpirationCheckInterval.TotalSeconds}] seconds." +
			             $@"Outdated records were not removed. It will be retried in [{storage.StorageOptions.ExpirationCheckInterval.TotalSeconds}] seconds.");
		}
		finally
		{
			distributedLock?.Dispose();
		}

		// wait for the interval specified
		cancellationToken.WaitHandle.WaitOne(storage.StorageOptions.ExpirationCheckInterval);
	}
}