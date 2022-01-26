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
public class ExpirationManager : IServerComponent
#pragma warning restore 618
{
	private const string DISTRIBUTED_LOCK_KEY = "locks:expiration:manager";
	private static readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(5);
	private readonly DocumentTypes[] documents = { DocumentTypes.Lock, DocumentTypes.Job, DocumentTypes.List, DocumentTypes.Set, DocumentTypes.Hash, DocumentTypes.Counter, DocumentTypes.State };
	private readonly ILog logger = LogProvider.For<ExpirationManager>();
	private readonly CosmosDbStorage storage;

	public ExpirationManager(CosmosDbStorage storage)
	{
		this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
	}

	public void Execute(CancellationToken cancellationToken)
	{
		bool isComplete = false;

		while (cancellationToken.IsCancellationRequested == false && isComplete == false)
		{
			CosmosDbDistributedLock? distributedLock = null;

			try
			{
				// check if the token was cancelled
				cancellationToken.ThrowIfCancellationRequested();

				// get the distributed lock
				distributedLock = new CosmosDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage);

				foreach (DocumentTypes type in documents)
				{
					cancellationToken.ThrowIfCancellationRequested();

					logger.Trace($"Removing outdated records from the [{type}] document.");

					int expireOn = DateTime.UtcNow.ToEpoch();
					string query = $"SELECT * FROM doc WHERE doc.type = {(int)type} AND IS_DEFINED(doc.expire_on) AND doc.expire_on < {expireOn}";

					// remove only the aggregate counters when the type is Counter
					if (type == DocumentTypes.Counter) query += $" AND doc.counterType = {(int)CounterTypes.Aggregate}";

					int deleted = storage.Container.ExecuteDeleteDocuments(query, new PartitionKey((int)type));

					logger.Trace($"Outdated [{deleted}] records removed from the [{type}] document.");
				}

				isComplete = true;
			}
			catch (CosmosDbDistributedLockException exception) when (exception.Key == DISTRIBUTED_LOCK_KEY)
			{
				logger.Debug($@"An exception was thrown during acquiring distributed lock on the [{DISTRIBUTED_LOCK_KEY}] resource within [{defaultLockTimeout.TotalSeconds}] seconds." +
				             @"Outdated records were not removed. It will be retried in [5] seconds.");

				// wait for 5 seconds
				Thread.Sleep(5000);
			}
			finally
			{
				distributedLock?.Dispose();
			}
		}

		// wait for the interval specified
		cancellationToken.WaitHandle.WaitOne(storage.StorageOptions.ExpirationCheckInterval);
	}
}