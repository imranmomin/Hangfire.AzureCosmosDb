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
    private readonly ILog logger = LogProvider.For<ExpirationManager>();
    private const string DISTRIBUTED_LOCK_KEY = "locks:expiration:manager";
    private static readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(5);
    private readonly DocumentTypes[] documents = { DocumentTypes.Lock, DocumentTypes.Job, DocumentTypes.List, DocumentTypes.Set, DocumentTypes.Hash, DocumentTypes.Counter, DocumentTypes.State };
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
            distributedLock = new CosmosDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage);

            foreach (DocumentTypes type in documents)
            {
                cancellationToken.ThrowIfCancellationRequested();

                logger.Trace($"Removing outdated records from the [{type}] document.");

                string query = $"SELECT doc._self FROM doc WHERE doc.type = {(int)type} AND IS_DEFINED(doc.expire_on) AND doc.expire_on < {expireOn}";

                // remove only the aggregate counters when the type is Counter
                if (type == DocumentTypes.Counter)
                {
                    query += $" AND doc.counterType = {(int)CounterTypes.Aggregate}";
                }

                int deleted = storage.Container.ExecuteDeleteDocuments(query, new PartitionKey((int)type), cancellationToken);

                logger.Trace($"Outdated [{deleted}] records removed from the [{type}] document.");
            }

            cancellationToken.WaitHandle.WaitOne(storage.StorageOptions.ExpirationCheckInterval);
        }
        catch (CosmosDbDistributedLockException exception) when (exception.Key == DISTRIBUTED_LOCK_KEY)
        {
            logger.Debug($@"An exception was thrown during acquiring distributed lock on the [{DISTRIBUTED_LOCK_KEY}] resource within [{defaultLockTimeout.TotalSeconds}] seconds." +
                         $@"Outdated records were not removed. It will be retried in [{storage.StorageOptions.ExpirationCheckInterval.TotalSeconds}] seconds.");
        }
        finally
        {
            distributedLock?.Dispose();
        }
    }
}