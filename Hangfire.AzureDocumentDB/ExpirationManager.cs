using System;
using System.Threading;

using Hangfire.Server;
using Hangfire.Logging;

using Hangfire.Azure.Helper;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;

namespace Hangfire.Azure
{
#pragma warning disable 618
    internal class ExpirationManager : IServerComponent
#pragma warning restore 618
    {
        private readonly ILog logger = LogProvider.For<ExpirationManager>();
        private const string DISTRIBUTED_LOCK_KEY = "locks:expiration:manager";
        private readonly TimeSpan defaultLockTimeout;
        private readonly DocumentTypes[] documents = { DocumentTypes.Lock, DocumentTypes.Job, DocumentTypes.List, DocumentTypes.Set, DocumentTypes.Hash, DocumentTypes.Counter };
        private readonly DocumentDbStorage storage;

        public ExpirationManager(DocumentDbStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            defaultLockTimeout = TimeSpan.FromSeconds(30) + storage.Options.ExpirationCheckInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            using (new DocumentDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
            {
                int expireOn = DateTime.UtcNow.ToEpoch();

                foreach (DocumentTypes type in documents)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    logger.Trace($"Removing outdated records from the '{type}' document.");

                    string query = $"SELECT doc._self FROM doc WHERE doc.type = {(int)type} AND IS_DEFINED(doc.expire_on) AND doc.expire_on < {expireOn}";

                    // remove only the aggregate counters when the type is Counter
                    if (type == DocumentTypes.Counter)
                    {
                        query += $" AND doc.counter_type = {(int)CounterTypes.Aggregate}";
                    }

                    int deleted = storage.Client.ExecuteDeleteDocuments(query);
                    
                    logger.Trace($"Outdated {deleted} records removed from the '{type}' document.");
                }
            }

            cancellationToken.WaitHandle.WaitOne(storage.Options.ExpirationCheckInterval);
        }
    }
}