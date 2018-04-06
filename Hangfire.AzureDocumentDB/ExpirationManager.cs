using System;
using System.Threading;
using System.Threading.Tasks;

using Hangfire.Server;
using Hangfire.Logging;
using Hangfire.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Hangfire.Azure
{
#pragma warning disable 618
    internal class ExpirationManager : IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog logger = LogProvider.For<ExpirationManager>();
        private const string DISTRIBUTED_LOCK_KEY = "expirationmanager";
        private static readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(5);
        private static readonly DocumentTypes[] documents = { DocumentTypes.Lock, DocumentTypes.Job, DocumentTypes.List, DocumentTypes.Set, DocumentTypes.Hash, DocumentTypes.Counter };
        private readonly TimeSpan checkInterval;
        private readonly DocumentDbStorage storage;
        private readonly Uri spDeleteExpiredDocumentsUri;

        public ExpirationManager(DocumentDbStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            checkInterval = storage.Options.ExpirationCheckInterval;
            spDeleteExpiredDocumentsUri = UriFactory.CreateStoredProcedureUri(storage.Options.DatabaseName, storage.Options.CollectionName, "deleteExpiredDocuments");
        }

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (DocumentTypes type in documents)
            {
                logger.Debug($"Removing outdated records from the '{type}' document.");

                using (new DocumentDbDistributedLock(DISTRIBUTED_LOCK_KEY, defaultLockTimeout, storage))
                {
                    Task<StoredProcedureResponse<int>> procedureTask = storage.Client.ExecuteStoredProcedureAsync<int>(spDeleteExpiredDocumentsUri, type);
                    Task task = procedureTask.ContinueWith(t => logger.Trace($"Outdated records removed {t.Result.Response} records from the '{type}' document."), TaskContinuationOptions.OnlyOnRanToCompletion);
                    task.Wait(cancellationToken);
                }

                cancellationToken.WaitHandle.WaitOne(checkInterval);
            }
        }
    }
}