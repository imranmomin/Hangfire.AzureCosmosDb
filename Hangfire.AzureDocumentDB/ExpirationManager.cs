using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.AzureDocumentDB.Entities;

namespace Hangfire.AzureDocumentDB
{
#pragma warning disable 618
    internal class ExpirationManager : IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.For<ExpirationManager>();
        private const string distributedLockKey = "expirationmanager";
        private static readonly TimeSpan defaultLockTimeout = TimeSpan.FromMinutes(5);
        private static readonly string[] documents = { "locks", "jobs", "lists", "sets", "hashs", "counters/aggregrated" };
        private readonly AzureDocumentDbConnection connection;
        private readonly TimeSpan checkInterval;

        public ExpirationManager(AzureDocumentDbStorage storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            connection = (AzureDocumentDbConnection)storage.GetConnection();
            checkInterval = storage.Options.ExpirationCheckInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (string document in documents)
            {
                Logger.Debug($"Removing outdated records from the '{document}' document.");

                using (new AzureDocumentDbDistributedLock(distributedLockKey, defaultLockTimeout, connection.Client))
                {
                    FirebaseResponse respone = connection.Client.Get($"{document}");
                    if (respone.StatusCode == System.Net.HttpStatusCode.OK)
                    {
                        Dictionary<string, FireEntity> collection = respone.ResultAs<Dictionary<string, FireEntity>>();
                        string[] references = collection?.Where(c => c.Value.ExpireOn.HasValue && c.Value.ExpireOn < DateTime.UtcNow).Select(c => c.Key).ToArray();
                        if (references != null && references.Length > 0)
                        {
                            ParallelOptions options = new ParallelOptions { CancellationToken = cancellationToken };
                            Parallel.ForEach(references, options, (reference) =>
                            {
                                options.CancellationToken.ThrowIfCancellationRequested();
                                connection.Client.Delete($"{document}/{reference}");
                            });
                        }
                    }
                }

                Logger.Trace($"Outdated records removed from the '{document}' document.");
                cancellationToken.WaitHandle.WaitOne(checkInterval);
            }
        }

    }
}