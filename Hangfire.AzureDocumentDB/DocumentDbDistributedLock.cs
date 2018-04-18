using System;
using System.Net;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;

namespace Hangfire.Azure
{
    internal class DocumentDbDistributedLock : IDisposable
    {
        private readonly DocumentDbStorage storage;
        private string resourceId;
        private readonly object syncLock = new object();

        public DocumentDbDistributedLock(string resource, TimeSpan timeout, DocumentDbStorage storage)
        {
            this.storage = storage;
            Acquire(resource, timeout);
        }

        public void Dispose() => Relase();

        private void Acquire(string name, TimeSpan timeout)
        {
            System.Diagnostics.Stopwatch acquireStart = new System.Diagnostics.Stopwatch();
            acquireStart.Start();

            while (string.IsNullOrEmpty(resourceId))
            {
                SqlQuerySpec sql = new SqlQuerySpec
                {
                    QueryText = "SELECT TOP 1 1 FROM doc WHERE doc.type = @type AND doc.name = @name AND doc.expire_on > @expireOn",
                    Parameters = new SqlParameterCollection
                    {
                        new SqlParameter("@name", name),
                        new SqlParameter("@type", DocumentTypes.Lock),
                        new SqlParameter("@expireOn", DateTime.UtcNow.ToEpoch())
                    }
                };

                bool exists = storage.Client.CreateDocumentQuery(storage.CollectionUri, sql).AsEnumerable().Any();
                if (exists == false)
                {
                    Lock @lock = new Lock { Name = name, ExpireOn = DateTime.UtcNow.Add(timeout) };
                    Task<ResourceResponse<Document>> task = storage.Client.CreateDocumentAsync(storage.CollectionUri, @lock);
                    Task continueTask = task.ContinueWith(t =>
                    {
                        if (t.Result.StatusCode == HttpStatusCode.Created)
                        {
                            resourceId = @lock.Id;
                        }
                    }, TaskContinuationOptions.OnlyOnRanToCompletion);
                    continueTask.Wait();
                }

                // check the timeout
                if (acquireStart.ElapsedMilliseconds > timeout.TotalMilliseconds)
                {
                    throw new DocumentDbDistributedLockException($"Could not place a lock on the resource '{name}': Lock timeout.");
                }

                // sleep for 1000 millisecond
                System.Threading.Thread.Sleep(1000);
            }
        }

        private void Relase()
        {
            if (!string.IsNullOrEmpty(resourceId))
            {
                lock (syncLock)
                {
                    Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, resourceId);
                    Task<string> task = storage.Client.DeleteDocumentAsync(uri).ContinueWith(t => resourceId = string.Empty);
                    task.Wait();
                }
            }
        }
    }
}