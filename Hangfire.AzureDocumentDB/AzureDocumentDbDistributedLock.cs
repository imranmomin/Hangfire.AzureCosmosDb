using System;
using System.Linq;
using System.Collections.Generic;

using Hangfire.AzureDocumentDB.Entities;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Hangfire.AzureDocumentDB
{
    internal class AzureDocumentDbDistributedLock : IDisposable
    {
        private readonly DocumentClient client;
        private string lockReference;
        private readonly object syncLock = new object();

        public AzureDocumentDbDistributedLock(string resource, TimeSpan timeout, DocumentClient client)
        {
            this.client = client;
            Acquire(resource, timeout);
        }

        public void Dispose() => Relase();

        private async void Acquire(string resource, TimeSpan timeout)
        {
            System.Diagnostics.Stopwatch acquireStart = new System.Diagnostics.Stopwatch();
            acquireStart.Start();

            while (true)
            {
                FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
                Lock response = client.CreateDocumentQuery<Lock>(UriFactory.CreateDocumentCollectionUri("locks", "locks"), queryOptions)
                                      .FirstOrDefault(l => l.Resource == resource);

                if (response == null)
                {
                    
                }
                foreach (Lock @lock in response)
                {
                    await client.DeleteDocumentAsync(UriFactory.CreateDocumentUri("", "", ""));
                }
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    Dictionary<string, Lock> locks = response.
                    string reference = locks?.Where(l => l.Value.Resource == resource).Select(l => l.Key).FirstOrDefault();
                    if (string.IsNullOrEmpty(reference))
                    {
                        response = client.Push("locks", new Lock { Resource = resource, ExpireOn = DateTime.UtcNow.Add(timeout) });
                        if (response.StatusCode == System.Net.HttpStatusCode.OK)
                        {
                            reference = ((PushResponse)response).Result.name;
                            if (!string.IsNullOrEmpty(reference))
                            {
                                lockReference = reference;
                                break;
                            }
                        }
                    }
                }

                // check the timeout
                if (acquireStart.ElapsedMilliseconds > timeout.TotalMilliseconds)
                {
                    throw new FirebaseDistributedLockException($"Could not place a lock on the resource '{resource}': Lock timeout.");
                }

                // sleep for 500 millisecond
                System.Threading.Thread.Sleep(500);
            }
        }

        private void Relase()
        {
            lock (syncLock)
            {
                QueryBuilder builder = QueryBuilder.New($@"equalTo=""{lockReference}""");
                builder.OrderBy("$key");
                FirebaseResponse response = client.Get("locks", builder);
                if (response.StatusCode == System.Net.HttpStatusCode.OK && !response.IsNull())
                {
                    client.Delete($"locks/{lockReference}");
                }
            }
        }
    }
}