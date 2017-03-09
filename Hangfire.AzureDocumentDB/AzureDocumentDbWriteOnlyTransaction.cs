using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

using Hangfire.AzureDocumentDB.Entities;
using Hangfire.AzureDocumentDB.Queue;
using Hangfire.States;
using Hangfire.Storage;



namespace Hangfire.AzureDocumentDB
{
    internal class AzureDocumentDbWriteOnlyTransaction : IWriteOnlyTransaction
    {
        private readonly AzureDocumentDbConnection connection;
        private readonly List<Action> commands = new List<Action>();

        public AzureDocumentDbWriteOnlyTransaction(AzureDocumentDbConnection connection)
        {
            this.connection = connection;
        }

        private void QueueCommand(Action command) => commands.Add(command);
        public void Commit() => commands.ForEach(command => command());
        public void Dispose() { }

        #region Queue

        public void AddToQueue(string queue, string jobId)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            IPersistentJobQueueProvider provider = connection.QueueProviders.GetProvider(queue);
            IPersistentJobQueue persistentQueue = provider.GetJobQueue();
            QueueCommand(() => persistentQueue.Enqueue(queue, jobId));
        }

        #endregion

        #region Counter

        public void DecrementCounter(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                Counter data = new Counter
                {
                    Value = -1
                };

                FirebaseResponse response = connection.Client.Push($"counters/raw/{key}", data);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new HttpRequestException(response.Body);
                }
            });
        }

        public void DecrementCounter(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (expireIn.Duration() != expireIn) throw new ArgumentException("The `expireIn` value must be positive.", nameof(expireIn));

            QueueCommand(() =>
            {
                Counter data = new Counter
                {
                    Value = -1,
                    ExpireOn = DateTime.UtcNow.Add(expireIn)
                };

                FirebaseResponse response = connection.Client.Push($"counters/raw/{key}", data);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new HttpRequestException(response.Body);
                }
            });
        }

        public void IncrementCounter(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                Counter data = new Counter
                {
                    Value = 1
                };

                FirebaseResponse response = connection.Client.Push($"counters/raw/{key}", data);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new HttpRequestException(response.Body);
                }
            });
        }

        public void IncrementCounter(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (expireIn.Duration() != expireIn) throw new ArgumentException("The `expireIn` value must be positive.", nameof(expireIn));

            QueueCommand(() =>
            {
                Counter data = new Counter
                {
                    Value = 1,
                    ExpireOn = DateTime.UtcNow.Add(expireIn)
                };

                FirebaseResponse response = connection.Client.Push($"counters/raw/{key}", data);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new HttpRequestException(response.Body);
                }
            });
        }

        #endregion

        #region Job

        public void ExpireJob(string jobId, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            if (expireIn.Duration() != expireIn) throw new ArgumentException("The `expireIn` value must be positive.", nameof(expireIn));

            QueueCommand(() =>
            {
                FirebaseResponse response = connection.Client.Set($"jobs/{jobId}/expire_on", DateTime.UtcNow.Add(expireIn));
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new HttpRequestException(response.Body);
                }
            });
        }

        public void PersistJob(string jobId)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            QueueCommand(() =>
            {
                FirebaseResponse response = connection.Client.Delete($"jobs/{jobId}/expire_on");
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new HttpRequestException(response.Body);
                }
            });
        }

        #endregion

        #region State

        public void SetJobState(string jobId, IState state)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            QueueCommand(() =>
            {
                FirebaseResponse response = connection.Client.Get($"jobs/{jobId}");
                if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
                {
                    Job job = response.ResultAs<Job>();
                    State data = new State
                    {
                        Name = state.Name,
                        Reason = state.Reason,
                        CreatedOn = DateTime.UtcNow,
                        Data = state.SerializeData()
                    };

                    response = connection.Client.Push($"states/{jobId}", data);
                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        string reference = ((PushResponse)response).Result.name;
                        job.StateId = reference;
                        job.StateName = state.Name;

                        connection.Client.Set($"jobs/{jobId}", job);
                    }
                }
            });
        }

        public void AddJobState(string jobId, IState state)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            QueueCommand(() =>
            {
                State data = new State
                {
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedOn = DateTime.UtcNow,
                    Data = state.SerializeData()
                };
                FirebaseResponse response = connection.Client.Push($"states/{jobId}", data);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new HttpRequestException(response.Body);
                }
            });
        }

        #endregion

        #region Set

        public void RemoveFromSet(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                QueryBuilder builder = QueryBuilder.New($@"equalTo=""{key}""");
                builder.OrderBy("key");
                FirebaseResponse response = connection.Client.Get("sets", builder);
                if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
                {
                    Dictionary<string, Set> sets = response.ResultAs<Dictionary<string, Set>>();
                    string reference = sets.Where(s => s.Value.Value == value).Select(s => s.Key).FirstOrDefault();
                    if (!string.IsNullOrEmpty(reference))
                    {
                        connection.Client.Delete($"sets/{reference}");
                    }
                }
            });
        }

        public void AddToSet(string key, string value) => AddToSet(key, value, 0.0);

        public void AddToSet(string key, string value, double score)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                Set data = new Set
                {
                    Key = key,
                    Value = value,
                    Score = score
                };

                QueryBuilder builder = QueryBuilder.New($@"equalTo=""{key}""");
                builder.OrderBy("key");
                FirebaseResponse response = connection.Client.Get("sets", builder);
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    Dictionary<string, Set> sets = response.ResultAs<Dictionary<string, Set>>();
                    string reference = sets?.Where(s => s.Value.Value == value).Select(s => s.Key).FirstOrDefault();

                    if (!string.IsNullOrEmpty(reference))
                    {
                        response = connection.Client.Set($"sets/{reference}", data);
                    }
                    else
                    {
                        response = connection.Client.Push("sets", data);
                    }


                    if (response.StatusCode != HttpStatusCode.OK)
                    {
                        throw new HttpRequestException(response.Body);
                    }
                }
            });
        }

        #endregion

        #region  Hash

        public void RemoveHash(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                FirebaseResponse response = connection.Client.Delete($"hashes/{key}");
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new HttpRequestException(response.Body);
                }
            });
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            QueueCommand(() =>
            {
                List<Hash> hashes = keyValuePairs.Select(k => new Hash
                {
                    Field = k.Key,
                    Value = k.Value
                }).ToList();

                FirebaseResponse response = connection.Client.Get($"hashes/{key}");
                if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
                {
                    // update 
                    Dictionary<string, Hash> collection = response.ResultAs<Dictionary<string, Hash>>();
                    string[] references = collection.Where(h => hashes.Any(k => k.Field == h.Value.Field))
                                                    .Select(h => h.Key)
                                                    .ToArray();
                    Parallel.ForEach(references, reference =>
                    {
                        Hash hash;
                        if (collection.TryGetValue(reference, out hash) && hashes.Any(k => k.Field == hash.Field))
                        {
                            string value = hashes.Where(k => k.Field == hash.Field).Select(k => k.Value).Single();
                            connection.Client.Set($"hashes/{key}/{reference}/value", value);

                            // remove the hash from the list
                            hashes.RemoveAll(x => x.Field == hash.Field);
                        }
                    });
                }

                // new 
                Parallel.ForEach(hashes, hash => connection.Client.Push($"hashes/{key}", hash));
            });
        }

        #endregion

        #region List

        public void InsertToList(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                List data = new List
                {
                    Key = key,
                    Value = value
                };

                FirebaseResponse response = connection.Client.Push("lists", data);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new HttpRequestException(response.Body);
                }
            });
        }

        public void RemoveFromList(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                QueryBuilder builder = QueryBuilder.New($@"equalTo=""{key}""");
                builder.OrderBy("key");
                FirebaseResponse response = connection.Client.Get("lists", builder);
                if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
                {
                    Dictionary<string, List> lists = response.ResultAs<Dictionary<string, List>>();
                    string reference = lists.Where(l => l.Value.Value == value).Select(k => k.Key).FirstOrDefault();
                    if (string.IsNullOrEmpty(reference))
                    {
                        response = connection.Client.Delete($"lists/{reference}");
                        if (response.StatusCode != HttpStatusCode.OK)
                        {
                            throw new HttpRequestException(response.Body);
                        }
                    }
                }
            });
        }

        public void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                QueryBuilder builder = QueryBuilder.New($@"equalTo=""{key}""");
                builder.OrderBy("key");
                FirebaseResponse response = connection.Client.Get("lists", builder);
                if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
                {
                    Dictionary<string, List> lists = response.ResultAs<Dictionary<string, List>>();
                    string[] listsReferences = lists.Skip(keepStartingFrom).Take(keepEndingAt)
                                                    .Select(l => l.Key)
                                                    .ToArray();

                    Parallel.ForEach(listsReferences, reference => connection.Client.Delete($"lists/{reference}"));
                }
            });
        }

        #endregion

    }
}
