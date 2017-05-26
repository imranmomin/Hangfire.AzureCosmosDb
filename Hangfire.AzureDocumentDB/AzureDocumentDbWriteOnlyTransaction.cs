using System;
using System.Linq;
using System.Globalization;
using System.Collections.Generic;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Hangfire.States;
using Hangfire.Storage;
using Hangfire.AzureDocumentDB.Queue;
using Hangfire.AzureDocumentDB.Helper;
using Hangfire.AzureDocumentDB.Entities;

namespace Hangfire.AzureDocumentDB
{
    internal class AzureDocumentDbWriteOnlyTransaction : IWriteOnlyTransaction
    {
        private readonly AzureDocumentDbConnection connection;
        private readonly List<Action> commands = new List<Action>();

        private readonly FeedOptions QueryOptions = new FeedOptions { MaxItemCount = 100 };

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
                    Key = key,
                    Type = CounterTypes.Raw,
                    Value = -1
                };

                connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data).GetAwaiter().GetResult();
            });
        }

        public void DecrementCounter(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));

            QueueCommand(() =>
            {
                Counter data = new Counter
                {
                    Key = key,
                    Type = CounterTypes.Raw,
                    Value = -1,
                    ExpireOn = DateTime.UtcNow.Add(expireIn)
                };

                connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data).GetAwaiter().GetResult();
            });
        }

        public void IncrementCounter(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                Counter data = new Counter
                {
                    Key = key,
                    Type = CounterTypes.Raw,
                    Value = 1
                };

                connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data).GetAwaiter().GetResult();
            });
        }

        public void IncrementCounter(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));

            QueueCommand(() =>
            {
                Counter data = new Counter
                {
                    Key = key,
                    Type = CounterTypes.Raw,
                    Value = 1,
                    ExpireOn = DateTime.UtcNow.Add(expireIn)
                };

                connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data).GetAwaiter().GetResult();
            });
        }

        #endregion

        #region Job

        public void ExpireJob(string jobId, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
            if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));

            QueueCommand(() =>
            {
                // TODO: move to stored procedure
                Job job = connection.Storage.Client.CreateDocumentQuery<Job>(connection.Storage.CollectionUri, QueryOptions)
                    .Where(j => j.Id == jobId)
                    .AsEnumerable()
                    .FirstOrDefault();

                if (job != null)
                {
                    job.ExpireOn = DateTime.UtcNow.Add(expireIn);
                    connection.Storage.Client.ReplaceDocumentWithRetriesAsync(job.SelfLink, job).GetAwaiter().GetResult();
                }
            });
        }

        public void PersistJob(string jobId)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            QueueCommand(() =>
            {
                Job job = connection.Storage.Client.CreateDocumentQuery<Job>(connection.Storage.CollectionUri, QueryOptions)
                    .Where(j => j.Id == jobId)
                    .AsEnumerable()
                    .FirstOrDefault();

                if (job != null && job.ExpireOn.HasValue)
                {
                    job.ExpireOn = null;
                    connection.Storage.Client.ReplaceDocumentWithRetriesAsync(job.SelfLink, job).GetAwaiter().GetResult();
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
                // TODO: move to stored procedure
                Job job = connection.Storage.Client.CreateDocumentQuery<Job>(connection.Storage.CollectionUri, QueryOptions)
                    .Where(j => j.Id == jobId)
                    .AsEnumerable()
                    .FirstOrDefault();

                if (job != null)
                {
                    State data = new State
                    {
                        JobId = jobId,
                        Name = state.Name,
                        Reason = state.Reason,
                        CreatedOn = DateTime.UtcNow,
                        Data = state.SerializeData()
                    };

                    ResourceResponse<Document> response = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data).GetAwaiter().GetResult();

                    job.StateId = response.Resource.Id;
                    job.StateName = state.Name;

                    connection.Storage.Client.ReplaceDocumentWithRetriesAsync(job.SelfLink, job).GetAwaiter().GetResult();
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
                    JobId = jobId,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedOn = DateTime.UtcNow,
                    Data = state.SerializeData()
                };

                connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data).GetAwaiter().GetResult();
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
                Set set = connection.Storage.Client.CreateDocumentQuery<Set>(connection.Storage.CollectionUri, QueryOptions)
                     .Where(s => s.Key == key && s.Value == value && s.DocumentType == DocumentTypes.Set)
                     .AsEnumerable()
                     .FirstOrDefault();

                if (set != null)
                {
                    connection.Storage.Client.DeleteDocumentWithRetriesAsync(set.SelfLink).GetAwaiter().GetResult();
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
                Set set = connection.Storage.Client.CreateDocumentQuery<Set>(connection.Storage.CollectionUri, QueryOptions)
                    .Where(s => s.Key == key && s.Value == value && s.DocumentType == DocumentTypes.Set)
                    .AsEnumerable()
                    .FirstOrDefault();

                if (set != null)
                {
                    set.Key = key;
                    set.Value = value;
                    set.Score = score;
                }
                else
                {
                    set = new Set
                    {
                        Key = key,
                        Value = value,
                        Score = score
                    };
                }

                connection.Storage.Client.UpsertDocumentWithRetriesAsync(connection.Storage.CollectionUri, set).GetAwaiter().GetResult();
            });
        }

        #endregion

        #region  Hash

        public void RemoveHash(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                // TODO: move to stored procedure
                List<Hash> hashes = connection.Storage.Client.CreateDocumentQuery<Hash>(connection.Storage.CollectionUri, QueryOptions)
                    .Where(h => h.Key == key && h.DocumentType == DocumentTypes.Hash)
                    .AsEnumerable()
                    .ToList();

                hashes.ForEach(hash => connection.Storage.Client.DeleteDocumentWithRetriesAsync(hash.SelfLink).GetAwaiter().GetResult());
            });
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            QueueCommand(() =>
            {
                // TODO: move to stored procedure
                Func<string, string> epoch = s =>
                {
                    DateTime date;
                    if (DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out date))
                    {
                        if (date.Equals(DateTime.MinValue)) return int.MinValue.ToString();
                        DateTime epochDateTime = new DateTime(1970, 1, 1);
                        TimeSpan epochTimeSpan = date - epochDateTime;
                        return ((int)epochTimeSpan.TotalSeconds).ToString(CultureInfo.InvariantCulture);
                    }
                    return s;
                };

                List<Hash> sources = keyValuePairs.Select(k => new Hash
                {
                    Key = key,
                    Field = k.Key,
                    Value = epoch(k.Value)
                }).ToList();

                List<Hash> hashes = connection.Storage.Client.CreateDocumentQuery<Hash>(connection.Storage.CollectionUri, QueryOptions)
                    .Where(h => h.Key == key && h.DocumentType == DocumentTypes.Hash)
                    .AsEnumerable()
                    .ToList();

                sources.ForEach(source =>
                {
                    Hash hash = hashes.FirstOrDefault(h => h.Key == source.Key && h.Field == source.Field);
                    if (hash != null) source.Id = hash.Id;
                });

                sources.ForEach(hash => connection.Storage.Client.UpsertDocumentWithRetriesAsync(connection.Storage.CollectionUri, hash).GetAwaiter().GetResult());
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

                connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data).GetAwaiter().GetResult();
            });
        }

        public void RemoveFromList(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                // TODO: move to stored procedure
                List data = connection.Storage.Client.CreateDocumentQuery<List>(connection.Storage.CollectionUri, QueryOptions)
                    .Where(l => l.Key == key && l.Value == value && l.DocumentType == DocumentTypes.List)
                    .AsEnumerable()
                    .FirstOrDefault();

                if (data != null)
                {
                    connection.Storage.Client.DeleteDocumentWithRetriesAsync(data.SelfLink).GetAwaiter().GetResult();
                }
            });
        }

        public void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                // TODO: move to stored procedure
                List<List> lists = connection.Storage.Client.CreateDocumentQuery<List>(connection.Storage.CollectionUri, QueryOptions)
                    .Where(l => l.Key == key && l.DocumentType == DocumentTypes.List)
                    .AsEnumerable()
                    .Skip(keepStartingFrom).Take(keepEndingAt)
                    .ToList();

                lists.ForEach(list => connection.Storage.Client.DeleteDocumentWithRetriesAsync(list.SelfLink).GetAwaiter().GetResult());
            });
        }

        #endregion

    }
}
