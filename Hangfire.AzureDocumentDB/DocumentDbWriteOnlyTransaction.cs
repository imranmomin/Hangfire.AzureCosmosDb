using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Azure.Queue;
using Hangfire.Azure.Documents;
using Microsoft.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Microsoft.Azure.Documents.Client;

namespace Hangfire.Azure
{
    internal class DocumentDbWriteOnlyTransaction : IWriteOnlyTransaction
    {
        private readonly DocumentDbConnection connection;
        private readonly List<Action> commands = new List<Action>();

        public DocumentDbWriteOnlyTransaction(DocumentDbConnection connection) => this.connection = connection;
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data);
                task.Wait();
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data);
                task.Wait();
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data);
                task.Wait();
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data);
                task.Wait();
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
                int epochTime = DateTime.UtcNow.Add(expireIn).ToEpoch();
                Uri spExpireJobUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "expireJob");
                Task<StoredProcedureResponse<bool>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<bool>(spExpireJobUri, jobId, epochTime);
                task.Wait();
            });
        }

        public void PersistJob(string jobId)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            QueueCommand(() =>
            {
                Uri spPersistJobUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "persistJob");
                Task<StoredProcedureResponse<bool>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<bool>(spPersistJobUri, jobId);
                task.Wait();
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
                State data = new State
                {
                    JobId = jobId,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedOn = DateTime.UtcNow,
                    Data = state.SerializeData()
                };

                Uri spSetJobStateUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "setJobState");
                Task<StoredProcedureResponse<bool>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<bool>(spSetJobStateUri, jobId, data);
                task.Wait();
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data);
                task.Wait();
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
                Uri spRemoveFromSetUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "removeFromSet");
                Task<StoredProcedureResponse<bool>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<bool>(spRemoveFromSetUri, key, value);
                task.Wait();
            });
        }

        public void AddToSet(string key, string value) => AddToSet(key, value, 0.0);

        public void AddToSet(string key, string value, double score)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                Set set = new Set
                {
                    Key = key,
                    Value = value,
                    Score = score
                };

                Uri spAddToSetUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "addToSet");
                Task<StoredProcedureResponse<bool>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<bool>(spAddToSetUri, set);
                task.Wait();
            });
        }

        #endregion

        #region  Hash

        public void RemoveHash(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                Uri spRemoveHashUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "removeHash");
                Task<StoredProcedureResponse<bool>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<bool>(spRemoveHashUri, key);
                task.Wait();
            });
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            QueueCommand(() =>
            {
                Hash[] sources = keyValuePairs.Select(k => new Hash
                {
                    Key = key,
                    Field = k.Key,
                    Value = k.Value.ToEpoch()
                }).ToArray();

                Uri spSetRangeHashUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "setRangeHash");
                Task<StoredProcedureResponse<int>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<int>(spSetRangeHashUri, key, sources);
                task.Wait();
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data);
                task.Wait();
            });
        }

        public void RemoveFromList(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                Uri spRemoveFromListUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "removeFromList");
                Task<StoredProcedureResponse<bool>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<bool>(spRemoveFromListUri, key, value);
                task.Wait();
            });
        }

        public void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                Uri spTrimListUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "trimList");
                Task<StoredProcedureResponse<bool>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<bool>(spTrimListUri, key, keepStartingFrom, keepEndingAt);
                task.Wait();
            });
        }

        #endregion

    }
}