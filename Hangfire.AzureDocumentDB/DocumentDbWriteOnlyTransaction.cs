using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using Hangfire.States;
using Hangfire.Storage;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Hangfire.Azure.Queue;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;

namespace Hangfire.Azure
{
    internal class DocumentDbWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly DocumentDbConnection connection;
        private readonly List<Action> commands = new List<Action>();
        private readonly Uri spPersistDocumentsUri;
        private readonly Uri spExpireDocumentsUri;
        private readonly Uri spDeleteDocumentsUri;

        public DocumentDbWriteOnlyTransaction(DocumentDbConnection connection)
        {
            this.connection = connection;
            spPersistDocumentsUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "persistDocuments");
            spExpireDocumentsUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "expireDocuments");
            spDeleteDocumentsUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "deleteDocuments");
        }

        private void QueueCommand(Action command) => commands.Add(command);
        public override void Commit() => commands.ForEach(command => command());
        public override void Dispose() { }

        #region Queue

        public override void AddToQueue(string queue, string jobId)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            IPersistentJobQueueProvider provider = connection.QueueProviders.GetProvider(queue);
            IPersistentJobQueue persistentQueue = provider.GetJobQueue();
            QueueCommand(() => persistentQueue.Enqueue(queue, jobId));
        }

        #endregion

        #region Counter

        public override void DecrementCounter(string key)
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentAsync(connection.Storage.CollectionUri, data);
                task.Wait();
            });
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentAsync(connection.Storage.CollectionUri, data);
                task.Wait();
            });
        }

        public override void IncrementCounter(string key)
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentAsync(connection.Storage.CollectionUri, data);
                task.Wait();
            });
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentAsync(connection.Storage.CollectionUri, data);
                task.Wait();
            });
        }

        #endregion

        #region Job

        public override void ExpireJob(string jobId, TimeSpan expireIn)
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

        public override void PersistJob(string jobId)
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

        public override void SetJobState(string jobId, IState state)
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

        public override void AddJobState(string jobId, IState state)
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentAsync(connection.Storage.CollectionUri, data);
                task.Wait();
            });
        }

        #endregion

        #region Set

        public override void RemoveFromSet(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                Set data = new Set
                {
                    Key = key,
                    Value = value
                };

                ProcedureResponse response;
                Uri spRemoveFromSetUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "removeFromSet");

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<ProcedureResponse>(spRemoveFromSetUri, data);
                    task.Wait();

                    response = task.Result;

                } while (response.Continuation);
            });
        }

        public override void AddToSet(string key, string value) => AddToSet(key, value, 0.0);

        public override void AddToSet(string key, string value, double score)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                Set set = new Set
                {
                    Key = key,
                    Value = value,
                    Score = score,
                    CreatedOn = DateTime.UtcNow
                };

                // loop until the affected records is not zero and attempts are less then equal to 3
                int affected;
                int attempts = 0;
                Uri spAddToSetUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "addToSet");

                do
                {
                    attempts++;

                    Task<StoredProcedureResponse<int>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<int>(spAddToSetUri, set);
                    task.Wait();

                    affected = task.Result;

                    // loop back if the result and attempts satisfy
                } while (affected == 0 && attempts < 3);

            });
        }

        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Set} AND doc.key = '{key}'";
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> procedureTask = connection.Storage.Client.ExecuteStoredProcedureAsync<ProcedureResponse>(spPersistDocumentsUri, query);
                    procedureTask.Wait();

                    response = procedureTask.Result;

                    // if the continuation is true; run the procedure again
                } while (response.Continuation);
            });
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Set} AND doc.key = '{key}'";
                int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> procedureTask = connection.Storage.Client.ExecuteStoredProcedureAsync<ProcedureResponse>(spExpireDocumentsUri, new RequestOptions
                    {
                        EnableScriptLogging = true
                    }, query, epoch);
                    procedureTask.Wait();

                    response = procedureTask.Result;

                    // if the continuation is true; run the procedure again
                } while (response.Continuation);
            });
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            QueueCommand(() => throw new NotImplementedException());
        }

        public override void RemoveSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Set} AND doc.key == '{key}'";
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> procedureTask = connection.Storage.Client.ExecuteStoredProcedureAsync<ProcedureResponse>(spDeleteDocumentsUri, query);
                    procedureTask.Wait();

                    response = procedureTask.Result;

                // if the continuation is true; run the procedure again
            } while (response.Continuation);
            });
        }

        #endregion

        #region  Hash

        public override void RemoveHash(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                Uri spRemoveHashUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "removeHash");
                Task<StoredProcedureResponse<bool>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<bool>(spRemoveHashUri, key);
                task.Wait();
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            QueueCommand(() =>
            {
                Data<Hash> data = new Data<Hash>
                {
                    Items = keyValuePairs.Select(k => new Hash
                    {
                        Key = key,
                        Field = k.Key,
                        Value = k.Value.TryParseToEpoch()
                    }).ToArray()
                };

                Uri spSetRangeHashUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "setRangeHash");
                Task<StoredProcedureResponse<int>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<int>(spSetRangeHashUri, key, data);
                task.Wait();
            });
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Hash} AND doc.key = '{key}'";
                int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> procedureTask = connection.Storage.Client.ExecuteStoredProcedureAsync<ProcedureResponse>(spExpireDocumentsUri, query, epoch);
                    procedureTask.Wait();

                    response = procedureTask.Result;

                // if the continuation is true; run the procedure again
            } while (response.Continuation);
            });
        }

        public override void PersistHash(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Hash} AND doc.key = '{key}'";
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> procedureTask = connection.Storage.Client.ExecuteStoredProcedureAsync<ProcedureResponse>(spPersistDocumentsUri, query);
                    procedureTask.Wait();

                    response = procedureTask.Result;

                // if the continuation is true; run the procedure again
            } while (response.Continuation);
            });
        }

        #endregion

        #region List

        public override void InsertToList(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                List data = new List
                {
                    Key = key,
                    Value = value,
                    CreatedOn = DateTime.UtcNow
                };

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentAsync(connection.Storage.CollectionUri, data);
                task.Wait();
            });
        }

        public override void RemoveFromList(string key, string value)
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

                ProcedureResponse response;

                do
                {
                    Uri spRemoveFromListUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "removeFromList");
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<ProcedureResponse>(spRemoveFromListUri, data);
                    task.Wait();

                    response = task.Result;

                } while (response.Continuation);

            });
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                Uri spTrimListUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "trimList");
                Task<StoredProcedureResponse<bool>> task = connection.Storage.Client.ExecuteStoredProcedureAsync<bool>(spTrimListUri, key, keepStartingFrom, keepEndingAt);
                task.Wait();
            });
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.List} AND doc.key = '{key}'";
                int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> procedureTask = connection.Storage.Client.ExecuteStoredProcedureAsync<ProcedureResponse>(spExpireDocumentsUri, query, epoch);
                    procedureTask.Wait();

                    response = procedureTask.Result;

                // if the continuation is true; run the procedure again
            } while (response.Continuation);
            });
        }

        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.List} AND doc.key = '{key}'";
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> procedureTask = connection.Storage.Client.ExecuteStoredProcedureAsync<ProcedureResponse>(spPersistDocumentsUri, query);
                    procedureTask.Wait();

                    response = procedureTask.Result;

                // if the continuation is true; run the procedure again
            } while (response.Continuation);
            });
        }

        #endregion

    }
}