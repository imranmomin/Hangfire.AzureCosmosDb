using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using Hangfire.States;
using Hangfire.Storage;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

using Hangfire.Azure.Queue;
using Hangfire.Azure.Helper;
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
        private readonly Uri spUpsertDocumentsUri;

        public DocumentDbWriteOnlyTransaction(DocumentDbConnection connection)
        {
            this.connection = connection;
            spPersistDocumentsUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "persistDocuments");
            spExpireDocumentsUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "expireDocuments");
            spDeleteDocumentsUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "deleteDocuments");
            spUpsertDocumentsUri = UriFactory.CreateStoredProcedureUri(connection.Storage.Options.DatabaseName, connection.Storage.Options.CollectionName, "upsertDocuments");
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data);
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data);
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data);
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data);
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
                int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();
                string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Job} AND doc.id = '{jobId}'";
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spExpireDocumentsUri, query, epoch);
                    task.Wait();

                    response = task.Result;

                    // if the continuation is true; run the procedure again
                } while (response.Continuation);
            });
        }

        public override void PersistJob(string jobId)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            QueueCommand(() =>
            {
                string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Job} AND doc.id = '{jobId}'";
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spPersistDocumentsUri, query);
                    task.Wait();

                    response = task.Result;

                    // if the continuation is true; run the procedure again
                } while (response.Continuation);
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
                Task<StoredProcedureResponse<bool>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<bool>(spSetJobStateUri, jobId, data);
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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data);
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
                string[] sets = connection.Storage.Client.CreateDocumentQuery<List>(connection.Storage.CollectionUri)
                    .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                    .Select(s => new { s.Id, s.Value })
                    .ToQueryResult()
                    .Where(s => s.Value == value)
                    .Select(s => s.Id)
                    .ToArray();

                if (sets.Length == 0) return;

                string ids = string.Join(",", sets.Select(s => $"'{s}'"));
                string query = $"SELECT doc._self FROM doc WHERE doc.type = {(int)DocumentTypes.Set} AND doc.id IN ({ids})";
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spDeleteDocumentsUri, query);
                    task.Wait();

                    response = task.Result;

                    // if the continuation is true; run the procedure again
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
                List<Set> sets = connection.Storage.Client.CreateDocumentQuery<Set>(connection.Storage.CollectionUri)
                    .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                    .ToQueryResult()
                    .Where(s => s.Value == value) // value may contain json string.. which interfere with query 
                    .ToList();

                if (sets.Count == 0)
                {
                    sets.Add(new Set
                    {
                        Key = key,
                        Value = value,
                        Score = score,
                        CreatedOn = DateTime.UtcNow
                    });
                }
                else
                {
                    // set the sets score
                    sets.ForEach(s => s.Score = score);
                }

                int affected = 0;
                Data<Set> data = new Data<Set>(sets);

                do
                {
                    // process only remaining items
                    data.Items = data.Items.Skip(affected).ToList();

                    Task<StoredProcedureResponse<int>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<int>(spUpsertDocumentsUri, data);
                    task.Wait();

                    // know how much was processed
                    affected += task.Result;

                } while (affected < data.Items.Count);
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
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spPersistDocumentsUri, query);
                    task.Wait();

                    response = task.Result;

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
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spExpireDocumentsUri, query, epoch);
                    task.Wait();

                    response = task.Result;

                    // if the continuation is true; run the procedure again
                } while (response.Continuation);
            });
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            QueueCommand(() =>
            {
                List<Set> sets = items.Select(value => new Set
                {
                    Key = key,
                    Value = value,
                    Score = 0.00,
                    CreatedOn = DateTime.UtcNow
                }).ToList();

                int affected = 0;
                Data<Set> data = new Data<Set>(sets);

                do
                {
                    // process only remaining items
                    data.Items = data.Items.Skip(affected).ToList();

                    Task<StoredProcedureResponse<int>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<int>(spUpsertDocumentsUri, data);
                    task.Wait();

                    // know how much was processed
                    affected += task.Result;

                } while (affected < data.Items.Count);
            });
        }

        public override void RemoveSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                string query = $"SELECT doc._self FROM doc WHERE doc.type = {(int)DocumentTypes.Set} AND doc.key == '{key}'";
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spDeleteDocumentsUri, query);
                    task.Wait();

                    response = task.Result;

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
                string query = $"SELECT doc._self FROM doc WHERE doc.type = {(int)DocumentTypes.Hash} AND doc.key == '{key}'";
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spDeleteDocumentsUri, query);
                    task.Wait();

                    response = task.Result;

                    // if the continuation is true; run the procedure again
                } while (response.Continuation);
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            QueueCommand(() =>
            {
                Data<Hash> data = new Data<Hash>();

                List<Hash> hashes = connection.Storage.Client.CreateDocumentQuery<Hash>(connection.Storage.CollectionUri)
                    .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key)
                    .ToQueryResult();

                Hash[] sources = keyValuePairs.Select(k => new Hash
                {
                    Key = key,
                    Field = k.Key,
                    Value = k.Value.TryParseToEpoch()
                }).ToArray();

                foreach (Hash source in sources)
                {
                    Hash hash = hashes.SingleOrDefault(h => h.Field == source.Field);
                    if (hash == null)
                    {
                        data.Items.Add(source);
                    }
                    else if (!string.Equals(hash.Value, source.Value, StringComparison.InvariantCultureIgnoreCase))
                    {
                        hash.Value = source.Value;
                        data.Items.Add(hash);
                    }
                }

                int affected = 0;

                do
                {
                    // process only remaining items
                    data.Items = data.Items.Skip(affected).ToList();

                    Task<StoredProcedureResponse<int>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<int>(spUpsertDocumentsUri, data);
                    task.Wait();

                    // know how much was processed
                    affected += task.Result;

                } while (affected < data.Items.Count);
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
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spExpireDocumentsUri, query, epoch);
                    task.Wait();

                    response = task.Result;

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
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spPersistDocumentsUri, query);
                    task.Wait();

                    response = task.Result;

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

                Task<ResourceResponse<Document>> task = connection.Storage.Client.CreateDocumentWithRetriesAsync(connection.Storage.CollectionUri, data);
                task.Wait();
            });
        }

        public override void RemoveFromList(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

            QueueCommand(() =>
            {
                string[] lists = connection.Storage.Client.CreateDocumentQuery<List>(connection.Storage.CollectionUri)
                    .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                    .Select(l => new { l.Id, l.Value })
                    .ToQueryResult()
                    .Where(l => l.Value == value)
                    .Select(l => l.Id)
                    .ToArray();

                if (lists.Length == 0) return;

                string ids = string.Join(",", lists.Select(l => $"'{l}'"));
                string query = $"SELECT doc._self FROM doc WHERE doc.type = {(int)DocumentTypes.List} AND doc.id IN ({ids})";
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spDeleteDocumentsUri, query);
                    task.Wait();

                    response = task.Result;

                    // if the continuation is true; run the procedure again
                } while (response.Continuation);

            });
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            QueueCommand(() =>
            {
                string[] lists = connection.Storage.Client.CreateDocumentQuery<List>(connection.Storage.CollectionUri)
                     .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                     .OrderByDescending(l => l.CreatedOn)
                     .Select(l => l.Id)
                     .ToQueryResult()
                     .Select((l, index) => new { Id = l, index })
                     .Where(l => l.index < keepStartingFrom || l.index > keepEndingAt)
                     .Select(l => l.Id)
                     .ToArray();

                if (lists.Length == 0) return;

                string ids = string.Join(",", lists.Select(l => $"'{l}'"));
                string query = $"SELECT doc._self FROM doc WHERE doc.type = {(int)DocumentTypes.List} AND doc.id IN ({ids})";
                ProcedureResponse response;

                do
                {
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spDeleteDocumentsUri, query);
                    task.Wait();

                    response = task.Result;

                    // if the continuation is true; run the procedure again
                } while (response.Continuation);

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
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spExpireDocumentsUri, query, epoch);
                    task.Wait();

                    response = task.Result;

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
                    Task<StoredProcedureResponse<ProcedureResponse>> task = connection.Storage.Client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spPersistDocumentsUri, query);
                    task.Wait();

                    response = task.Result;

                    // if the continuation is true; run the procedure again
                } while (response.Continuation);
            });
        }

        #endregion

    }
}