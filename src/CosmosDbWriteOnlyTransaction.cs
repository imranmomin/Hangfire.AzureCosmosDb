using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Queue;
using Hangfire.States;
using Hangfire.Storage;

using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;

namespace Hangfire.Azure;

public class CosmosDbWriteOnlyTransaction : JobStorageTransaction
{
    private readonly CosmosDbConnection connection;
    private readonly List<Action> commands = new();

    public CosmosDbWriteOnlyTransaction(CosmosDbConnection connection) => this.connection = connection;

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

            Task<ItemResponse<Counter>> task = connection.Storage.Container.CreateItemAsync(data, new PartitionKey((int)DocumentTypes.Counter));
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

            Task<ItemResponse<Counter>> task = connection.Storage.Container.CreateItemAsync(data, new PartitionKey((int)DocumentTypes.Counter));
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

            Task<ItemResponse<Counter>> task = connection.Storage.Container.CreateItemAsync(data, new PartitionKey((int)DocumentTypes.Counter));
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

            Task<ItemResponse<Counter>> task = connection.Storage.Container.CreateItemAsync(data, new PartitionKey((int)DocumentTypes.Counter));
            task.Wait();
        });
    }

    #endregion

    #region Job

    public override void ExpireJob(string jobId, TimeSpan expireIn)
    {
        if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));
        if (expireIn.Duration() != expireIn) throw new ArgumentException(@"The `expireIn` value must be positive.", nameof(expireIn));
        int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();

        QueueCommand(() =>
        {
            string queryJobs = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Job} AND doc.id = '{jobId}'";
            connection.Storage.Container.ExecuteExpireDocuments(queryJobs, epoch, new PartitionKey((int)DocumentTypes.Job));
        });
            
        // we need to also remove the state documents
        QueueCommand(() =>
        {
            string queryStates = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.State} AND doc.job_id = '{jobId}'";
            connection.Storage.Container.ExecuteExpireDocuments(queryStates, epoch, new PartitionKey((int)DocumentTypes.State));
        });
    }

    public override void PersistJob(string jobId)
    {
        if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

        QueueCommand(() =>
        {
            string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Job} AND doc.id = '{jobId}'";
            connection.Storage.Container.ExecutePersistDocuments(query, new PartitionKey((int)DocumentTypes.Job));
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

            Task<ItemResponse<State>> task = connection.Storage.Container.CreateItemAsync(data, new PartitionKey((int)DocumentTypes.State));
            Task<StoredProcedureExecuteResponse<bool>> spTask = task.ContinueWith(_ => connection.Storage.Container.Scripts.ExecuteStoredProcedureAsync<bool>("setJobState", new PartitionKey((int)DocumentTypes.Job), new dynamic[] { jobId, data })).Unwrap();
            spTask.Wait();
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

            Task<ItemResponse<State>> task = connection.Storage.Container.CreateItemAsync(data, new PartitionKey((int)DocumentTypes.State));
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
            PartitionKey partitionKey = new PartitionKey((int)DocumentTypes.Set);
            string[] sets = connection.Storage.Container.GetItemLinqQueryable<List>(requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
                .Where(s => s.DocumentType == DocumentTypes.Set && s.Key == key)
                .Select(s => new { s.Id, s.Value })
                .ToQueryResult()
                .Where(s => s.Value == value) // value may contain json string.. which interfere with query 
                .Select(s => s.Id)
                .ToArray();

            if (sets.Length == 0) return;

            string ids = string.Join(",", sets.Select(s => $"'{s}'"));
            string query = $"SELECT doc._self FROM doc WHERE doc.type = {(int)DocumentTypes.Set} AND doc.id IN ({ids})";
            connection.Storage.Container.ExecuteDeleteDocuments(query, partitionKey);
        });
    }

    public override void AddToSet(string key, string value) => AddToSet(key, value, 0.0);

    public override void AddToSet(string key, string value, double score)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

        QueueCommand(() =>
        {
            PartitionKey partitionKey = new PartitionKey((int)DocumentTypes.Set);
            List<Set> sets = connection.Storage.Container.GetItemLinqQueryable<Set>(requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
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

            Data<Set> data = new Data<Set>(sets);
            connection.Storage.Container.ExecuteUpsertDocuments(data, partitionKey);
        });
    }

    public override void PersistSet(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        QueueCommand(() =>
        {
            string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Set} AND doc.key = '{key}'";
            connection.Storage.Container.ExecutePersistDocuments(query, new PartitionKey((int)DocumentTypes.Set));
        });
    }

    public override void ExpireSet(string key, TimeSpan expireIn)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        QueueCommand(() =>
        {
            string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Set} AND doc.key = '{key}'";
            int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();
            connection.Storage.Container.ExecuteExpireDocuments(query, epoch, new PartitionKey((int)DocumentTypes.Set));
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

            Data<Set> data = new Data<Set>(sets);
            connection.Storage.Container.ExecuteUpsertDocuments(data, new PartitionKey((int)DocumentTypes.Set));
        });
    }

    public override void RemoveSet(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        QueueCommand(() =>
        {
            string query = $"SELECT doc._self FROM doc WHERE doc.type = {(int)DocumentTypes.Set} AND doc.key = '{key}'";
            connection.Storage.Container.ExecuteDeleteDocuments(query, new PartitionKey((int)DocumentTypes.Set));
        });
    }

    #endregion

    #region  Hash

    public override void RemoveHash(string key)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

        QueueCommand(() =>
        {
            string query = $"SELECT doc._self FROM doc WHERE doc.type = {(int)DocumentTypes.Hash} AND doc.key = '{key}'";
            connection.Storage.Container.ExecuteDeleteDocuments(query, new PartitionKey((int)DocumentTypes.Hash));
        });
    }

    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

        QueueCommand(() =>
        {
            Data<Hash> data = new Data<Hash>();

            PartitionKey partitionKey = new PartitionKey((int)DocumentTypes.Hash);
            List<Hash> hashes = connection.Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
                .Where(h => h.DocumentType == DocumentTypes.Hash && h.Key == key)
                .OrderByDescending(x => x.ExpireOn)
                .ToQueryResult()
                .ToList();

            Hash[] sources = keyValuePairs.Select(k => new Hash
            {
                Key = key,
                Field = k.Key,
                Value = k.Value.TryParseToEpoch()
            }).ToArray();

            foreach (Hash source in sources)
            {
                Hash? hash = hashes.FirstOrDefault(h => h.Field == source.Field);
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

            connection.Storage.Container.ExecuteUpsertDocuments(data, partitionKey);
        });
    }

    public override void ExpireHash(string key, TimeSpan expireIn)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

        QueueCommand(() =>
        {
            string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Hash} AND doc.key = '{key}'";
            int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();
            connection.Storage.Container.ExecuteExpireDocuments(query, epoch, new PartitionKey((int)DocumentTypes.Hash));
        });
    }

    public override void PersistHash(string key)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

        QueueCommand(() =>
        {
            string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.Hash} AND doc.key = '{key}'";
            connection.Storage.Container.ExecutePersistDocuments(query, new PartitionKey((int)DocumentTypes.Hash));
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

            Task<ItemResponse<List>> task = connection.Storage.Container.CreateItemAsync(data, new PartitionKey((int)DocumentTypes.List));
            task.Wait();
        });
    }

    public override void RemoveFromList(string key, string value)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
        if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));

        QueueCommand(() =>
        {
            PartitionKey partitionKey = new PartitionKey((int)DocumentTypes.List);
            string[] lists = connection.Storage.Container.GetItemLinqQueryable<List>(requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
                .Where(l => l.DocumentType == DocumentTypes.List && l.Key == key)
                .Select(l => new { l.Id, l.Value })
                .ToQueryResult()
                .Where(l => l.Value == value)
                .Select(l => l.Id)
                .ToArray();

            if (lists.Length == 0) return;

            string ids = string.Join(",", lists.Select(l => $"'{l}'"));
            string query = $"SELECT doc._self FROM doc WHERE doc.type = {(int)DocumentTypes.List} AND doc.id IN ({ids})";
            connection.Storage.Container.ExecuteDeleteDocuments(query, partitionKey);
        });
    }

    public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
    {
        if (string.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

        QueueCommand(() =>
        {
            PartitionKey partitionKey = new PartitionKey((int)DocumentTypes.List);
            string[] lists = connection.Storage.Container.GetItemLinqQueryable<List>(requestOptions: new QueryRequestOptions { PartitionKey = partitionKey })
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
            connection.Storage.Container.ExecuteDeleteDocuments(query, partitionKey);
        });
    }

    public override void ExpireList(string key, TimeSpan expireIn)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        QueueCommand(() =>
        {
            string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.List} AND doc.key = '{key}'";
            int epoch = DateTime.UtcNow.Add(expireIn).ToEpoch();
            connection.Storage.Container.ExecuteExpireDocuments(query, epoch, new PartitionKey((int)DocumentTypes.List));
        });
    }

    public override void PersistList(string key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        QueueCommand(() =>
        {
            string query = $"SELECT * FROM doc WHERE doc.type = {(int)DocumentTypes.List} AND doc.key = '{key}'";
            connection.Storage.Container.ExecutePersistDocuments(query, new PartitionKey((int)DocumentTypes.List));
        });
    }

    #endregion

}