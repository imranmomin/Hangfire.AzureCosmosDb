using System;
using System.Net;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using Newtonsoft.Json;
using Microsoft.Azure.Documents.Client;

using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Hangfire.AzureDocumentDB.Json;
using Hangfire.AzureDocumentDB.Queue;
using Hangfire.AzureDocumentDB.Entities;



namespace Hangfire.AzureDocumentDB
{
    internal sealed class AzureDocumentDbConnection : JobStorageConnection
    {
        public DocumentClient Client { get; }
        public PersistentJobQueueProviderCollection QueueProviders { get; }

        public AzureDocumentDbConnection(AzureDocumentDbStorage storage)
        {
            Client = new DocumentClient(storage.);
            QueueProviders = storage.QueueProviders;
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout) => new AzureDocumentDbDistributedLock(resource, timeout, Client);
        public override IWriteOnlyTransaction CreateWriteTransaction() => new AzureDocumentDbWriteOnlyTransaction(this);

        #region Job

        public override string CreateExpiredJob(Common.Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            InvocationData invocationData = InvocationData.Serialize(job);
            PushResponse response = Client.Push("jobs", new Entities.Job
            {
                InvocationData = invocationData,
                Arguments = invocationData.Arguments,
                CreatedOn = createdAt,
                ExpireOn = createdAt.Add(expireIn),

                Parameters = parameters.Select(p => new Parameter
                {
                    Name = p.Key,
                    Value = p.Value
                }).ToArray()
            });

            if (response.StatusCode == HttpStatusCode.OK)
            {
                return response.Result.name;
            }

            return string.Empty;
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException(nameof(queues));

            IPersistentJobQueueProvider[] providers = queues.Select(q => QueueProviders.GetProvider(q))
                                                            .Distinct()
                                                            .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException($"Multiple provider instances registered for queues: {string.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
            }

            IPersistentJobQueue persistentQueue = providers.Single().GetJobQueue();
            IFetchedJob queue = persistentQueue.Dequeue(queues, cancellationToken);
            return queue;
        }

        public override JobData GetJobData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            FirebaseResponse response = Client.Get($"jobs/{jobId}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Entities.Job data = response.ResultAs<Entities.Job>();
                InvocationData invocationData = data.InvocationData;
                invocationData.Arguments = data.Arguments;

                Common.Job job = null;
                JobLoadException loadException = null;

                try
                {
                    job = invocationData.Deserialize();
                }
                catch (JobLoadException ex)
                {
                    loadException = ex;
                }

                return new JobData
                {
                    Job = job,
                    State = data.StateName,
                    CreatedAt = data.CreatedOn,
                    LoadException = loadException
                };
            }

            return null;
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            FirebaseResponse response = Client.Get($"jobs/{jobId}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Entities.Job job = response.ResultAs<Entities.Job>();
                response = Client.Get($"states/{jobId}/{job.StateId}");
                if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
                {
                    State data = response.ResultAs<State>();
                    return new StateData
                    {
                        Name = data.Name,
                        Reason = data.Reason,
                        Data = data.Data.Trasnform()
                    };
                }
            }

            return null;
        }

        #endregion

        #region Parameter

        public override string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            FirebaseResponse response = Client.Get($"jobs/{id}/parameter");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Parameter[] parameters = response.ResultAs<Parameter[]>();
                return parameters.Where(p => p.Name == name)
                                 .Select(p => p.Value).FirstOrDefault();
            }

            return null;
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            Parameter parameter = new Parameter
            {
                Name = name,
                Value = value
            };

            FirebaseResponse response = Client.Get($"jobs/{id}/parameters");
            if (response.StatusCode == HttpStatusCode.OK)
            {
                Parameter[] parameters = response.ResultAs<Parameter[]>();
                int index = parameters.Where(p => p.Name == name).Select((p, i) => i + 1).FirstOrDefault();

                if (index > 0) Client.Set($"jobs/{id}/parameters/{index - 1}/value", value);
                else Client.Set($"jobs/{id}/parameters/{parameters.Length}", parameter);
            }
        }

        #endregion

        #region Set

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueryBuilder builder = QueryBuilder.New($@"equalTo=""{key}""");
            builder.OrderBy("key");
            FirebaseResponse response = Client.Get("sets", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Set> collections = response.ResultAs<Dictionary<string, Set>>();
                DateTime? expireOn = collections.Select(c => c.Value).Min(s => s.ExpireOn);
                if (expireOn.HasValue)
                {
                    return expireOn.Value - DateTime.UtcNow;
                }
            }

            return TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueryBuilder builder = QueryBuilder.New($@"equalTo=""{key}""");
            builder.OrderBy("key");
            FirebaseResponse response = Client.Get("sets", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Set> collections = response.ResultAs<Dictionary<string, Set>>();
                return collections.Skip(startingFrom).Take(endingAt).Select(c => c.Value).Select(s => s.Value).ToList();
            }

            return new List<string>();
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            long value = 0;
            FirebaseResponse response = Client.Get($"counters/raw/{key}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Counter> collections = response.ResultAs<Dictionary<string, Counter>>();
                value += collections.Sum(c => c.Value.Value);
            }

            response = Client.Get($"counters/aggregrated/{key}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Counter> collections = response.ResultAs<Dictionary<string, Counter>>();
                value += collections.Sum(c => c.Value.Value);
            }

            return value;
        }

        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueryBuilder builder = QueryBuilder.New($@"equalTo=""{key}""");
            builder.OrderBy("key");
            FirebaseResponse response = Client.Get("sets", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Set> collections = response.ResultAs<Dictionary<string, Set>>();
                return collections.LongCount();
            }

            return default(long);
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueryBuilder builder = QueryBuilder.New($@"equalTo=""{key}""");
            builder.OrderBy("key");
            FirebaseResponse response = Client.Get("sets", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Set> sets = response.ResultAs<Dictionary<string, Set>>();
                List<string> data = sets.Select(s => s.Value.Value).ToList();
                return new HashSet<string>(data);
            }

            return new HashSet<string>();
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            QueryBuilder builder = QueryBuilder.New($@"equalTo=""{key}""");
            builder.OrderBy("key");
            FirebaseResponse response = Client.Get("sets", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Set> sets = response.ResultAs<Dictionary<string, Set>>();
                return sets.Select(s => s.Value)
                           .OrderBy(s => s.Score)
                           .Where(s => s.Score >= fromScore && s.Score <= toScore)
                           .Select(s => s.Value).FirstOrDefault();
            }

            return string.Empty;
        }

        #endregion

        #region Server

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            QueryBuilder builder = QueryBuilder.New($@"equalTo=""{serverId}""");
            builder.OrderBy("server_id");
            FirebaseResponse response = Client.Get("servers", builder);
            if (response.StatusCode == HttpStatusCode.OK)
            {
                Dictionary<string, Entities.Server> servers = response.ResultAs<Dictionary<string, Entities.Server>>();
                string reference = servers?.Where(s => s.Value.ServerId == serverId).Select(s => s.Key).FirstOrDefault();

                Entities.Server server;
                if (!string.IsNullOrEmpty(reference) && servers.TryGetValue(reference, out server))
                {
                    server.LastHeartbeat = DateTime.UtcNow;
                    server.Workers = context.WorkerCount;
                    server.Queues = context.Queues;

                    response = Client.Set($"servers/{reference}", server);
                    if (response.StatusCode != HttpStatusCode.OK)
                    {
                        throw new HttpRequestException(response.Body);
                    }
                }
                else
                {
                    server = new Entities.Server
                    {
                        ServerId = serverId,
                        Workers = context.WorkerCount,
                        Queues = context.Queues,
                        CreatedOn = DateTime.UtcNow,
                        LastHeartbeat = DateTime.UtcNow
                    };

                    response = Client.Push("servers", server);
                    if (response.StatusCode != HttpStatusCode.OK)
                    {
                        throw new HttpRequestException(response.Body);
                    }
                }
            }
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            QueryBuilder builder = QueryBuilder.New($@"equalTo=""{serverId}""");
            builder.OrderBy("server_id");
            FirebaseResponse response = Client.Get("servers", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Entities.Server> servers = response.ResultAs<Dictionary<string, Entities.Server>>();
                string reference = servers.Where(s => s.Value.ServerId == serverId).Select(s => s.Key).FirstOrDefault();
                if (!string.IsNullOrEmpty(reference))
                {
                    response = Client.Set($"servers/{reference}/last_heartbeat", DateTime.UtcNow);
                    if (response.StatusCode != HttpStatusCode.OK)
                    {
                        throw new HttpRequestException(response.Body);
                    }
                }
            }
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            QueryBuilder builder = QueryBuilder.New($@"equalTo=""{serverId}""");
            builder.OrderBy("server_id");
            FirebaseResponse response = Client.Get("servers", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Entities.Server> servers = response.ResultAs<Dictionary<string, Entities.Server>>();
                string reference = servers.Where(s => s.Value.ServerId == serverId).Select(s => s.Key).Single();
                if (!string.IsNullOrEmpty(reference))
                {
                    Client.Delete($"servers/{reference}");
                }
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            }

            FirebaseResponse response = Client.Get("servers");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Entities.Server> servers = response.ResultAs<Dictionary<string, Entities.Server>>();

                // get the firebase reference key for each timeout server
                DateTime lastHeartbeat = DateTime.UtcNow.Add(timeOut.Negate());
                string[] references = servers.Where(s => s.Value.LastHeartbeat < lastHeartbeat)
                                             .Select(s => s.Key)
                                             .ToArray();

                // remove all timeout server.
                Array.ForEach(references, reference => Client.Delete($"servers/{reference}"));
                return references.Length;
            }

            return default(int);
        }

        #endregion

        #region Hash

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            FirebaseResponse response = Client.Get($"hashes/{key}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Hash> hashes = response.ResultAs<Dictionary<string, Hash>>();
                return hashes.Select(h => h.Value).ToDictionary(h => h.Field, h => h.Value);
            }

            return new Dictionary<string, string>();
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            List<Hash> hashes = keyValuePairs.Select(k => new Hash
            {
                Field = k.Key,
                Value = k.Value
            }).ToList();

            FirebaseResponse response = Client.Get($"hashes/{key}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Hash> existingHashes = response.ResultAs<Dictionary<string, Hash>>();
                string[] references = existingHashes.Where(h => hashes.Any(k => k.Field == h.Value.Field))
                                                    .Select(h => h.Key)
                                                    .ToArray();
                // updates 
                Parallel.ForEach(references, reference =>
                {
                    Hash hash;
                    if (existingHashes.TryGetValue(reference, out hash) && hashes.Any(k => k.Field == hash.Field))
                    {
                        string value = hashes.Where(k => k.Field == hash.Field).Select(k => k.Value).Single();
                        Client.Set($"hashes/{key}/{reference}/value", value);

                        // remove the hash from the list
                        hashes.RemoveAll(x => x.Field == hash.Field);
                    }
                });
            }

            // new 
            Parallel.ForEach(hashes, hash => Client.Push($"hashes/{key}", hash));
        }

        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueryBuilder builder = QueryBuilder.New().Shallow(true);
            FirebaseResponse response = Client.Get($"hashes/{key}", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                string[] hashes = response.ResultAs<string[]>();
                return hashes.LongCount();
            }

            return default(long);
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));

            FirebaseResponse response = Client.Get($"hashes/{key}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Hash> hashes = response.ResultAs<Dictionary<string, Hash>>();
                return hashes.Select(h => h.Value).Where(h => h.Field == name).Select(v => v.Value).FirstOrDefault();
            }

            return string.Empty;
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            FirebaseResponse response = Client.Get($"hashes/{key}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Hash> hashes = response.ResultAs<Dictionary<string, Hash>>();
                DateTime? expireOn = hashes.Select(h => h.Value).Min(h => h.ExpireOn);
                if (expireOn.HasValue) return expireOn.Value - DateTime.UtcNow;
            }

            return TimeSpan.FromSeconds(-1);
        }

        #endregion

        #region List

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            FirebaseResponse response = Client.Get($"lists/{key}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, List> lists = response.ResultAs<Dictionary<string, List>>();
                return lists.Select(l => l.Value).Select(l => l.Value).ToList();
            }

            return new List<string>();
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            FirebaseResponse response = Client.Get($"lists/{key}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, List> lists = response.ResultAs<Dictionary<string, List>>();
                return lists.Select(l => l.Value)
                            .OrderBy(l => l.ExpireOn)
                            .Skip(startingFrom).Take(endingAt)
                            .Select(l => l.Value).ToList();
            }

            return new List<string>();
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            FirebaseResponse response = Client.Get($"lists/{key}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, List> lists = response.ResultAs<Dictionary<string, List>>();
                DateTime? expireOn = lists.Select(l => l.Value).Min(l => l.ExpireOn);
                if (expireOn.HasValue) return expireOn.Value - DateTime.UtcNow;
            }

            return TimeSpan.FromSeconds(-1);
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueryBuilder builder = QueryBuilder.New().Shallow(true);
            FirebaseResponse response = Client.Get($"lists/{key}", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                string[] lists = response.ResultAs<string[]>();
                return lists.LongCount();
            }

            return default(long);
        }

        #endregion

    }
}