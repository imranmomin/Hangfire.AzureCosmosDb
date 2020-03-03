using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

using Hangfire.Azure.Queue;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Documents;

using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure
{
    internal sealed class CosmosDbMonitoringApi : IMonitoringApi
    {
        private readonly CosmosDbStorage storage;
        private readonly object cacheLock = new object();
        private static readonly TimeSpan cacheTimeout = TimeSpan.FromSeconds(2);
        private static DateTime cacheUpdated;
        private static StatisticsDto cacheStatisticsDto;

        public CosmosDbMonitoringApi(CosmosDbStorage storage) => this.storage = storage;

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            List<QueueWithTopEnqueuedJobsDto> queueJobs = new List<QueueWithTopEnqueuedJobsDto>();

            var tuples = storage.QueueProviders
                .Select(x => x.GetJobQueueMonitoringApi())
                .SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
                .OrderBy(x => x.Queue)
                .ToArray();

            foreach (var tuple in tuples)
            {
                (int? EnqueuedCount, int? FetchedCount) counters = tuple.Monitoring.GetEnqueuedAndFetchedCount(tuple.Queue);
                JobList<EnqueuedJobDto> jobs = EnqueuedJobs(tuple.Queue, 0, 5);

                queueJobs.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Length = counters.EnqueuedCount ?? 0,
                    Fetched = counters.FetchedCount ?? 0,
                    Name = tuple.Queue,
                    FirstJobs = jobs
                });
            }

            return queueJobs;
        }

        public IList<ServerDto> Servers()
        {
            return storage.Container.GetItemLinqQueryable<Documents.Server>(requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Server) })
                .Where(s => s.DocumentType == DocumentTypes.Server)
                .OrderByDescending(s => s.CreatedOn)
                .ToQueryResult()
                .Select(server => new ServerDto
                {
                    Name = server.ServerId,
                    Heartbeat = server.LastHeartbeat,
                    Queues = server.Queues,
                    StartedAt = server.CreatedOn,
                    WorkersCount = server.Workers
                })
                .ToList();
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            Task<ItemResponse<Documents.Job>> task = storage.Container.ReadItemAsync<Documents.Job>(jobId, new PartitionKey((int)DocumentTypes.Job));
            task.Wait();

            if (task.Result.Resource != null)
            {
                Documents.Job job = task.Result;
                InvocationData invocationData = job.InvocationData;
                invocationData.Arguments = job.Arguments;

                List<StateHistoryDto> states = storage.Container.GetItemLinqQueryable<State>()
                    .Where(s => s.DocumentType == DocumentTypes.State && s.JobId == jobId)
                    .OrderByDescending(s => s.CreatedOn)
                    .Select(s => new StateHistoryDto
                    {
                        Data = s.Data,
                        CreatedAt = s.CreatedOn,
                        Reason = s.Reason,
                        StateName = s.Name
                    })
                    .ToQueryResult()
                    .ToList();

                return new JobDetailsDto
                {
                    Job = invocationData.DeserializeJob(),
                    CreatedAt = job.CreatedOn,
                    ExpireAt = job.ExpireOn,
                    Properties = job.Parameters.ToDictionary(p => p.Name, p => p.Value),
                    History = states
                };
            }

            return null;
        }

        public StatisticsDto GetStatistics()
        {
            lock (cacheLock)
            {
                if (cacheStatisticsDto == null || cacheUpdated.Add(cacheTimeout) < DateTime.UtcNow)
                {
                    Dictionary<string, long> results = new Dictionary<string, long>();

                    // get counts of jobs on state
                    QueryDefinition sql = new QueryDefinition("SELECT doc.state_name, COUNT(1) AS StateCount FROM doc WHERE doc.type = @type AND IS_DEFINED(doc.state_name) AND doc.state_name IN (@state) GROUP BY doc.state_name")
                        .WithParameter("@type", (int)DocumentTypes.Job)
                        .WithParameter("@state", new[] { EnqueuedState.StateName, FailedState.StateName, ProcessingState.StateName, ScheduledState.StateName });

                    List<(string state, int stateCount)> states = storage.Container.GetItemQueryIterator<(string state, int stateCount)>(sql, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) })
                         .ToQueryResult()
                         .ToList();

                    foreach ((string state, int stateCount) state in states)
                    {
                        results.Add(state.state, state.stateCount);
                    }
                    
                    // get counts of servers
                    sql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type")
                       .WithParameter("@type", (int)DocumentTypes.Server);

                    long servers = storage.Container.GetItemQueryIterator<long>(sql)
                        .ToQueryResult()
                        .FirstOrDefault();

                    results.Add("servers", servers);

                    // get sum of stats:succeeded / stats:deleted counters
                    sql = new QueryDefinition("SELECT doc.key, SUM(doc['value']) AS TOTAL FROM doc WHERE doc.type = @type AND doc.key IN (@key) GROUP BY doc.key")
                        .WithParameter("@key", new[] { "stats:succeeded", "stats:deleted" })
                        .WithParameter("@type", (int)DocumentTypes.Counter);

                    List<(string key, int total)> counters = storage.Container.GetItemQueryIterator<(string key, int total)>(sql)
                        .ToQueryResult()
                        .ToList();

                    foreach ((string key, int total) counter in counters)
                    {
                        results.Add(counter.key, counter.total);
                    }


                    sql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.key = @key")
                        .WithParameter("@key", "recurring-jobs")
                        .WithParameter("@type", (int)DocumentTypes.Set);

                    long jobs = storage.Container.GetItemQueryIterator<long>(sql)
                        .ToQueryResult()
                        .FirstOrDefault();

                    results.Add("recurring-jobs", jobs);

                    long getValueOrDefault(string key) => results.TryGetValue(key, out long value) ? value : default;

                    // ReSharper disable once UseObjectOrCollectionInitializer
                    cacheStatisticsDto = new StatisticsDto
                    {
                        Enqueued = getValueOrDefault("enqueued"),
                        Failed = getValueOrDefault("failed"),
                        Processing = getValueOrDefault("processing"),
                        Scheduled = getValueOrDefault("scheduled"),
                        Succeeded = getValueOrDefault("stats:succeeded"),
                        Deleted = getValueOrDefault("stats:deleted"),
                        Recurring = getValueOrDefault("recurring-jobs"),
                        Servers = getValueOrDefault("servers"),
                    };

                    cacheStatisticsDto.Queues = storage.QueueProviders
                        .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
                        .Count();

                    cacheUpdated = DateTime.UtcNow;
                }

                return cacheStatisticsDto;
            }
        }

        #region Job List

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            string queryText = $"SELECT * FROM doc WHERE doc.type = @type AND doc.name = @name AND NOT IS_DEFINED(doc.fetched_at) ORDER BY doc.created_on OFFSET {from} LIMIT {perPage}";
            return GetJobsOnQueue(queryText, queue, (state, job, fetchedAt) => new EnqueuedJobDto
            {
                Job = job,
                State = state.Name,
                InEnqueuedState = EnqueuedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                EnqueuedAt = EnqueuedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase)
                   ? JobHelper.DeserializeNullableDateTime(state.Data["EnqueuedAt"])
                   : null
            });
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            string queryText = $"SELECT * FROM doc WHERE doc.type = @type AND doc.name = @name AND IS_DEFINED(doc.fetched_at) ORDER BY doc.created_on OFFSET {from} LIMIT {perPage}";
            return GetJobsOnQueue(queryText, queue, (state, job, fetchedAt) => new FetchedJobDto
            {
                Job = job,
                State = state.Name,
                FetchedAt = fetchedAt
            });
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return GetJobsOnState(ProcessingState.StateName, from, count, (state, job) => new ProcessingJobDto
            {
                Job = job,
                InProcessingState = ProcessingState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                ServerId = state.Data.ContainsKey("ServerId") ? state.Data["ServerId"] : state.Data["ServerName"],
                StartedAt = JobHelper.DeserializeDateTime(state.Data["StartedAt"])
            });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobsOnState(ScheduledState.StateName, from, count, (state, job) => new ScheduledJobDto
            {
                Job = job,
                InScheduledState = ScheduledState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                EnqueueAt = JobHelper.DeserializeDateTime(state.Data["EnqueueAt"]),
                ScheduledAt = JobHelper.DeserializeDateTime(state.Data["ScheduledAt"])
            });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobsOnState(SucceededState.StateName, from, count, (state, job) => new SucceededJobDto
            {
                Job = job,
                InSucceededState = SucceededState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                Result = state.Data.ContainsKey("Result") ? state.Data["Result"] : null,
                TotalDuration = state.Data.ContainsKey("PerformanceDuration") && state.Data.ContainsKey("Latency")
                                ? (long?)long.Parse(state.Data["PerformanceDuration"]) + long.Parse(state.Data["Latency"])
                                : null,
                SucceededAt = JobHelper.DeserializeNullableDateTime(state.Data["SucceededAt"])
            });
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return GetJobsOnState(FailedState.StateName, from, count, (state, job) => new FailedJobDto
            {
                Job = job,
                InFailedState = FailedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                Reason = state.Reason,
                FailedAt = JobHelper.DeserializeNullableDateTime(state.Data["FailedAt"]),
                ExceptionDetails = state.Data["ExceptionDetails"],
                ExceptionMessage = state.Data["ExceptionMessage"],
                ExceptionType = state.Data["ExceptionType"]
            });
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return GetJobsOnState(DeletedState.StateName, from, count, (state, job) => new DeletedJobDto
            {
                Job = job,
                InDeletedState = DeletedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
                DeletedAt = JobHelper.DeserializeNullableDateTime(state.Data["DeletedAt"])
            });
        }

        private JobList<T> GetJobsOnState<T>(string stateName, int from, int count, Func<State, Common.Job, T> selector)
        {
            List<KeyValuePair<string, T>> jobs = new List<KeyValuePair<string, T>>();

            List<Documents.Job> filterJobs = storage.Container.GetItemLinqQueryable<Documents.Job>()
                .Where(j => j.DocumentType == DocumentTypes.Job && j.StateName == stateName)
                .OrderByDescending(j => j.CreatedOn)
                .Skip(from).Take(count)
                .ToQueryResult()
                .ToList();

            string[] ids = filterJobs.Select(x => x.StateId).ToArray();
            List<State> states = storage.Container.GetItemLinqQueryable<State>()
                .Where(x => ids.Contains(x.Id))
                .ToQueryResult()
                .ToList();

            filterJobs.ForEach(job =>
            {
                State state = states.Find(x => x.Id == job.StateId);
                InvocationData invocationData = job.InvocationData;
                invocationData.Arguments = job.Arguments;

                T data = selector(state, invocationData.DeserializeJob());
                jobs.Add(new KeyValuePair<string, T>(job.Id, data));
            });

            return new JobList<T>(jobs);
        }

        private JobList<T> GetJobsOnQueue<T>(string queryText, string queue, Func<State, Common.Job, DateTime?, T> selector)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            List<KeyValuePair<string, T>> jobs = new List<KeyValuePair<string, T>>();

            QueryDefinition sql = new QueryDefinition(queryText)
                .WithParameter("@type", (int)DocumentTypes.Queue)
                .WithParameter("@name", queue);

            List<Documents.Queue> queues = storage.Container.GetItemQueryIterator<Documents.Queue>(sql)
                .ToQueryResult()
                .ToList();

            string[] ids = queues.Select(x => x.JobId).ToArray();
            List<Documents.Job> filterJobs = storage.Container.GetItemLinqQueryable<Documents.Job>()
                .Where(x => ids.Contains(x.Id))
                .ToQueryResult()
                .ToList();

            ids = filterJobs.Select(x => x.StateId).ToArray();
            List<State> states = storage.Container.GetItemLinqQueryable<State>()
                .Where(x => ids.Contains(x.Id))
                .ToQueryResult()
                .ToList();

            queues.ForEach(queueItem =>
            {
                Documents.Job job = filterJobs.Find(x => x.Id == queueItem.JobId);
                InvocationData invocationData = job.InvocationData;
                invocationData.Arguments = job.Arguments;

                State state = states.Find(x => x.Id == job.StateId);

                T data = selector(state, invocationData.DeserializeJob(), queueItem.FetchedAt);
                jobs.Add(new KeyValuePair<string, T>(job.Id, data));
            });

            return new JobList<T>(jobs);
        }

        #endregion

        #region Counts

        public long EnqueuedCount(string queue)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));

            IPersistentJobQueueProvider provider = storage.QueueProviders.GetProvider(queue);
            IPersistentJobQueueMonitoringApi monitoringApi = provider.GetJobQueueMonitoringApi();
            (int? EnqueuedCount, int? FetchedCount) counters = monitoringApi.GetEnqueuedAndFetchedCount(queue);
            return counters.EnqueuedCount ?? 0;
        }

        public long FetchedCount(string queue)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));

            IPersistentJobQueueProvider provider = storage.QueueProviders.GetProvider(queue);
            IPersistentJobQueueMonitoringApi monitoringApi = provider.GetJobQueueMonitoringApi();
            (int? EnqueuedCount, int? FetchedCount) counters = monitoringApi.GetEnqueuedAndFetchedCount(queue);
            return counters.FetchedCount ?? 0;
        }

        public long ScheduledCount() => GetNumberOfJobsByStateName(ScheduledState.StateName);

        public long FailedCount() => GetNumberOfJobsByStateName(FailedState.StateName);

        public long ProcessingCount() => GetNumberOfJobsByStateName(ProcessingState.StateName);

        public long SucceededListCount() => GetNumberOfJobsByStateName(SucceededState.StateName);

        public long DeletedListCount() => GetNumberOfJobsByStateName(DeletedState.StateName);

        private long GetNumberOfJobsByStateName(string state)
        {
            QueryDefinition sql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type AND IS_DEFINED(doc.state_name) AND doc.state_name = @state")
                .WithParameter("@type", (int)DocumentTypes.Job)
                .WithParameter("@state", state);

            return storage.Container.GetItemQueryIterator<long>(sql)
                  .ToQueryResult()
                  .FirstOrDefault();
        }

        public IDictionary<DateTime, long> SucceededByDatesCount() => GetDatesTimelineStats("succeeded");

        public IDictionary<DateTime, long> FailedByDatesCount() => GetDatesTimelineStats("failed");

        public IDictionary<DateTime, long> HourlySucceededJobs() => GetHourlyTimelineStats("succeeded");

        public IDictionary<DateTime, long> HourlyFailedJobs() => GetHourlyTimelineStats("failed");

        private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {
            DateTime endDate = DateTime.UtcNow;
            List<DateTime> dates = new List<DateTime>();
            for (int i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            Dictionary<string, DateTime> keys = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd-HH}", x => x);
            return GetTimelineStats(keys);
        }

        private Dictionary<DateTime, long> GetDatesTimelineStats(string type)
        {
            DateTime endDate = DateTime.UtcNow.Date;
            List<DateTime> dates = new List<DateTime>();
            for (int i = 0; i < 7; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            Dictionary<string, DateTime> keys = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd}", x => x);
            return GetTimelineStats(keys);
        }

        private Dictionary<DateTime, long> GetTimelineStats(Dictionary<string, DateTime> keys)
        {
            Dictionary<DateTime, long> result = keys.ToDictionary(k => k.Value, v => default(long));
            string[] filter = keys.Keys.ToArray();

            QueryDefinition sql = new QueryDefinition("SELECT doc.key, SUM(doc['value']) AS Total FROM doc WHERE doc.type = @type AND doc.counterType = @counterType AND doc.key IN (@keys) GROUP BY doc.key")
                .WithParameter("@counterType", (int)CounterTypes.Aggregate)
                .WithParameter("@type", (int)DocumentTypes.Counter)
                .WithParameter("@keys", filter);

            List<(string key, int total)> data = storage.Container.GetItemQueryIterator<(string key, int total)>(sql)
                .ToQueryResult()
                .ToList();

            foreach (string key in keys.Keys)
            {
                DateTime date = keys.Where(k => k.Key == key).Select(k => k.Value).First();
                (string key, int total) item = data.SingleOrDefault(x => x.key == key);
                result[date] = item.total;
            }

            return result;
        }

        #endregion
    }
}
