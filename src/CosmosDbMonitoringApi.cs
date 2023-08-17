using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Queue;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Job = Hangfire.Common.Job;

namespace Hangfire.Azure;

internal sealed class CosmosDbMonitoringApi : IMonitoringApi
{
    private static readonly TimeSpan cacheTimeout = TimeSpan.FromSeconds(2);
    private static DateTime cacheUpdated;
    private static StatisticsDto? cacheStatisticsDto;
    private readonly object cacheLock = new();
    private readonly QueryDefinition recurringJobSql;
    private readonly QueryDefinition serverSql;
    private readonly QueryDefinition stateSql;
    private readonly QueryDefinition statsSql;
    private readonly CosmosDbStorage storage;

    public CosmosDbMonitoringApi(CosmosDbStorage storage)
    {
        this.storage = storage;

        // state query
        string[] stateNames = new[]
        {
            EnqueuedState.StateName,
            FailedState.StateName,
            ProcessingState.StateName,
            ScheduledState.StateName,
            SucceededState.StateName,
            AwaitingState.StateName
        }.Select(x => $"'{x}'").ToArray();

        stateSql = new QueryDefinition($"SELECT doc.state_name AS state, COUNT(1) AS stateCount FROM doc WHERE IS_DEFINED(doc.state_name) AND doc.state_name IN ({string.Join(",", stateNames)}) GROUP BY doc.state_name");

        // server query
        serverSql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc");

        // stats query
        string[] keys =
        {
            "'stats:succeeded'",
            "'stats:deleted'"
        };

        statsSql = new QueryDefinition($"SELECT doc.key, SUM(doc['value']) AS total FROM doc WHERE doc.key IN ({string.Join(",", keys)}) GROUP BY doc.key");

        // recurring job query
        recurringJobSql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.key = @key")
            .WithParameter("@key", "recurring-jobs");
    }

    public IList<QueueWithTopEnqueuedJobsDto> Queues()
    {
        var tuples = storage.QueueProviders
            .Select(x => x.GetJobQueueMonitoringApi())
            .SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
            .OrderBy(x => x.Queue)
            .ToArray();

        List<QueueWithTopEnqueuedJobsDto> queueJobs = new(tuples.Length);

        // ReSharper disable once LoopCanBeConvertedToQuery
        foreach (var tuple in tuples)
        {
            (int? enqueuedCount, int? fetchedCount) = tuple.Monitoring.GetEnqueuedAndFetchedCount(tuple.Queue);
            JobList<EnqueuedJobDto> jobs = EnqueuedJobs(tuple.Queue, 0, 5);

            queueJobs.Add(new QueueWithTopEnqueuedJobsDto
            {
                Length = enqueuedCount ?? 0,
                Fetched = fetchedCount ?? 0,
                Name = tuple.Queue,
                FirstJobs = jobs
            });
        }

        return queueJobs;
    }

    public IList<ServerDto> Servers() => storage.Container.GetItemLinqQueryable<Documents.Server>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Server })
        .OrderByDescending(s => s.CreatedOn)
        .ToQueryResult()
        .Select(server => new ServerDto
        {
            Name = server.Id,
            Heartbeat = server.LastHeartbeat,
            Queues = server.Queues,
            StartedAt = server.CreatedOn,
            WorkersCount = server.Workers
        }).ToList();

    public JobDetailsDto? JobDetails(string jobId)
    {
        if (string.IsNullOrEmpty(jobId))
        {
            throw new ArgumentNullException(nameof(jobId));
        }

        if (Guid.TryParse(jobId, out Guid _) == false)
        {
            return null;
        }

        try
        {
            Documents.Job data = storage.Container.ReadItemWithRetries<Documents.Job>(jobId, PartitionKeys.Job);

            List<StateHistoryDto> states = storage.Container.GetItemLinqQueryable<State>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.State })
                .Where(s => s.JobId == jobId)
                .OrderByDescending(s => s.CreatedOn)
                .ToQueryResult()
                .Select(s => new StateHistoryDto
                {
                    Data = s.Data,
                    CreatedAt = s.CreatedOn,
                    Reason = s.Reason,
                    StateName = s.Name
                }).ToList();

            List<Parameter> parameters = data.Parameters.ToList();

            Job? job = null;
            InvocationData? invocationData = null;

            try
            {
                invocationData = data.InvocationData;
                invocationData.Arguments = data.Arguments;
                job = invocationData.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                parameters.Add(new Parameter { Name = "DBG_Exception", Value = (ex.InnerException ?? ex).Message });
            }

            if (invocationData != null)
            {
                parameters.Add(new Parameter { Name = "DBG_Type", Value = invocationData.Type });
                parameters.Add(new Parameter { Name = "DBG_Method", Value = invocationData.Method });
                parameters.Add(new Parameter { Name = "DBG_Args", Value = invocationData.Arguments });
            }
            else
            {
                parameters.Add(new Parameter { Name = "DBG_Payload", Value = JsonConvert.SerializeObject(data.InvocationData) });
                parameters.Add(new Parameter { Name = "DBG_Args", Value = JsonConvert.SerializeObject(data.Arguments) });
            }

            return new JobDetailsDto
            {
                Job = job,
                CreatedAt = data.CreatedOn,
                ExpireAt = data.ExpireOn,
                Properties = parameters.GroupBy(x => x.Name).Select(x => x.First()).ToDictionary(p => p.Name, p => p.Value),
                History = states
            };
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            /* ignored */
        }
        catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
        {
            /* ignored */
        }

        return null;
    }

    public StatisticsDto GetStatistics()
    {
        lock (cacheLock)
        {
            // if cached and not expired then return the cached item
            if (cacheStatisticsDto != null && cacheUpdated.Add(cacheTimeout) >= DateTime.UtcNow)
            {
                return cacheStatisticsDto;
            }

            // get counts of jobs on state
            List<(string state, int stateCount)> states = storage.Container.GetItemQueryIterator<JObject>(stateSql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Job })
                .ToQueryResult()
                .Select(x => (x.Value<string>("state"), x.Value<int>("stateCount")))
                .ToList()!;

            Dictionary<string, long> results = states.ToDictionary<(string state, int stateCount), string, long>(state => state.state.ToLower(), state => state.stateCount);

            // get counts of servers
            long servers = storage.Container.GetItemQueryIterator<long>(serverSql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Server })
                .ToQueryResult()
                .FirstOrDefault();

            results.Add("servers", servers);

            // get sum of stats:succeeded / stats:deleted counters
            List<(string key, int total)> counters = storage.Container.GetItemQueryIterator<JObject>(statsSql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Counter })
                .ToQueryResult()
                .Select(x => (x.Value<string>("key"), x.Value<int>("total")))
                .ToList()!;

            foreach ((string key, int total) in counters)
            {
                results.Add(key, total);
            }

            // get the recurring job counts
            long jobs = storage.Container.GetItemQueryIterator<long>(recurringJobSql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Set })
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
                Servers = getValueOrDefault("servers")
            };

            cacheStatisticsDto.Queues = storage.QueueProviders
                .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
                .Count();

            cacheUpdated = DateTime.UtcNow;

            return cacheStatisticsDto;
        }
    }

    #region Job List

    public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
    {
        string queryText = $"SELECT * FROM doc WHERE doc.name = @name AND NOT IS_DEFINED(doc.fetched_at) ORDER BY doc.created_on OFFSET {from} LIMIT {perPage}";
        return GetJobsOnQueue(queryText, queue, (state, job, _) => new EnqueuedJobDto
        {
            Job = job,
            State = state.Name,
            InEnqueuedState = EnqueuedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
            EnqueuedAt = JobHelper.DeserializeNullableDateTime(state.Data.TryGetValue("EnqueuedAt", out string enqueuedAt) ? enqueuedAt : null)
        });
    }

    public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
    {
        string queryText = $"SELECT * FROM doc WHERE doc.name = @name AND IS_DEFINED(doc.fetched_at) ORDER BY doc.created_on OFFSET {from} LIMIT {perPage}";
        return GetJobsOnQueue(queryText, queue, (state, job, fetchedAt) => new FetchedJobDto { Job = job, State = state.Name, FetchedAt = fetchedAt });
    }

    public JobList<ProcessingJobDto> ProcessingJobs(int from, int count) => GetJobsOnState(ProcessingState.StateName, from, count,
        (state, job) => new ProcessingJobDto
        {
            Job = job,
            InProcessingState = ProcessingState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
            ServerId = state.Data.TryGetValue("ServerId", out string serverId) ? serverId : state.Data.TryGetValue("ServerName", out string serverName) ? serverName : null,
            StartedAt = JobHelper.DeserializeDateTime(state.Data.TryGetValue("StartedAt", out string startedAt) ? startedAt : null)
        });

    public JobList<ScheduledJobDto> ScheduledJobs(int from, int count) => GetJobsOnState(ScheduledState.StateName, from, count,
        (state, job) => new ScheduledJobDto
        {
            Job = job,
            InScheduledState = ScheduledState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
            EnqueueAt = JobHelper.DeserializeDateTime(state.Data.TryGetValue("EnqueueAt", out string enqueuedAt) ? enqueuedAt : null),
            ScheduledAt = JobHelper.DeserializeDateTime(state.Data.TryGetValue("ScheduledAt", out string scheduledAt) ? scheduledAt : null)
        });

    public JobList<SucceededJobDto> SucceededJobs(int from, int count) => GetJobsOnState(SucceededState.StateName, from, count, (state, job) => new SucceededJobDto
    {
        Job = job,
        InSucceededState = SucceededState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
        Result = state.Data.TryGetValue("Result", out string result) ? result : null,
        TotalDuration = state.Data.TryGetValue("PerformanceDuration", out string performanceDuration) && state.Data.TryGetValue("Latency", out string latency)
            ? (long?)long.Parse(performanceDuration) + long.Parse(latency)
            : null,
        SucceededAt = JobHelper.DeserializeNullableDateTime(state.Data.TryGetValue("SucceededAt", out string succeededAt) ? succeededAt : null)
    });

    public JobList<FailedJobDto> FailedJobs(int from, int count) => GetJobsOnState(FailedState.StateName, from, count, (state, job) => new FailedJobDto
    {
        Job = job,
        InFailedState = FailedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
        Reason = state.Reason,
        FailedAt = JobHelper.DeserializeNullableDateTime(state.Data.TryGetValue("FailedAt", out string failedAt) ? failedAt : null),
        ExceptionDetails = state.Data.TryGetValue("ExceptionDetails", out string exceptionDetails) ? exceptionDetails : null,
        ExceptionMessage = state.Data.TryGetValue("ExceptionMessage", out string exceptionMessage) ? exceptionMessage : null,
        ExceptionType = state.Data.TryGetValue("ExceptionType", out string exceptionType) ? exceptionType : null
    });

    public JobList<DeletedJobDto> DeletedJobs(int from, int count) => GetJobsOnState(DeletedState.StateName, from, count,
        (state, job) => new DeletedJobDto { Job = job, InDeletedState = DeletedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase), DeletedAt = JobHelper.DeserializeNullableDateTime(state.Data.TryGetValue("DeletedAt", out string deletedAt) ? deletedAt : null) });

    private JobList<T> GetJobsOnState<T>(string stateName, int from, int count, Func<State, Job, T> selector)
    {
        List<KeyValuePair<string, T>> jobs = new();

        List<Documents.Job> filterJobs = storage.Container.GetItemLinqQueryable<Documents.Job>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Job })
            .Where(j => j.StateName == stateName)
            .OrderByDescending(j => j.CreatedOn)
            .Skip(from).Take(count)
            .ToQueryResult()
            .ToList();

        if (filterJobs.Count == 0)
        {
            return new JobList<T>(jobs);
        }

        string[] filterJobStateIds = filterJobs.Select(x => x.StateId).ToArray();
        List<State> states = storage.Container.GetItemLinqQueryable<State>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.State })
            .Where(x => filterJobStateIds.Contains(x.Id))
            .ToQueryResult()
            .ToList();

        filterJobs.ForEach(x =>
        {
            T data = default!;

            try
            {
                State state = states.Find(s => s.Id == x.StateId);

                InvocationData invocationData = x.InvocationData;
                invocationData.Arguments = x.Arguments;
                Job? job = invocationData.DeserializeJob();

                data = selector(state, job);
            }
            catch (JobLoadException)
            {
                /* Ignore */
            }

            jobs.Add(new KeyValuePair<string, T>(x.Id, data));
        });

        return new JobList<T>(jobs);
    }

    private JobList<T> GetJobsOnQueue<T>(string queryText, string queue, Func<State, Job, DateTime?, T> selector)
    {
        if (string.IsNullOrEmpty(queue))
        {
            throw new ArgumentNullException(nameof(queue));
        }

        List<KeyValuePair<string, T>> jobs = new();

        QueryDefinition sql = new QueryDefinition(queryText)
            .WithParameter("@name", queue);

        List<Documents.Queue> queues = storage.Container.GetItemQueryIterator<Documents.Queue>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Queue })
            .ToQueryResult()
            .ToList();

        if (queues.Count == 0)
        {
            return new JobList<T>(jobs);
        }

        string[] queueJobIds = queues.Select(x => x.JobId).ToArray();
        List<Documents.Job> filterJobs = storage.Container.GetItemLinqQueryable<Documents.Job>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Job })
            .Where(x => queueJobIds.Contains(x.Id))
            .ToQueryResult()
            .ToList();

        string[] filteredJobStateIds = filterJobs.Select(x => x.StateId).ToArray();
        List<State> states = storage.Container.GetItemLinqQueryable<State>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.State })
            .Where(x => filteredJobStateIds.Contains(x.Id))
            .ToQueryResult()
            .ToList();

        foreach (Documents.Queue queueItem in queues)
        {
            T data = default!;

            try
            {
                Documents.Job sqlJob = filterJobs.Find(x => x.Id == queueItem.JobId);
                State? state = states.SingleOrDefault(x => x.Id == sqlJob.StateId);

                InvocationData invocationData = sqlJob.InvocationData;
                invocationData.Arguments = sqlJob.Arguments;
                Job? job = invocationData.DeserializeJob();

                data = selector(state, job, queueItem.FetchedAt);
            }
            catch (JobLoadException)
            {
                /* Ignore */
            }

            jobs.Add(new KeyValuePair<string, T>(queueItem.JobId, data));
        }

        return new JobList<T>(jobs);
    }

    #endregion

    #region Counts

    public long EnqueuedCount(string queue)
    {
        if (string.IsNullOrEmpty(queue))
        {
            throw new ArgumentNullException(nameof(queue));
        }

        IPersistentJobQueueProvider provider = storage.QueueProviders.GetProvider(queue);
        IPersistentJobQueueMonitoringApi monitoringApi = provider.GetJobQueueMonitoringApi();
        (int? EnqueuedCount, int? FetchedCount) counters = monitoringApi.GetEnqueuedAndFetchedCount(queue);
        return counters.EnqueuedCount ?? 0;
    }

    public long FetchedCount(string queue)
    {
        if (string.IsNullOrEmpty(queue))
        {
            throw new ArgumentNullException(nameof(queue));
        }

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
        QueryDefinition sql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE IS_DEFINED(doc.state_name) AND doc.state_name = @state")
            .WithParameter("@state", state);

        return storage.Container.GetItemQueryIterator<long>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Job })
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
        List<DateTime> dates = new();
        for (int i = 0; i < 20; i++)
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
        List<DateTime> dates = new();
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
        Dictionary<DateTime, long> result = keys.ToDictionary(k => k.Value, _ => default(long));
        string[] filter = keys.Keys.Select(x => $"'{x}'").ToArray();

        QueryDefinition sql = new($"SELECT doc.key, SUM(doc['value']) AS total FROM doc WHERE doc.counterType = {(int)CounterTypes.Aggregate} AND doc.key IN ({string.Join(",", filter)}) GROUP BY doc.key");

        List<(string key, int total)> data = storage.Container.GetItemQueryIterator<JObject>(sql, requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Counter })
            .ToQueryResult()
            .Select(x => (x.Value<string>("key"), x.Value<int>("total")))
            .ToList()!;

        foreach (string key in keys.Keys)
        {
            DateTime date = keys.Where(k => k.Key == key).Select(k => k.Value).Single();
            (string key, int total) item = data.SingleOrDefault(x => x.key == key);
            result[date] = item.total;
        }

        return result;
    }

    #endregion
}