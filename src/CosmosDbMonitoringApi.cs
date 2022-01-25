using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Queue;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Job = Hangfire.Common.Job;

namespace Hangfire.Azure;

public sealed class CosmosDbMonitoringApi : IMonitoringApi
{
	private static readonly TimeSpan cacheTimeout = TimeSpan.FromSeconds(2);
	private static DateTime cacheUpdated;
	private static StatisticsDto? cacheStatisticsDto;
	private readonly object cacheLock = new();
	private readonly string[] stateNames = new[] { EnqueuedState.StateName, FailedState.StateName, ProcessingState.StateName, ScheduledState.StateName, SucceededState.StateName, AwaitingState.StateName }.Select(x => $"'{x}'").ToArray();
	private readonly CosmosDbStorage storage;

	public CosmosDbMonitoringApi(CosmosDbStorage storage)
	{
		this.storage = storage;
	}

	public IList<QueueWithTopEnqueuedJobsDto> Queues()
	{
		List<QueueWithTopEnqueuedJobsDto> queueJobs = new();

		var tuples = storage.QueueProviders
			.Select(x => x.GetJobQueueMonitoringApi())
			.SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
			.OrderBy(x => x.Queue)
			.ToArray();

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

	public IList<ServerDto> Servers() => storage.Container.GetItemLinqQueryable<Documents.Server>(requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Server) })
		.Where(s => s.DocumentType == DocumentTypes.Server)
		.OrderByDescending(s => s.CreatedOn)
		.ToQueryResult()
		.Select(server => new ServerDto
		{
			Name = server.ServerId,
			Heartbeat = server.LastHeartbeat.ToLocalTime(),
			Queues = server.Queues,
			StartedAt = server.CreatedOn,
			WorkersCount = server.Workers
		})
		.ToList();

	public JobDetailsDto? JobDetails(string jobId)
	{
		if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

		Task<ItemResponse<Documents.Job>> task = storage.Container.ReadItemAsync<Documents.Job>(jobId, new PartitionKey((int)DocumentTypes.Job));
		task.Wait();

		// if the resource is not found return null;
		if (task.Result.Resource == null) return null;

		Documents.Job job = task.Result;
		InvocationData invocationData = job.InvocationData;
		invocationData.Arguments = job.Arguments;

		List<StateHistoryDto> states = storage.Container.GetItemLinqQueryable<State>(requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.State) })
			.Where(s => s.DocumentType == DocumentTypes.State && s.JobId == jobId)
			.OrderByDescending(s => s.CreatedOn)
			.ToQueryResult()
			.Select(s => new StateHistoryDto
			{
				Data = s.Data,
				CreatedAt = s.CreatedOn.ToLocalTime(),
				Reason = s.Reason,
				StateName = s.Name
			}).ToList();

		return new JobDetailsDto
		{
			Job = invocationData.DeserializeJob(),
			CreatedAt = job.CreatedOn.ToLocalTime(),
			ExpireAt = job.ExpireOn?.ToLocalTime(),
			Properties = job.Parameters.ToDictionary(p => p.Name, p => p.Value.TryParseEpochToDate(out string? x) ? x : p.Value),
			History = states
		};

	}

	public StatisticsDto GetStatistics()
	{
		lock (cacheLock)
		{
			// if cached and not expired then return the cached item
			if (cacheStatisticsDto != null && cacheUpdated.Add(cacheTimeout) >= DateTime.UtcNow) return cacheStatisticsDto;

			// get counts of jobs on state
			QueryDefinition sql = new("SELECT doc.state_name AS state, COUNT(1) AS stateCount FROM doc WHERE doc.type = @type AND IS_DEFINED(doc.state_name) " +
			                          $"AND doc.state_name IN ({string.Join(",", stateNames)}) GROUP BY doc.state_name");

			sql.WithParameter("@type", (int)DocumentTypes.Job);

			List<(string state, int stateCount)> states = storage.Container.GetItemQueryIterator<JObject>(sql, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) })
				.ToQueryResult()
				.Select(x => (x.Value<string>("state"), x.Value<int>("stateCount")))
				.ToList()!;

			Dictionary<string, long> results = states.ToDictionary<(string state, int stateCount), string, long>(state => state.state.ToLower(), state => state.stateCount);

			// get counts of servers
			sql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type")
				.WithParameter("@type", (int)DocumentTypes.Server);

			long servers = storage.Container.GetItemQueryIterator<long>(sql, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Server) })
				.ToQueryResult()
				.FirstOrDefault();

			results.Add("servers", servers);

			// get sum of stats:succeeded / stats:deleted counters
			string[] keys = { "'stats:succeeded'", "'stats:deleted'" };
			sql = new QueryDefinition($"SELECT doc.key, SUM(doc['value']) AS total FROM doc WHERE doc.type = @type AND doc.key IN ({string.Join(",", keys)}) GROUP BY doc.key")
				.WithParameter("@type", (int)DocumentTypes.Counter);

			List<(string key, int total)> counters = storage.Container.GetItemQueryIterator<JObject>(sql, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Counter) })
				.ToQueryResult()
				.Select(x => (x.Value<string>("key"), x.Value<int>("total")))
				.ToList()!;

			foreach ((string key, int total) in counters)
			{
				results.Add(key, total);
			}

			sql = new QueryDefinition("SELECT TOP 1 VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.key = @key")
				.WithParameter("@key", "recurring-jobs")
				.WithParameter("@type", (int)DocumentTypes.Set);

			long jobs = storage.Container.GetItemQueryIterator<long>(sql, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Set) })
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
		string queryText = $"SELECT * FROM doc WHERE doc.type = @type AND doc.name = @name AND NOT IS_DEFINED(doc.fetched_at) ORDER BY doc.created_on OFFSET {from} LIMIT {perPage}";
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
		string queryText = $"SELECT * FROM doc WHERE doc.type = @type AND doc.name = @name AND IS_DEFINED(doc.fetched_at) ORDER BY doc.created_on OFFSET {from} LIMIT {perPage}";
		return GetJobsOnQueue(queryText, queue, (state, job, fetchedAt) => new FetchedJobDto
		{
			Job = job,
			State = state.Name,
			FetchedAt = fetchedAt
		});
	}

	public JobList<ProcessingJobDto> ProcessingJobs(int from, int count) => GetJobsOnState(ProcessingState.StateName, from, count, (state, job) => new ProcessingJobDto
	{
		Job = job,
		InProcessingState = ProcessingState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
		ServerId = state.Data.TryGetValue("ServerId", out string serverId) ? serverId : state.Data.TryGetValue("ServerName", out string serverName) ? serverName : null,
		StartedAt = JobHelper.DeserializeDateTime(state.Data.TryGetValue("StartedAt", out string startedAt) ? startedAt : null)
	});

	public JobList<ScheduledJobDto> ScheduledJobs(int from, int count) => GetJobsOnState(ScheduledState.StateName, from, count, (state, job) => new ScheduledJobDto
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

	public JobList<DeletedJobDto> DeletedJobs(int from, int count) => GetJobsOnState(DeletedState.StateName, from, count, (state, job) => new DeletedJobDto
	{
		Job = job,
		InDeletedState = DeletedState.StateName.Equals(state.Name, StringComparison.OrdinalIgnoreCase),
		DeletedAt = JobHelper.DeserializeNullableDateTime(state.Data.TryGetValue("DeletedAt", out string deletedAt) ? deletedAt : null)
	});

	private JobList<T> GetJobsOnState<T>(string stateName, int from, int count, Func<State, Job, T> selector)
	{
		List<KeyValuePair<string, T>> jobs = new();

		List<Documents.Job> filterJobs = storage.Container.GetItemLinqQueryable<Documents.Job>(requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) })
			.Where(j => j.DocumentType == DocumentTypes.Job && j.StateName == stateName)
			.OrderByDescending(j => j.CreatedOn)
			.Skip(from).Take(count)
			.ToQueryResult()
			.ToList();

		string[] ids = filterJobs.Select(x => x.StateId).ToArray();
		List<State> states = storage.Container.GetItemLinqQueryable<State>(requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.State) })
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

	private JobList<T> GetJobsOnQueue<T>(string queryText, string queue, Func<State, Job, DateTime?, T> selector)
	{
		if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
		List<KeyValuePair<string, T>> jobs = new();

		QueryDefinition sql = new QueryDefinition(queryText)
			.WithParameter("@type", (int)DocumentTypes.Queue)
			.WithParameter("@name", queue);

		List<Documents.Queue> queues = storage.Container.GetItemQueryIterator<Documents.Queue>(sql, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Queue) })
			.ToQueryResult()
			.ToList();

		string[] queueJobIds = queues.Select(x => x.JobId).ToArray();
		List<Documents.Job> filterJobs = storage.Container.GetItemLinqQueryable<Documents.Job>(requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) })
			.Where(x => queueJobIds.Contains(x.Id))
			.ToQueryResult()
			.ToList();

		string[] filteredJobIds = filterJobs.Select(x => x.StateId).ToArray();
		List<State> states = storage.Container.GetItemLinqQueryable<State>(requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.State) })
			.Where(x => filteredJobIds.Contains(x.Id))
			.ToQueryResult()
			.ToList();

		queues.ForEach(queueItem =>
		{
			Documents.Job job = filterJobs.Find(x => x.Id == queueItem.JobId);
			InvocationData invocationData = job.InvocationData;
			invocationData.Arguments = job.Arguments;

			State? state = states.SingleOrDefault(x => x.Id == job.StateId);

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

		return storage.Container.GetItemQueryIterator<long>(sql, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Job) })
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

		QueryDefinition sql = new QueryDefinition($"SELECT doc.key, SUM(doc['value']) AS total FROM doc WHERE doc.type = @type AND doc.key IN ({string.Join(",", filter)}) GROUP BY doc.key")
			.WithParameter("@type", (int)DocumentTypes.Counter);

		List<(string key, int total)> data = storage.Container.GetItemQueryIterator<JObject>(sql, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey((int)DocumentTypes.Counter) })
			.ToQueryResult()
			.Select(x => (x.Value<string>("key"), x.Value<int>("total")))
			.ToList()!;

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