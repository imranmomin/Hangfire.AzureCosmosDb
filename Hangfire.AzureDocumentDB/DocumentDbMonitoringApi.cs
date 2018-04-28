using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Microsoft.Azure.Documents;
using Hangfire.Storage.Monitoring;
using Microsoft.Azure.Documents.Client;

using Hangfire.Azure.Queue;
using Hangfire.Azure.Documents;

namespace Hangfire.Azure
{
    internal sealed class DocumentDbMonitoringApi : IMonitoringApi
    {
        private readonly DocumentDbStorage storage;
        private readonly FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };

        public DocumentDbMonitoringApi(DocumentDbStorage storage) => this.storage = storage;

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
            return storage.Client.CreateDocumentQuery<Documents.Server>(storage.CollectionUri, queryOptions)
                .Where(s => s.DocumentType == DocumentTypes.Server)
                .OrderByDescending(s => s.CreatedOn)
                .AsEnumerable()
                .Select(server => new ServerDto
                {
                    Name = server.ServerId,
                    Heartbeat = server.LastHeartbeat,
                    Queues = server.Queues,
                    StartedAt = server.CreatedOn,
                    WorkersCount = server.Workers
                }).ToList();
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, jobId);
            Task<DocumentResponse<Documents.Job>> task = storage.Client.ReadDocumentAsync<Documents.Job>(uri);
            Documents.Job job = task.Result;

            if (job != null)
            {
                InvocationData invocationData = job.InvocationData;
                invocationData.Arguments = job.Arguments;

                List<StateHistoryDto> states = storage.Client.CreateDocumentQuery<State>(storage.CollectionUri, queryOptions)
                    .Where(s => s.DocumentType == DocumentTypes.State && s.JobId == jobId)
                    .OrderByDescending(s => s.CreatedOn)
                    .AsEnumerable()
                    .Select(s => new StateHistoryDto
                    {
                        Data = s.Data,
                        CreatedAt = s.CreatedOn,
                        Reason = s.Reason,
                        StateName = s.Name
                    }).ToList();

                return new JobDetailsDto
                {
                    Job = invocationData.Deserialize(),
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
            Dictionary<string, long> results = new Dictionary<string, long>();

            // get counts of jobs groupby on state
            Dictionary<string, long> states = storage.Client.CreateDocumentQuery<Documents.Job>(storage.CollectionUri, queryOptions)
                .Where(j => j.DocumentType == DocumentTypes.Job)
                .Select(j => j.StateName)
                .AsEnumerable()
                .Where(j => !string.IsNullOrEmpty(j))
                .GroupBy(j => j)
                .ToDictionary(g => g.Key, g => g.LongCount());

            results = results.Concat(states).ToDictionary(k => k.Key, v => v.Value);

            // get counts of servers
            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT VALUE COUNT(1) FROM doc WHERE doc.type = @type",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@type", DocumentTypes.Server),
                }
            };

            long servers = storage.Client.CreateDocumentQuery<long>(storage.CollectionUri, sql)
                .AsEnumerable()
                .FirstOrDefault();

            results.Add("Servers", servers);

            // get sum of stats:succeeded counters  raw / aggregate
            Dictionary<string, long> counters = storage.Client.CreateDocumentQuery<Counter>(storage.CollectionUri, queryOptions)
                .Where(c => c.DocumentType == DocumentTypes.Counter && (c.Key == "stats:succeeded" || c.Key == "stats:deleted"))
                .AsEnumerable()
                .GroupBy(c => c.Key)
                .ToDictionary(g => g.Key, g => (long)g.Sum(c => c.Value));

            results = results.Concat(counters).ToDictionary(k => k.Key, v => v.Value);

            sql = new SqlQuerySpec
            {
                QueryText = "SELECT VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.key = @key",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@key", "recurring-jobs"),
                    new SqlParameter("@type", DocumentTypes.Set),
                }
            };

            long count = storage.Client.CreateDocumentQuery<long>(storage.CollectionUri, sql)
                .AsEnumerable()
                .FirstOrDefault();

            results.Add("recurring-jobs", count);

            long GetValueOrDefault(string key) => results.Where(r => r.Key == key).Select(r => r.Value).SingleOrDefault();

            // ReSharper disable once UseObjectOrCollectionInitializer
            StatisticsDto statistics = new StatisticsDto
            {
                Enqueued = GetValueOrDefault("Enqueued"),
                Failed = GetValueOrDefault("Failed"),
                Processing = GetValueOrDefault("Processing"),
                Scheduled = GetValueOrDefault("Scheduled"),
                Succeeded = GetValueOrDefault("stats:succeeded"),
                Deleted = GetValueOrDefault("stats:deleted"),
                Recurring = GetValueOrDefault("recurring-jobs"),
                Servers = GetValueOrDefault("Servers"),
            };

            statistics.Queues = storage.QueueProviders
                .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
                .Count();

            return statistics;
        }

        #region Job List

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            string queryText = "SELECT * FROM doc WHERE doc.type = @type AND doc.name = @name AND NOT is_defined(doc.fetched_at) ORDER BY doc.created_on";
            return GetJobsOnQueue(queryText, queue, from, perPage, (state, job, fetchedAt) => new EnqueuedJobDto
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
            string queryText = "SELECT * FROM doc WHERE doc.type = @type AND doc.name = @name AND is_defined(doc.fetched_at) ORDER BY doc.created_on";
            return GetJobsOnQueue(queryText, queue, from, perPage, (state, job, fetchedAt) => new FetchedJobDto
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
                ExceptionType = state.Data["ExceptionType"],
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
            // TODO: move to stored procedure
            List<KeyValuePair<string, T>> jobs = new List<KeyValuePair<string, T>>();

            List<Documents.Job> filterJobs = storage.Client.CreateDocumentQuery<Documents.Job>(storage.CollectionUri, queryOptions)
                .Where(j => j.DocumentType == DocumentTypes.Job && j.StateName == stateName)
                .OrderByDescending(j => j.CreatedOn)
                .AsEnumerable()
                .Skip(from).Take(count)
                .ToList();

            filterJobs.ForEach(job =>
            {
                Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, job.StateId);
                Task<DocumentResponse<State>> task = storage.Client.ReadDocumentAsync<State>(uri);

                State state = task.Result;
                if (state != null)
                {
                    InvocationData invocationData = job.InvocationData;
                    invocationData.Arguments = job.Arguments;

                    T data = selector(state, invocationData.Deserialize());
                    jobs.Add(new KeyValuePair<string, T>(job.Id, data));
                }
            });

            return new JobList<T>(jobs);
        }

        private JobList<T> GetJobsOnQueue<T>(string queryText, string queue, int from, int count, Func<State, Common.Job, DateTime?, T> selector)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            List<KeyValuePair<string, T>> jobs = new List<KeyValuePair<string, T>>();

            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = queryText,
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@type", DocumentTypes.Queue),
                    new SqlParameter("@name", queue)
                }
            };

            List<Documents.Queue> queues = storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri, sql)
                .AsEnumerable()
                .OrderBy(q => q.CreatedOn)
                .Skip(from).Take(count)
                .ToList();

            queues.ForEach(queueItem =>
            {
                Uri uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, queueItem.JobId);
                Task<DocumentResponse<Documents.Job>> task = storage.Client.ReadDocumentAsync<Documents.Job>(uri);

                Documents.Job job = task.Result;
                if (job != null)
                {
                    InvocationData invocationData = job.InvocationData;
                    invocationData.Arguments = job.Arguments;

                    uri = UriFactory.CreateDocumentUri(storage.Options.DatabaseName, storage.Options.CollectionName, job.StateId);
                    Task<DocumentResponse<State>> stateTask = storage.Client.ReadDocumentAsync<State>(uri);
                   
                    T data = selector(stateTask.Result, invocationData.Deserialize(), queueItem.FetchedAt);
                    jobs.Add(new KeyValuePair<string, T>(job.Id, data));
                }
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
            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT VALUE COUNT(1) FROM doc WHERE doc.type = @type AND doc.state_name = @state",
                Parameters = new SqlParameterCollection
                {
                    new SqlParameter("@state", state),
                    new SqlParameter("@type", DocumentTypes.Job),
                }
            };

            return storage.Client.CreateDocumentQuery<long>(storage.CollectionUri, sql)
                  .AsEnumerable()
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

            Dictionary<string, int> data = storage.Client.CreateDocumentQuery<Counter>(storage.CollectionUri, queryOptions)
                .Where(c => c.Type == CounterTypes.Aggregrate && c.DocumentType == DocumentTypes.Counter)
                .AsEnumerable()
                .Where(c => keys.ContainsKey(c.Key))
                .ToDictionary(k => k.Key, k => k.Value);

            foreach (string key in keys.Keys)
            {
                DateTime date = keys.Where(k => k.Key == key).Select(k => k.Value).First();
                result[date] = data.ContainsKey(key) ? data[key] : 0;
            }

            return result;
        }

        #endregion
    }
}
