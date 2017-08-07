using System;
using System.Linq;
using System.Collections.Generic;

using Hangfire.Common;
using Hangfire.Storage;
using Hangfire.Azure.Queue;
using Hangfire.Azure.Documents;
using Microsoft.Azure.Documents;
using Hangfire.Storage.Monitoring;
using Microsoft.Azure.Documents.Client;

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

            Array.ForEach(storage.Options.Queues, queue =>
            {
                long enqueueCount = EnqueuedCount(queue);
                JobList<EnqueuedJobDto> jobs = EnqueuedJobs(queue, 0, 1);
                queueJobs.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Length = enqueueCount,
                    Fetched = 0,
                    Name = queue,
                    FirstJobs = jobs
                });
            });

            return queueJobs;
        }

        public IList<ServerDto> Servers()
        {
            return storage.Client.CreateDocumentQuery<Documents.Server>(storage.CollectionUri, queryOptions)
                .Where(s => s.DocumentType == DocumentTypes.Server)
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

            Documents.Job job = storage.Client.CreateDocumentQuery<Documents.Job>(storage.CollectionUri, queryOptions)
                .Where(j => j.Id == jobId)
                .AsEnumerable()
                .FirstOrDefault();

            if (job != null)
            {
                InvocationData invocationData = job.InvocationData;
                invocationData.Arguments = job.Arguments;

                List<StateHistoryDto> states = storage.Client.CreateDocumentQuery<State>(storage.CollectionUri, queryOptions)
                    .Where(s => s.JobId == jobId)
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
                QueryText = "SELECT VALUE COUNT(1) FROM c WHERE c.type = @type",
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
                .Where(c => (c.Key == "stats:succeeded" || c.Key == "stats:deleted") && c.DocumentType == DocumentTypes.Counter)
                .AsEnumerable()
                .GroupBy(c => c.Key)
                .ToDictionary(g => g.Key, g => (long)g.Sum(c => c.Value));

            results = results.Concat(counters).ToDictionary(k => k.Key, v => v.Value);

            sql = new SqlQuerySpec
            {
                QueryText = "SELECT VALUE COUNT(1) FROM c WHERE c.key = @key AND c.type = @type",
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
            return new StatisticsDto
            {
                Enqueued = GetValueOrDefault("Enqueued"),
                Failed = GetValueOrDefault("Failed"),
                Processing = GetValueOrDefault("Processing"),
                Scheduled = GetValueOrDefault("Scheduled"),
                Succeeded = GetValueOrDefault("stats:succeeded"),
                Deleted = GetValueOrDefault("stats:deleted"),
                Recurring = GetValueOrDefault("recurring-jobs"),
                Servers = GetValueOrDefault("Servers"),
                Queues = storage.Options.Queues.LongLength
            };
        }

        #region Job List

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            return GetJobsOnQueue(queue, from, perPage, (state, job) => new EnqueuedJobDto
            {
                Job = job,
                State = state
            });
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            return GetJobsOnQueue(queue, from, perPage, (state, job) => new FetchedJobDto
            {
                Job = job,
                State = state
            });
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return GetJobsOnState(States.ProcessingState.StateName, from, count, (state, job) => new ProcessingJobDto
            {
                Job = job,
                ServerId = state.Data.ContainsKey("ServerId") ? state.Data["ServerId"] : state.Data["ServerName"],
                StartedAt = JobHelper.DeserializeDateTime(state.Data["StartedAt"])
            });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobsOnState(States.ScheduledState.StateName, from, count, (state, job) => new ScheduledJobDto
            {
                Job = job,
                EnqueueAt = JobHelper.DeserializeDateTime(state.Data["EnqueueAt"]),
                ScheduledAt = JobHelper.DeserializeDateTime(state.Data["ScheduledAt"])
            });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobsOnState(States.SucceededState.StateName, from, count, (state, job) => new SucceededJobDto
            {
                Job = job,
                Result = state.Data.ContainsKey("Result") ? state.Data["Result"] : null,
                TotalDuration = state.Data.ContainsKey("PerformanceDuration") && state.Data.ContainsKey("Latency")
                                ? (long?)long.Parse(state.Data["PerformanceDuration"]) + long.Parse(state.Data["Latency"])
                                : null,
                SucceededAt = JobHelper.DeserializeNullableDateTime(state.Data["SucceededAt"])
            });
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return GetJobsOnState(States.FailedState.StateName, from, count, (state, job) => new FailedJobDto
            {
                Job = job,
                Reason = state.Reason,
                FailedAt = JobHelper.DeserializeNullableDateTime(state.Data["FailedAt"]),
                ExceptionDetails = state.Data["ExceptionDetails"],
                ExceptionMessage = state.Data["ExceptionMessage"],
                ExceptionType = state.Data["ExceptionType"],
            });
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return GetJobsOnState(States.DeletedState.StateName, from, count, (state, job) => new DeletedJobDto
            {
                Job = job,
                DeletedAt = JobHelper.DeserializeNullableDateTime(state.Data["DeletedAt"])
            });
        }

        private JobList<T> GetJobsOnState<T>(string stateName, int from, int count, Func<State, Common.Job, T> selector)
        {
            // TODO: move to stored procedure
            List<KeyValuePair<string, T>> jobs = new List<KeyValuePair<string, T>>();

            List<Documents.Job> filterJobs = storage.Client.CreateDocumentQuery<Documents.Job>(storage.CollectionUri, queryOptions)
                .Where(j => j.DocumentType == DocumentTypes.Job && j.StateName == stateName)
                .AsEnumerable()
                .OrderByDescending(j => j.CreatedOn)
                .Skip(from).Take(count)
                .ToList();

            List<State> states = storage.Client.CreateDocumentQuery<State>(storage.CollectionUri, queryOptions)
                .Where(s => s.DocumentType == DocumentTypes.State)
                .AsEnumerable()
                .Where(s => filterJobs.Any(j => j.StateId == s.Id))
                .ToList();

            filterJobs.ForEach(job =>
            {
                State state = states.SingleOrDefault(s => s.Id == job.StateId);

                InvocationData invocationData = job.InvocationData;
                invocationData.Arguments = job.Arguments;

                T data = selector(state, invocationData.Deserialize());
                jobs.Add(new KeyValuePair<string, T>(job.Id, data));
            });

            return new JobList<T>(jobs);
        }

        private JobList<T> GetJobsOnQueue<T>(string queue, int from, int count, Func<string, Common.Job, T> selector)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));

            List<KeyValuePair<string, T>> jobs = new List<KeyValuePair<string, T>>();

            List<Documents.Queue> queues = storage.Client.CreateDocumentQuery<Documents.Queue>(storage.CollectionUri, queryOptions)
                .Where(q => q.Name == queue && q.DocumentType == DocumentTypes.Queue)
                .AsEnumerable()
                .Skip(from).Take(count)
                .ToList();

            List<Documents.Job> filterJobs = storage.Client.CreateDocumentQuery<Documents.Job>(storage.CollectionUri, queryOptions)
                .Where(j => j.DocumentType == DocumentTypes.Job)
                .AsEnumerable()
                .OrderByDescending(j => j.CreatedOn)
                .Where(j => queues.Any(q => q.JobId == j.Id))
                .ToList();

            queues.ForEach(queueItem =>
            {
                Documents.Job job = filterJobs.Single(j => j.Id == queueItem.JobId);
                InvocationData invocationData = job.InvocationData;
                invocationData.Arguments = job.Arguments;

                T data = selector(job.StateName, invocationData.Deserialize());
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
            return monitoringApi.GetEnqueuedCount(queue);
        }

        public long FetchedCount(string queue) => EnqueuedCount(queue);

        public long ScheduledCount() => GetNumberOfJobsByStateName(States.ScheduledState.StateName);

        public long FailedCount() => GetNumberOfJobsByStateName(States.FailedState.StateName);

        public long ProcessingCount() => GetNumberOfJobsByStateName(States.ProcessingState.StateName);

        public long SucceededListCount() => GetNumberOfJobsByStateName(States.SucceededState.StateName);

        public long DeletedListCount() => GetNumberOfJobsByStateName(States.DeletedState.StateName);

        private long GetNumberOfJobsByStateName(string state)
        {
            SqlQuerySpec sql = new SqlQuerySpec
            {
                QueryText = "SELECT VALUE COUNT(1) FROM c WHERE c.state_name = @state AND c.type = @type",
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
            List<DateTime> dates = Enumerable.Range(0, 24).Select(x => DateTime.UtcNow.AddHours(-x)).ToList();
            Dictionary<string, DateTime> keys = dates.ToDictionary(x => $"stats:{type}:{x:yyyy-MM-dd-HH}", x => x);
            return GetTimelineStats(keys);
        }

        private Dictionary<DateTime, long> GetDatesTimelineStats(string type)
        {
            List<DateTime> dates = Enumerable.Range(0, 7).Select(x => DateTime.UtcNow.AddDays(-x)).ToList();
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
