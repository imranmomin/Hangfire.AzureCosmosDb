using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Collections.Generic;

using Hangfire.Common;
using Hangfire.Storage;
using Hangfire.AzureDocumentDB.Queue;
using Hangfire.AzureDocumentDB.Entities;
using Hangfire.Storage.Monitoring;

namespace Hangfire.AzureDocumentDB
{
    internal sealed class AzureDocumentDbMonitoringApi : IMonitoringApi
    {
        private readonly AzureDocumentDbConnection connection;
        private readonly AzureDocumentDbStorage storage;

        public AzureDocumentDbMonitoringApi(AzureDocumentDbStorage storage)
        {
            this.storage = storage;
            connection = (AzureDocumentDbConnection)storage.GetConnection();
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            List<QueueWithTopEnqueuedJobsDto> queueJobs = new List<QueueWithTopEnqueuedJobsDto>();

            Parallel.ForEach(storage.Options.Queues, queue =>
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
            List<ServerDto> servers = new List<ServerDto>();

            FirebaseResponse response = connection.Client.Get("servers");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Entities.Server> collections = response.ResultAs<Dictionary<string, Entities.Server>>();
                servers = collections?.Select(s => new ServerDto
                {
                    Name = s.Value.ServerId,
                    Heartbeat = s.Value.LastHeartbeat,
                    Queues = s.Value.Queues,
                    StartedAt = s.Value.CreatedOn,
                    WorkersCount = s.Value.Workers
                }).ToList();
            }

            return servers;
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            if (string.IsNullOrEmpty(jobId)) throw new ArgumentNullException(nameof(jobId));

            List<StateHistoryDto> states = new List<StateHistoryDto>();

            FirebaseResponse response = connection.Client.Get($"jobs/{jobId}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Entities.Job job = response.ResultAs<Entities.Job>();
                InvocationData invocationData = job.InvocationData;
                invocationData.Arguments = job.Arguments;

                response = connection.Client.Get($"states/{jobId}");
                if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
                {
                    Dictionary<string, State> collections = response.ResultAs<Dictionary<string, State>>();
                    states = collections.Select(s => new StateHistoryDto
                    {
                        Data = s.Value.Data.Trasnform(),
                        CreatedAt = s.Value.CreatedOn,
                        Reason = s.Value.Reason,
                        StateName = s.Value.Name
                    }).ToList();
                }

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
            int count = 0;
            Dictionary<string, long> results = new Dictionary<string, long>();

            // get counts of jobs groupby on state
            FirebaseResponse response = connection.Client.Get("jobs");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Entities.Job> collections = response.ResultAs<Dictionary<string, Entities.Job>>();
                Dictionary<string, long> data = collections.Select(c => c.Value)
                                                           .GroupBy(j => j.StateName)
                                                           .Where(g => !string.IsNullOrEmpty(g.Key))
                                                           .ToDictionary(g => g.Key, g => g.LongCount());
                results = results.Concat(data).ToDictionary(k => k.Key, v => v.Value);
            }

            // get counts of servers
            QueryBuilder builder = QueryBuilder.New();
            builder.Shallow(true);
            response = connection.Client.Get("servers", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, bool> collections = response.ResultAs<Dictionary<string, bool>>();
                results.Add("Servers", collections.LongCount());
            }

            // get sum of stats:deleted counters / aggregatedcounter 
            response = connection.Client.Get("counters/raw/stats:succeeded");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Counter> collections = response.ResultAs<Dictionary<string, Counter>>();
                count += collections.Sum(c => c.Value.Value);
            }

            response = connection.Client.Get("counters/aggregrated/stats:succeeded");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Counter counter = response.ResultAs<Counter>();
                count += counter.Value;
            }
            results.Add("stats:succeeded", count);

            // get sum of stats:deleted counters / aggregatedcounter 
            count = 0;
            response = connection.Client.Get("counters/raw/stats:deleted");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Counter> collections = response.ResultAs<Dictionary<string, Counter>>();
                count += collections.Sum(c => c.Value.Value);
            }

            response = connection.Client.Get("counters/aggregrated/stats:deleted");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Counter counter = response.ResultAs<Counter>();
                count += counter.Value;
            }
            results.Add("stats:deleted", count);

            // get recurring-jobs count from sets
            builder = QueryBuilder.New(@"equalTo=""recurring-jobs""");
            builder.OrderBy("key");
            response = connection.Client.Get("sets", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Set> collections = response.ResultAs<Dictionary<string, Set>>();
                results.Add("recurring-jobs", collections.LongCount());
            }

            Func<string, long> getValueOrDefault = (key) => results.Where(r => r.Key == key).Select(r => r.Value).SingleOrDefault();
            return new StatisticsDto
            {
                Enqueued = getValueOrDefault("Enqueued"),
                Failed = getValueOrDefault("Failed"),
                Processing = getValueOrDefault("Processing"),
                Scheduled = getValueOrDefault("Scheduled"),
                Succeeded = getValueOrDefault("stats:succeeded"),
                Deleted = getValueOrDefault("stats:deleted"),
                Recurring = getValueOrDefault("recurring-jobs"),
                Servers = getValueOrDefault("Servers"),
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
            List<KeyValuePair<string, T>> jobs = new List<KeyValuePair<string, T>>();

            QueryBuilder builder = QueryBuilder.New($@"equalTo=""{stateName}""");
            builder.OrderBy("state_name");
            FirebaseResponse response = connection.Client.Get("jobs", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Entities.Job> collections = response.ResultAs<Dictionary<string, Entities.Job>>();
                string[] references = collections.Skip(from).Take(count).Select(k => k.Key).ToArray();
                Parallel.ForEach(references, reference =>
                {
                    Entities.Job job;
                    if (collections.TryGetValue(reference, out job))
                    {
                        FirebaseResponse stateResponse = connection.Client.Get($"states/{reference}/{job.StateId}");
                        if (stateResponse.StatusCode == HttpStatusCode.OK && !stateResponse.IsNull())
                        {
                            State state = stateResponse.ResultAs<State>();
                            state.Data = state.Data.Trasnform();

                            InvocationData invocationData = job.InvocationData;
                            invocationData.Arguments = job.Arguments;

                            T data = selector(state, invocationData.Deserialize());
                            jobs.Add(new KeyValuePair<string, T>(reference, data));
                        }
                    }
                });
            }

            return new JobList<T>(jobs);
        }

        private JobList<T> GetJobsOnQueue<T>(string queue, int from, int count, Func<string, Common.Job, T> selector)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));

            List<KeyValuePair<string, T>> jobs = new List<KeyValuePair<string, T>>();

            FirebaseResponse response = connection.Client.Get($"queue/{queue}");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, string> collection = response.ResultAs<Dictionary<string, string>>();
                string[] references = collection.Skip(from).Take(count).Select(k => k.Value).ToArray();
                Parallel.ForEach(references, reference =>
                {
                    FirebaseResponse jobResponse = connection.Client.Get($"jobs/{reference}");
                    if (jobResponse.StatusCode == HttpStatusCode.OK && !jobResponse.IsNull())
                    {
                        Entities.Job job = jobResponse.ResultAs<Entities.Job>();
                        InvocationData invocationData = job.InvocationData;
                        invocationData.Arguments = job.Arguments;

                        T data = selector(job.StateName, invocationData.Deserialize());
                        jobs.Add(new KeyValuePair<string, T>(reference, data));
                    }
                });
            }

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
            QueryBuilder builder = QueryBuilder.New($@"equalTo=""{state}""");
            builder.OrderBy("state_name");
            FirebaseResponse response = connection.Client.Get("jobs", builder);
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Entities.Job> jobs = response.ResultAs<Dictionary<string, Entities.Job>>();
                return jobs.LongCount();
            }

            return default(long);
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

            FirebaseResponse response = connection.Client.Get("counters/aggregrated");
            if (response.StatusCode == HttpStatusCode.OK && !response.IsNull())
            {
                Dictionary<string, Counter> collections = response.ResultAs<Dictionary<string, Counter>>();
                Dictionary<string, int> data = collections.Where(k => keys.ContainsKey(k.Key)).ToDictionary(k => k.Key, k => k.Value.Value);

                foreach (string key in keys.Keys)
                {
                    DateTime date = keys.Where(k => k.Key == key).Select(k => k.Value).First();
                    result[date] = data.ContainsKey(key) ? data[key] : 0;
                }
            }

            return result;
        }

        #endregion
    }
}
