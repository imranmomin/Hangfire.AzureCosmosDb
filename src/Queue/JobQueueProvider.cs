namespace Hangfire.Azure.Queue;

internal class JobQueueProvider : IPersistentJobQueueProvider
{
	private readonly JobQueueMonitoringApi monitoringQueue;
	private readonly JobQueue queue;

	public JobQueueProvider(CosmosDbStorage storage)
	{
		queue = new JobQueue(storage);
		monitoringQueue = new JobQueueMonitoringApi(storage);
	}

	public IPersistentJobQueue GetJobQueue() => queue;

	public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi() => monitoringQueue;
}