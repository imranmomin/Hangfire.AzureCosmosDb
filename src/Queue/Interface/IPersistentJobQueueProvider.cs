// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Queue;

public interface IPersistentJobQueueProvider
{
    IPersistentJobQueue GetJobQueue();
    IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi();
}