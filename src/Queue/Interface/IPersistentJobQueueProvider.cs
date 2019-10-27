// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Queue
{
    internal interface IPersistentJobQueueProvider
    {
        IPersistentJobQueue GetJobQueue();
        IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi();
    }
}