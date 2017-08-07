using System.Collections.Generic;

// ReSharper disable UnusedMemberInSuper.Global
// ReSharper disable UnusedMember.Global
namespace Hangfire.Azure.Queue
{
    internal interface IPersistentJobQueueMonitoringApi
    {
        IEnumerable<string> GetQueues();
        IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage);
        IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage);
        int GetEnqueuedCount(string queue);
    }
}
