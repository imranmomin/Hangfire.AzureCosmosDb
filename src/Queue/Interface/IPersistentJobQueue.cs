using System.Threading;

using Hangfire.Storage;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Queue
{
    internal interface IPersistentJobQueue
    {
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);
        void Enqueue(string queue, string jobId);
    }
}
