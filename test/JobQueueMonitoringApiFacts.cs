using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Azure.Queue;
using Hangfire.Azure.Tests.Fixtures;
using Xunit;

namespace Hangfire.Azure.Tests;

public class JobQueueMonitoringApiFacts : IClassFixture<ContainerFixture>
{
    private ContainerFixture ContainerFixture { get; }
    private CosmosDbStorage Storage { get; }

    public JobQueueMonitoringApiFacts(ContainerFixture containerFixture)
    {
        ContainerFixture = containerFixture;
        Storage = containerFixture.Storage;
    }

    [Fact]
    public void GetQueues_WhenIsEmpty()
    {
        // clean
        ContainerFixture.Clean();

        // act
        JobQueueMonitoringApi monitoring = new JobQueueMonitoringApi(Storage);
        IEnumerable<string> queues = monitoring.GetQueues();

        // assert
        Assert.Empty(queues);
    }

    [Fact]
    public void GetQueues_WhenIsNotEmpty()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        JobQueue jobQueue = new JobQueue(Storage);
        jobQueue.Enqueue("default", Guid.NewGuid().ToString());
        jobQueue.Enqueue("high", Guid.NewGuid().ToString());

        // act
        JobQueueMonitoringApi monitoring = new JobQueueMonitoringApi(Storage);
        IEnumerable<string> queues = monitoring.GetQueues();

        //assert
        Assert.NotEmpty(queues);
    }

    [Theory]
    [InlineData("default")]
    public void GetEnqueuedCount_WhenIsEmpty(string queue)
    {
        // clean
        ContainerFixture.Clean();

        // act
        JobQueueMonitoringApi monitoring = new JobQueueMonitoringApi(Storage);
        long count = monitoring.GetEnqueuedCount(queue);

        //assert
        Assert.Equal(0, count);
    }

    [Theory]
    [InlineData("default")]
    public void GetEnqueuedCount_WhenIsNotEmpty(string queue)
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        JobQueue jobQueue = new JobQueue(Storage);
        jobQueue.Enqueue(queue, Guid.NewGuid().ToString());
        jobQueue.Enqueue(queue, Guid.NewGuid().ToString());

        // act
        JobQueueMonitoringApi monitoring = new JobQueueMonitoringApi(Storage);
        long count = monitoring.GetEnqueuedCount(queue);

        //assert
        Assert.Equal(2, count);
    }

    [Theory]
    [InlineData("default")]
    public void GetEnqueuedCount_WhenFetched(string queue)
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        JobQueue jobQueue = new JobQueue(Storage);
        jobQueue.Enqueue(queue, Guid.NewGuid().ToString());
        jobQueue.Enqueue(queue, Guid.NewGuid().ToString());

        jobQueue.Dequeue(new[] { queue }, CancellationToken.None);

        // act
        JobQueueMonitoringApi monitoring = new JobQueueMonitoringApi(Storage);
        long count = monitoring.GetEnqueuedCount(queue);

        //assert
        Assert.Equal(1, count);
    }

    [Theory]
    [InlineData("default")]
    public void GetEnqueuedJobIds_WhenIsNotEmpty(string queue)
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        JobQueue jobQueue = new JobQueue(Storage);
        string jobId = Guid.NewGuid().ToString();
        jobQueue.Enqueue(queue, jobId);

        // act
        JobQueueMonitoringApi monitoring = new JobQueueMonitoringApi(Storage);
        IEnumerable<string> ids = monitoring.GetEnqueuedJobIds(queue, 0, 10);

        //assert
        Assert.NotEmpty(ids);
    }

    [Theory]
    [InlineData("default")]
    public void GetEnqueuedJobIds_WhenFetched(string queue)
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        JobQueue jobQueue = new JobQueue(Storage);
        jobQueue.Enqueue(queue, Guid.NewGuid().ToString());
        string jobId = Guid.NewGuid().ToString();
        jobQueue.Enqueue(queue, jobId);

        jobQueue.Dequeue(new[] { queue }, CancellationToken.None);

        // act
        JobQueueMonitoringApi monitoring = new JobQueueMonitoringApi(Storage);
        string id = monitoring.GetEnqueuedJobIds(queue, 0, 10).Single();

        //assert
        Assert.Equal(jobId, id);
    }

    [Theory]
    [InlineData("default")]
    public void GetFetchedJobIds_WhenIsNotEmpty(string queue)
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        JobQueue jobQueue = new JobQueue(Storage);
        string jobId = Guid.NewGuid().ToString();
        jobQueue.Enqueue(queue, jobId);
        jobQueue.Dequeue(new[] { queue }, CancellationToken.None);

        // act
        JobQueueMonitoringApi monitoring = new JobQueueMonitoringApi(Storage);
        IEnumerable<string> ids = monitoring.GetFetchedJobIds(queue, 0, 10);

        //assert
        Assert.NotEmpty(ids);
    }

    [Theory]
    [InlineData("default")]
    public void GetFetchedJobIds_WhenFetched(string queue)
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        JobQueue jobQueue = new JobQueue(Storage);
        string jobId = Guid.NewGuid().ToString();
        jobQueue.Enqueue(queue, jobId);
        jobQueue.Enqueue(queue, Guid.NewGuid().ToString());
        jobQueue.Dequeue(new[] { queue }, CancellationToken.None);

        // act
        JobQueueMonitoringApi monitoring = new JobQueueMonitoringApi(Storage);
        string id = monitoring.GetFetchedJobIds(queue, 0, 10).Single();

        //assert
        Assert.Equal(jobId, id);
    }

    [Theory]
    [InlineData("default")]
    public void GetEnqueuedAndFetchedCount_WhenNotEmpty(string queue)
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        JobQueue jobQueue = new JobQueue(Storage);
        string jobId = Guid.NewGuid().ToString();
        jobQueue.Enqueue(queue, jobId);
        jobQueue.Enqueue(queue, Guid.NewGuid().ToString());
        jobQueue.Dequeue(new[] { queue }, CancellationToken.None);

        // act
        JobQueueMonitoringApi monitoring = new JobQueueMonitoringApi(Storage);
        (int? EnqueuedCount, int? FetchedCount) record = monitoring.GetEnqueuedAndFetchedCount(queue);

        //assert
        Assert.Equal(1, record.EnqueuedCount);
        Assert.Equal(1, record.FetchedCount);
    }
}