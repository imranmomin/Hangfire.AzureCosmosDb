using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Tests.Fixtures;
using Xunit;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Queue;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure.Tests;

public class JobQueueFacts : IClassFixture<ContainerFixture>
{
    private static readonly string[] defaultQueues = { "default" };
    private CosmosDbStorage Storage { get; }
    private ContainerFixture ContainerFixture { get; }

    public JobQueueFacts(ContainerFixture containerFixture)
    {
        this.ContainerFixture = containerFixture;
        Storage = containerFixture.Storage;
    }

    [Fact]
    public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
    {
        // act
        JobQueue jobQueue = new JobQueue(Storage);
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => jobQueue.Dequeue(null!, CancellationToken.None));

        // asset 
        Assert.Equal("queues", exception.ParamName);
    }

    [Fact]
    public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
    {
        // act
        JobQueue jobQueue = new JobQueue(Storage);
        ArgumentException exception = Assert.Throws<ArgumentException>(() => jobQueue.Dequeue(Array.Empty<string>(), CancellationToken.None));

        // asset 
        Assert.Equal("queues", exception.ParamName);
    }

    [Fact]
    public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
    {
        // act
        CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();
        JobQueue jobQueue = new JobQueue(Storage);

        // assert
        Assert.Throws<OperationCanceledException>(() => jobQueue.Dequeue(defaultQueues, cancellationTokenSource.Token));
    }

    [Theory]
    [InlineData("default", "c8732989-96e1-40e5-af16-dcf4d79cb4d6")]
    [InlineData("high", "4c5f2c17-54ad-4e9e-ae38-c9ff750febd9")]
    public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue(string queue, string jobId)
    {
        // clean all the documents for the container
        ContainerFixture.Clean();

        //arrange 
        JobQueue jobQueue = new JobQueue(Storage);
        jobQueue.Enqueue("high", queue.Equals("high") ? jobId : Guid.NewGuid().ToString());
        jobQueue.Enqueue("default", queue.Equals("default") ? jobId : Guid.NewGuid().ToString());

        //act
        FetchedJob job = (FetchedJob)jobQueue.Dequeue(new[] { queue }, CancellationToken.None);

        //assert
        Assert.Equal(jobId, job.JobId);
        Assert.Equal(queue, job.Queue);
    }

    [Theory]
    [InlineData("default")]
    public void Dequeue_ShouldFetchTimedOutJobs_FromTheSpecifiedQueue(string queue)
    {
        // clean all the documents for the container
        ContainerFixture.Clean();

        //arrange 
        Documents.Queue document = new Documents.Queue
        {
            Name = queue,
            JobId = Guid.NewGuid().ToString(),
            FetchedAt = DateTime.UtcNow.AddMinutes(-16),
            CreatedOn = DateTime.UtcNow
        };
        Task<ItemResponse<Documents.Queue>> response = Storage.Container.CreateItemWithRetriesAsync(document, new PartitionKey((int)DocumentTypes.Queue));
        response.Wait();

        //act
        JobQueue jobQueue = new JobQueue(Storage);
        FetchedJob job = (FetchedJob)jobQueue.Dequeue(new[] { queue }, CancellationToken.None);

        //assert
        Assert.Equal(document.JobId, job.JobId);
        Assert.Equal(queue, job.Queue);
    }

    [Theory]
    [InlineData("default", "c8732989-96e1-40e5-af16-dcf4d79cb4d6")]
    public void Dequeue_ShouldFetchNonTimedOutJobs_FromTheSpecifiedQueue(string queue, string jobId)
    {
        // clean all the documents for the container
        ContainerFixture.Clean();

        //arrange 
        Documents.Queue document = new Documents.Queue
        {
            Name = queue,
            JobId = Guid.NewGuid().ToString(),
            FetchedAt = DateTime.UtcNow.AddMinutes(-10),
            CreatedOn = DateTime.UtcNow.AddMinutes(-1)
        };
        Task<ItemResponse<Documents.Queue>> response = Storage.Container.CreateItemWithRetriesAsync(document, new PartitionKey((int)DocumentTypes.Queue));
        response.Wait();

        JobQueue jobQueue = new JobQueue(Storage);
        jobQueue.Enqueue(queue, jobId);

        //act
        FetchedJob job = (FetchedJob)jobQueue.Dequeue(defaultQueues, CancellationToken.None);

        //assert
        Assert.Equal(jobId, job.JobId);
        Assert.Equal(queue, job.Queue);
        Assert.True((job.FetchedAt!.Value - DateTime.UtcNow).TotalSeconds <= 5);
    }

    [Fact]
    public void Dequeue_ShouldSetFetchedAt_WhenTheJobIsFetched()
    {
        // clean all the documents for the container
        ContainerFixture.Clean();

        //arrange 
        JobQueue jobQueue = new JobQueue(Storage);
        jobQueue.Enqueue("default", Guid.NewGuid().ToString());

        //act
        FetchedJob job = (FetchedJob)jobQueue.Dequeue(defaultQueues, CancellationToken.None);

        //assert
        Assert.NotNull(job.FetchedAt);
    }

    [Fact]
    public void Dequeue_ShouldSetFetchedAtToNull_WhenTheJobRequeue()
    {
        // clean all the documents for the container
        ContainerFixture.Clean();

        //arrange 
        JobQueue jobQueue = new JobQueue(Storage);
        jobQueue.Enqueue("default", Guid.NewGuid().ToString());
        //act

        FetchedJob job = (FetchedJob)jobQueue.Dequeue(defaultQueues, CancellationToken.None);
        job.Requeue();

        //assert
        Assert.Null(job.FetchedAt);
    }

    [Fact]
    public void Dequeue_ShouldRemoveFromQueue_WhenTheJobIsRemovedFromQueue()
    {
        // clean all the documents for the container
        ContainerFixture.Clean();

        //arrange 
        JobQueue jobQueue = new JobQueue(Storage);
        jobQueue.Enqueue("default", Guid.NewGuid().ToString());

        //act
        FetchedJob job = (FetchedJob)jobQueue.Dequeue(defaultQueues, CancellationToken.None);
        job.RemoveFromQueue();
        long count = Storage.GetMonitoringApi().EnqueuedCount("default");

        //assert
        Assert.Equal(0, count);
    }

    [Theory]
    [InlineData("default", "c8732989-96e1-40e5-af16-dcf4d79cb4d6")]
    [InlineData("high", "c8732989-96e1-40e5-af16-dcf4d79cb4d6")]
    public void Enqueue_AddsAJobToTheQueue(string queue, string jobId)
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        JobQueue jobQueue = new JobQueue(Storage);
        jobQueue.Enqueue(queue, jobId);

        // act
        string query = "SELECT * FROM doc WHERE doc.type = @type AND doc.name = @queue";
        QueryDefinition sql = new QueryDefinition(query)
            .WithParameter("@type", (int)DocumentTypes.Queue)
            .WithParameter("@queue", queue);

        PartitionKey partitionKey = new((int)DocumentTypes.Queue);
        QueryRequestOptions queryRequestOptions = new QueryRequestOptions { PartitionKey = partitionKey, MaxItemCount = 1 };

        Documents.Queue? data = Storage.Container.GetItemQueryIterator<Documents.Queue>(sql, requestOptions: queryRequestOptions)
            .ToQueryResult()
            .FirstOrDefault();

        Assert.NotNull(data);
        Assert.Equal(jobId, data!.JobId);
        Assert.Equal(queue, data.Name);
    }
}