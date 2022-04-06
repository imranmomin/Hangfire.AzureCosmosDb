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
using Xunit.Abstractions;

namespace Hangfire.Azure.Tests;

public class JobQueueFacts : IClassFixture<ContainerFixture>
{
	private static readonly string[] defaultQueues = { "default" };
	private CosmosDbStorage Storage { get; }
	private ContainerFixture ContainerFixture { get; }

	public JobQueueFacts(ContainerFixture containerFixture, ITestOutputHelper testOutputHelper)
	{
		ContainerFixture = containerFixture;
		Storage = containerFixture.Storage;

		ContainerFixture.SetupLogger(testOutputHelper);
	}

	[Fact]
	public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
	{
		// act
		JobQueue jobQueue = new(Storage);
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => jobQueue.Dequeue(null!, CancellationToken.None));

		// asset 
		Assert.Equal("queues", exception.ParamName);
	}

	[Fact]
	public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
	{
		// act
		JobQueue jobQueue = new(Storage);
		ArgumentException exception = Assert.Throws<ArgumentException>(() => jobQueue.Dequeue(Array.Empty<string>(), CancellationToken.None));

		// asset 
		Assert.Equal("queues", exception.ParamName);
	}

	[Fact]
	public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
	{
		// act
		CancellationTokenSource cancellationTokenSource = new();
		cancellationTokenSource.Cancel();
		JobQueue jobQueue = new(Storage);

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
		JobQueue jobQueue = new(Storage);
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
		Documents.Queue document = new()
		{
			Name = queue,
			JobId = Guid.NewGuid().ToString(),
			FetchedAt = DateTime.UtcNow.AddMinutes(-16),
			CreatedOn = DateTime.UtcNow
		};
		Storage.Container.CreateItemWithRetries(document, PartitionKeys.Queue);

		//act
		JobQueue jobQueue = new(Storage);
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
		Documents.Queue document = new()
		{
			Name = queue,
			JobId = Guid.NewGuid().ToString(),
			FetchedAt = DateTime.UtcNow.AddMinutes(-10),
			CreatedOn = DateTime.UtcNow.AddMinutes(-1)
		};
		Storage.Container.CreateItemWithRetries(document, PartitionKeys.Queue);

		JobQueue jobQueue = new(Storage);
		jobQueue.Enqueue(queue, jobId);

		//act
		FetchedJob job = (FetchedJob)jobQueue.Dequeue(new[] { queue }, CancellationToken.None);

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
		JobQueue jobQueue = new(Storage);
		jobQueue.Enqueue("default", Guid.NewGuid().ToString());
		jobQueue.Enqueue("default", Guid.NewGuid().ToString());
		jobQueue.Enqueue("default", Guid.NewGuid().ToString());
		jobQueue.Enqueue("default", Guid.NewGuid().ToString());
		jobQueue.Enqueue("default", Guid.NewGuid().ToString());
		jobQueue.Enqueue("default", Guid.NewGuid().ToString());
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
		JobQueue jobQueue = new(Storage);
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
		JobQueue jobQueue = new(Storage);
		jobQueue.Enqueue("default", Guid.NewGuid().ToString());

		//act
		FetchedJob job = (FetchedJob)jobQueue.Dequeue(defaultQueues, CancellationToken.None);
		job.RemoveFromQueue();

		JobQueueMonitoringApi monitoringApi = new(Storage);
		(int? enqueuedCount, int? fetchedCount) data = monitoringApi.GetEnqueuedAndFetchedCount("default");

		//assert
		Assert.Equal(0, data.enqueuedCount);
		Assert.Equal(0, data.fetchedCount);
	}

	[Theory]
	[InlineData("default", "c8732989-96e1-40e5-af16-dcf4d79cb4d6")]
	[InlineData("high", "c8732989-96e1-40e5-af16-dcf4d79cb4d6")]
	public void Enqueue_AddsAJobToTheQueue(string queue, string jobId)
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		JobQueue jobQueue = new(Storage);
		jobQueue.Enqueue(queue, jobId);

		// act
		const string query = "SELECT * FROM doc WHERE doc.name = @queue";
		QueryDefinition sql = new QueryDefinition(query)
			.WithParameter("@queue", queue);

		PartitionKey partitionKey = new((int)DocumentTypes.Queue);
		QueryRequestOptions queryRequestOptions = new()
			{ PartitionKey = partitionKey, MaxItemCount = 1 };

		Documents.Queue? data = Storage.Container.GetItemQueryIterator<Documents.Queue>(sql, requestOptions: queryRequestOptions)
			.ToQueryResult()
			.FirstOrDefault();

		Assert.NotNull(data);
		Assert.Equal(jobId, data!.JobId);
		Assert.Equal(queue, data.Name);
	}

	[Fact]
	public void Dequeue_ShouldThrowOperationCancelled_WhenTokenIsCancelled()
	{
		// clean all the documents for the container
		ContainerFixture.Clean();

		//arrange 
		JobQueue jobQueue = new(Storage);
		jobQueue.Enqueue("default", Guid.NewGuid().ToString());

		//act
		const string lockKey = "locks:job:dequeue";
		CosmosDbDistributedLock distributedLock = new(lockKey, TimeSpan.FromSeconds(2), Storage);
		CancellationTokenSource cancellationSource = new();
		cancellationSource.Cancel();

		// assert
		OperationCanceledException exception = Assert.Throws<OperationCanceledException>(() => jobQueue.Dequeue(defaultQueues, cancellationSource.Token));
		distributedLock.Dispose();
	}
}