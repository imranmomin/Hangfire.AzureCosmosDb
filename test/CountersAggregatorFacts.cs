using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Tests.Fixtures;
using Microsoft.Azure.Cosmos;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Azure.Tests;

public class CountersAggregatorFacts : IClassFixture<ContainerFixture>
{
	private CosmosDbStorage Storage { get; }
	private ContainerFixture ContainerFixture { get; }

	public CountersAggregatorFacts(ContainerFixture containerFixture, ITestOutputHelper testOutputHelper)
	{
		ContainerFixture = containerFixture;
		Storage = containerFixture.Storage;

		ContainerFixture.SetupLogger(testOutputHelper);
	}

	[Fact]
	public void CountersAggregator_ThrowsException_WhenStorageIsNull()
	{
		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new CountersAggregator(null!));

		// assert
		Assert.Equal("storage", exception.ParamName);
	}

	[Fact]
	public void CountersAggregatorExecutesProperly_WhenAggregateCounterDoesNotExists()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Counter> counters = new()
		{
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddHours(1), Type = CounterTypes.Raw },
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddHours(1), Type = CounterTypes.Raw }
		};
		Data<Counter> data = new(counters);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Counter);

		CountersAggregator aggregator = new(Storage);
		CancellationTokenSource cts = new();

		// act
		aggregator.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Counter
		};
		Counter counter = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Single();

		Assert.Equal(2, counter.Value);
		Assert.Equal(CounterTypes.Aggregate, counter.Type);
	}

	[Fact]
	public void CountersAggregatorExecutesProperly_WhenAggregateCounterExists()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Counter> counters = new()
		{
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddHours(1), Type = CounterTypes.Raw },
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddHours(1), Type = CounterTypes.Raw },
			new Counter { Id = "700129197C369DA5D65629D85A1D2B6B", Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddHours(1), Type = CounterTypes.Aggregate }
		};
		Data<Counter> data = new(counters);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Counter);

		CountersAggregator aggregator = new(Storage);
		CancellationTokenSource cts = new();

		// act
		aggregator.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Counter
		};
		Counter counter = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Single();

		Assert.Equal(3, counter.Value);
		Assert.Equal(CounterTypes.Aggregate, counter.Type);
	}

	[Fact]
	public void CountersAggregatorExecutesProperly_HandlesLockException_WhenAggregateCounterExists()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Counter> counters = new()
		{
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddHours(1), Type = CounterTypes.Raw },
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddHours(1), Type = CounterTypes.Raw },
			new Counter { Id = "700129197C369DA5D65629D85A1D2B6B", Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddHours(1), Type = CounterTypes.Aggregate }
		};
		Data<Counter> data = new(counters);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Counter);

		// add a lock
		const string lockKey = "locks:counters:aggregator";
		CosmosDbDistributedLock distributedLock = new(lockKey, TimeSpan.FromSeconds(2), Storage);

		Task.Run(async () =>
		{
			await Task.Delay(5000);
			distributedLock.Dispose();
		});

		// get the aggregator
		CountersAggregator aggregator = new(Storage);
		CancellationTokenSource cts = new();

		// act
		aggregator.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new() { PartitionKey = PartitionKeys.Counter };
		List<Counter> results = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.ToList();

		Assert.Equal(3, results.Count);
	}

	[Fact]
	public void CountersAggregatorExecutesProperly_WhenRawCounterAreInLargeNumbers()
	{
		// clean
		ContainerFixture.Clean();

		Storage.StorageOptions.CountersAggregateMaxItemCount = 100;

		// arrange
		List<Counter> counters = Enumerable.Range(0, 1000)
			.Select(_ => new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddHours(1), Type = CounterTypes.Raw })
			.ToList();
		Data<Counter> data = new(counters);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Counter);

		CountersAggregator aggregator = new(Storage);
		CancellationTokenSource cts = new();

		// act
		aggregator.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Counter
		};
		Counter counter = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: queryRequestOptions)
			.Where(x => x.Type == CounterTypes.Aggregate)
			.ToQueryResult()
			.Single();

		Assert.Equal(1000, counter.Value);
		Assert.Equal(CounterTypes.Aggregate, counter.Type);

		// reset
		Storage.StorageOptions.CountersAggregateMaxItemCount = 1;
	}
}