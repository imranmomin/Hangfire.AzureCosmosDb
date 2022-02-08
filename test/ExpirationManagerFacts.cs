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

public class ExpirationManagerFacts : IClassFixture<ContainerFixture>
{
	private CosmosDbStorage Storage { get; }
	private ContainerFixture ContainerFixture { get; }

	public ExpirationManagerFacts(ContainerFixture containerFixture, ITestOutputHelper testOutputHelper)
	{
		ContainerFixture = containerFixture;
		Storage = containerFixture.Storage;

		ContainerFixture.SetupLogger(testOutputHelper);
	}

	[Fact]
	public void Storage_ThrowsException_WhenStorageIsNull()
	{
		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null!));

		// assert
		Assert.Equal("storage", exception.ParamName);
	}

	[Fact]
	public void Execute_RemovesOutdatedRecords()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Counter> counters = new()
		{
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddMonths(-1), Type = CounterTypes.Aggregate },
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddMonths(-1), Type = CounterTypes.Aggregate }
		};
		Data<Counter> data = new(counters);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Counter);

		ExpirationManager manager = new(Storage);
		CancellationTokenSource cts = new();

		// act
		manager.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Counter
		};
		int result = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Count();

		Assert.Equal(0, result);
	}

	[Fact]
	public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Counter> counters = new()
		{
			new Counter { Key = "key", Value = 1, Type = CounterTypes.Aggregate },
			new Counter { Key = "key", Value = 1, Type = CounterTypes.Aggregate }
		};
		Data<Counter> data = new(counters);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Counter);

		ExpirationManager manager = new(Storage);
		CancellationTokenSource cts = new();

		// act
		manager.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Counter
		};
		int result = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Count();

		Assert.Equal(2, result);
	}

	[Fact]
	public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Counter> counters = new()
		{
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddMonths(1), Type = CounterTypes.Aggregate },
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddMonths(1), Type = CounterTypes.Aggregate }
		};
		Data<Counter> data = new(counters);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Counter);

		ExpirationManager manager = new(Storage);
		CancellationTokenSource cts = new();

		// act
		manager.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Counter
		};
		int result = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Count();

		Assert.Equal(2, result);
	}

	[Fact]
	public void Execute_Processes_AggregatedCounterTable()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Counter> counters = new()
		{
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddMonths(-1), Type = CounterTypes.Aggregate },
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddMonths(-1), Type = CounterTypes.Aggregate },
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddMonths(-1), Type = CounterTypes.Raw }
		};
		Data<Counter> data = new(counters);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Counter);

		ExpirationManager manager = new(Storage);
		CancellationTokenSource cts = new();

		// act
		manager.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Counter
		};
		int result = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: queryRequestOptions)
			.Where(x => x.Type == CounterTypes.Raw)
			.ToQueryResult()
			.Count();

		Assert.Equal(1, result);

		result = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: queryRequestOptions)
			.Where(x => x.Type == CounterTypes.Aggregate)
			.ToQueryResult()
			.Count();

		Assert.Equal(0, result);
	}

	[Fact]
	public void Execute_Processes_JobTable()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Job> jobs = new()
		{
			new Job { CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddMonths(-1) },
			new Job { CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddMonths(-1) }
		};
		Data<Job> data = new(jobs);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Job);

		ExpirationManager manager = new(Storage);
		CancellationTokenSource cts = new();

		// act
		manager.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Job
		};
		int result = Storage.Container.GetItemLinqQueryable<Job>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Count();

		Assert.Equal(0, result);
	}

	[Fact]
	public void Execute_Processes_ListTable()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<List> lists = new()
		{
			new List { CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddMonths(-1) },
			new List { CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddMonths(-1) }
		};
		Data<List> data = new(lists);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.List);

		ExpirationManager manager = new(Storage);
		CancellationTokenSource cts = new();

		// act
		manager.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.List
		};
		int result = Storage.Container.GetItemLinqQueryable<List>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Count();

		Assert.Equal(0, result);
	}

	[Fact]
	public void Execute_Processes_SetTable()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Set> sets = new()
		{
			new Set { CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddMonths(-1) },
			new Set { CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddMonths(-1) }
		};
		Data<Set> data = new(sets);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Set);

		ExpirationManager manager = new(Storage);
		CancellationTokenSource cts = new();

		// act
		manager.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Set
		};
		int result = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Count();

		Assert.Equal(0, result);
	}

	[Fact]
	public void Execute_Processes_HashTable()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Hash> hashes = new()
		{
			new Hash { ExpireOn = DateTime.UtcNow.AddMonths(-1) },
			new Hash { ExpireOn = DateTime.UtcNow.AddMonths(-1) }
		};
		Data<Hash> data = new(hashes);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);

		ExpirationManager manager = new(Storage);
		CancellationTokenSource cts = new();

		// act
		manager.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Hash
		};
		int result = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Count();

		Assert.Equal(0, result);
	}

	[Fact]
	public void Execute_Processes_StateTable()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<State> states = new()
		{
			new State { CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddMonths(-1) },
			new State { CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddMonths(-1) }
		};
		Data<State> data = new(states);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.State);

		ExpirationManager manager = new(Storage);
		CancellationTokenSource cts = new();

		// act
		manager.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.State
		};
		int result = Storage.Container.GetItemLinqQueryable<State>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Count();

		Assert.Equal(0, result);
	}

	[Fact]
	public void Execute_HandlesLockException_WhenAggregateCounterExists()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Counter> counters = new()
		{
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddMonths(-1), Type = CounterTypes.Aggregate },
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddMonths(-1), Type = CounterTypes.Aggregate },
			new Counter { Key = "key", Value = 1, ExpireOn = DateTime.UtcNow.AddMonths(-1), Type = CounterTypes.Raw }
		};
		Data<Counter> data = new(counters);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Counter);

		const string lockKey = "locks:expiration:manager";
		Task<ItemResponse<Lock>> task = Storage.Container.CreateItemWithRetriesAsync(new Lock { Id = lockKey, TimeToLive = (int)TimeSpan.FromMinutes(1).TotalSeconds }, PartitionKeys.Lock);
		task.Wait();

		ExpirationManager manager = new(Storage);
		CancellationTokenSource cts = new();

		// act
		manager.Execute(cts.Token);

		// assert
		QueryRequestOptions queryRequestOptions = new() { PartitionKey = PartitionKeys.Counter };
		int result = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Count();

		Assert.Equal(3, result);
	}
}