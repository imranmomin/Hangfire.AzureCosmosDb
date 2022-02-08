using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Tests.Fixtures;
using Microsoft.Azure.Cosmos;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Azure.Tests;

public class CosmosDbDistributedLockFacts : IClassFixture<ContainerFixture>
{
	private ContainerFixture ContainerFixture { get; }

	private CosmosDbStorage Storage { get; }

	public CosmosDbDistributedLockFacts(ContainerFixture containerFixture, ITestOutputHelper testOutputHelper)
	{
		ContainerFixture = containerFixture;
		Storage = containerFixture.Storage;

		ContainerFixture.SetupLogger(testOutputHelper);
	}

	[Fact]
	public void Ctor_ThrowsAnException_WhenStorageIsNull()
	{
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new CosmosDbDistributedLock("hello", TimeSpan.Zero, null!));
		Assert.Equal("storage", exception.ParamName);
	}

	[Fact]
	public void Ctor_ThrowsAnException_WhenResourceIsNullOrEmpty()
	{
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new CosmosDbDistributedLock(null!, TimeSpan.Zero, Storage));
		Assert.Equal("resource", exception.ParamName);
	}

	[Fact]
	public void Ctor_AcquiresExclusiveApplicationLock_OnSession()
	{
		// Clean
		ContainerFixture.Clean();

		// arrange
		const string resource = "lock:test";
		CosmosDbDistributedLock distributedLock = new(resource, TimeSpan.FromSeconds(5), Storage);

		// assert
		Lock? result = null;
		try
		{
			Task<ItemResponse<Lock>> response = Storage.Container.ReadItemWithRetriesAsync<Lock>(resource, PartitionKeys.Lock);
			response.Wait();
			result = response.Result;
		}
		catch
		{
			/* ignored */
		}

		Assert.NotNull(result);

		// clean
		distributedLock.Dispose();
	}

	[Fact]
	public void Dispose_ReleasesExclusiveApplicationLock()
	{
		// Clean
		ContainerFixture.Clean();

		// arrange
		const string resource = "lock:test";
		CosmosDbDistributedLock distributedLock = new(resource, TimeSpan.FromSeconds(5), Storage);
		distributedLock.Dispose();

		// assert
		Lock? result = null;
		try
		{
			Task<ItemResponse<Lock>> response = Storage.Container.ReadItemWithRetriesAsync<Lock>(resource, PartitionKeys.Lock);
			response.Wait();
			result = response.Result;
		}
		catch
		{
			/* ignored */
		}

		Assert.Null(result);
	}

	[Fact]
	public void DistributedLocks_AreReEntrant_FromTheSameThread_OnTheSameResource()
	{
		// Clean
		ContainerFixture.Clean();

		// arrange
		const string resource = "lock:test";
		using CosmosDbDistributedLock outer = new(resource, TimeSpan.FromSeconds(5), Storage);
		using CosmosDbDistributedLock inner = new(resource, TimeSpan.FromSeconds(5), Storage);

		// Assert
		Lock? result = null;
		try
		{
			Task<ItemResponse<Lock>> response = Storage.Container.ReadItemWithRetriesAsync<Lock>(resource, PartitionKeys.Lock);
			response.Wait();
			result = response.Result;
		}
		catch
		{
			/* ignored */
		}

		Assert.NotNull(result);

		FieldInfo? acquiredLocksField = typeof(CosmosDbDistributedLock).GetField("acquiredLocks", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Static);
		Assert.NotNull(acquiredLocksField);

		ThreadLocal<Dictionary<string, int>> outerValue = (ThreadLocal<Dictionary<string, int>>)acquiredLocksField!.GetValue(outer)!;
		Assert.Equal(2, outerValue.Value![resource]);

		ThreadLocal<Dictionary<string, int>> innerValue = (ThreadLocal<Dictionary<string, int>>)acquiredLocksField!.GetValue(inner)!;
		Assert.Equal(2, innerValue.Value![resource]);

		FieldInfo? lockField = typeof(CosmosDbDistributedLock).GetField("lock", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Default);
		Assert.NotNull(lockField);

		Lock outerLock = (Lock)lockField!.GetValue(outer)!;
		Assert.NotNull(outerLock);

		Lock? innerLock = (Lock)lockField!.GetValue(inner)!;
		Assert.Null(innerLock);
	}

	[Fact]
	public void DistributeLocks_ThrowsCosmosDbDistributedLockException()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		const string resource = "locks:test";
		Task<ItemResponse<Lock>> task = Storage.Container.CreateItemWithRetriesAsync(new Lock { Id = resource, TimeToLive = (int)TimeSpan.FromMinutes(1).TotalSeconds }, PartitionKeys.Lock);
		task.Wait();

		// act
		CosmosDbDistributedLockException exception = Assert.Throws<CosmosDbDistributedLockException>(() => new CosmosDbDistributedLock(resource, TimeSpan.FromSeconds(1), Storage));

		// assert
		Assert.Equal(resource, exception.Key);
	}

	[Fact]
	public void DistributeLocks_ShouldSend_KeepAliveQuery()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		const string resource = "locks:test";
		using CosmosDbDistributedLock distributedLock = new(resource, TimeSpan.FromSeconds(6), Storage);

		// assert
		Lock? result = null;
		try
		{
			Task<ItemResponse<Lock>> response = Storage.Container.ReadItemWithRetriesAsync<Lock>(resource, PartitionKeys.Lock);
			response.Wait();
			result = response.Result;
		}
		catch
		{
			/* ignored */
		}

		Assert.NotNull(result);

		// lets sleep
		Thread.Sleep(3000);

		FieldInfo? lockField = typeof(CosmosDbDistributedLock).GetField("lock", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Default);
		Assert.NotNull(lockField);

		Lock @lock = (Lock)lockField!.GetValue(distributedLock)!;
		Assert.NotEqual(result!.LastHeartBeat, @lock.LastHeartBeat);
	}
}