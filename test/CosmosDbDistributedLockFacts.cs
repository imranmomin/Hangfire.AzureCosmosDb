using System;
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
		using (new CosmosDbDistributedLock(resource, TimeSpan.FromSeconds(5), Storage))
		{
			using (new CosmosDbDistributedLock(resource, TimeSpan.FromSeconds(5), Storage))
			{
			
			}
		}

	}
}