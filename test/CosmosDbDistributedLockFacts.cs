using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Tests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Azure.Tests;

public class CosmosDbDistributedLockFacts : IClassFixture<ContainerFixture>
{
    public CosmosDbDistributedLockFacts(ContainerFixture containerFixture, ITestOutputHelper testOutputHelper)
    {
        ContainerFixture = containerFixture;
        Storage = containerFixture.Storage;

        ContainerFixture.SetupLogger(testOutputHelper);
    }

    private ContainerFixture ContainerFixture { get; }

    private CosmosDbStorage Storage { get; }

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
            result = Storage.Container.ReadItemWithRetries<Lock>(resource, PartitionKeys.Lock);
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
            result = Storage.Container.ReadItemWithRetries<Lock>(resource, PartitionKeys.Lock);
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
            result = Storage.Container.ReadItemWithRetries<Lock>(resource, PartitionKeys.Lock);
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
        Storage.Container.CreateItemWithRetries(new Lock { Id = resource, TimeToLive = (int)TimeSpan.FromMinutes(1).TotalSeconds }, PartitionKeys.Lock);

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
        using CosmosDbDistributedLock distributedLock = new(resource, TimeSpan.FromMinutes(1), Storage);

        Lock? result = null;
        try
        {
            result = Storage.Container.ReadItemWithRetries<Lock>(resource, PartitionKeys.Lock);
        }
        catch
        {
            /* ignored */
        }

        Assert.NotNull(result);

        // act
        Thread.Sleep(1000);
        distributedLock.KeepLockAlive(result!);

        // assert
        FieldInfo? lockField = typeof(CosmosDbDistributedLock).GetField("lock", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Default);
        Assert.NotNull(lockField);

        Lock @lock = (Lock)lockField!.GetValue(distributedLock)!;
        Assert.NotEqual(result!.LastHeartBeat, @lock.LastHeartBeat);
    }

    [Fact]
    public void DistributeLocks_ShouldHandlerNotFound_KeepAliveQuery()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        const string resource = "locks:test";
        using CosmosDbDistributedLock distributedLock = new(resource, TimeSpan.FromMinutes(1), Storage);

        // act
        Lock result = new() { LastHeartBeat = DateTime.UtcNow.AddSeconds(10) };
        distributedLock.KeepLockAlive(result);

        // assert
        FieldInfo? lockField = typeof(CosmosDbDistributedLock).GetField("lock", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Default);
        Assert.NotNull(lockField);

        Lock @lock = (Lock)lockField!.GetValue(distributedLock)!;
        Assert.NotEqual(result.LastHeartBeat, @lock.LastHeartBeat);
    }

    [Fact]
    public void DistributeLocks_ShouldHandlerPreconditionFailed_KeepAliveQuery()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        const string resource = "locks:test";
        using CosmosDbDistributedLock distributedLock = new(resource, TimeSpan.FromMinutes(1), Storage);
        FieldInfo? lockField = typeof(CosmosDbDistributedLock).GetField("lock", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Default);
        Lock @lock = (Lock)lockField?.GetValue(distributedLock)!;

        // act
        Lock result = new() { Id = @lock.Id, ETag = $"{@lock.ETag}Z", LastHeartBeat = DateTime.UtcNow.AddSeconds(10) };
        result.ETag = $"{result.ETag}Z";
        distributedLock.KeepLockAlive(result);

        // assert
        Assert.NotEqual(result.LastHeartBeat, @lock.LastHeartBeat);
    }
}