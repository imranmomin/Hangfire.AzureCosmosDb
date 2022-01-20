using System;
using System.Collections.Generic;
using Hangfire.Azure.Queue;
using Hangfire.Azure.Tests.Fixtures;
using Xunit;

namespace Hangfire.Azure.Tests;

public class PersistentJobQueueProviderCollectionFacts : IClassFixture<ContainerFixture>
{
    private CosmosDbStorage Storage { get; }

    public PersistentJobQueueProviderCollectionFacts(ContainerFixture containerFixture)
    {
        Storage = containerFixture.Storage;
    }

    [Fact]
    public void PersistentJobQueueProviderCollection_ShouldThrowException_WhenProviderIsNull()
    {
        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new PersistentJobQueueProviderCollection(null!));

        // assert
        Assert.Equal("provider", exception.ParamName);
    }

    [Fact]
    public void PersistentJobQueueProviderCollection_HasProvider()
    {
        // arrange
        JobQueueProvider provider = new JobQueueProvider(Storage);
        PersistentJobQueueProviderCollection collection = new PersistentJobQueueProviderCollection(provider);

        // act
        IEnumerable<IPersistentJobQueueProvider> providers = getAllProviders();

        // assert
        Assert.NotEmpty(providers);

        IEnumerable<IPersistentJobQueueProvider> getAllProviders()
        {
            using IEnumerator<IPersistentJobQueueProvider> enumerator = collection.GetEnumerator();
            while (enumerator.MoveNext())
                yield return enumerator.Current;
        }
    }
}