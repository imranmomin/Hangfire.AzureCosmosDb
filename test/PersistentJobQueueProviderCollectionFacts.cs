using System;
using System.Collections.Generic;
using Hangfire.Azure.Queue;
using Hangfire.Azure.Tests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Azure.Tests;

public class PersistentJobQueueProviderCollectionFacts : IClassFixture<ContainerFixture>
{
    public PersistentJobQueueProviderCollectionFacts(ContainerFixture containerFixture, ITestOutputHelper testOutputHelper)
    {
        Storage = containerFixture.Storage;
        containerFixture.SetupLogger(testOutputHelper);
    }

    private CosmosDbStorage Storage { get; }

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
        JobQueueProvider provider = new(Storage);
        PersistentJobQueueProviderCollection collection = new(provider);

        // act
        IEnumerable<IPersistentJobQueueProvider> providers = getAllProviders();

        // assert
        Assert.NotEmpty(providers);

        IEnumerable<IPersistentJobQueueProvider> getAllProviders()
        {
            using IEnumerator<IPersistentJobQueueProvider> enumerator = collection.GetEnumerator();
            while (enumerator.MoveNext())
            {
                yield return enumerator.Current;
            }
        }
    }
}