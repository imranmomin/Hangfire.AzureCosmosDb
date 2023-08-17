using Hangfire.Azure.Queue;
using Hangfire.Azure.Tests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Azure.Tests;

public class JobQueueProviderFacts : IClassFixture<ContainerFixture>
{
    public JobQueueProviderFacts(ContainerFixture containerFixture, ITestOutputHelper testOutputHelper)
    {
        Storage = containerFixture.Storage;
        containerFixture.SetupLogger(testOutputHelper);
    }

    private CosmosDbStorage Storage { get; }

    [Fact]
    public void GetJobQueue_WhenIsNotNull()
    {
        // arrange
        JobQueueProvider provider = new(Storage);

        // act
        IPersistentJobQueue queue = provider.GetJobQueue();

        // assert
        Assert.NotNull(queue);
    }

    [Fact]
    public void GetJobQueueMonitoringApi_WhenIsNotNull()
    {
        // arrange
        JobQueueProvider provider = new(Storage);

        // act
        IPersistentJobQueueMonitoringApi queue = provider.GetJobQueueMonitoringApi();

        // assert
        Assert.NotNull(queue);
    }
}