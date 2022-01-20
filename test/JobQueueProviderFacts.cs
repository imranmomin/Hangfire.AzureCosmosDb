using Hangfire.Azure.Queue;
using Hangfire.Azure.Tests.Fixtures;
using Xunit;

namespace Hangfire.Azure.Tests;

public class JobQueueProviderFacts : IClassFixture<ContainerFixture>
{
    private ContainerFixture ContainerFixture { get; }
    private CosmosDbStorage Storage { get; }

    public JobQueueProviderFacts(ContainerFixture containerFixture)
    {
        ContainerFixture = containerFixture;
        Storage = containerFixture.Storage;
    }

    [Fact]
    public void GetJobQueue_WhenIsNotNull()
    {
        // arrange
        JobQueueProvider provider = new JobQueueProvider(Storage);
        
        // act
        IPersistentJobQueue queue = provider.GetJobQueue();
        
        // assert
        Assert.NotNull(queue);
    }
    
    [Fact]
    public void GetJobQueueMonitoringApi_WhenIsNotNull()
    {
        // arrange
        JobQueueProvider provider = new JobQueueProvider(Storage);
        
        // act
        IPersistentJobQueueMonitoringApi queue = provider.GetJobQueueMonitoringApi();
        
        // assert
        Assert.NotNull(queue);
    }
}