using System;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Xunit.Abstractions;

namespace Hangfire.Azure.Tests.Fixtures;

// ReSharper disable once ClassNeverInstantiated.Global
public class ContainerFixture : IDisposable
{
    private bool disposed;

    public ContainerFixture()
    {
        LogProvider.SetCurrentLogProvider(null);

        IConfiguration configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", false, false)
            .AddEnvironmentVariables()
            .Build();

        IConfigurationSection section = configuration.GetSection("CosmosDB");
        string url = section.GetValue<string>("Url");
        string secret = section.GetValue<string>("Secret");
        string database = section.GetValue<string>("Database");
        string container = section.GetValue<string>("Container");

        CosmosDbStorageOptions option = new()
        {
            CountersAggregateInterval = TimeSpan.Zero,
            ExpirationCheckInterval = TimeSpan.Zero,
            QueuePollInterval = TimeSpan.Zero,
            CountersAggregateMaxItemCount = 1,
            TransactionalLockTimeout = TimeSpan.Zero
        };

        Storage = CosmosDbStorage.Create(url, secret, database, container, storageOptions: option);
    }

    internal CosmosDbStorage Storage { get; }

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        disposed = true;

        Clean();
    }

    public void SetupLogger(ITestOutputHelper testOutputHelper) => LogProvider.SetCurrentLogProvider(new TestLogger(testOutputHelper));

    public void Clean()
    {
        const string query = "SELECT * FROM doc";
        foreach (object? type in Enum.GetValues(typeof(DocumentTypes)))
        {
            Storage.Container.ExecuteDeleteDocuments(query, new PartitionKey((int)type));
        }
    }

    private class TestLogger : ILogProvider
    {
        private readonly ITestOutputHelper testOutputHelper;

        public TestLogger(ITestOutputHelper testOutputHelper) => this.testOutputHelper = testOutputHelper;

        public ILog GetLogger(string name) => new TestLog(testOutputHelper);
    }

    private class TestLog : ILog
    {
        private readonly ITestOutputHelper? testOutputHelper;

        public TestLog(ITestOutputHelper testOutputHelper) => this.testOutputHelper = testOutputHelper;

        public bool Log(LogLevel logLevel, Func<string>? messageFunc, Exception? exception = null)
        {
            if (messageFunc != null && testOutputHelper != null)
            {
                testOutputHelper.WriteLine("[{0:O}] - [{1}] - {2} {3}", DateTime.UtcNow, logLevel, messageFunc(), exception?.Message);
            }

            return true;
        }
    }
}