using System;
using Hangfire.Azure.Helper;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;

namespace Hangfire.Azure.Tests.Fixtures;

// ReSharper disable once ClassNeverInstantiated.Global
public class ContainerFixture : IDisposable
{
    private bool disposed = false;
    public CosmosDbStorage Storage { get; }

    public ContainerFixture()
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", false, false)
            .AddEnvironmentVariables()
            .Build();

        IConfigurationSection section = configuration.GetSection("CosmosDB");
        string url = section.GetValue<string>("Url");
        string secret = section.GetValue<string>("Secret");
        string database = section.GetValue<string>("Database");
        string container = section.GetValue<string>("Container");

        Storage = CosmosDbStorage.Create(url, secret, database, container);
    }

    public void Clean()
    {
        const string query = "SELECT * FROM doc";
        foreach (var type in Enum.GetValues(typeof(Documents.DocumentTypes)))
        {
            Storage.Container.ExecuteDeleteDocuments(query, new PartitionKey((int)type));
        }
    }

    public void Dispose()
    {
        if (disposed) return;
        disposed = true;

        Clean();
        Storage.Dispose();
    }
}