# Hangfire.AzureCosmosDB

[![Official Site](https://img.shields.io/badge/site-hangfire.io-blue.svg)](http://hangfire.io)
[![Latest version](https://img.shields.io/nuget/v/Hangfire.AzureCosmosDB.svg)](https://www.nuget.org/packages/Hangfire.AzureCosmosDB)
[![Build status](https://ci.appveyor.com/api/projects/status/uvxh94dhxcokga47?svg=true)](https://ci.appveyor.com/project/imranmomin/hangfire-azurecosmosdb)

This repo will add a [Microsoft Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/introduction) storage support to [Hangfire](http://hangfire.io) - fire-and-forget, delayed and recurring tasks runner for .NET. Scalable and reliable background job runner. Supports multiple servers, CPU and I/O intensive, long-running and short-running jobs.


## Installation

[Hangfire.AzureCosmosDB](https://www.nuget.org/packages/Hangfire.AzureCosmosDB) is available as a NuGet package. Install it using the NuGet Package Console window:

```powershell
PM> Install-Package Hangfire.AzureCosmosDB
```


## Usage

Use one the following ways to initialize `CosmosDbStorage`

```csharp
GlobalConfiguration.Configuration.UseAzureCosmosDbStorage("<url>", "<authSecret>", "<databaseName>", "<collectionName>");

Hangfire.Azure.CosmosDbStorage storage = new Hangfire.Azure.CosmosDbStorage("<url>", "<authSecret>", "<databaseName>", "<collectionName>");
GlobalConfiguration.Configuration.UseStorage(storage);
```

```csharp
// customize any options
Hangfire.Azure.CosmosDbStorageOptions options = new Hangfire.Azure.CosmosDbStorageOptions
{
    ExpirationCheckInterval = TimeSpan.FromMinutes(2),
    CountersAggregateInterval = TimeSpan.FromMinutes(2),
    QueuePollInterval = TimeSpan.FromSeconds(15)
};

GlobalConfiguration.Configuration.UseAzureCosmosDbStorage("<url>", "<authSecret>", "<databaseName>", "<collectionName>", cosmoClientOptions, options);

// or 

Hangfire.Azure.CosmosDbStorage storage = new Hangfire.Azure.CosmosDbStorage("<url>", "<authSecret>", "<databaseName>", "<collectionName>", cosmoClientOptions, options);
GlobalConfiguration.Configuration.UseStorage(storage);
```

## Recommendations
- Keep seperate database/collection for the hangfire.


## Questions? Problems?

Open-source project are developing more smoothly, when all discussions are held in public.

If you have any questions or problems related to Hangfire.AzureCosmosDB itself or this storage implementation or want to discuss new features, please create under [issues](https://github.com/imranmomin/Hangfire.AzureCosmosDB/issues/new) and assign the correct label for discussion. 

If you've discovered a bug, please report it to the [GitHub Issues](https://github.com/imranmomin/Hangfire.AzureCosmosDB/pulls). Detailed reports with stack traces, actual and expected behavours are welcome.
