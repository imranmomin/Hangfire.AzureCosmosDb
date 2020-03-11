# Hangfire.AzureCosmosDB

[![Official Site](https://img.shields.io/badge/site-hangfire.io-blue.svg)](http://hangfire.io)
[![Latest version](https://img.shields.io/nuget/v/Hangfire.AzureCosmosDB.svg)](https://www.nuget.org/packages/Hangfire.AzureCosmosDB)
[![Build status](https://ci.appveyor.com/api/projects/status/np818jy8h1sa4v6n?svg=true)](https://ci.appveyor.com/project/imranmomin/hangfire-azurecosmosdb)

This repo will add a [Microsoft Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/introduction) storage support to [Hangfire](http://hangfire.io) - fire-and-forget, delayed and recurring tasks runner for .NET. Scalable and reliable background job runner. Supports multiple servers, CPU and I/O intensive, long-running and short-running jobs.


## Installation

[Hangfire.AzureCosmosDB](https://www.nuget.org/packages/Hangfire.AzureCosmosDB) is available on NuGet.

Package Manager
```powershell
PM> Install-Package Hangfire.AzureCosmosDB
```

.NET CLI
```
> dotnet add package Hangfire.AzureCosmosDB
```

PackageReference
```xml
<PackageReference Include="Hangfire.AzureCosmosDB" Version="0.0.0" />
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
- Keep seperate database/collection for the hangfire. (Now you can [enable free tier](https://docs.microsoft.com/en-us/azure/cosmos-db/optimize-dev-test#azure-cosmos-db-free-tier) on Azure)

## SDK Support
This package only supports the latest SDK v3 [Microsoft.Azure.Cosmos](https://www.nuget.org/packages/Microsoft.Azure.Cosmos). If you want the support for the latest SDK v2 [Microsoft.Azure.DocumentDB](https://www.nuget.org/packages/Microsoft.Azure.DocumentDB/), you will have to use [Hangfire.AzureDocumentDB](https://www.nuget.org/packages/Hangfire.AzureDocumentDB)

## Questions? Problems?

Open-source project are developing more smoothly, when all discussions are held in public.

If you have any questions or problems related to Hangfire.AzureCosmosDB itself or this storage implementation or want to discuss new features, please create under [issues](https://github.com/imranmomin/Hangfire.AzureCosmosDB/issues/new) and assign the correct label for discussion. 

If you've discovered a bug, please report it to the [GitHub Issues](https://github.com/imranmomin/Hangfire.AzureCosmosDB/pulls). Detailed reports with stack traces, actual and expected behavours are welcome.
