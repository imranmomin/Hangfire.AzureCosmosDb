# Hangfire.AzureDocumentDB

[![Official Site](https://img.shields.io/badge/site-hangfire.io-blue.svg)](http://hangfire.io)
[![Latest version](https://img.shields.io/badge/nuget-v1.0.0-blue.svg)](https://www.nuget.org/packages/Hangfire.AzureDocumentDB) 
[![Build status](https://ci.appveyor.com/api/projects/status/8bail001djs64inu?svg=true)](https://ci.appveyor.com/project/imranmomin/hangfire-azuredocumentdb)

This repo will add a [Microsoft Azure DocumentDB](https://azure.microsoft.com/en-ca/services/documentdb) storage support to [Hangfire](http://hangfire.io) - fire-and-forget, delayed and recurring tasks runner for .NET. Scalable and reliable background job runner. Supports multiple servers, CPU and I/O intensive, long-running and short-running jobs.

Installation

-------------

[Hangfire.AzureDocumentDB](https://www.nuget.org/packages/Hangfire.AzureDocumentDB) is available as a NuGet package. Install it using the NuGet Package Console window:

```powershell

PM> Install-Package Hangfire.AzureDocumentDB
```

Usage

-------------

Use one the following ways to initialize `AzureDocumentDBStorage`

```csharp
GlobalConfiguration.Configuration.UseAzureDocumentDBStorage("<url>", "<authSecret>");

AzureDocumentDB.AzureDocumentDBStorage azureDocumentDBStorage = new AzureDocumentDB.AzureDocumentDBStorage("<url>", "<authSecret>", "<databaseName>");
GlobalConfiguration.Configuration.UseStorage(azureDocumentDBStorage);

// customize any options
AzureDocumentDB.AzureDocumentDBStorageOptions azureDocumentDBStorageOptions = new AzureDocumentDB.AzureDocumentDBStorageOptions
{
    Queues = new[] { "default", "critical" },
    RequestTimeout = System.TimeSpan.FromSeconds(30),
    ExpirationCheckInterval = TimeSpan.FromMinutes(15);
    CountersAggregateInterval = TimeSpan.FromMinutes(1);
    QueuePollInterval = TimeSpan.FromSeconds(2);
};
AzureDocumentDB.AzureDocumentDBStorage azureDocumentDBStorage = new AzureDocumentDB.AzureDocumentDBStorage("<url>", "<authSecret>", "<databaseName>", azureDocumentDBStorageOptions);
GlobalConfiguration.Configuration.UseStorage(azureDocumentDBStorage);
```

Limitations

-------------

Currently, the storage will create individual collections. In future will try to get an option to use the specified collections.

* Servers
* Queues
* Jobs
* Hashes
* Sets
* Lists
* Counters
* States
* Locks

Questions? Problems?

-------------

Open-source project are developing more smoothly, when all discussions are held in public.

If you have any questions or problems related to Hangfire.AzureDocumentDB itself or this storage implementation or want to discuss new features, please create under [issues](https://github.com/imranmomin/Hangfire.AzureDocumentDB/issues/new) and assign the correct label for discussion. 

If you've discovered a bug, please report it to the [GitHub Issues](https://github.com/imranmomin/Hangfire.AzureDocumentDB/pulls). Detailed reports with stack traces, actual and expected behavours are welcome.