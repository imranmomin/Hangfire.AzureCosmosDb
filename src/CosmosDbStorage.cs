using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Queue;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Hangfire.Azure;

/// <summary>
///     CosmosDbStorage extend the storage option for Hangfire.
/// </summary>
[SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
internal sealed class CosmosDbStorage : JobStorage, IDisposable
{
	private readonly string containerName;
	private readonly string databaseName;
	private readonly ILog logger = LogProvider.For<CosmosDbStorage>();

	private readonly JsonSerializerSettings settings = new()
	{
		NullValueHandling = NullValueHandling.Ignore,
		DateTimeZoneHandling = DateTimeZoneHandling.Utc,
		ContractResolver = new CamelCasePropertyNamesContractResolver
		{
			NamingStrategy = new CamelCaseNamingStrategy(false, false)
		}
	};

	private bool disposed;

	/// <summary>
	///     Creates an instance of CosmosDbStorage
	/// </summary>
	/// <param name="url">The url string to Cosmos Database</param>
	/// <param name="authSecret">The secret key for the Cosmos Database</param>
	/// <param name="databaseName">The name of the database to connect with</param>
	/// <param name="containerName">The name of the collection/container on the database</param>
	/// <param name="options">The CosmosClientOptions object to override any of the options</param>
	/// <param name="storageOptions">The CosmosDbStorageOptions object to override any of the options</param>
	private CosmosDbStorage(string url, string authSecret, string databaseName, string containerName, CosmosClientOptions? options = null, CosmosDbStorageOptions? storageOptions = null)
	{
		this.databaseName = databaseName;
		this.containerName = containerName;
		StorageOptions = storageOptions ?? new CosmosDbStorageOptions();

		JobQueueProvider provider = new(this);
		QueueProviders = new PersistentJobQueueProviderCollection(provider);

		options ??= new CosmosClientOptions();
		options.ApplicationName = "Hangfire";
		options.Serializer = new CosmosJsonSerializer(settings);
		Client = new CosmosClient(url, authSecret, options);
	}

	internal PersistentJobQueueProviderCollection QueueProviders { get; }

	internal CosmosDbStorageOptions StorageOptions { get; set; }

	private CosmosClient Client { get; }

	internal Container Container { get; private set; } = null!;

	public void Dispose()
	{
		if (disposed) return;
		disposed = true;
		Client.Dispose();
	}

	/// <summary>
	/// </summary>
	/// <returns></returns>
	public override IStorageConnection GetConnection() => new CosmosDbConnection(this);

	/// <summary>
	/// </summary>
	/// <returns></returns>
	public override IMonitoringApi GetMonitoringApi() => new CosmosDbMonitoringApi(this);

#pragma warning disable 618
	/// <summary>
	/// </summary>
	/// <returns></returns>
	public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore 618
	{
		yield return new ExpirationManager(this);
		yield return new CountersAggregator(this);
	}

	/// <summary>
	///     Prints out the storage options
	/// </summary>
	/// <param name="logger"></param>
	public override void WriteOptionsToLog(ILog logger)
	{
		logger.Info("Using the following options for Azure Cosmos DB job storage:");
		logger.Info($"     Cosmos DB Url: [{Client.Endpoint.AbsoluteUri}]");
		logger.Info($"     Request Timeout: [{Client.ClientOptions.RequestTimeout}]");
		logger.Info($"     Counter Aggregate Interval: [{StorageOptions.CountersAggregateInterval.TotalSeconds}] seconds");
		logger.Info($"     Queue Poll Interval: [{StorageOptions.QueuePollInterval.TotalSeconds}] seconds");
		logger.Info($"     Expiration Check Interval: [{StorageOptions.ExpirationCheckInterval.TotalSeconds}] seconds");
	}

	/// <summary>
	///     Return the name of the database
	/// </summary>
	/// <returns></returns>
	public override string ToString() => $"Cosmos DB : {databaseName}";

	/// <summary>
	///     Creates and returns an instance of CosmosDbStorage
	/// </summary>
	/// <param name="url">The url string to Cosmos Database</param>
	/// <param name="authSecret">The secret key for the Cosmos Database</param>
	/// <param name="databaseName">The name of the database to connect with</param>
	/// <param name="containerName">The name of the collection/container on the database</param>
	/// <param name="options">The CosmosClientOptions object to override any of the options</param>
	/// <param name="storageOptions">The CosmosDbStorageOptions object to override any of the options</param>
	public static CosmosDbStorage Create(string url, string authSecret, string databaseName, string containerName, CosmosClientOptions? options = null, CosmosDbStorageOptions? storageOptions = null)
	{
		CosmosDbStorage storage = new(url, authSecret, databaseName, containerName, options, storageOptions);
		storage.InitializeAsync().Wait();
		return storage;
	}

	/// <summary>
	///     Creates and returns an instance of CosmosDbStorage
	/// </summary>
	/// <param name="url">The url string to Cosmos Database</param>
	/// <param name="authSecret">The secret key for the Cosmos Database</param>
	/// <param name="databaseName">The name of the database to connect with</param>
	/// <param name="containerName">The name of the collection/container on the database</param>
	/// <param name="options">The CosmosClientOptions object to override any of the options</param>
	/// <param name="storageOptions">The CosmosDbStorageOptions object to override any of the options</param>
	/// <param name="cancellationToken">A cancellation token</param>
	public static async Task<CosmosDbStorage> CreateAsync(string url, string authSecret, string databaseName, string containerName,
		CosmosClientOptions? options = null,
		CosmosDbStorageOptions? storageOptions = null,
		CancellationToken cancellationToken = default)
	{
		CosmosDbStorage storage = new(url, authSecret, databaseName, containerName, options, storageOptions);
		await storage.InitializeAsync(cancellationToken);
		return storage;
	}

	private async Task InitializeAsync(CancellationToken cancellationToken = default)
	{
		// create database
		logger.Info($"Creating database : [{databaseName}]");
		DatabaseResponse databaseResponse = await Client.CreateDatabaseIfNotExistsAsync(databaseName, cancellationToken: cancellationToken);

		// create container
		logger.Info($"Creating container : [{containerName}]");
		Database resultDatabase = databaseResponse.Database;

		ContainerProperties properties = new()
		{
			Id = containerName,
			DefaultTimeToLive = -1,
			PartitionKeyPath = "/type",
			PartitionKeyDefinitionVersion = PartitionKeyDefinitionVersion.V2
		};

		// add the index policy
		Collection<CompositePath> compositeIndexes = new()
		{
			new CompositePath { Path = "/name", Order = CompositePathSortOrder.Ascending },
			new CompositePath { Path = "/created_on", Order = CompositePathSortOrder.Ascending }
		};
		properties.IndexingPolicy.CompositeIndexes.Add(compositeIndexes);

		ContainerResponse containerResponse = await resultDatabase.CreateContainerIfNotExistsAsync(properties, cancellationToken: cancellationToken);
		Container = containerResponse.Container;

		// create stored procedures 
		Assembly assembly = Assembly.GetExecutingAssembly();
		string[] storedProcedureFiles = assembly.GetManifestResourceNames().Where(n => n.EndsWith(".js")).ToArray();
		foreach (string storedProcedureFile in storedProcedureFiles)
		{
			logger.Info($"Creating storedprocedure : [{storedProcedureFile}]");
			Stream? stream = assembly.GetManifestResourceStream(storedProcedureFile);

			// if the stream is null skip and continue to next resource
			if (stream == null) continue;

			await using MemoryStream memoryStream = new();
			await stream.CopyToAsync(memoryStream, cancellationToken);

			StoredProcedureProperties sp = new()
			{
				Body = Encoding.UTF8.GetString(memoryStream.ToArray()),
				Id = Path.GetFileNameWithoutExtension(storedProcedureFile)?
					.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries)
					.Last()
			};

			const string query = "SELECT * FROM doc where doc.id = @Id";
			QueryDefinition queryDefinition = new(query);
			queryDefinition.WithParameter("@Id", sp.Id);

			using FeedIterator<StoredProcedureProperties> iterator = Container.Scripts.GetStoredProcedureQueryIterator<StoredProcedureProperties>(queryDefinition);
			if (iterator.HasMoreResults)
			{
				FeedResponse<StoredProcedureProperties> storedProcedure = await iterator.ReadNextAsync(cancellationToken);
				if (storedProcedure.Count == 0) await Container.Scripts.CreateStoredProcedureAsync(sp, cancellationToken: cancellationToken);
				else await Container.Scripts.ReplaceStoredProcedureAsync(sp, cancellationToken: cancellationToken);
			}

			// close the stream
			stream.Close();
		}
	}
}