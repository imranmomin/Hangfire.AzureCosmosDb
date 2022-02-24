using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Server;
using Hangfire.Storage;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Hangfire.Azure.Tests;

public class CosmosDbStorageFacts
{
	private readonly string url;
	private readonly string secret;
	private readonly string database;
	private readonly string container;

	public CosmosDbStorageFacts()
	{
		IConfiguration configuration = new ConfigurationBuilder()
			.AddJsonFile("appsettings.json", false, false)
			.AddEnvironmentVariables()
			.Build();

		IConfigurationSection section = configuration.GetSection("CosmosDB");
		url = section.GetValue<string>("Url");
		secret = section.GetValue<string>("Secret");
		database = section.GetValue<string>("Database");
		container = section.GetValue<string>("Container");
	}

	[Fact]
	public void Ctor_ThrowsAnException_WhenUrlIsNullOrEmpty()
	{
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new CosmosDbStorage(null!, string.Empty, string.Empty, string.Empty));
		Assert.Equal("url", exception.ParamName);
	}

	[Fact]
	public void Ctor_ThrowsAnException_WhenSecretIsNullOrEmpty()
	{
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new CosmosDbStorage("http://", null!, string.Empty, string.Empty));
		Assert.Equal("authSecret", exception.ParamName);
	}

	[Fact]
	public void Ctor_ThrowsAnException_WhenDatabaseNameIsNullOrEmpty()
	{
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new CosmosDbStorage("http://", Guid.NewGuid().ToString(), null!, string.Empty));
		Assert.Equal("databaseName", exception.ParamName);
	}

	[Fact]
	public void Ctor_ThrowsAnException_WhenContainerNameIsNullOrEmpty()
	{
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new CosmosDbStorage("http://", Guid.NewGuid().ToString(), "hangfire", string.Empty));
		Assert.Equal("containerName", exception.ParamName);
	}

	[Fact]
	public void Ctor_CanCreateCosmosDbStorage_WithExistingConnection()
	{
		CosmosDbStorage storage = new(url, secret, database, container);
		Assert.NotNull(storage);
	}

	[Fact]
	public void GetMonitoringApi_ReturnsNonNullInstance()
	{
		CosmosDbStorage storage = new(url, secret, database, container);
		IMonitoringApi api = storage.GetMonitoringApi();
		Assert.NotNull(api);
	}

	[Fact]
	public void GetConnection_ReturnsNonNullInstance()
	{
		CosmosDbStorage storage = new(url, secret, database, container);
		using CosmosDbConnection connection = (CosmosDbConnection)storage.GetConnection();
		Assert.NotNull(connection);
	}

	[Fact]
	public void GetComponents_ReturnsAllNeededComponents()
	{
		CosmosDbStorage storage = new(url, secret, database, container);
		IEnumerable<IServerComponent> components = storage.GetComponents();
		Type[] componentTypes = components.Select(x => x.GetType()).ToArray();
		Assert.Contains(typeof(ExpirationManager), componentTypes);
	}
}