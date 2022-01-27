using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Tests.Fixtures;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Microsoft.Azure.Cosmos;
using Xunit;
using Xunit.Abstractions;
using Job = Hangfire.Common.Job;

namespace Hangfire.Azure.Tests;

public class CosmosDbConnectionFacts : IClassFixture<ContainerFixture>
{
	private CosmosDbStorage Storage { get; }

	private ContainerFixture ContainerFixture { get; }

	public CosmosDbConnectionFacts(ContainerFixture containerFixture, ITestOutputHelper testOutputHelper)
	{
		ContainerFixture = containerFixture;
		Storage = containerFixture.Storage;
		ContainerFixture.SetupLogger(testOutputHelper);
	}

	[Fact]
	public void DbConnection_ShouldThrowNullException_WhenStorageIsNull()
	{
		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new CosmosDbConnection(null!));

		// asset
		Assert.Equal("storage", exception.ParamName);
	}

	[Fact]
	public void DbConnection_CreateExpiredJob_ShouldThrowNullException_WhenJobIsNull()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.CreateExpiredJob(null, new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.Zero));

		// assert
		Assert.Equal("job", exception.ParamName);
	}

	[Fact]
	public void DbConnection_CreateExpiredJob_ShouldThrowNullException_WhenParameterIsNull()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();
		Job job = Job.FromExpression(() => SampleMethod("hello"));

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.CreateExpiredJob(job, null, DateTime.UtcNow, TimeSpan.Zero));

		// assert
		Assert.Equal("parameters", exception.ParamName);
	}

	[Fact]
	public void CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		DateTime createdAt = new(2012, 12, 12);
		Dictionary<string, string> parameters = new()
		{
			{ "Key1", "Value1" },
			{ "Key2", "Value2" },
			{ "Key3", "Value3" }
		};

		// act
		string? jobId = connection.CreateExpiredJob(Job.FromExpression(() => SampleMethod("Hello")), parameters, createdAt, TimeSpan.FromDays(1));

		// assert
		Assert.NotNull(jobId);
		Assert.NotEmpty(jobId);

		Task<ItemResponse<Documents.Job>> task = Storage.Container.ReadItemWithRetriesAsync<Documents.Job>(jobId, new PartitionKey((int)DocumentTypes.Job));
		Documents.Job? sqlJob = task.Result.Resource;

		Assert.Equal(jobId, sqlJob.Id);
		Assert.Equal(createdAt, sqlJob.CreatedOn.ToLocalTime());
		Assert.Null(sqlJob.StateId);
		Assert.Null(sqlJob.StateName);

		Assert.Equal("Value1", sqlJob.Parameters.SingleOrDefault(x => x.Name == "Key1")?.Value);
		Assert.Equal("Value2", sqlJob.Parameters.SingleOrDefault(x => x.Name == "Key2")?.Value);
		Assert.Equal("Value3", sqlJob.Parameters.SingleOrDefault(x => x.Name == "Key3")?.Value);

		InvocationData invocationData = sqlJob.InvocationData;
		invocationData.Arguments = sqlJob.Arguments;

		Job? deserializeJob = invocationData.DeserializeJob();
		Assert.Equal(typeof(CosmosDbConnectionFacts), deserializeJob.Type);
		Assert.Equal("SampleMethod", deserializeJob.Method.Name);
		Assert.Equal("Hello", deserializeJob.Args[0]);

		Assert.True(createdAt.AddDays(1).AddMinutes(-1) < sqlJob.ExpireOn!.Value.ToLocalTime());
		Assert.True(sqlJob.ExpireOn.Value.ToLocalTime() < createdAt.AddDays(1).AddMinutes(1));
	}

	[Fact]
	public void CreateExpiredJob_CanCreateParametersWithNullValues()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		DateTime createdAt = new(2012, 12, 12);
		Dictionary<string, string?> parameters = new()
		{
			{ "Key1", null },
			{ "Key2", null },
			{ "Key3", null }
		};

		// act
		string? jobId = connection.CreateExpiredJob(Job.FromExpression(() => SampleMethod("Hello")), parameters, createdAt, TimeSpan.FromDays(1));

		// assert
		Assert.NotNull(jobId);
		Assert.NotEmpty(jobId);

		Task<ItemResponse<Documents.Job>> task = Storage.Container.ReadItemWithRetriesAsync<Documents.Job>(jobId, new PartitionKey((int)DocumentTypes.Job));
		Documents.Job? sqlJob = task.Result.Resource;

		Assert.Null(sqlJob.Parameters.SingleOrDefault(x => x.Name == "Key1")?.Value);
		Assert.Null(sqlJob.Parameters.SingleOrDefault(x => x.Name == "Key2")?.Value);
		Assert.Null(sqlJob.Parameters.SingleOrDefault(x => x.Name == "Key3")?.Value);
	}

	[Fact]
	public void CreateExpiredJob_CanCreateJobWithoutParameters()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		DateTime createdAt = new(2012, 12, 12);
		Dictionary<string, string> parameters = new();

		// act
		string? jobId = connection.CreateExpiredJob(Job.FromExpression(() => SampleMethod("Hello")), parameters, createdAt, TimeSpan.FromDays(1));

		// assert
		Assert.NotNull(jobId);
		Assert.NotEmpty(jobId);

		Task<ItemResponse<Documents.Job>> task = Storage.Container.ReadItemWithRetriesAsync<Documents.Job>(jobId, new PartitionKey((int)DocumentTypes.Job));
		Documents.Job? sqlJob = task.Result.Resource;

		Assert.Empty(sqlJob.Parameters);
	}

	[Fact]
	public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetJobData(null));

		// assert
		Assert.Equal("jobId", exception.ParamName);
	}

	[Fact]
	public void GetJobData_ReturnsNull_WhenIdentifierCannotBeParsedAsGuid()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		JobData? job = connection.GetJobData("1");

		// assert
		Assert.Null(job);
	}


	[Fact]
	public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		JobData? job = connection.GetJobData(Guid.NewGuid().ToString());

		// assert
		Assert.Null(job);
	}

	[Fact]
	public void GetJobData_ReturnsResult_WhenJobExists()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		Job job = Job.FromExpression(() => SampleMethod("wrong"));
		InvocationData invocationData = InvocationData.SerializeJob(job);
		DateTime createdAt = new(2012, 12, 12);
		Dictionary<string, string?> parameters = new()
		{
			{ "Key1", null },
			{ "Key2", null },
			{ "Key3", null }
		};

		Documents.Job entityJob = new()
		{
			InvocationData = invocationData,
			Arguments = invocationData.Arguments,
			CreatedOn = createdAt,
			Parameters = parameters.Select(p => new Parameter
			{
				Name = p.Key,
				Value = p.Value
			}).ToArray(),
			StateId = Guid.NewGuid().ToString(),
			StateName = SucceededState.StateName
		};
		Task<ItemResponse<Documents.Job>> task = Storage.Container.CreateItemWithRetriesAsync(entityJob, new PartitionKey((int)DocumentTypes.Job));
		task.Wait();

		// act
		JobData? result = connection.GetJobData(task.Result.Resource.Id);

		// assert
		Assert.NotNull(result);
		Assert.NotNull(result.Job);
		Assert.Equal(SucceededState.StateName, result.State);
		Assert.Equal("wrong", result.Job.Args[0]);
		Assert.Null(result.LoadException);
		Assert.True(createdAt == result.CreatedAt);
	}

	[Fact]
	public void GetJobData_ReturnsResult_WithJobLoadException()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		Job job = Job.FromExpression(() => SampleMethod("wrong"));
		InvocationData invocationData = InvocationData.SerializeJob(job);
		DateTime createdAt = new(2012, 12, 12);
		Dictionary<string, string?> parameters = new()
		{
			{ "Key1", null },
			{ "Key2", null },
			{ "Key3", null }
		};

		Documents.Job entityJob = new()
		{
			InvocationData = invocationData,
			Arguments = string.Empty,
			CreatedOn = createdAt,
			Parameters = parameters.Select(p => new Parameter
			{
				Name = p.Key,
				Value = p.Value
			}).ToArray(),
			StateId = Guid.NewGuid().ToString(),
			StateName = SucceededState.StateName
		};
		Task<ItemResponse<Documents.Job>> task = Storage.Container.CreateItemWithRetriesAsync(entityJob, new PartitionKey((int)DocumentTypes.Job));
		task.Wait();

		// act
		JobData? result = connection.GetJobData(task.Result.Resource.Id);

		// assert
		Assert.NotNull(result.LoadException);
	}

	[Fact]
	public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act 
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetStateData(null));

		// assert
		Assert.Equal("jobId", exception.ParamName);
	}

	[Fact]
	public void GetStateData_ReturnsNull_WhenJobIdCannotBeParsedAsGuid()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act 
		StateData? state = connection.GetStateData("1");

		// assert
		Assert.Null(state);
	}

	[Fact]
	public void GetStateData_ReturnsNull_WhenThereIsNoSuchJobId()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act 
		StateData? state = connection.GetStateData(Guid.NewGuid().ToString());

		// assert
		Assert.Null(state);
	}

	[Fact]
	public void GetStateData_ReturnsNull_WhenThereIsNoSuchStateId()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		Documents.Job entityJob = new()
		{
			CreatedOn = DateTime.UtcNow,
			StateId = Guid.NewGuid().ToString()
		};
		Task<ItemResponse<Documents.Job>> task = Storage.Container.CreateItemWithRetriesAsync(entityJob, new PartitionKey((int)DocumentTypes.Job));
		task.Wait();

		// act 
		StateData? state = connection.GetStateData(entityJob.Id);

		// assert
		Assert.Null(state);
	}

	[Fact]
	public void GetStateData_ReturnsCorrectData()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		Documents.Job entityJob = new()
		{
			CreatedOn = DateTime.UtcNow,
			StateId = Guid.NewGuid().ToString()
		};
		Task<ItemResponse<Documents.Job>> task = Storage.Container.CreateItemWithRetriesAsync(entityJob, new PartitionKey((int)DocumentTypes.Job));
		task.Wait();

		// set the job state
		Dictionary<string, string> data = new()
		{
			{ "Key", "Value" }
		};
		IWriteOnlyTransaction transaction = connection.CreateWriteTransaction();
		transaction.SetJobState(entityJob.Id, new SucceededState(data, 1, 2) { Reason = "Reason" });
		transaction.Commit();

		// act 
		StateData? state = connection.GetStateData(entityJob.Id);

		// assert
		Assert.NotNull(state);
		Assert.Equal(SucceededState.StateName, state.Name);
		Assert.Equal("Reason", state.Reason);
		Assert.Equal("1", state.Data["Latency"]);
	}

	[Fact]
	public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.SetJobParameter(null!, "name", "value"));

		// assert
		Assert.Equal("id", exception.ParamName);
	}

	[Fact]
	public void SetParameter_ThrowsAnException_WhenNameIsNull()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.SetJobParameter("1", null!, "value"));

		// assert
		Assert.Equal("name", exception.ParamName);
	}

	[Fact]
	public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		Job job = Job.FromExpression(() => SampleMethod("wrong"));
		string? id = connection.CreateExpiredJob(job, new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.Zero);

		// set the parameter
		connection.SetJobParameter(id, "name", "value");
		string? value = connection.GetJobParameter(id, "name");

		// assert
		Assert.Equal("value", value);
	}

	[Fact]
	public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		Job job = Job.FromExpression(() => SampleMethod("wrong"));
		Dictionary<string, string> parameters = new()
		{
			{ "name", "value" }
		};
		string? id = connection.CreateExpiredJob(job, parameters, DateTime.UtcNow, TimeSpan.Zero);

		// set the parameter
		connection.SetJobParameter(id, "name", "value1");
		string? value = connection.GetJobParameter(id, "name");

		// assert
		Assert.Equal("value1", value);
	}

	[Fact]
	public void SetParameter_CanAcceptNulls_AsValues()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		Job job = Job.FromExpression(() => SampleMethod("wrong"));
		Dictionary<string, string> parameters = new();
		string? id = connection.CreateExpiredJob(job, parameters, DateTime.UtcNow, TimeSpan.Zero);

		// set the parameter
		connection.SetJobParameter(id, "name", null);
		string? value = connection.GetJobParameter(id, "name");

		// assert
		Assert.Null(value);
	}

	[Fact]
	public void GetParameter_ThrowsAnException_WhenJobIdIsNull()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetJobParameter(null, "name"));

		// assert
		Assert.Equal("id", exception.ParamName);
	}

	[Fact]
	public void GetParameter_ReturnNull_WhenJobIsNotGuid()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		string? value = connection.GetJobParameter("1", "name");

		// assert
		Assert.Null(value);
	}

	[Fact]
	public void GetParameter_ReturnNull_WhenJobIsNotFound()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		string? value = connection.GetJobParameter(Guid.NewGuid().ToString(), "name");

		// assert
		Assert.Null(value);
	}

	[Fact]
	public void GetParameter_ThrowsAnException_WhenNameIsNull()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetJobParameter("1", null));

		// assert
		Assert.Equal("name", exception.ParamName);
	}

	[Fact]
	public void GetParameter_ReturnsParameterValue_WhenJobExists()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		Job job = Job.FromExpression(() => SampleMethod("wrong"));
		Dictionary<string, string> parameters = new();
		string? id = connection.CreateExpiredJob(job, parameters, DateTime.UtcNow, TimeSpan.Zero);
		connection.SetJobParameter(id, "name", "value");
		string? value = connection.GetJobParameter(id, "name");

		// assert
		Assert.Equal("value", value);
	}

	[Fact]
	public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetFirstByLowestScoreFromSet(null, 0, 1));

		// assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();
		ArgumentException exception = Assert.Throws<ArgumentException>(() => connection.GetFirstByLowestScoreFromSet("key", 0, -1));

		// assert
		Assert.Equal("toScore", exception.ParamName);
	}

	[Fact]
	public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();
		string? result = connection.GetFirstByLowestScoreFromSet("key", 0, 1);

		// assert
		Assert.Null(result);
	}

	[Fact]
	public void GetFirstByLowestScoreFromSet_ThrowsArgException_WhenRequestingLessThanZero()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		ArgumentException exception = Assert.Throws<ArgumentException>(() => connection.GetFirstByLowestScoreFromSet("key", 0, 1, -1));

		// assert
		Assert.Equal("count", exception.ParamName);
	}

	[Fact]
	public void GetFirstByLowestScoreFromSet_ReturnsEmpty_WhenNoneExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		List<string> result = connection.GetFirstByLowestScoreFromSet("key", 0, 1, 10);

		// assert
		Assert.Empty(result);
	}

	[Fact]
	public void GetFirstByLowestScoreFromSet_ReturnsN_WhenMoreThanNExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		IWriteOnlyTransaction transaction = connection.CreateWriteTransaction();
		transaction.AddToSet("key", "1234", 1.0);
		transaction.AddToSet("key", "567", -1.0);
		transaction.AddToSet("key", "890", -5.0);
		transaction.AddToSet("another-key", "abcd", -2.0);
		transaction.Commit();

		// act
		List<string>? result = connection.GetFirstByLowestScoreFromSet("key", -10.0, 10.0, 2);

		// assert
		Assert.Equal(2, result.Count);
		Assert.Equal("890", result.ElementAt(0));
		Assert.Equal("567", result.ElementAt(1));
	}

	[Fact]
	public void GetFirstByLowestScoreFromSet_ReturnsN_WhenMoreThanNExist_And_RequestedCountIsGreaterThanN()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		IWriteOnlyTransaction transaction = connection.CreateWriteTransaction();
		transaction.AddToSet("key", "1234", 1.0);
		transaction.AddToSet("key", "567", -1.0);
		transaction.AddToSet("key", "890", -5.0);
		transaction.AddToSet("another-key", "abcd", -2.0);
		transaction.Commit();

		// act
		List<string>? result = connection.GetFirstByLowestScoreFromSet("another-key", -10.0, 10.0, 5);

		// assert
		Assert.Single(result);
		Assert.Equal("abcd", result.First());
	}

	[Fact]
	public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		IWriteOnlyTransaction transaction = connection.CreateWriteTransaction();
		transaction.AddToSet("key", "1.0", 1.0);
		transaction.AddToSet("key", "-1.0", -1.0);
		transaction.AddToSet("key", "-5.0", -5.0);
		transaction.AddToSet("another-key", "-2.0", -2.0);
		transaction.Commit();

		// act
		string? result = connection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

		// assert
		Assert.Equal("-1.0", result);
	}

	[Fact]
	public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		var exception = Assert.Throws<ArgumentNullException>(() => connection.AnnounceServer(null, new ServerContext()));

		//assert
		Assert.Equal("serverId", exception.ParamName);
	}

	[Fact]
	public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		var exception = Assert.Throws<ArgumentNullException>(() => connection.AnnounceServer("server", null));

		//assert
		Assert.Equal("context", exception.ParamName);
	}

	[Fact]
	public void AnnounceServer_CreatesOrUpdatesARecord()
	{
		//clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		ServerContext context1 = new()
		{
			Queues = new[] { "critical", "default" },
			WorkerCount = 4
		};
		connection.AnnounceServer("server", context1);

		Task<ItemResponse<Documents.Server>> readTask = Storage.Container.ReadItemWithRetriesAsync<Documents.Server>("server", new PartitionKey((int)DocumentTypes.Server));
		readTask.Wait();
		Documents.Server? server = readTask.Result.Resource;

		//assert
		Assert.Equal("server", server.Id);
		Assert.Equal(4, server.Workers);
		Assert.Contains("critical", server.Queues);
		Assert.Contains("default", server.Queues);

		//act 
		ServerContext context2 = new()
		{
			Queues = new[] { "default" },
			WorkerCount = 1000
		};
		connection.AnnounceServer("server", context2);

		readTask = Storage.Container.ReadItemWithRetriesAsync<Documents.Server>("server", new PartitionKey((int)DocumentTypes.Server));
		readTask.Wait();
		Documents.Server? sameServer = readTask.Result.Resource;

		//assert
		Assert.Equal("server", sameServer.Id);
		Assert.Equal(1000, sameServer.Workers);
		Assert.DoesNotContain("critical", sameServer.Queues);
		Assert.Contains("default", sameServer.Queues);
	}

	[Fact]
	public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();

		//act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.RemoveServer(null));

		//assert
		Assert.Equal("serverId", exception.ParamName);
	}

	[Fact]
	public void RemoveServer_RemovesAServerRecord()
	{
		//clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		connection.AnnounceServer("server", new ServerContext());
		connection.AnnounceServer("server1", new ServerContext());

		// act
		connection.RemoveServer("server1");

		Documents.Server server = Storage.Container.GetItemLinqQueryable<Documents.Server>()
			.ToQueryResult()
			.Single();

		//assert
		Assert.NotEqual("server1", server.Id);
	}

	[Fact]
	public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.Heartbeat(null));

		// assert
		Assert.Equal("serverId", exception.ParamName);
	}

	[Fact]
	public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Documents.Server> servers = new()
		{
			new Documents.Server
			{
				Id = "server1",
				CreatedOn = DateTime.UtcNow,
				LastHeartbeat = new DateTime(2021, 1, 1)
			},
			new Documents.Server
			{
				Id = "server2",
				CreatedOn = DateTime.UtcNow,
				LastHeartbeat = new DateTime(2021, 1, 1)
			}
		};
		Data<Documents.Server> data = new(servers);
		Storage.Container.ExecuteUpsertDocuments(data, new PartitionKey((int)DocumentTypes.Server));

		// act
		IStorageConnection connection = Storage.GetConnection();
		connection.Heartbeat("server1");

		Dictionary<string, DateTime> result = Storage.Container.GetItemLinqQueryable<Documents.Server>()
			.ToQueryResult()
			.ToDictionary(k => k.Id, v => v.LastHeartbeat.ToLocalTime());

		Assert.NotEqual(2021, result["server1"].Year);
		Assert.Equal(2021, result["server2"].Year);
	}

	[Fact]
	public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		ArgumentException exception = Assert.Throws<ArgumentException>(() => connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-5)));

		// assert
		Assert.Equal("timeOut", exception.ParamName);
	}

	[Fact]
	public void RemoveTimedOutServers_DoItsWorkPerfectly()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Documents.Server> servers = new()
		{
			new Documents.Server
			{
				Id = "server1",
				CreatedOn = DateTime.UtcNow,
				LastHeartbeat = DateTime.UtcNow.AddDays(-1)
			},
			new Documents.Server
			{
				Id = "server2",
				CreatedOn = DateTime.UtcNow,
				LastHeartbeat = DateTime.UtcNow.AddHours(-12)
			}
		};
		Data<Documents.Server> data = new(servers);
		Storage.Container.ExecuteUpsertDocuments(data, new PartitionKey((int)DocumentTypes.Server));

		// act
		IStorageConnection connection = Storage.GetConnection();
		connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

		Documents.Server result = Storage.Container.GetItemLinqQueryable<Documents.Server>()
			.ToQueryResult()
			.Single();

		Assert.Equal("server2", result.Id);
	}

#pragma warning disable xUnit1013
	// ReSharper disable once MemberCanBePrivate.Global
	// ReSharper disable once UnusedParameter.Global
	public static void SampleMethod(string arg) { }
#pragma warning restore xUnit1013
}