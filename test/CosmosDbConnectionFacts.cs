using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Queue;
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

		Task<ItemResponse<Documents.Job>> task = Storage.Container.ReadItemWithRetriesAsync<Documents.Job>(jobId, PartitionKeys.Job);
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

		Task<ItemResponse<Documents.Job>> task = Storage.Container.ReadItemWithRetriesAsync<Documents.Job>(jobId, PartitionKeys.Job);
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

		Task<ItemResponse<Documents.Job>> task = Storage.Container.ReadItemWithRetriesAsync<Documents.Job>(jobId, PartitionKeys.Job);
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
		Task<ItemResponse<Documents.Job>> task = Storage.Container.CreateItemWithRetriesAsync(entityJob, PartitionKeys.Job);
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
		Task<ItemResponse<Documents.Job>> task = Storage.Container.CreateItemWithRetriesAsync(entityJob, PartitionKeys.Job);
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
		Task<ItemResponse<Documents.Job>> task = Storage.Container.CreateItemWithRetriesAsync(entityJob, PartitionKeys.Job);
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
		Task<ItemResponse<Documents.Job>> task = Storage.Container.CreateItemWithRetriesAsync(entityJob, PartitionKeys.Job);
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
			{ "name", "value" },
			{ "name1", "value1" },
			{ "name2", "value2" }
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
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.AnnounceServer(null, new ServerContext()));

		//assert
		Assert.Equal("serverId", exception.ParamName);
	}

	[Fact]
	public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.AnnounceServer("server", null));

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

		Task<ItemResponse<Documents.Server>> readTask = Storage.Container.ReadItemWithRetriesAsync<Documents.Server>("server", PartitionKeys.Server);
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

		readTask = Storage.Container.ReadItemWithRetriesAsync<Documents.Server>("server", PartitionKeys.Server);
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
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Server
		};
		Documents.Server server = Storage.Container.GetItemLinqQueryable<Documents.Server>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Single();

		//assert
		Assert.NotEqual("server1", server.Id);
	}

	[Fact]
	public void RemoveServer_RemovesAServerRecord_WhenDoesNotGate()
	{
		//clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		connection.RemoveServer("server1");

		// act
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Server
		};
		List<Documents.Server> results = Storage.Container.GetItemLinqQueryable<Documents.Server>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.ToList();

		//assert
		Assert.Empty(results);
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
			new Documents.Server { Id = "server1", CreatedOn = DateTime.UtcNow, LastHeartbeat = new DateTime(2021, 1, 1) },
			new Documents.Server { Id = "server2", CreatedOn = DateTime.UtcNow, LastHeartbeat = new DateTime(2021, 1, 1) }
		};
		Data<Documents.Server> data = new(servers);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Server);

		// act
		IStorageConnection connection = Storage.GetConnection();
		connection.Heartbeat("server1");

		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Server
		};
		Dictionary<string, DateTime> result = Storage.Container.GetItemLinqQueryable<Documents.Server>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.ToDictionary(k => k.Id, v => v.LastHeartbeat.ToLocalTime());

		Assert.NotEqual(2021, result["server1"].Year);
		Assert.Equal(2021, result["server2"].Year);
	}

	[Fact]
	public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenIdDoesNotExists()
	{
		// clean
		ContainerFixture.Clean();

		// act
		IStorageConnection connection = Storage.GetConnection();
		connection.Heartbeat("server1");

		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Server
		};
		Dictionary<string, DateTime> result = Storage.Container.GetItemLinqQueryable<Documents.Server>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.ToDictionary(k => k.Id, v => v.LastHeartbeat.ToLocalTime());

		Assert.Empty(result);
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
			new Documents.Server { Id = "server1", CreatedOn = DateTime.UtcNow, LastHeartbeat = DateTime.UtcNow.AddDays(-1) },
			new Documents.Server { Id = "server2", CreatedOn = DateTime.UtcNow, LastHeartbeat = DateTime.UtcNow.AddHours(-12) }
		};
		Data<Documents.Server> data = new(servers);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Server);

		// act
		IStorageConnection connection = Storage.GetConnection();
		connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Server
		};
		Documents.Server result = Storage.Container.GetItemLinqQueryable<Documents.Server>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.Single();

		Assert.Equal("server2", result.Id);
	}

	[Fact]
	public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetAllItemsFromSet(null));

		//assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		HashSet<string> result = connection.GetAllItemsFromSet("some-set");

		//assert
		Assert.NotNull(result);
		Assert.Empty(result);
	}

	[Fact]
	public void GetAllItemsFromSet_ReturnsAllItems()
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
		HashSet<string> result = connection.GetAllItemsFromSet("key");

		//assert
		Assert.Equal(3, result.Count);
		Assert.Contains("1.0", result);
		Assert.Contains("-1.0", result);
		Assert.Contains("-5.0", result);
	}

	[Fact]
	public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.SetRangeInHash(null, new Dictionary<string, string>()));

		//assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.SetRangeInHash("some-hash", null));

		//assert
		Assert.Equal("keyValuePairs", exception.ParamName);
	}

	[Fact]
	public void SetRangeInHash_MergesAllRecords()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();
		connection.SetRangeInHash("some-hash", new Dictionary<string, string>
		{
			{ "Key1", "Value1" },
			{ "Key2", "Value2" }
		});

		// act
		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Hash
		};
		Dictionary<string, string?> result = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.ToDictionary(x => x.Field, x => x.Value);

		//assert
		Assert.Equal(2, result.Count);
		Assert.Equal("Value1", result["Key1"]);
		Assert.Equal("Value2", result["Key2"]);
	}

	[Fact]
	public void SetRangeInHash_CanCreateFieldsWithNullValues()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		connection.SetRangeInHash("some-hash", new Dictionary<string, string?>
		{
			{ "Key1", null },
			{ "Key2", "Value2" }
		});

		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Hash
		};
		Dictionary<string, string?> result = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: queryRequestOptions)
			.ToQueryResult()
			.ToDictionary(x => x.Field, x => x.Value);

		//assert
		Assert.Equal(2, result.Count);
		Assert.Null(result["Key1"]);
		Assert.Equal("Value2", result["Key2"]);
	}

	[Fact]
	public void SetRangeInHash_UpdatesExistingValue()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Hash> hashes = new()
		{
			new Hash { Key = "some-hash", Field = "key1", Value = "value1" },
			new Hash { Key = "some-hash", Field = "key2", Value = "value2" },
			new Hash { Key = "some-hash", Field = "key3", Value = "value3" },
			new Hash { Key = "some-other-hash", Field = "key1", Value = "value1" }
		};
		Data<Hash> data = new(hashes);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);

		// act
		IStorageConnection connection = Storage.GetConnection();
		connection.SetRangeInHash("some-hash", new Dictionary<string, string?>
		{
			{ "key1", "VALUE-1" }
		});

		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Hash
		};
		Dictionary<string, string?> result = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: queryRequestOptions)
			.Where(x => x.Key == "some-hash")
			.ToQueryResult()
			.ToDictionary(x => x.Field, x => x.Value);

		Dictionary<string, string?> result2 = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: queryRequestOptions)
			.Where(x => x.Key == "some-other-hash")
			.ToQueryResult()
			.ToDictionary(x => x.Field, x => x.Value);

		//assert
		Assert.Equal(3, result.Count);
		Assert.Equal("VALUE-1", result["key1"]);
		Assert.Equal("value2", result["key2"]);
		Assert.Equal("value3", result["key3"]);

		Assert.Single(result2);
		Assert.Equal("value1", result2["key1"]);
	}

	[Fact]
	public void SetRangeInHash_UpdatesExistingValueAndRemoveDuplicate_WhenAlreadyHasMultipleFieldForAKey()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Hash> hashes = new()
		{
			new Hash { Key = "some-hash", Field = "key1", Value = "value1" },
			new Hash { Key = "some-hash", Field = "key1", Value = "value2" },
			new Hash { Key = "some-hash", Field = "key3", Value = "value3" },
			new Hash { Key = "some-other-hash", Field = "key1", Value = "value1" }
		};
		Data<Hash> data = new(hashes);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);

		// act
		IStorageConnection connection = Storage.GetConnection();
		connection.SetRangeInHash("some-hash", new Dictionary<string, string?>
		{
			{ "key1", "VALUE-1" }
		});

		QueryRequestOptions queryRequestOptions = new()
		{
			PartitionKey = PartitionKeys.Hash
		};
		Dictionary<string, string?> result = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: queryRequestOptions)
			.Where(x => x.Key == "some-hash")
			.ToQueryResult()
			.ToDictionary(x => x.Field, x => x.Value);

		Dictionary<string, string?> result2 = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: queryRequestOptions)
			.Where(x => x.Key == "some-other-hash")
			.ToQueryResult()
			.ToDictionary(x => x.Field, x => x.Value);

		//assert
		Assert.Equal(2, result.Count);
		Assert.Equal("VALUE-1", result["key1"]);
		Assert.Equal("value3", result["key3"]);

		Assert.Single(result2);
		Assert.Equal("value1", result2["key1"]);
	}

	[Fact]
	public void SetRangeInHash_Handles_DistributedLock()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Hash> hashes = new()
		{
			new Hash { Key = "some-hash", Field = "key1", Value = "value1" }
		};
		Data<Hash> data = new(hashes);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);

		// act
		IStorageConnection connection = Storage.GetConnection();

		// create a lock and dispose after 5 seconds
		Task.Run(async () =>
		{
			CosmosDbDistributedLock distributedLock = new("locks:set:hash", TimeSpan.FromMinutes(2), Storage);
			await Task.Delay(5000);
			distributedLock.Dispose();
		});

		// lets set the range
		connection.SetRangeInHash("some-hash", new Dictionary<string, string?> { { "key1", "VALUE-1" } });

		//assert
		QueryRequestOptions queryRequestOptions = new() { PartitionKey = PartitionKeys.Hash };
		Dictionary<string, string?> result = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: queryRequestOptions)
			.Where(x => x.Key == "some-hash")
			.ToQueryResult()
			.ToDictionary(x => x.Field, x => x.Value);

		Assert.Single(result);
		Assert.Equal("VALUE-1", result["key1"]);
	}

	[Fact]
	public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetAllEntriesFromHash(null));

		//assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		IStorageConnection connection = Storage.GetConnection();

		// act
		Dictionary<string, string> result = connection.GetAllEntriesFromHash("some-hash");

		// assert
		Assert.Null(result);
	}

	[Fact]
	public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Hash> hashes = new()
		{
			new Hash { Key = "some-hash", Field = "key1", Value = "value1" },
			new Hash { Key = "some-hash", Field = "key2", Value = "value2" },
			new Hash { Key = "some-hash", Field = "key3", Value = "value3" },
			new Hash { Key = "some-other-hash", Field = "key1", Value = "value1" }
		};
		Data<Hash> data = new(hashes);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);

		// act
		IStorageConnection connection = Storage.GetConnection();
		Dictionary<string, string> result = connection.GetAllEntriesFromHash("some-hash");

		// assert
		Assert.NotNull(result);
		Assert.Equal(3, result.Count);
		Assert.Equal("value1", result["key1"]);
		Assert.Equal("value2", result["key2"]);
		Assert.Equal("value3", result["key3"]);
	}

	[Fact]
	public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetSetCount(null));

		// assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetSetCount_ReturnsZero_WhenSetDoesNotExist()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act
		long result = connection.GetSetCount("my-set");

		// assert
		Assert.Equal(0, result);
	}

	[Fact]
	public void GetSetCount_ReturnsNumberOfElements_InASet()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Set> sets = new()
		{
			new Set { Key = "set-1", Value = "value1", CreatedOn = DateTime.UtcNow },
			new Set { Key = "set-2", Value = "value2", CreatedOn = DateTime.UtcNow },
			new Set { Key = "set-3", Value = "value3", CreatedOn = DateTime.UtcNow },
			new Set { Key = "set-1", Value = "value1", CreatedOn = DateTime.UtcNow }
		};
		Data<Set> data = new(sets);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Set);

		// act
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		long result = connection.GetSetCount("set-1");

		// assert
		Assert.Equal(2, result);
	}

	[Fact]
	public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetRangeFromSet(null, 0, 1));

		// assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetRangeFromSet_ReturnsPagedElements()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Set> sets = new()
		{
			new Set { Key = "set-1", Value = "1", CreatedOn = DateTime.UtcNow },
			new Set { Key = "set-1", Value = "2", CreatedOn = DateTime.UtcNow },
			new Set { Key = "set-1", Value = "3", CreatedOn = DateTime.UtcNow },
			new Set { Key = "set-1", Value = "4", CreatedOn = DateTime.UtcNow },
			new Set { Key = "set-2", Value = "4", CreatedOn = DateTime.UtcNow },
			new Set { Key = "set-1", Value = "5", CreatedOn = DateTime.UtcNow }
		};
		Data<Set> data = new(sets);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Set);

		// act
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		List<string> result = connection.GetRangeFromSet("set-1", 2, 3);

		// assert
		Assert.Equal(new[] { "3", "4" }, result);
	}

	[Fact]
	public void GetCounter_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetCounter(null));

		// assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetCounter_ReturnsZero_WhenKeyDoesNotExist()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act
		long result = connection.GetCounter("my-counter");

		// assert
		Assert.Equal(0, result);
	}

	[Fact]
	public void GetCounter_ReturnsSumOfValues_InRawAndAggregate()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Counter> counters = new()
		{
			new Counter { Key = "counter-1", Value = 1, Type = CounterTypes.Raw },
			new Counter { Key = "counter-2", Value = 1, Type = CounterTypes.Raw },
			new Counter { Key = "counter-1", Value = 1, Type = CounterTypes.Aggregate },
			new Counter { Key = "counter-1", Value = 1, Type = CounterTypes.Aggregate },
			new Counter { Key = "counter-2", Value = 1, Type = CounterTypes.Raw },
			new Counter { Key = "counter-1", Value = 1, Type = CounterTypes.Raw },
		};
		Data<Counter> data = new(counters);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Counter);

		// act
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		long result = connection.GetCounter("counter-1");

		// assert
		Assert.Equal(4, result);
	}

	[Fact]
	public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetHashCount(null));

		// assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetHashCount_ReturnsZero_WhenKeyDoesNotExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		long result = connection.GetHashCount("my-hash");

		// assert
		Assert.Equal(0, result);
	}

	[Fact]
	public void GetHashCount_ReturnsNumber_OfHashFields()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Hash> hashes = new()
		{
			new Hash { Key = "some-hash", Field = "key1", Value = "value1" },
			new Hash { Key = "some-hash", Field = "key2", Value = "value2" },
			new Hash { Key = "some-hash", Field = "key3", Value = "value3" },
			new Hash { Key = "some-other-hash", Field = "key1", Value = "value1" }
		};
		Data<Hash> data = new(hashes);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);

		// Act
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		long result = connection.GetHashCount("some-hash");

		// Assert
		Assert.Equal(3, result);
	}

	[Fact]
	public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetHashTtl(null));

		// assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetHashTtl_ReturnsNegativeValue_WhenHashDoesNotExist()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		TimeSpan result = connection.GetHashTtl("my-hash");

		// assert
		Assert.True(result < TimeSpan.Zero);
	}

	[Fact]
	public void GetHashTtl_ReturnsExpirationTimeForHash()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Hash> hashes = new()
		{
			new Hash { Key = "some-hash", Field = "key1", Value = "value1", ExpireOn = DateTime.UtcNow.AddHours(1) },
			new Hash { Key = "some-other-hash", Field = "key1", Value = "value1" }
		};
		Data<Hash> data = new(hashes);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);

		// Act
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		TimeSpan result = connection.GetHashTtl("some-hash");

		// Assert
		Assert.True(TimeSpan.FromMinutes(59) < result);
		Assert.True(result < TimeSpan.FromMinutes(61));
	}

	[Fact]
	public void GetListCount_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetListCount(null));

		// assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetListCount_ReturnsZero_WhenListDoesNotExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		long result = connection.GetListCount("my-list");

		// assert
		Assert.Equal(0, result);
	}

	[Fact]
	public void GetListCount_ReturnsTheNumberOfListElements()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<List> lists = new()
		{
			new List { Key = "list-1", CreatedOn = DateTime.UtcNow },
			new List { Key = "list-1", CreatedOn = DateTime.UtcNow }
		};
		Data<List> data = new(lists);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.List);

		// Act
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		long result = connection.GetListCount("list-1");

		// assert
		Assert.Equal(2, result);
	}

	[Fact]
	public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetListTtl(null));

		// assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetListTtl_ReturnsNegativeValue_WhenListDoesNotExist()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		TimeSpan result = connection.GetListTtl("my-list");

		// assert
		Assert.True(result < TimeSpan.Zero);
	}

	[Fact]
	public void GetListTtl_ReturnsExpirationTimeForList()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<List> lists = new()
		{
			new List { Key = "list-1", CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddHours(1) },
			new List { Key = "list-1", CreatedOn = DateTime.UtcNow }
		};
		Data<List> data = new(lists);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.List);

		// Act
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		TimeSpan result = connection.GetListTtl("list-1");

		// Assert
		Assert.True(TimeSpan.FromMinutes(59) < result);
		Assert.True(result < TimeSpan.FromMinutes(61));
	}

	[Fact]
	public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetValueFromHash(null, "name"));

		// assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetValueFromHash("key", null));

		// assert
		Assert.Equal("name", exception.ParamName);
	}

	[Fact]
	public void GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		string? result = connection.GetValueFromHash("my-hash", "name");

		// assert
		Assert.Null(result);
	}

	[Fact]
	public void GetValueFromHash_ReturnsValue_OfAGivenField()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Hash> hashes = new()
		{
			new Hash { Key = "some-hash", Field = "key1", Value = "value1" },
			new Hash { Key = "some-hash", Field = "key2", Value = "value2" },
			new Hash { Key = "some-other-hash", Field = "key1", Value = "value1" }
		};
		Data<Hash> data = new(hashes);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);

		// act
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		string? result = connection.GetValueFromHash("some-hash", "key1");

		// Assert
		Assert.Equal("value1", result);
	}

	[Fact]
	public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetRangeFromList(null, 0, 1));

		// assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetRangeFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		List<string> result = connection.GetRangeFromList("my-list", 0, 1);

		// assert
		Assert.Empty(result);
	}

	[Fact]
	public void GetRangeFromList_ReturnsAllEntries_WithinGivenBounds()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<List> lists = new()
		{
			new List { Key = "list-1", Value = "1", CreatedOn = DateTime.UtcNow.AddSeconds(1) },
			new List { Key = "list-1", Value = "2", CreatedOn = DateTime.UtcNow.AddSeconds(2) },
			new List { Key = "list-1", Value = "3", CreatedOn = DateTime.UtcNow.AddSeconds(3) },
			new List { Key = "list-1", Value = "4", CreatedOn = DateTime.UtcNow.AddSeconds(4) },
			new List { Key = "list-1", Value = "5", CreatedOn = DateTime.UtcNow.AddSeconds(5) }
		};
		Data<List> data = new(lists);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.List);

		// Act
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		List<string> result = connection.GetRangeFromList("list-1", 1, 2);

		//assert
		Assert.Equal(new[] { "4", "3" }, result);
	}

	[Fact]
	public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetAllItemsFromList(null));

		// assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetAllItemsFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		List<string> result = connection.GetAllItemsFromList("my-list");

		// assert
		Assert.Empty(result);
	}

	[Fact]
	public void GetAllItemsFromList_ReturnsAllItems_FromAGivenList()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<List> lists = new()
		{
			new List { Key = "list-1", Value = "1", CreatedOn = DateTime.UtcNow.AddSeconds(10) },
			new List { Key = "list-1", Value = "2", CreatedOn = DateTime.UtcNow.AddSeconds(2) },
			new List { Key = "list-1", Value = "3", CreatedOn = DateTime.UtcNow.AddSeconds(3) },
			new List { Key = "list-2", Value = "4", CreatedOn = DateTime.UtcNow.AddSeconds(4) },
			new List { Key = "list-1", Value = "5", CreatedOn = DateTime.UtcNow.AddSeconds(5) }
		};
		Data<List> data = new(lists);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.List);

		// Act
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		var result = connection.GetAllItemsFromList("list-1");

		// assert
		Assert.Equal(new[] { "1", "5", "3", "2" }, result);
	}

	[Fact]
	public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.GetSetTtl(null));

		// assert
		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void GetSetTtl_ReturnsNegativeValue_WhenSetDoesNotExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		TimeSpan result = connection.GetSetTtl("my-set");

		// assert
		Assert.True(result < TimeSpan.Zero);
	}

	[Fact]
	public void GetSetTtl_ReturnsExpirationTime_OfAGivenSet()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		List<Set> sets = new()
		{
			new Set { Key = "set-1", Value = "value1", CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddHours(1) },
			new Set { Key = "set-2", Value = "value2", CreatedOn = DateTime.UtcNow }
		};
		Data<Set> data = new(sets);
		Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Set);

		// act
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();
		TimeSpan result = connection.GetSetTtl("set-1");

		// assert
		Assert.True(TimeSpan.FromMinutes(59) < result);
		Assert.True(result < TimeSpan.FromMinutes(61));
	}

	[Theory]
	[InlineData("default", "c8732989-96e1-40e5-af16-dcf4d79cb4d6")]
	[InlineData("high", "4c5f2c17-54ad-4e9e-ae38-c9ff750febd9")]
	public void FetchNextJob_DelegatesItsExecution_ToTheQueue(string queue, string jobId)
	{
		// clean all the documents for the container
		ContainerFixture.Clean();

		//arrange 
		JobQueue jobQueue = new(Storage);
		jobQueue.Enqueue("high", queue.Equals("high") ? jobId : Guid.NewGuid().ToString());
		jobQueue.Enqueue("default", queue.Equals("default") ? jobId : Guid.NewGuid().ToString());

		//act
		IStorageConnection connection = Storage.GetConnection();
		FetchedJob job = (FetchedJob)connection.FetchNextJob(new[] { queue }, CancellationToken.None);

		//assert
		Assert.Equal(jobId, job.JobId);
		Assert.Equal(queue, job.Queue);
	}

	[Fact]
	public void FetchNextJob_ThrowsAnException_WhenQueueIsNull()
	{
		//act
		IStorageConnection connection = Storage.GetConnection();
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => (FetchedJob)connection.FetchNextJob(null, CancellationToken.None));

		//assert
		Assert.Equal("queues", exception.ParamName);
	}

	[Fact]
	public void FetchNextJob_ThrowsAnException_WhenQueueIsEmpty()
	{
		//act
		IStorageConnection connection = Storage.GetConnection();
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => (FetchedJob)connection.FetchNextJob(Array.Empty<string>(), CancellationToken.None));

		//assert
		Assert.Equal("queues", exception.ParamName);
	}

	[Fact]
	public void AcquireDistributedLock_ThrowsAnException_WhenResourceIsNullOrEmpty()
	{
		// arrange
		JobStorageConnection connection = (JobStorageConnection)Storage.GetConnection();

		// act 
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => connection.AcquireDistributedLock(null, TimeSpan.Zero));

		// assert
		Assert.Equal("resource", exception.ParamName);
	}

	[Fact]
	public void AcquireLock_ReturnsNonNullInstance()
	{
		//clean
		ContainerFixture.Clean();

		//arrange 
		IStorageConnection connection = Storage.GetConnection();

		// act & assert
		using IDisposable @lock = connection.AcquireDistributedLock("dequeue:lock", TimeSpan.Zero);
		Assert.NotNull(@lock);
		@lock.Dispose();
	}


#pragma warning disable xUnit1013
	// ReSharper disable once MemberCanBePrivate.Global
	// ReSharper disable once UnusedParameter.Global
	public static void SampleMethod(string arg) { }
#pragma warning restore xUnit1013
}