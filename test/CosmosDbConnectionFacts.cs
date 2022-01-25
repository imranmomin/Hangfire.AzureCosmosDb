using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Tests.Fixtures;
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
		DateTime createdAt = new(2012, 12, 12);
		IStorageConnection connection = Storage.GetConnection();
		Dictionary<string, string> parameters = new()
			{ { "Key1", "Value1" }, { "Key2", "Value2" }, { "Key3", "Value3" } };

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
		DateTime createdAt = new(2012, 12, 12);
		IStorageConnection connection = Storage.GetConnection();
		Dictionary<string, string?> parameters = new()
			{ { "Key1", null }, { "Key2", null }, { "Key3", null } };

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
		DateTime createdAt = new(2012, 12, 12);
		IStorageConnection connection = Storage.GetConnection();
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
		DateTime createdAt = new(2012, 12, 12);
		Dictionary<string, string?> parameters = new()
			{ { "Key1", null }, { "Key2", null }, { "Key3", null } };
		Job job = Job.FromExpression(() => SampleMethod("wrong"));
		InvocationData invocationData = InvocationData.SerializeJob(job);
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
		DateTime createdAt = new(2012, 12, 12);
		Dictionary<string, string?> parameters = new()
			{ { "Key1", null }, { "Key2", null }, { "Key3", null } };
		Job job = Job.FromExpression(() => SampleMethod("wrong"));
		InvocationData invocationData = InvocationData.SerializeJob(job);
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
			{ { "Key", "Value" } };
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

#pragma warning disable xUnit1013
	// ReSharper disable once MemberCanBePrivate.Global
	// ReSharper disable once UnusedParameter.Global
	public static void SampleMethod(string arg) { }
#pragma warning restore xUnit1013
}