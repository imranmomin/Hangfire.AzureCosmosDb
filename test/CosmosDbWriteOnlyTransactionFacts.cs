using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Azure.Tests.Fixtures;
using Hangfire.States;
using Hangfire.Storage;
using Microsoft.Azure.Cosmos;
using Moq;
using Xunit;
using Xunit.Abstractions;
using Job = Hangfire.Common.Job;

namespace Hangfire.Azure.Tests;

public class CosmosDbWriteOnlyTransactionFacts : IClassFixture<ContainerFixture>
{
	private ContainerFixture ContainerFixture { get; }

	private CosmosDbStorage Storage { get; }

	public CosmosDbWriteOnlyTransactionFacts(ContainerFixture containerFixture, ITestOutputHelper testOutputHelper)
	{
		ContainerFixture = containerFixture;
		Storage = containerFixture.Storage;

		ContainerFixture.SetupLogger(testOutputHelper);
	}

	[Fact]
	public void Ctor_ThrowsAnException_IfConnectionIsNull()
	{
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new CosmosDbWriteOnlyTransaction(null!));
		Assert.Equal("connection", exception.ParamName);
	}

	[Fact]
	public void ExpireJob_ThrowsAnException_WhenJobIdIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			transaction.ExpireJob(null!, TimeSpan.Zero);
			transaction.Commit();
		});

		//assert
		Assert.Equal("jobId", exception.ParamName);
	}

	[Fact]
	public void ExpireJob_ThrowsAnException_WhenExpireInIsNegate()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentException exception = Assert.Throws<ArgumentException>(() =>
		{
			string id = Guid.NewGuid().ToString();
			TimeSpan expireIn = TimeSpan.FromSeconds(1).Negate();
			transaction.ExpireJob(id, expireIn);
			transaction.Commit();
		});

		//assert
		Assert.Equal("expireIn", exception.ParamName);
	}

	[Fact]
	public void ExpireJob_SetsJobExpirationData()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);
		Job? job = Job.FromExpression(() => SampleMethod("wrong"));
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

		// job 1
		Documents.Job sqlJob1 = Storage.Container.CreateItemWithRetries(entityJob, PartitionKeys.Job);

		// job 2
		entityJob.Id = Guid.NewGuid().ToString();
		Documents.Job sqlJob2 = Storage.Container.CreateItemWithRetries(entityJob, PartitionKeys.Job);

		// act
		transaction.ExpireJob(sqlJob1.Id, TimeSpan.FromHours(24));
		transaction.Commit();

		//assert
		sqlJob1 = Storage.Container.ReadItemWithRetries<Documents.Job>(sqlJob1.Id, PartitionKeys.Job);
		Assert.NotNull(sqlJob1.ExpireOn);
		Assert.True(DateTime.UtcNow.AddHours(23) < sqlJob1.ExpireOn && sqlJob1.ExpireOn < DateTime.UtcNow.AddHours(25));

		sqlJob2 = Storage.Container.ReadItemWithRetries<Documents.Job>(sqlJob2.Id, PartitionKeys.Job);
		Assert.Null(sqlJob2.ExpireOn);
	}

	[Fact]
	public void PersistJob_ThrowsAnException_WhenJobIdIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			transaction.PersistJob(null!);
			transaction.Commit();
		});

		//assert
		Assert.Equal("jobId", exception.ParamName);
	}

	[Fact]
	public void PersistJob_ClearsTheJobExpirationData()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);
		Job? job = Job.FromExpression(() => SampleMethod("wrong"));
		InvocationData invocationData = InvocationData.SerializeJob(job);
		DateTime createdAt = new(2012, 12, 12);
		Dictionary<string, string?> parameters = new()
		{
			{ "Key1", null },
			{ "Key2", null },
			{ "Key3", null }
		};

		DateTime expireOn = DateTime.UtcNow;
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
			StateName = SucceededState.StateName,
			ExpireOn = expireOn
		};

		// job 1
		Documents.Job sqlJob1 = Storage.Container.CreateItemWithRetries(entityJob, PartitionKeys.Job);

		// job 2
		entityJob.Id = Guid.NewGuid().ToString();
		Documents.Job sqlJob2 = Storage.Container.CreateItemWithRetries(entityJob, PartitionKeys.Job);

		// act
		transaction.PersistJob(sqlJob1.Id);
		transaction.Commit();

		// assert
		sqlJob1 = Storage.Container.ReadItemWithRetries<Documents.Job>(sqlJob1.Id, PartitionKeys.Job);
		Assert.Null(sqlJob1.ExpireOn);

		sqlJob2 = Storage.Container.ReadItemWithRetries<Documents.Job>(sqlJob2.Id, PartitionKeys.Job);
		Assert.NotNull(sqlJob2.ExpireOn);
		Assert.Equal(sqlJob2.ExpireOn!.Value.ToEpoch(), expireOn.ToEpoch());
	}

	[Fact]
	public void SetJobState_ThrowsAnException_WhenJobIdIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			IState? state = new Mock<IState>().Object;
			transaction.SetJobState(null!, state);
			transaction.Commit();
		});

		Assert.Equal("jobId", exception.ParamName);
	}

	[Fact]
	public void SetJobState_ThrowsAnException_WhenStateIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			string id = Guid.NewGuid().ToString();
			transaction.SetJobState(id, null!);
			transaction.Commit();
		});

		Assert.Equal("state", exception.ParamName);
	}

	[Fact]
	public void SetJobState_AppendsAStateAndSetItToTheJob()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);
		Job? job = Job.FromExpression(() => SampleMethod("wrong"));
		InvocationData invocationData = InvocationData.SerializeJob(job);
		DateTime createdAt = new(2012, 12, 12);
		Dictionary<string, string?> parameters = new()
		{
			{ "Key1", null },
			{ "Key2", null },
			{ "Key3", null }
		};

		DateTime expireOn = DateTime.UtcNow;
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
			ExpireOn = expireOn
		};

		// job 1
		Documents.Job sqlJob1 = Storage.Container.CreateItemWithRetries(entityJob, PartitionKeys.Job);

		// job 2
		entityJob.Id = Guid.NewGuid().ToString();
		Documents.Job sqlJob2 = Storage.Container.CreateItemWithRetries(entityJob, PartitionKeys.Job);

		Mock<IState> state = new();
		state.Setup(x => x.Name).Returns("State");
		state.Setup(x => x.Reason).Returns("Reason");
		state.Setup(x => x.SerializeData()).Returns(new Dictionary<string, string> { { "Name", "Value" } });

		// act
		transaction.SetJobState(sqlJob1.Id, state.Object);
		transaction.Commit();

		// assert
		sqlJob1 = Storage.Container.ReadItemWithRetries<Documents.Job>(sqlJob1.Id, PartitionKeys.Job);
		Assert.Equal("State", sqlJob1.StateName);
		Assert.NotNull(sqlJob1.StateId);

		sqlJob2 = Storage.Container.ReadItemWithRetries<Documents.Job>(sqlJob2.Id, PartitionKeys.Job);
		Assert.Null(sqlJob2.StateName);
		Assert.Null(sqlJob2.StateId);

		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.State };
		State? jobState = Storage.Container.GetItemLinqQueryable<State>(requestOptions: requestOptions)
			.ToQueryResult()
			.SingleOrDefault();

		Assert.NotNull(jobState);
		Assert.Equal(sqlJob1.Id, jobState!.JobId);
		Assert.Equal(sqlJob1.StateName, jobState.Name);
		Assert.Equal("State", jobState.Name);
		Assert.Equal("Reason", jobState.Reason);
		Assert.Single(jobState.Data);
		Assert.Equal("Value", jobState.Data["Name"]);
	}

	[Fact]
	public void SetJobState_CanBeCalledWithNullReasonAndData()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);
		Job? job = Job.FromExpression(() => SampleMethod("wrong"));
		InvocationData invocationData = InvocationData.SerializeJob(job);
		DateTime createdAt = new(2012, 12, 12);
		Dictionary<string, string?> parameters = new()
		{
			{ "Key1", null },
			{ "Key2", null },
			{ "Key3", null }
		};

		DateTime expireOn = DateTime.UtcNow;
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
			ExpireOn = expireOn
		};

		// job 1
		Documents.Job sqlJob1 = Storage.Container.CreateItemWithRetries(entityJob, PartitionKeys.Job);

		Mock<IState> state = new();
		state.Setup(x => x.Name).Returns("State");
		state.Setup(x => x.Reason).Returns((string)null!);
		state.Setup(x => x.SerializeData()).Returns(((Dictionary<string, string>)null!)!);

		transaction.SetJobState(sqlJob1.Id, state.Object);
		transaction.Commit();

		// assert
		sqlJob1 = Storage.Container.ReadItemWithRetries<Documents.Job>(sqlJob1.Id, PartitionKeys.Job);
		Assert.Equal("State", sqlJob1.StateName);
		Assert.NotNull(sqlJob1.StateId);

		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.State };
		State? jobState = Storage.Container.GetItemLinqQueryable<State>(requestOptions: requestOptions)
			.ToQueryResult()
			.SingleOrDefault();

		Assert.NotNull(jobState);
		Assert.Equal(sqlJob1.Id, jobState!.JobId);
		Assert.Equal(sqlJob1.StateName, jobState.Name);
		Assert.Equal("State", jobState.Name);
		Assert.Null(jobState.Reason);
		Assert.Null(jobState.Data);
	}

	[Fact]
	public void AddJobState_ThrowsAnException_WhenJobIdIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			IState? state = new Mock<IState>().Object;
			transaction.AddJobState(null!, state);
			transaction.Commit();
		});

		// assert
		Assert.Equal("jobId", exception.ParamName);
	}

	[Fact]
	public void AddJobState_ThrowsAnException_WhenJStateIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			string id = Guid.NewGuid().ToString();
			transaction.AddJobState(id, null!);
			transaction.Commit();
		});

		// assert
		Assert.Equal("state", exception.ParamName);
	}

	[Fact]
	public void AddJobState_JustAddsANewRecordInATable()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);
		Job? job = Job.FromExpression(() => SampleMethod("wrong"));
		InvocationData invocationData = InvocationData.SerializeJob(job);
		DateTime createdAt = new(2012, 12, 12);
		Dictionary<string, string?> parameters = new()
		{
			{ "Key1", null },
			{ "Key2", null },
			{ "Key3", null }
		};

		DateTime expireOn = DateTime.UtcNow;
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
			ExpireOn = expireOn
		};

		// job 1
		Documents.Job sqlJob1 = Storage.Container.CreateItemWithRetries(entityJob, PartitionKeys.Job);

		Mock<IState> state = new();
		state.Setup(x => x.Name).Returns("State");
		state.Setup(x => x.Reason).Returns("Reason");
		state.Setup(x => x.SerializeData())
			.Returns(new Dictionary<string, string> { { "Name", "Value" } });

		transaction.AddJobState(sqlJob1.Id, state.Object);
		transaction.Commit();

		// assert
		sqlJob1 = Storage.Container.ReadItemWithRetries<Documents.Job>(sqlJob1.Id, PartitionKeys.Job);
		Assert.Null(sqlJob1.StateName);
		Assert.Null(sqlJob1.StateId);

		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.State };
		State? jobState = Storage.Container.GetItemLinqQueryable<State>(requestOptions: requestOptions)
			.ToQueryResult()
			.SingleOrDefault();

		Assert.NotNull(jobState);
		Assert.Equal(sqlJob1.Id, jobState!.JobId);
		Assert.Equal("State", jobState.Name);
		Assert.Equal("Reason", jobState.Reason);
		Assert.Single(jobState.Data);
		Assert.Equal("Value", jobState.Data["Name"]);
	}

	[Fact]
	public void AddJobState_CanBeCalledWithNullReasonAndData()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);
		Job? job = Job.FromExpression(() => SampleMethod("wrong"));
		InvocationData invocationData = InvocationData.SerializeJob(job);
		DateTime createdAt = new(2012, 12, 12);
		Dictionary<string, string?> parameters = new()
		{
			{ "Key1", null },
			{ "Key2", null },
			{ "Key3", null }
		};

		DateTime expireOn = DateTime.UtcNow;
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
			ExpireOn = expireOn
		};

		// job 1
		Documents.Job sqlJob1 = Storage.Container.CreateItemWithRetries(entityJob, PartitionKeys.Job);

		Mock<IState> state = new();
		state.Setup(x => x.Name).Returns("State");
		state.Setup(x => x.Reason).Returns((string)null!);
		state.Setup(x => x.SerializeData()).Returns(((Dictionary<string, string>)null!)!);

		transaction.AddJobState(sqlJob1.Id, state.Object);
		transaction.Commit();

		// assert
		sqlJob1 = Storage.Container.ReadItemWithRetries<Documents.Job>(sqlJob1.Id, PartitionKeys.Job);
		Assert.Null(sqlJob1.StateName);
		Assert.Null(sqlJob1.StateId);

		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.State };
		State? jobState = Storage.Container.GetItemLinqQueryable<State>(requestOptions: requestOptions)
			.ToQueryResult()
			.SingleOrDefault();

		Assert.NotNull(jobState);
		Assert.Equal(sqlJob1.Id, jobState!.JobId);
		Assert.Equal("State", jobState.Name);
		Assert.Null(jobState.Reason);
		Assert.Null(jobState.Data);
	}

	[Fact]
	public void AddToQueue_ThrowsAnException_WhenQueueIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			transaction.AddToQueue(null!, "my-job");
			transaction.Commit();
		});

		// assert
		Assert.Equal("queue", exception.ParamName);
	}

	[Fact]
	public void AddToQueue_ThrowsAnException_WhenJobIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			transaction.AddToQueue("default", null!);
			transaction.Commit();
		});

		// assert
		Assert.Equal("jobId", exception.ParamName);
	}

	[Fact]
	public void AddToQueue_CallsEnqueue_OnTargetPersistentQueue()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		//act
		string jobId = Guid.NewGuid().ToString();
		transaction.AddToQueue("default", jobId);
		transaction.Commit();

		// assert
		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Queue };
		Documents.Queue? queue = Storage.Container.GetItemLinqQueryable<Documents.Queue>(requestOptions: requestOptions)
			.ToQueryResult()
			.SingleOrDefault();

		Assert.NotNull(queue);
		Assert.Equal("default", queue!.Name);
		Assert.Equal(jobId, queue.JobId);
		Assert.Null(queue.FetchedAt);
	}

	[Fact]
	public void IncrementCounter_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			transaction.IncrementCounter(null!);
			transaction.Commit();
		});

		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void IncrementCounter_AddsRecordToCounterTable_WithPositiveValue()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		transaction.IncrementCounter("my-key");
		transaction.Commit();

		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Counter };
		Counter? record = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: requestOptions)
			.ToQueryResult()
			.SingleOrDefault();

		Assert.NotNull(record);
		Assert.Equal("my-key", record!.Key);
		Assert.Equal(1, record.Value);
		Assert.Null(record.ExpireOn);
	}

	[Fact]
	public void IncrementCounter_WithExpiry_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			transaction.IncrementCounter(null!, TimeSpan.FromHours(1));
			transaction.Commit();
		});

		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void IncrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		transaction.IncrementCounter("my-key", TimeSpan.FromDays(1));
		transaction.Commit();

		//assert
		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Counter };
		Counter? record = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: requestOptions)
			.ToQueryResult()
			.SingleOrDefault();

		Assert.NotNull(record);
		Assert.Equal("my-key", record!.Key);
		Assert.Equal(1, record.Value);
		Assert.NotNull(record.ExpireOn);
		Assert.True(DateTime.UtcNow.AddHours(23) < record.ExpireOn!.Value);
		Assert.True(record.ExpireOn.Value < DateTime.UtcNow.AddHours(25));
	}

	[Fact]
	public void IncrementCounter_WithExistingKey_AddsAnotherRecord()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		transaction.IncrementCounter("my-key");
		transaction.IncrementCounter("my-key");
		transaction.Commit();

		//assert
		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Counter };
		var record = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: requestOptions)
			.ToQueryResult()
			.GroupBy(x => x.Key)
			.Select(x => new
			{
				x.Key,
				Value = x.Sum(z => z.Value),
				ExpireOn = x.Max(z => z.ExpireOn)
			})
			.SingleOrDefault();

		Assert.NotNull(record);
		Assert.Equal("my-key", record!.Key);
		Assert.Equal(2, record.Value);
		Assert.Null(record.ExpireOn);
	}

	[Fact]
	public void DecrementCounter_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			transaction.DecrementCounter(null!);
			transaction.Commit();
		});

		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void DecrementCounter_AddsRecordToCounterTable_WithNegativeValue()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		transaction.DecrementCounter("my-key");
		transaction.Commit();

		//assert
		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Counter };
		Counter? record = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: requestOptions)
			.ToQueryResult()
			.SingleOrDefault();

		Assert.NotNull(record);
		Assert.Equal("my-key", record!.Key);
		Assert.Equal(-1, record.Value);
		Assert.Null(record.ExpireOn);
	}

	[Fact]
	public void DecrementCounter_WithExpiry_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			transaction.DecrementCounter(null!, TimeSpan.FromHours(1));
			transaction.Commit();
		});

		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void DecrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		transaction.DecrementCounter("my-key", TimeSpan.FromDays(1));
		transaction.Commit();

		//assert
		Counter? record = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: new QueryRequestOptions() { PartitionKey = PartitionKeys.Counter })
			.ToQueryResult()
			.SingleOrDefault();

		Assert.NotNull(record);
		Assert.Equal("my-key", record!.Key);
		Assert.Equal(-1, record.Value);
		Assert.NotNull(record.ExpireOn);
		Assert.True(DateTime.UtcNow.AddHours(23) < record.ExpireOn!.Value);
		Assert.True(record.ExpireOn.Value < DateTime.UtcNow.AddHours(25));
	}

	[Fact]
	public void DecrementCounter_WithExistingKey_AddsAnotherRecord()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		transaction.DecrementCounter("my-key");
		transaction.DecrementCounter("my-key");
		transaction.Commit();

		//assert
		var record = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: new QueryRequestOptions() { PartitionKey = PartitionKeys.Counter })
			.ToQueryResult()
			.GroupBy(x => x.Key)
			.Select(x => new
			{
				x.Key,
				Value = x.Sum(z => z.Value),
				ExpireOn = x.Max(z => z.ExpireOn)
			})
			.SingleOrDefault();

		Assert.NotNull(record);
		Assert.Equal("my-key", record!.Key);
		Assert.Equal(-2, record.Value);
		Assert.Null(record.ExpireOn);
	}

	[Fact]
	public void AddToSet_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			transaction.AddToSet(null!, "value");
			transaction.Commit();
		});

		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void AddToSet_ThrowsAnException_WhenValueIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			transaction.AddToSet("my-set", null!);
			transaction.Commit();
		});

		Assert.Equal("value", exception.ParamName);
	}

	[Fact]
	public void AddToSet_AddsARecord_IfThereIsNo_SuchKeyAndValue()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		transaction.AddToSet("my-key", "my-value");
		transaction.Commit();

		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
		Set? record = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
			.ToQueryResult()
			.SingleOrDefault();

		Assert.NotNull(record);
		Assert.Equal("my-key", record!.Key);
		Assert.Equal("my-value", record.Value);
		Assert.Equal(0.0, record.Score, 2);
	}

	[Fact]
	public void AddToSet_AddsARecord_WhenKeyIsExists_ButValuesAreDifferent()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		transaction.AddToSet("my-key", "my-value");
		transaction.AddToSet("my-key", "another-value");
		transaction.Commit();

		//assert
		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
		List<Set> records = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
			.ToQueryResult()
			.ToList();

		Assert.Equal(2, records.Count);
	}

	[Fact]
	public void AddToSet_DoesNotAddARecord_WhenBothKeyAndValueAreExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		transaction.AddToSet("my-key", "my-value");
		transaction.AddToSet("my-key", "my-value");
		transaction.Commit();

		//assert
		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
		List<Set> records = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
			.ToQueryResult()
			.ToList();

		Assert.Single(records);
	}

	[Fact]
	public void AddToSet_WithScore_ThrowsAnException_WhenKeyIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			transaction.AddToSet(null!, "value", 1.2D);
			transaction.Commit();
		});

		Assert.Equal("key", exception.ParamName);
	}

	[Fact]
	public void AddToSet_WithScore_ThrowsAnException_WhenValueIsNull()
	{
		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
		{
			transaction.AddToSet("my-set", null!, 1.2D);
			transaction.Commit();
		});

		Assert.Equal("value", exception.ParamName);
	}

	[Fact]
	public void AddToSet_WithScore_AddsARecordWithScore_WhenBothKeyAndValueAreNotExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		transaction.AddToSet("my-key", "my-value", 3.2);
		transaction.Commit();

		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
		Set? record = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
			.ToQueryResult()
			.SingleOrDefault();

		Assert.NotNull(record);
		Assert.Equal("my-key", record!.Key);
		Assert.Equal("my-value", record.Value);
		Assert.Equal(3.2, record.Score, 3);
	}

	[Fact]
	public void AddToSet_WithScore_UpdatesAScore_WhenBothKeyAndValueAreExist()
	{
		// clean
		ContainerFixture.Clean();

		// arrange
		CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
		CosmosDbWriteOnlyTransaction transaction = new(connection);

		// act
		transaction.AddToSet("my-key", "my-value");
		transaction.AddToSet("my-key", "my-value", 3.2);
		transaction.Commit();

		QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
		Set? record = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
			.ToQueryResult()
			.SingleOrDefault();

		Assert.NotNull(record);
		Assert.Equal("my-key", record!.Key);
		Assert.Equal("my-value", record.Value);
		Assert.Equal(3.2, record.Score, 3);
	}


#pragma warning disable xUnit1013
// ReSharper disable once MemberCanBePrivate.Global
// ReSharper disable once UnusedParameter.Global
	public static void SampleMethod(string arg) { }
#pragma warning restore xUnit1013
}