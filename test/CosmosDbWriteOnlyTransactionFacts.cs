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
    public CosmosDbWriteOnlyTransactionFacts(ContainerFixture containerFixture, ITestOutputHelper testOutputHelper)
    {
        ContainerFixture = containerFixture;
        Storage = containerFixture.Storage;

        ContainerFixture.SetupLogger(testOutputHelper);
    }

    private ContainerFixture ContainerFixture { get; }

    private CosmosDbStorage Storage { get; }

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
        Dictionary<string, string?> parameters = new() { { "Key1", null }, { "Key2", null }, { "Key3", null } };

        Documents.Job entityJob = new()
        {
            InvocationData = invocationData,
            Arguments = string.Empty,
            CreatedOn = createdAt,
            Parameters = parameters.Select(p => new Parameter { Name = p.Key, Value = p.Value }).ToArray(),
            StateId = Guid.NewGuid().ToString(),
            StateName = SucceededState.StateName
        };

        // job 1
        Documents.Job sqlJob1 = Storage.Container.CreateItemWithRetries(entityJob, PartitionKeys.Job);
        foreach (int index in Enumerable.Range(0, 5))
        {
            Mock<IState> state = new();
            state.Setup(x => x.Name).Returns($"State-{index}");
            state.Setup(x => x.Reason).Returns((string)null!);
            state.Setup(x => x.SerializeData()).Returns(((Dictionary<string, string>)null!)!);
            transaction.SetJobState(sqlJob1.Id, state.Object);
        }

        transaction.Commit();

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

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.State };
        List<State> jobStates = Storage.Container.GetItemLinqQueryable<State>(requestOptions: requestOptions)
            .Where(x => x.JobId == sqlJob1.Id)
            .ToQueryResult()
            .ToList();

        Assert.Equal(5, jobStates.Count);
        Assert.NotNull(jobStates[0].ExpireOn);
        Assert.True(DateTime.UtcNow.AddHours(23) < jobStates[0].ExpireOn && jobStates[0].ExpireOn < DateTime.UtcNow.AddHours(25));
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
        Dictionary<string, string?> parameters = new() { { "Key1", null }, { "Key2", null }, { "Key3", null } };

        DateTime expireOn = DateTime.UtcNow;
        Documents.Job entityJob = new()
        {
            InvocationData = invocationData,
            Arguments = string.Empty,
            CreatedOn = createdAt,
            Parameters = parameters.Select(p => new Parameter { Name = p.Key, Value = p.Value }).ToArray(),
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
        Dictionary<string, string?> parameters = new() { { "Key1", null }, { "Key2", null }, { "Key3", null } };

        DateTime expireOn = DateTime.UtcNow;
        Documents.Job entityJob = new()
        {
            InvocationData = invocationData,
            Arguments = string.Empty,
            CreatedOn = createdAt,
            Parameters = parameters.Select(p => new Parameter { Name = p.Key, Value = p.Value }).ToArray(),
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
        Dictionary<string, string?> parameters = new() { { "Key1", null }, { "Key2", null }, { "Key3", null } };

        DateTime expireOn = DateTime.UtcNow;
        Documents.Job entityJob = new()
        {
            InvocationData = invocationData,
            Arguments = string.Empty,
            CreatedOn = createdAt,
            Parameters = parameters.Select(p => new Parameter { Name = p.Key, Value = p.Value }).ToArray(),
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
        Dictionary<string, string?> parameters = new() { { "Key1", null }, { "Key2", null }, { "Key3", null } };

        DateTime expireOn = DateTime.UtcNow;
        Documents.Job entityJob = new()
        {
            InvocationData = invocationData,
            Arguments = string.Empty,
            CreatedOn = createdAt,
            Parameters = parameters.Select(p => new Parameter { Name = p.Key, Value = p.Value }).ToArray(),
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
        Dictionary<string, string?> parameters = new() { { "Key1", null }, { "Key2", null }, { "Key3", null } };

        DateTime expireOn = DateTime.UtcNow;
        Documents.Job entityJob = new()
        {
            InvocationData = invocationData,
            Arguments = string.Empty,
            CreatedOn = createdAt,
            Parameters = parameters.Select(p => new Parameter { Name = p.Key, Value = p.Value }).ToArray(),
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
            .Select(x => new { x.Key, Value = x.Sum(z => z.Value), ExpireOn = x.Max(z => z.ExpireOn) })
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
        Counter? record = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Counter })
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
        var record = Storage.Container.GetItemLinqQueryable<Counter>(requestOptions: new QueryRequestOptions { PartitionKey = PartitionKeys.Counter })
            .ToQueryResult()
            .GroupBy(x => x.Key)
            .Select(x => new { x.Key, Value = x.Sum(z => z.Value), ExpireOn = x.Max(z => z.ExpireOn) })
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

    [Fact]
    public void RemoveFromSet_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.RemoveFromSet(null!, "value");
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void RemoveFromSet_ThrowsAnException_WhenValueIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.RemoveFromSet("my-set", null!);
            transaction.Commit();
        });

        Assert.Equal("value", exception.ParamName);
    }

    [Fact]
    public void RemoveFromSet_RemovesARecord_WithGivenKeyAndValue()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.AddToSet("my-key", "my-value");
        transaction.RemoveFromSet("my-key", "my-value");
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
        List<Set> record = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToList();

        Assert.Empty(record);
    }

    [Fact]
    public void RemoveFromSet_DoesNotRemoveRecord_WithSameKey_AndDifferentValue()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.AddToSet("my-key", "my-value");
        transaction.RemoveFromSet("my-key", "different-value");
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
        List<Set> record = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToList();

        Assert.Single(record);
    }


    [Fact]
    public void RemoveFromSet_DoesNotRemoveRecord_WithSameValue_AndDifferentKey()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.AddToSet("my-key", "my-value");
        transaction.RemoveFromSet("different-key", "my-value");
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
        List<Set> record = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToList();

        Assert.Single(record);
    }

    [Fact]
    public void InsertToList_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.InsertToList(null!, "value");
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void InsertToList_ThrowsAnException_WhenValueIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.InsertToList("my-list", null!);
            transaction.Commit();
        });

        Assert.Equal("value", exception.ParamName);
    }

    [Fact]
    public void InsertToList_AddsARecord_WithGivenValues()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.InsertToList("my-key", "my-value");
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        List? record = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .SingleOrDefault();

        Assert.NotNull(record);
        Assert.Equal("my-key", record!.Key);
        Assert.Equal("my-value", record.Value);
    }

    [Fact]
    public void InsertToList_AddsAnotherRecord_WhenBothKeyAndValueAreExist()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.InsertToList("my-key", "my-value");
        transaction.InsertToList("my-key", "my-value");
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        List<List> records = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToList();

        Assert.Equal(2, records.Count);
    }

    [Fact]
    public void RemoveFromList_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.RemoveFromList(null!, "value");
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void RemoveFromList_ThrowsAnException_WhenValueIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.RemoveFromList("my-list", null!);
            transaction.Commit();
        });

        Assert.Equal("value", exception.ParamName);
    }

    [Fact]
    public void RemoveFromList_RemovesAllRecords_WithGivenKeyAndValue()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.InsertToList("my-key", "my-value");
        transaction.InsertToList("my-key", "my-value");
        transaction.RemoveFromList("my-key", "my-value");
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        List<List> records = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToList();

        Assert.Empty(records);
    }

    [Fact]
    public void RemoveFromList_DoesNotRemoveRecords_WithSameKey_ButDifferentValue()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.InsertToList("my-key", "my-value");
        transaction.RemoveFromList("my-key", "different-value");
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        List<List> records = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToList();

        Assert.Single(records);
    }

    [Fact]
    public void RemoveFromList_DoesNotRemoveRecords_WithSameValue_ButDifferentKey()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.InsertToList("my-key", "my-value");
        transaction.RemoveFromList("different-key", "my-value");
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        List<List> records = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToList();

        Assert.Single(records);
    }

    [Fact]
    public void TrimList_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.TrimList(null!, 0, 1);
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void TrimList_TrimsAList_ToASpecifiedRange()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.InsertToList("my-key", "0");
        transaction.InsertToList("my-key", "1");
        transaction.InsertToList("my-key", "2");
        transaction.InsertToList("my-key", "3");
        transaction.TrimList("my-key", 1, 2);
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        List<List> records = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToList();

        Assert.Equal(2, records.Count);
        Assert.Equal("1", records[0].Value);
        Assert.Equal("2", records[1].Value);
    }

    [Fact]
    public void TrimList_RemovesRecordsToEnd_IfKeepEndingAt_GreaterThanMaxElementIndex()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.InsertToList("my-key", "0");
        transaction.InsertToList("my-key", "1");
        transaction.InsertToList("my-key", "2");
        transaction.TrimList("my-key", 1, 100);
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        List<List> records = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToList();

        Assert.Equal(2, records.Count);
        Assert.Equal("0", records[0].Value);
        Assert.Equal("1", records[1].Value);
    }

    [Fact]
    public void TrimList_RemovesAllRecords_WhenStartingFromValue_GreaterThanMaxElementIndex()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.InsertToList("my-key", "0");
        transaction.TrimList("my-key", 1, 100);
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        List<List> records = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToList();

        Assert.Empty(records);
    }

    [Fact]
    public void TrimList_RemovesAllRecords_IfStartFromGreaterThanEndingAt()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.InsertToList("my-key", "0");
        transaction.TrimList("my-key", 1, 0);
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        List<List> records = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToList();

        Assert.Empty(records);
    }

    [Fact]
    public void TrimList_RemovesRecords_OnlyOfAGivenKey()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.InsertToList("my-key", "0");
        transaction.TrimList("another-key", 1, 0);
        transaction.Commit();

        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        List<List> records = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToList();

        Assert.Single(records);
    }

    [Fact]
    public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.SetRangeInHash(null!, new Dictionary<string, string?>());
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.SetRangeInHash("some-hash", null!);
            transaction.Commit();
        });

        Assert.Equal("keyValuePairs", exception.ParamName);
    }

    [Fact]
    public void SetRangeInHash_MergesAllRecords()
    {
        //clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.SetRangeInHash("some-hash", new Dictionary<string, string?> { { "Key1", "Value1" }, { "Key2", "Value2" } });
        transaction.Commit();

        //assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Hash };
        Dictionary<string, string?> records = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToDictionary(x => x.Field, x => x.Value);


        Assert.Equal(2, records.Count);
        Assert.Equal("Value1", records["Key1"]);
        Assert.Equal("Value2", records["Key2"]);
    }

    [Fact]
    public void SetRangeInHash_CanSetANullValue()
    {
        //clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.SetRangeInHash("some-hash", new Dictionary<string, string?> { { "Key1", null! } });
        transaction.Commit();

        //assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Hash };
        Dictionary<string, string?> records = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToDictionary(x => x.Field, x => x.Value);

        Assert.Single(records);
        Assert.Null(records["Key1"]);
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
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);
        transaction.SetRangeInHash("some-hash", new Dictionary<string, string?> { { "key1", "VALUE-1" } });
        transaction.Commit();

        QueryRequestOptions queryRequestOptions = new() { PartitionKey = PartitionKeys.Hash };
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
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);
        transaction.SetRangeInHash("some-hash", new Dictionary<string, string?> { { "key1", "VALUE-1" } });
        transaction.Commit();

        QueryRequestOptions queryRequestOptions = new() { PartitionKey = PartitionKeys.Hash };
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
    public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.RemoveHash(null!);
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void RemoveHash_RemovesAllHashRecords()
    {
        //clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        transaction.SetRangeInHash("some-hash", new Dictionary<string, string?> { { "Key1", "Value1" }, { "Key2", "Value2" } });
        transaction.Commit();

        transaction.RemoveHash("some-hash");
        transaction.Commit();

        //assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Hash };
        Dictionary<string, string?> records = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToDictionary(x => x.Field, x => x.Value);

        Assert.Empty(records);
    }

    [Fact]
    public void AddRangeToSet_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.AddRangeToSet(null!, new List<string>());
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void AddRangeToSet_ThrowsAnException_WhenItemsValueIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.AddRangeToSet("my-set", null!);
            transaction.Commit();
        });

        Assert.Equal("items", exception.ParamName);
    }

    [Fact]
    public void AddRangeToSet_AddsAllItems_ToAGivenSet()
    {
        //clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        List<string> items = new() { "1", "2", "3" };
        transaction.AddRangeToSet("my-set", items);
        transaction.Commit();

        //assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
        List<string> records = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
            .Where(x => x.Key == "my-set")
            .Select(x => x.Value)
            .ToQueryResult()
            .ToList();

        Assert.Equal(items, records);
    }

    [Fact]
    public void RemoveSet_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.RemoveSet(null!);
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void RemoveSet_RemovesASet_WithAGivenKey()
    {
        //clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        List<string> items = new() { "1", "2", "3" };
        transaction.AddRangeToSet("set-1", items);
        transaction.AddRangeToSet("set-2", items);
        transaction.Commit();

        transaction.RemoveSet("set-1");
        transaction.Commit();

        //assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
        List<string> records = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
            .Where(x => x.Key == "set-1")
            .Select(x => x.Value)
            .ToQueryResult()
            .ToList();

        Assert.Empty(records);

        records = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
            .Where(x => x.Key == "set-2")
            .Select(x => x.Value)
            .ToQueryResult()
            .ToList();

        Assert.Equal(items, records);
    }

    [Fact]
    public void ExpireHash_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.ExpireHash(null!, TimeSpan.FromMinutes(5));
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void ExpireHash_SetsExpirationTimeOnAHash_WithGivenKey()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        List<Hash> hashes = new() { new Hash { Key = "some-hash", Field = "key1", Value = "value1" }, new Hash { Key = "some-other-hash", Field = "key1", Value = "value1" } };
        Data<Hash> data = new(hashes);
        Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);


        // Act
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);
        transaction.ExpireHash("some-hash", TimeSpan.FromMinutes(60));
        transaction.Commit();

        //assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Hash };
        Dictionary<string, DateTime?> records = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToDictionary(x => x.Key, x => x.ExpireOn);

        Assert.True(DateTime.UtcNow.AddMinutes(59) < records["some-hash"]);
        Assert.True(records["some-hash"] < DateTime.UtcNow.AddMinutes(61));
        Assert.Null(records["some-other-hash"]);
    }

    [Fact]
    public void ExpireSet_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.ExpireSet(null!, TimeSpan.FromSeconds(45));
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void ExpireSet_SetsExpirationTime_OnASet_WithGivenKey()
    {
        //clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        List<string> items = new() { "1" };
        transaction.AddRangeToSet("set-1", items);
        transaction.AddRangeToSet("set-2", items);
        transaction.Commit();


        // Act
        transaction.ExpireSet("set-1", TimeSpan.FromMinutes(60));
        transaction.Commit();

        // Assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
        Dictionary<string, DateTime?> records = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToDictionary(x => x.Key, x => x.ExpireOn);

        Assert.True(DateTime.UtcNow.AddMinutes(59) < records["set-1"]);
        Assert.True(records["set-1"] < DateTime.UtcNow.AddMinutes(61));
        Assert.Null(records["set-2"]);
    }

    [Fact]
    public void ExpireList_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.ExpireList(null!, TimeSpan.FromSeconds(45));
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void ExpireList_SetsExpirationTime_OnAList_WithGivenKey()
    {
        //clean
        ContainerFixture.Clean();

        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);
        transaction.InsertToList("list-1", "1");
        transaction.InsertToList("list-2", "2");
        transaction.Commit();

        // Act
        transaction.ExpireList("list-1", TimeSpan.FromMinutes(60));
        transaction.Commit();

        // Assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        Dictionary<string, DateTime?> records = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToDictionary(x => x.Key, x => x.ExpireOn);

        Assert.True(DateTime.UtcNow.AddMinutes(59) < records["list-1"]);
        Assert.True(records["list-1"] < DateTime.UtcNow.AddMinutes(61));
        Assert.Null(records["list-2"]);
    }

    [Fact]
    public void PersistHash_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.PersistHash(null!);
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void PersistHash_ClearsExpirationTime_OnAGivenHash()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        List<Hash> hashes = new()
        {
            new Hash
            {
                Key = "hash-1",
                Field = "key1",
                Value = "value1",
                ExpireOn = DateTime.Now.AddDays(1)
            },
            new Hash
            {
                Key = "hash-2",
                Field = "key1",
                Value = "value1",
                ExpireOn = DateTime.Now.AddDays(1)
            }
        };

        Data<Hash> data = new(hashes);
        Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);

        // Act
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);
        transaction.PersistHash("hash-1");
        transaction.Commit();

        // Assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Hash };
        Dictionary<string, DateTime?> records = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToDictionary(x => x.Key, x => x.ExpireOn);

        Assert.Null(records["hash-1"]);
        Assert.NotNull(records["hash-2"]);
    }

    [Fact]
    public void PersistHash_ClearsExpirationTime_OnAGivenHash_MultipleDocuments()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        List<Hash> hashes = new()
        {
            new Hash
            {
                Key = "hash-1",
                Field = "key1",
                Value = "value1",
                ExpireOn = DateTime.Now.AddDays(1)
            },
            new Hash
            {
                Key = "hash-1",
                Field = "key2",
                Value = "value2",
                ExpireOn = DateTime.Now.AddDays(1)
            },
            new Hash
            {
                Key = "hash-1",
                Field = "key3",
                Value = "value3",
                ExpireOn = DateTime.Now.AddDays(1)
            },
            new Hash
            {
                Key = "hash-1",
                Field = "key4",
                Value = "value4",
                ExpireOn = DateTime.Now.AddDays(1)
            },
            new Hash
            {
                Key = "hash-2",
                Field = "key1",
                Value = "value1",
                ExpireOn = DateTime.Now.AddDays(1)
            }
        };

        Data<Hash> data = new(hashes);
        Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Hash);

        // Act
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);
        transaction.PersistHash("hash-1");
        transaction.Commit();

        // Assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Hash };
        Dictionary<string, DateTime?> records = Storage.Container.GetItemLinqQueryable<Hash>(requestOptions: requestOptions)
            .ToQueryResult()
            .GroupBy(x => x.Key)
            .ToDictionary(x => x.Key, x => x.Max(z => z.ExpireOn));

        Assert.Null(records["hash-1"]);
        Assert.NotNull(records["hash-2"]);
    }

    [Fact]
    public void PersistSet_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.PersistSet(null!);
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void PersistSet_ClearsExpirationTime_OnAGivenKey()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        List<Set> sets = new()
        {
            new Set
            {
                Key = "set-2",
                Value = "value2",
                CreatedOn = DateTime.UtcNow,
                ExpireOn = DateTime.Now.AddDays(1)
            },
            new Set
            {
                Key = "set-3",
                Value = "value3",
                CreatedOn = DateTime.UtcNow,
                ExpireOn = DateTime.Now.AddDays(1)
            },
            new Set
            {
                Key = "set-1",
                Value = "value1",
                CreatedOn = DateTime.UtcNow,
                ExpireOn = DateTime.Now.AddDays(1)
            }
        };

        Data<Set> data = new(sets);
        Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Set);

        // Act
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);
        transaction.PersistSet("set-1");
        transaction.Commit();

        // Assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
        Dictionary<string, DateTime?> records = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToDictionary(x => x.Key, x => x.ExpireOn);

        Assert.Null(records["set-1"]);
        Assert.NotNull(records["set-2"]);
        Assert.NotNull(records["set-3"]);
    }

    [Fact]
    public void PersistSet_ClearsExpirationTime_OnAGivenKey_MultipleDocuments()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        List<Set> sets = new()
        {
            new Set
            {
                Key = "set-2",
                Value = "value2",
                CreatedOn = DateTime.UtcNow,
                ExpireOn = DateTime.Now.AddDays(1)
            },
            new Set
            {
                Key = "set-3",
                Value = "value3",
                CreatedOn = DateTime.UtcNow,
                ExpireOn = DateTime.Now.AddDays(1)
            },
            new Set
            {
                Key = "set-1",
                Value = "value1",
                CreatedOn = DateTime.UtcNow,
                ExpireOn = DateTime.Now.AddDays(1)
            },
            new Set
            {
                Key = "set-1",
                Value = "value1",
                CreatedOn = DateTime.UtcNow,
                ExpireOn = DateTime.Now.AddDays(1)
            },
            new Set
            {
                Key = "set-1",
                Value = "value1",
                CreatedOn = DateTime.UtcNow,
                ExpireOn = DateTime.Now.AddDays(1)
            },
            new Set
            {
                Key = "set-1",
                Value = "value1",
                CreatedOn = DateTime.UtcNow,
                ExpireOn = DateTime.Now.AddDays(1)
            }
        };

        Data<Set> data = new(sets);
        Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.Set);

        // Act
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);
        transaction.PersistSet("set-1");
        transaction.Commit();

        // Assert
        // Assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.Set };
        Dictionary<string, DateTime?> records = Storage.Container.GetItemLinqQueryable<Set>(requestOptions: requestOptions)
            .ToQueryResult()
            .GroupBy(x => x.Key)
            .ToDictionary(x => x.Key, x => x.Max(z => z.ExpireOn));

        Assert.Null(records["set-1"]);
        Assert.NotNull(records["set-2"]);
        Assert.NotNull(records["set-3"]);
    }

    [Fact]
    public void PersistList_ThrowsAnException_WhenKeyIsNull()
    {
        // arrange
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);

        // act
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() =>
        {
            transaction.PersistList(null!);
            transaction.Commit();
        });

        Assert.Equal("key", exception.ParamName);
    }

    [Fact]
    public void PersistList_ClearsExpirationTime_OnAGivenKey()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        List<List> lists = new() { new List { Key = "list-1", CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddHours(1) }, new List { Key = "list-2", CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddHours(1) } };
        Data<List> data = new(lists);
        Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.List);

        // Act
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);
        transaction.PersistList("list-1");
        transaction.Commit();

        // Assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        Dictionary<string, DateTime?> records = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .ToDictionary(x => x.Key, x => x.ExpireOn);

        Assert.Null(records["list-1"]);
        Assert.NotNull(records["list-2"]);
    }

    [Fact]
    public void PersistList_ClearsExpirationTime_OnAGivenKey_MultipleDocuments()
    {
        // clean
        ContainerFixture.Clean();

        // arrange
        List<List> lists = new()
        {
            new List { Key = "list-1", CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddHours(1) },
            new List { Key = "list-2", CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddHours(1) },
            new List { Key = "list-1", CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddHours(1) },
            new List { Key = "list-1", CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddHours(1) },
            new List { Key = "list-1", CreatedOn = DateTime.UtcNow, ExpireOn = DateTime.UtcNow.AddHours(1) }
        };

        Data<List> data = new(lists);
        Storage.Container.ExecuteUpsertDocuments(data, PartitionKeys.List);

        // Act
        CosmosDbConnection connection = (CosmosDbConnection)Storage.GetConnection();
        CosmosDbWriteOnlyTransaction transaction = new(connection);
        transaction.PersistList("list-1");
        transaction.Commit();

        // Assert
        QueryRequestOptions requestOptions = new() { PartitionKey = PartitionKeys.List };
        Dictionary<string, DateTime?> records = Storage.Container.GetItemLinqQueryable<List>(requestOptions: requestOptions)
            .ToQueryResult()
            .GroupBy(x => x.Key)
            .ToDictionary(x => x.Key, x => x.Max(z => z.ExpireOn));

        Assert.Null(records["list-1"]);
        Assert.NotNull(records["list-2"]);
    }

#pragma warning disable xUnit1013
// ReSharper disable once MemberCanBePrivate.Global
// ReSharper disable once UnusedParameter.Global
    public static void SampleMethod(string arg) { }
#pragma warning restore xUnit1013
}