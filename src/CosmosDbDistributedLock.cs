using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure;

public class CosmosDbDistributedLock : IDisposable
{
	private readonly ILog logger = LogProvider.For<CosmosDbDistributedLock>();
	private readonly PartitionKey partitionKey = new((int)DocumentTypes.Lock);
	private readonly string resource;
	private readonly CosmosDbStorage storage;
	private bool disposed;

	private Lock? @lock;
	private Timer? timer;

	public CosmosDbDistributedLock(string resource, TimeSpan timeout, CosmosDbStorage storage)
	{
		this.resource = resource;
		this.storage = storage;
		Acquire(timeout);
	}

	public void Dispose()
	{
		if (disposed) return;
		if (@lock == null) return;

		try
		{
			Task task = storage.Container.DeleteItemWithRetriesAsync<Lock>(@lock.Id, partitionKey);
			task.Wait();
		}
		catch (Exception exception)
		{
			logger.ErrorException($"Unable to release the lock for [{resource}]", exception);
		}
		finally
		{
			disposed = true;
			timer?.Dispose();
			logger.Trace($"Lock released for [{resource}]");
		}
	}

	private void Acquire(TimeSpan timeout)
	{
		logger.Trace($"Trying to acquire lock for [{resource}] within [{timeout.TotalSeconds}] seconds");

		Stopwatch acquireStart = new();
		acquireStart.Start();

		// ttl for lock document
		// this is if the expiration manager was not able to remove the orphan lock in time.
		double ttl = Math.Max(15, timeout.TotalSeconds) * 1.5;

		while (@lock == null)
		{
			Lock data = new()
			{
				Id = resource,
				ExpireOn = DateTime.UtcNow.Add(timeout),
				LastHeartBeat = DateTime.UtcNow,
				TimeToLive = (int)ttl
			};

			try
			{
				Task<ItemResponse<Lock>> createTask = storage.Container.CreateItemWithRetriesAsync(data, partitionKey);
				createTask.Wait();

				@lock = createTask.Result.Resource;
				break;
			}
			catch (Exception ex)
			{
				logger.ErrorException($"Unable to create a lock for resource [{resource}]", ex);
			}

			// check the timeout
			if (acquireStart.ElapsedMilliseconds > timeout.TotalMilliseconds)
				throw new CosmosDbDistributedLockException($"Could not place a lock on the resource [{resource}]: Lock timeout reached [{timeout.TotalSeconds}] seconds.", resource);

			logger.Trace($"Unable to acquire lock for [{resource}]. Will try after [2] seconds");
			Thread.Sleep(2000);
		}

		// set the timer for the KeepLockAlive callbacks
		TimeSpan period = TimeSpan.FromSeconds(ttl).Divide(2);
		period = period.TotalSeconds < 1 ? TimeSpan.FromSeconds(1) : period;
		timer = new Timer(KeepLockAlive, null, period, period);

		logger.Trace($"Acquired lock for [{resource}] for [{timeout.TotalSeconds}] seconds; in [{acquireStart.Elapsed.TotalMilliseconds:#.##}] ms. " +
		             $"Keep-alive query will be sent every {period.TotalSeconds} seconds until disposed");
	}

	/// <summary>
	///     this is to update the document so that the ttl gets reset and does not removes the document pre-maturely
	/// </summary>
	private void KeepLockAlive(object data)
	{
		if (@lock == null) return;
		if (disposed) return;

		try
		{
			logger.Trace($"Preparing the Keep-alive query for lock: [{@lock.Id}]");

			PatchOperation[] patchOperations =
			{
				PatchOperation.Set("/last_heartbeat", DateTime.UtcNow.ToEpoch())
			};

			PatchItemRequestOptions patchItemRequestOptions = new()
			{
				IfMatchEtag = @lock.ETag
			};

			Task<ItemResponse<Lock>> task = storage.Container.PatchItemWithRetriesAsync<Lock>(@lock.Id, partitionKey, patchOperations, patchItemRequestOptions);
			task.Wait();

			@lock = task.Result;

			logger.Trace($"Keep-alive query for lock: [{@lock.Id}] sent");
		}
		catch (AggregateException aggregateException) when (aggregateException.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
		{
			/* ignore */
		}
		catch (Exception ex)
		{
			logger.DebugException($"Unable to execute keep-alive query for the lock: [{@lock.Id}]", ex);
		}
	}
}