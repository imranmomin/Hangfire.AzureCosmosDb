using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure;

internal class CosmosDbDistributedLock : IDisposable
{
	private readonly ILog logger = LogProvider.For<CosmosDbDistributedLock>();
	private readonly string resource;
	private readonly CosmosDbStorage storage;
	private bool disposed;
	private static readonly ThreadLocal<Dictionary<string, int>> acquiredLocks = new(() => new Dictionary<string, int>());
	private readonly object syncLock = new();

	private Lock? @lock;
	private Timer? timer;

	public CosmosDbDistributedLock(string resource, TimeSpan timeout, CosmosDbStorage storage)
	{
		this.resource = string.IsNullOrWhiteSpace(resource) ? throw new ArgumentNullException(nameof(resource)) : resource;
		this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
		Acquire(timeout);
	}

	public void Dispose()
	{
		if (disposed) return;
		disposed = true;

		if (acquiredLocks.Value.ContainsKey(resource) == false) return;
		acquiredLocks.Value[resource] -= 1;
		if (acquiredLocks.Value[resource] > 0) return;

		lock (syncLock)
		{
			try
			{
				storage.Container.DeleteItemWithRetries<Lock>(resource, PartitionKeys.Lock);
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
			{
				logger.Trace($"Unable to release the lock for resource [{resource}]. Status - 404 Not Found");
			}
			catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
			{
				logger.Trace($"Unable to release the lock for resource [{resource}]. Status - 404 Not Found");
			}
			catch (Exception exception)
			{
				logger.ErrorException($"Unable to release the lock for [{resource}]", exception);
			}
			finally
			{
				acquiredLocks.Value.Remove(resource);
				timer?.Dispose();
				@lock = null;
				logger.Trace($"Lock released for [{resource}]");
			}
		}
	}

	private void Acquire(TimeSpan timeout)
	{
		lock (syncLock)
		{
			if (acquiredLocks.Value.ContainsKey(resource))
			{
				logger.Trace($"Lock for [{resource}] already exists from the local thread. Will use the same lock");

				acquiredLocks.Value[resource] += 1;
				return;
			}
		}

		logger.Trace($"Trying to acquire lock for [{resource}] within [{timeout.TotalSeconds}] seconds");

		Stopwatch acquireStart = new();
		acquireStart.Start();

		// ttl for lock document
		// this is if the expiration manager was not able to remove the orphan lock in time.
		double ttl = Math.Max(15, timeout.TotalSeconds) * 1.5;

		do
		{
			Lock data = new()
			{
				Id = resource,
				LastHeartBeat = DateTime.UtcNow,
				TimeToLive = (int)ttl
			};

			try
			{
				@lock = storage.Container.CreateItemWithRetries(data, PartitionKeys.Lock);
				break;
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
			{
				logger.Trace($"Unable to create a lock for resource [{resource}]. Status - 409 Conflict");
			}
			catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.Conflict })
			{
				logger.Trace($"Unable to create a lock for resource [{resource}]. Status - 409 Conflict");
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

		} while (@lock == null);

		// set the timer for the KeepLockAlive callbacks
		TimeSpan period = TimeSpan.FromSeconds(ttl).Divide(2);
		period = period.TotalSeconds < 1 ? TimeSpan.FromSeconds(1) : period;
		timer = new Timer(KeepLockAlive, @lock, TimeSpan.FromSeconds(1), period);

		// add the resource to the local 
		acquiredLocks.Value.Add(resource, 1);

		logger.Trace($"Acquired lock for [{resource}] for [{timeout.TotalSeconds}] seconds; in [{acquireStart.Elapsed.TotalMilliseconds:#.##}] ms. " +
		             $"Keep-alive query will be sent every {period.TotalSeconds} seconds until disposed");
	}

	/// <summary>
	///     this is to update the document so that the ttl gets reset and does not removes the document pre-maturely
	/// </summary>
	private void KeepLockAlive(object data)
	{
		if (disposed) return;

		lock (syncLock)
		{
			if (data is not Lock temp) return;

			try
			{
				logger.Trace($"Preparing the Keep-alive query for lock: [{temp.Id}]");

				PatchItemRequestOptions patchItemRequestOptions = new() { IfMatchEtag = temp.ETag };
				PatchOperation[] patchOperations =
				{
					PatchOperation.Set("/last_heartbeat", DateTime.UtcNow.ToEpoch())
				};

				@lock = storage.Container.PatchItemWithRetries<Lock>(temp.Id, PartitionKeys.Lock, patchOperations, patchItemRequestOptions);

				logger.Trace($"Keep-alive query for lock: [{temp.Id}] sent");
			}
			catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
			{
				/* ignore */
			}
			catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
			{
				/* ignore */
			}
			catch (Exception ex)
			{
				logger.DebugException($"Unable to execute keep-alive query for the lock: [{temp.Id}]", ex);
			}
		}
	}
}