using System;
using System.Net;
using System.Threading;
using Hangfire.Azure.Documents;
using Hangfire.Azure.Documents.Helper;
using Hangfire.Azure.Helper;
using Hangfire.Logging;
using Hangfire.Storage;
using Microsoft.Azure.Cosmos;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Queue;

internal class FetchedJob : IFetchedJob
{
    private readonly ILog logger = LogProvider.GetLogger(typeof(FetchedJob));
    private readonly PartitionKey partitionKey = new((int)DocumentTypes.Queue);
    private readonly CosmosDbStorage storage;
    private readonly object syncRoot = new();
    private readonly Timer timer;
    private Documents.Queue data;
    private bool disposed;
    private bool removedFromQueue;
    private bool reQueued;

    public FetchedJob(CosmosDbStorage storage, Documents.Queue data)
    {
        this.storage = storage;
        this.data = data;

        TimeSpan keepAliveInterval = storage.StorageOptions.JobKeepAliveInterval;
        timer = new Timer(KeepAliveJobCallback, null, keepAliveInterval, Timeout.InfiniteTimeSpan);
        logger.Trace($"Job [{data.JobId}] will send a Keep-Alive query every [{keepAliveInterval.TotalSeconds}] seconds until disposed");
    }

    public string Queue => data.Name;

    public DateTime? FetchedAt => data.FetchedAt;

    private string Id => data.Id;

    public string JobId => data.JobId;

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        disposed = true;

        timer.Dispose();

        lock (syncRoot)
        {
            if (!removedFromQueue && !reQueued)
            {
                Requeue();
            }
        }
    }

    public void RemoveFromQueue()
    {
        if (removedFromQueue)
        {
            return;
        }

        lock (syncRoot)
        {
            try
            {
                storage.Container.DeleteItemWithRetries<Documents.Queue>(Id, partitionKey);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                logger.Trace($"Unable to remove the job [{JobId}] from the queue [{Queue}]. Status - 404 Not Found");
            }
            catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
            {
                logger.Trace($"Unable to remove the job [{JobId}] from the queue [{Queue}]. Status - 404 Not Found");
            }
            finally
            {
                removedFromQueue = true;
            }
        }
    }

    public void Requeue()
    {
        if (reQueued)
        {
            return;
        }

        lock (syncRoot)
        {
            try
            {
                PatchItemRequestOptions patchItemRequestOptions = new() { IfMatchEtag = data.ETag };
                PatchOperation[] patchOperations =
                {
                    PatchOperation.Remove("/fetched_at"),
                    PatchOperation.Set("/created_on", DateTime.UtcNow.ToEpoch())
                };

                data = storage.Container.PatchItemWithRetries<Documents.Queue>(Id, partitionKey, patchOperations, patchItemRequestOptions);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                logger.Trace($"Unable to requeue the job [{JobId}] from the queue [{Queue}]. Status - 404 Not Found");
            }
            catch (AggregateException ex) when (ex.InnerException is CosmosException { StatusCode: HttpStatusCode.NotFound })
            {
                logger.Trace($"Unable to requeue the job [{JobId}] from the queue [{Queue}]. Status - 404 Not Found");
            }
            finally
            {
                reQueued = true;
            }
        }
    }

    // ReSharper disable once MemberCanBePrivate.Global
    internal void KeepAliveJobCallback(object obj)
    {
        if (disposed)
        {
            return;
        }

        lock (syncRoot)
        {
            if (reQueued || removedFromQueue)
            {
                return;
            }

            try
            {
                PatchItemRequestOptions patchItemRequestOptions = new() { IfMatchEtag = data.ETag };
                PatchOperation[] patchOperations =
                {
                    PatchOperation.Set("/fetched_at", DateTime.UtcNow.ToEpoch())
                };

                data = storage.Container.PatchItemWithRetries<Documents.Queue>(data.Id, partitionKey, patchOperations, patchItemRequestOptions);

                // set the timer for the next callback
                TimeSpan keepAliveInterval = storage.StorageOptions.JobKeepAliveInterval;
                timer.Change(keepAliveInterval, Timeout.InfiniteTimeSpan);

                logger.Trace($"Keep-alive query for job: [{data.Id}] sent");
            }
            catch (Exception ex) when (ex is CosmosException { StatusCode: HttpStatusCode.NotFound } or AggregateException { InnerException: CosmosException { StatusCode: HttpStatusCode.NotFound } })
            {
                logger.Trace($"Job [{data.Id}] keep-alive query failed. Most likely the job was removed from the queue");
            }
            catch (Exception ex) when (ex is CosmosException { StatusCode: HttpStatusCode.BadRequest } or AggregateException { InnerException: CosmosException { StatusCode: HttpStatusCode.BadRequest } })
            {
                logger.Trace($"Job [{data.Id}] keep-alive query failed. Most likely the job was updated by some other server");
            }
            catch (Exception ex)
            {
                logger.DebugException($"Unable to execute keep-alive query for job [{data.Id}]", ex);
            }
        }
    }
}