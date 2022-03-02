using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Hangfire.Logging;
using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure.Helper;

internal static class ClientHelper
{
	/// <summary>
	///     Creates a document as an asynchronous operation in the Azure Cosmos DB service.
	/// </summary>
	/// <param name="container"></param>
	/// <param name="document">the document object.</param>
	/// <param name="partitionKey"></param>
	/// <param name="requestOptions"></param>
	/// <returns></returns>
	internal static ItemResponse<T> CreateItemWithRetries<T>(this Container container, T document, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null)
	{
		Task<ItemResponse<T>> task = container.CreateItemAsync(document, partitionKey, requestOptions);
		return task
			.ExecuteWithRetriesAsync()
			.ExecuteSynchronously();
	}

	/// <summary>
	///     Reads a <see cref="T:Microsoft.Azure.Documents.Document" /> as a generic type T from the Azure Cosmos DB service as an asynchronous operation.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <param name="container"></param>
	/// <param name="id"></param>
	/// <param name="partitionKey"></param>
	/// <param name="requestOptions"></param>
	/// <returns></returns>
	internal static ItemResponse<T> ReadItemWithRetries<T>(this Container container, string id, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null)
	{
		Task<ItemResponse<T>> task = container.ReadItemAsync<T>(id, partitionKey, requestOptions);
		return task
			.ExecuteWithRetriesAsync()
			.ExecuteSynchronously();
	}

	/// <summary>
	///     Upsert a document as an asynchronous operation in the Azure Cosmos DB service.
	/// </summary>
	/// <param name="container"></param>
	/// <param name="document">the document object.</param>
	/// <param name="requestOptions"></param>
	/// <param name="partitionKey"></param>
	internal static ItemResponse<T> UpsertItemWithRetries<T>(this Container container, T document, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null)
	{
		Task<ItemResponse<T>> task = container.UpsertItemAsync(document, partitionKey, requestOptions);
		return task
			.ExecuteWithRetriesAsync()
			.ExecuteSynchronously();
	}

	/// <summary>
	///     Delete a document as an asynchronous operation from the Azure Cosmos DB service.
	/// </summary>
	/// <param name="container"></param>
	/// <param name="requestOptions"></param>
	/// <param name="id"></param>
	/// <param name="partitionKey"></param>
	internal static ItemResponse<T> DeleteItemWithRetries<T>(this Container container, string id, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null)
	{
		Task<ItemResponse<T>> task = container.DeleteItemAsync<T>(id, partitionKey, requestOptions);
		return task
			.ExecuteWithRetriesAsync()
			.ExecuteSynchronously();
	}

	/// <summary>
	///     Replaces a document as an asynchronous operation in the Azure Cosmos DB service.
	/// </summary>
	/// <param name="container"></param>
	/// <param name="patchOperations"></param>
	/// <param name="patchItemRequestOptions"></param>
	/// <param name="id"></param>
	/// <param name="partitionKey"></param>
	/// <returns></returns>
	internal static ItemResponse<T> PatchItemWithRetries<T>(this Container container, string id, PartitionKey partitionKey, IReadOnlyList<PatchOperation> patchOperations, PatchItemRequestOptions? patchItemRequestOptions = null)
	{
		Task<ItemResponse<T>> task = container.PatchItemAsync<T>(id, partitionKey, patchOperations, patchItemRequestOptions);
		return task
			.ExecuteWithRetriesAsync()
			.ExecuteSynchronously();
	}

	/// <summary>
	///     Execute the function with retries on throttle
	/// </summary>
	internal static async Task<T> ExecuteWithRetriesAsync<T>(this Task<T> task)
	{
		ILog logger = LogProvider.GetCurrentClassLogger();
		Exception? exception = null;
		int retry = 0;
		bool complete;

		do
		{
			TimeSpan? timeSpan = null;
			complete = true;

			try
			{
				return await task;
			}
			catch (CosmosException ex) when ((int)ex.StatusCode == 429)
			{
				timeSpan = ex.RetryAfter;
				logger.Error($"{ex.Message} Status - 429 TooManyRequests");
			}
			catch (AggregateException ex) when (ex.InnerException is CosmosException de && (int)de.StatusCode == 429)
			{
				timeSpan = de.RetryAfter;
				logger.Error($"{ex.Message} Status - 429 TooManyRequests");
			}
			catch (Exception ex)
			{
				exception = ex;
				retry += 1;
				complete = false;
			}
			finally
			{
				if (timeSpan.HasValue)
				{
					logger.Trace($"Status - 429 TooManyRequests. Will wait for [{timeSpan.Value.TotalSeconds}] seconds.");
					await Task.Delay(timeSpan.Value);
				}
			}

		} while (retry <= 3 && complete == false);

		return await Task.FromException<T>(exception!);
	}
}