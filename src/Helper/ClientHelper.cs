using System;
using System.Collections.Generic;
using System.Threading;
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
	/// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
	/// <returns></returns>
	internal static Task<ItemResponse<T>> CreateItemWithRetriesAsync<T>(this Container container, T document, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) =>
		container.ExecuteWithRetries(x => x.CreateItemAsync(document, partitionKey, requestOptions, cancellationToken));

	/// <summary>
	///     Reads a <see cref="T:Microsoft.Azure.Documents.Document" /> as a generic type T from the Azure Cosmos DB service as an asynchronous operation.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <param name="container"></param>
	/// <param name="id"></param>
	/// <param name="partitionKey"></param>
	/// <param name="requestOptions"></param>
	/// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
	/// <returns></returns>
	internal static Task<ItemResponse<T>> ReadItemWithRetriesAsync<T>(this Container container, string id, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) =>
		container.ExecuteWithRetries(x => x.ReadItemAsync<T>(id, partitionKey, requestOptions, cancellationToken));

	/// <summary>
	///     Upsert a document as an asynchronous operation in the Azure Cosmos DB service.
	/// </summary>
	/// <param name="container"></param>
	/// <param name="document">the document object.</param>
	/// <param name="requestOptions"></param>
	/// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
	/// <param name="partitionKey"></param>
	internal static Task<ItemResponse<T>> UpsertItemWithRetriesAsync<T>(this Container container, T document, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) =>
		container.ExecuteWithRetries(x => x.UpsertItemAsync(document, partitionKey, requestOptions, cancellationToken));

	/// <summary>
	///     Delete a document as an asynchronous operation from the Azure Cosmos DB service.
	/// </summary>
	/// <param name="container"></param>
	/// <param name="requestOptions"></param>
	/// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
	/// <param name="id"></param>
	/// <param name="partitionKey"></param>
	internal static Task<ItemResponse<T>> DeleteItemWithRetriesAsync<T>(this Container container, string id, PartitionKey partitionKey, ItemRequestOptions? requestOptions = null, CancellationToken cancellationToken = default) =>
		container.ExecuteWithRetries(x => x.DeleteItemAsync<T>(id, partitionKey, requestOptions, cancellationToken));

	/// <summary>
	///     Replaces a document as an asynchronous operation in the Azure Cosmos DB service.
	/// </summary>
	/// <param name="container"></param>
	/// <param name="patchOperations"></param>
	/// <param name="patchItemRequestOptions"></param>
	/// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
	/// <param name="id"></param>
	/// <param name="partitionKey"></param>
	/// <returns></returns>
	internal static Task<ItemResponse<T>> PatchItemWithRetriesAsync<T>(this Container container, string id, PartitionKey partitionKey, IReadOnlyList<PatchOperation> patchOperations,
		PatchItemRequestOptions? patchItemRequestOptions = null,
		CancellationToken cancellationToken = default) => container.ExecuteWithRetries(x => x.PatchItemAsync<T>(id, partitionKey, patchOperations, patchItemRequestOptions, cancellationToken));

	/// <summary>
	///     Execute the function with retries on throttle
	/// </summary>
	private static async Task<T> ExecuteWithRetries<T>(this Container container, Func<Container, Task<T>> function)
	{
		ILog logger = LogProvider.GetCurrentClassLogger();

		while (true)
		{
			TimeSpan? timeSpan = null;

			try
			{
				Task<T> task = function(container);
				return await task;
			}
			catch (CosmosException ex) when ((int)ex.StatusCode == 429)
			{
				timeSpan = ex.RetryAfter;
				logger.ErrorException("Status [429] received", ex);
			}
			catch (AggregateException ex) when (ex.InnerException is CosmosException de && (int)de.StatusCode == 429)
			{
				timeSpan = de.RetryAfter;
				logger.ErrorException("Status [429] received", ex);
			}
			finally
			{
				if (timeSpan.HasValue)
				{
					logger.Trace($"Status [429] received. Will wait for [{timeSpan.Value.TotalSeconds}] seconds.");
					await Task.Delay(timeSpan.Value);
				}
			}
		}
	}
}