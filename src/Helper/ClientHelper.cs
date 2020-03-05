using System;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos;

namespace Hangfire.Azure.Helper
{
    internal static class ClientHelper
    {
        /// <summary>
        /// Creates a document as an asynchronous operation in the Azure Cosmos DB service.
        /// </summary>
        /// <param name="container"></param>
        /// <param name="document">the document object.</param>
        /// <param name="partitionKey"></param>
        /// <param name="requestOptions"></param>
        /// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
        /// <returns></returns>
        internal static Task<ItemResponse<T>> CreateItemWithRetriesAsync<T>(this Container container, T document, PartitionKey partitionKey, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        {
            return Task.Run(async () => await container.ExecuteWithRetries(x => x.CreateItemAsync(document, partitionKey, requestOptions, cancellationToken)), cancellationToken);
        }

        /// <summary>
        /// Reads a <see cref="T:Microsoft.Azure.Documents.Document" /> as a generic type T from the Azure Cosmos DB service as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="container"></param>
        /// <param name="id"></param>
        /// <param name="partitionKey"></param>
        /// <param name="requestOptions"></param>
        /// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
        /// <returns></returns>
        internal static Task<ItemResponse<T>> ReadItemWithRetriesAsync<T>(this Container container, string id, PartitionKey partitionKey, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        {
            return Task.Run(async () => await container.ExecuteWithRetries(x => x.ReadItemAsync<T>(id, partitionKey, requestOptions, cancellationToken)), cancellationToken);
        }

        /// <summary>
        /// Upsert a document as an asynchronous operation in the Azure Cosmos DB service.
        /// </summary>
        /// <param name="container"></param>
        /// <param name="document">the document object.</param>
        /// <param name="requestOptions"></param>
        /// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
        /// <param name="partitionKey"></param>
        internal static Task<ItemResponse<T>> UpsertItemWithRetriesAsync<T>(this Container container, T document, PartitionKey partitionKey, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        {
            return Task.Run(async () => await container.ExecuteWithRetries(x => x.UpsertItemAsync(document, partitionKey, requestOptions, cancellationToken)), cancellationToken);
        }

        /// <summary>
        /// Delete a document as an asynchronous operation from the Azure Cosmos DB service.
        /// </summary>
        /// <param name="container"></param>
        /// <param name="requestOptions"></param>
        /// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
        /// <param name="id"></param>
        /// <param name="partitionKey"></param>
        internal static Task<ItemResponse<T>> DeleteItemWithRetriesAsync<T>(this Container container, string id, PartitionKey partitionKey, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        {
            return Task.Run(async () => await container.ExecuteWithRetries(x => x.DeleteItemAsync<T>(id, partitionKey, requestOptions, cancellationToken)), cancellationToken);
        }

        /// <summary>
        /// Replaces a document as an asynchronous operation in the Azure Cosmos DB service.
        /// </summary>
        /// <param name="container"></param>
        /// <param name="document">the updated document.</param>
        /// <param name="requestOptions"></param>
        /// <param name="cancellationToken">(Optional) <see cref="T:System.Threading.CancellationToken" /> representing request cancellation.</param>
        /// <param name="id"></param>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        internal static Task<ItemResponse<T>> ReplaceItemWithRetriesAsync<T>(this Container container, T document, string id, PartitionKey partitionKey, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = default)
        {
            return Task.Run(async () => await container.ExecuteWithRetries(x => x.ReplaceItemAsync(document, id, partitionKey, requestOptions, cancellationToken)), cancellationToken);
        }

        /// <summary>
        /// Execute the function with retries on throttle
        /// </summary>
        private static async Task<T> ExecuteWithRetries<T>(this Container container, Func<Container, Task<T>> function)
        {
            while (true)
            {
                TimeSpan? timeSpan = null;

                try
                {
                    return await function(container);
                }
                catch (CosmosException ex) when ((int)ex.StatusCode == 429)
                {
                    timeSpan = ex.RetryAfter;
                }
                catch (AggregateException ex) when (ex.InnerException is CosmosException de && (int)de.StatusCode == 429)
                {
                    timeSpan = de.RetryAfter;
                }
                finally
                {
                    if (timeSpan.HasValue)
                    {
                        await Task.Delay(timeSpan.Value);
                    }
                }
            }
        }
    }
}
