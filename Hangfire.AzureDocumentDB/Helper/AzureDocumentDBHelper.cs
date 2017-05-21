using System;
using System.Threading.Tasks;

using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Hangfire.AzureDocumentDB.Helper
{
    internal static class AzureDocumentDBHelper
    {
        private const int RequestRateTooLargeException = 429;

        internal static async Task<ResourceResponse<Document>> CreateDocumentWithRetriesAsync(this DocumentClient client, Uri documentCollectionUri, object document, RequestOptions options = null, bool disableAutomaticIdGeneration = false)
        {
            return await ExecuteWithRetries(() => client.CreateDocumentAsync(documentCollectionUri, document, options, disableAutomaticIdGeneration));
        }

        internal static async Task<ResourceResponse<Document>> ReplaceDocumentWithRetriesAsync(this DocumentClient client, string documentLink, object document, RequestOptions options = null)
        {
            return await ExecuteWithRetries(() => client.ReplaceDocumentAsync(documentLink, document, options));
        }

        internal static async Task<ResourceResponse<Document>> UpsertDocumentWithRetriesAsync(this DocumentClient client, Uri documentCollectionUri, object document, RequestOptions options = null, bool disableAutomaticIdGeneration = false)
        {
            return await ExecuteWithRetries(() => client.UpsertDocumentAsync(documentCollectionUri, document, options, disableAutomaticIdGeneration));
        }

        internal static async Task<ResourceResponse<Document>> DeleteDocumentWithRetriesAsync(this DocumentClient client, string documentLink)
        {
            return await ExecuteWithRetries(() => client.DeleteDocumentAsync(documentLink));
        }

        private static async Task<TResult> ExecuteWithRetries<TResult>(Func<Task<TResult>> function)
        {
            TimeSpan sleepTime = TimeSpan.Zero;
            int retriesCount = 0;

            while (retriesCount < 3)
            {
                retriesCount += 1;

                try
                {
                    return await function();
                }
                catch (DocumentClientException documentException)
                {
                    if ((int)documentException.StatusCode != RequestRateTooLargeException)
                    {
                        throw;
                    }
                    sleepTime = documentException.RetryAfter;
                }
                catch (AggregateException ex)
                {
                    if (!(ex.InnerException is DocumentClientException))
                    {
                        throw;
                    }

                    DocumentClientException documentException = (DocumentClientException)ex.InnerException;
                    if ((int)documentException.StatusCode != RequestRateTooLargeException)
                    {
                        throw;
                    }
                    sleepTime = documentException.RetryAfter;
                }

                await Task.Delay(sleepTime);
            }

            throw new AzureDocumentDbDistributedRetryException($"Failed to execute the task after 3 retries. Please check the rate limits on the collection/database");
        }

    }
}