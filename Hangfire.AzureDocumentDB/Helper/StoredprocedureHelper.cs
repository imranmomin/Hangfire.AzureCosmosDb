using System;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Azure.Documents.Client;

using Hangfire.Azure.Documents;

namespace Hangfire.Azure.Helper
{
    internal static class StoredprocedureHelper
    {
        private static Uri spPersistDocumentsUri;
        private static Uri spExpireDocumentsUri;
        private static Uri spDeleteDocumentsUri;
        private static Uri spUpsertDocumentsUri;

        internal static void Setup(string databaseName, string collectionName)
        {
            spPersistDocumentsUri = UriFactory.CreateStoredProcedureUri(databaseName, collectionName, "persistDocuments");
            spExpireDocumentsUri = UriFactory.CreateStoredProcedureUri(databaseName, collectionName, "expireDocuments");
            spDeleteDocumentsUri = UriFactory.CreateStoredProcedureUri(databaseName, collectionName, "deleteDocuments");
            spUpsertDocumentsUri = UriFactory.CreateStoredProcedureUri(databaseName, collectionName, "upsertDocuments");
        }

        internal static int ExecuteUpsertDocuments<T>(this DocumentClient client, Data<T> data)
        {
            int affected = 0;
            Data<T> records = new Data<T>(data.Items);
            do
            {
                records.Items = data.Items.Skip(affected).ToList();
                Task<StoredProcedureResponse<int>> task = client.ExecuteStoredProcedureWithRetriesAsync<int>(spUpsertDocumentsUri, records);
                task.Wait();
                affected += task.Result;
            } while (affected < data.Items.Count);
            return affected;
        }

        internal static int ExecuteDeleteDocuments(this DocumentClient client, string query)
        {
            int affected = 0;
            ProcedureResponse response;
            do
            {
                Task<StoredProcedureResponse<ProcedureResponse>> task = client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spDeleteDocumentsUri, query);
                task.Wait();
                response = task.Result;
                affected += response.Affected;
            } while (response.Continuation);
            return affected;
        }

        internal static void ExecutePersistDocuments(this DocumentClient client, string query)
        {
            ProcedureResponse response;
            do
            {
                Task<StoredProcedureResponse<ProcedureResponse>> task = client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spPersistDocumentsUri, query);
                task.Wait();
                response = task.Result;
            } while (response.Continuation);
        }

        internal static void ExecuteExpireDocuments(this DocumentClient client, string query, int epoch)
        {
            ProcedureResponse response;
            do
            {
                Task<StoredProcedureResponse<ProcedureResponse>> task = client.ExecuteStoredProcedureWithRetriesAsync<ProcedureResponse>(spExpireDocumentsUri, query, epoch);
                task.Wait();
                response = task.Result;
            } while (response.Continuation);
        }
    }
}
