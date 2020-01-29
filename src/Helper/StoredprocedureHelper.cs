using System.Linq;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos;
using Hangfire.Azure.Documents;
using Microsoft.Azure.Cosmos.Scripts;

namespace Hangfire.Azure.Helper
{
    internal static class StoredprocedureHelper
    {
        internal static int ExecuteUpsertDocuments<T>(this Container container, Data<T> data, PartitionKey partitionKey)
        {
            int affected = 0;
            Data<T> records = new Data<T>(data.Items);
            do
            {
                records.Items = data.Items.Skip(affected).ToList();
                Task<StoredProcedureExecuteResponse<int>> task = container.Scripts.ExecuteStoredProcedureAsync<int>("upsertDocuments", partitionKey, (dynamic)records);
                task.Wait();
                affected += task.Result.Resource;
            } while (affected < data.Items.Count);
            return affected;
        }

        internal static int ExecuteDeleteDocuments(this Container container, string query, PartitionKey partitionKey)
        {
            int affected = 0;
            ProcedureResponse response;
            do
            {
                Task<StoredProcedureExecuteResponse<ProcedureResponse>> task = container.Scripts.ExecuteStoredProcedureAsync<ProcedureResponse>("deleteDocuments", partitionKey, (dynamic)query);
                task.Wait();
                response = task.Result;
                affected += response.Affected;
            } while (response.Continuation);
            return affected;
        }

        internal static void ExecutePersistDocuments(this Container container, string query, PartitionKey partitionKey)
        {
            ProcedureResponse response;
            do
            {
                Task<StoredProcedureExecuteResponse<ProcedureResponse>> task = container.Scripts.ExecuteStoredProcedureAsync<ProcedureResponse>("persistDocuments", partitionKey, (dynamic)query);
                task.Wait();
                response = task.Result;
            } while (response.Continuation);
        }

        internal static void ExecuteExpireDocuments(this Container container, string query, int epoch, PartitionKey partitionKey)
        {
            ProcedureResponse response;
            do
            {
                Task<StoredProcedureExecuteResponse<ProcedureResponse>> task = container.Scripts.ExecuteStoredProcedureAsync<ProcedureResponse>("expireDocuments", partitionKey, (dynamic)query, (dynamic)epoch);
                task.Wait();
                response = task.Result;
            } while (response.Continuation);
        }
    }
}
