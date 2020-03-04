using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.Cosmos;
using Hangfire.Azure.Documents;
using Microsoft.Azure.Cosmos.Scripts;

namespace Hangfire.Azure.Helper
{
    internal static class StoredprocedureHelper
    {
        internal static int ExecuteUpsertDocuments<T>(this Container container, Data<T> data, PartitionKey partitionKey, CancellationToken cancellationToken = default)
        {
            int affected = 0;
            Data<T> records = new Data<T>(data.Items);
            do
            {
                records.Items = data.Items.Skip(affected).ToList();
                Task<StoredProcedureExecuteResponse<int>> task = container.Scripts.ExecuteStoredProcedureAsync<int>("upsertDocuments", partitionKey, new dynamic[] { records }, cancellationToken: cancellationToken);
                task.Wait();
                affected += task.Result.Resource;
            } while (affected < data.Items.Count);
            return affected;
        }

        internal static int ExecuteDeleteDocuments(this Container container, string query, PartitionKey partitionKey, CancellationToken cancellationToken = default)
        {
            int affected = 0;
            ProcedureResponse response;
            do
            {
                Task<StoredProcedureExecuteResponse<ProcedureResponse>> task = container.Scripts.ExecuteStoredProcedureAsync<ProcedureResponse>("deleteDocuments", partitionKey, new dynamic[] { query }, cancellationToken: cancellationToken);
                task.Wait();
                response = task.Result;
                affected += response.Affected;
            } while (response.Continuation);
            return affected;
        }

        internal static void ExecutePersistDocuments(this Container container, string query, PartitionKey partitionKey, CancellationToken cancellationToken = default)
        {
            ProcedureResponse response;
            do
            {
                Task<StoredProcedureExecuteResponse<ProcedureResponse>> task = container.Scripts.ExecuteStoredProcedureAsync<ProcedureResponse>("persistDocuments", partitionKey, new dynamic[] { query }, cancellationToken: cancellationToken);
                task.Wait();
                response = task.Result;
            } while (response.Continuation);
        }

        internal static void ExecuteExpireDocuments(this Container container, string query, int epoch, PartitionKey partitionKey, CancellationToken cancellationToken = default)
        {
            ProcedureResponse response;
            do
            {
                Task<StoredProcedureExecuteResponse<ProcedureResponse>> task = container.Scripts.ExecuteStoredProcedureAsync<ProcedureResponse>("expireDocuments", partitionKey, new dynamic[] { query, epoch }, cancellationToken: cancellationToken);
                task.Wait();
                response = task.Result;
            } while (response.Continuation);
        }
    }
}
